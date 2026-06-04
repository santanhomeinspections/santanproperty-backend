/**
 * San Tan Property Inspections — Backend Server v3
 * + Agreement signature flow (sign online, PDF to R2, DB tracking)
 */

require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const rateLimit  = require('express-rate-limit');
const { google } = require('googleapis');
const { v4: uuidv4 } = require('uuid');
const { Pool }   = require('pg');
const crypto     = require('crypto');

// ── HMAC LINK SIGNING ─────────────────────────────────────────
// Adds an ?s= query param to action URLs (confirm/cancel/agreement).
// Token by itself is already a UUID, so guessing is infeasible — the HMAC
// is defense in depth against token exfiltration (DB leak, backup compromise).
// Without our secret, an attacker holding a leaked token still can't construct
// a working URL.
//
// Graceful migration: links sent BEFORE this code shipped have no signature.
// verifySignedToken accepts them (legacy path, logs a warning) so live pending
// emails still work. After ~14 days you can flip LINK_HMAC_STRICT=true to
// require signatures on all incoming requests.
const LINK_HMAC_SECRET = process.env.LINK_HMAC_SECRET || process.env.ADMIN_PASSWORD || 'change-me';
if (!process.env.LINK_HMAC_SECRET) {
  console.warn('⚠️  LINK_HMAC_SECRET not set — falling back to ADMIN_PASSWORD. Set a dedicated 32+ char random value in Railway env vars.');
}
const LINK_HMAC_STRICT = String(process.env.LINK_HMAC_STRICT || '').toLowerCase() === 'true';

function signToken(token) {
  // 12 hex chars = 48 bits of entropy on top of an already-unguessable UUID.
  // Short enough to keep URLs reasonable; long enough that brute force is impractical.
  return crypto.createHmac('sha256', LINK_HMAC_SECRET).update(String(token)).digest('hex').slice(0, 12);
}

function verifySignedToken(token, providedSig) {
  if (!token) return { ok: false, reason: 'missing-token' };
  if (!providedSig) {
    // Legacy path — link issued before HMAC shipped, or someone hand-crafted a URL.
    if (LINK_HMAC_STRICT) return { ok: false, reason: 'missing-sig' };
    return { ok: true, legacy: true };
  }
  const expected = signToken(token);
  // timingSafeEqual requires equal-length buffers
  const a = Buffer.from(providedSig);
  const b = Buffer.from(expected);
  if (a.length !== b.length) return { ok: false, reason: 'sig-mismatch' };
  try {
    if (crypto.timingSafeEqual(a, b)) return { ok: true };
  } catch (_) {}
  return { ok: false, reason: 'sig-mismatch' };
}

// Helper: append signed query param to a URL.
function withSig(url, token) {
  const sep = url.indexOf('?') === -1 ? '?' : '&';
  return url + sep + 's=' + signToken(token);
}

// ── ADMIN SESSION COOKIE ──────────────────────────────────────
// Lightweight signed-cookie auth for the admin (no express-session dependency).
// A login cookie is "operator.expiryMs.hmac" where the hmac signs
// "operator.expiryMs" with the same HMAC secret used for links. adminRole()
// reads this cookie. Valid for 30 days; sliding renewal happens on each login.
const ADMIN_COOKIE = 'stp_admin';
const ADMIN_SESSION_MS = 30 * 24 * 60 * 60 * 1000; // 30 days

function makeAdminCookie(operator) {
  const exp = Date.now() + ADMIN_SESSION_MS;
  const payload = operator + '.' + exp;
  const sig = crypto.createHmac('sha256', LINK_HMAC_SECRET).update(payload).digest('hex');
  return payload + '.' + sig;
}
// Returns the operator from a valid cookie value, or null.
function readAdminCookie(value) {
  if (!value || typeof value !== 'string') return null;
  const parts = value.split('.');
  if (parts.length !== 3) return null;
  const [operator, expStr, sig] = parts;
  const exp = Number(expStr);
  if (!exp || Date.now() > exp) return null;
  const expected = crypto.createHmac('sha256', LINK_HMAC_SECRET).update(operator + '.' + exp).digest('hex');
  const a = Buffer.from(sig), b = Buffer.from(expected);
  if (a.length !== b.length) return null;
  try { if (!crypto.timingSafeEqual(a, b)) return null; } catch (_) { return null; }
  if (operator !== 'jaren' && operator !== 'jeff') return null;
  return operator;
}
// Minimal cookie parser — avoids adding the cookie-parser dependency.
function getCookie(req, name) {
  const raw = req.headers['cookie'];
  if (!raw) return null;
  const parts = raw.split(';');
  for (let i = 0; i < parts.length; i++) {
    const idx = parts[i].indexOf('=');
    if (idx === -1) continue;
    const k = parts[i].slice(0, idx).trim();
    if (k === name) return decodeURIComponent(parts[i].slice(idx + 1).trim());
  }
  return null;
}

// ── INPUT VALIDATION ──────────────────────────────────────────
// Server-side regex checks for email and phone format. Booking form validates
// in the browser, but the server takes whatever it's given — these guard the
// API endpoints against malformed values from custom scripts or bypassed forms.

function isValidEmail(s) {
  if (!s || typeof s !== 'string') return false;
  if (s.length > 254) return false;  // RFC 5321 hard limit
  // Permissive but practical: local@domain.tld, no spaces, no double-dot
  // shenanigans, no leading/trailing punctuation around the @.
  return /^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$/.test(s) && !/\.\./.test(s);
}

function isValidPhone(s) {
  if (!s || typeof s !== 'string') return false;
  // Strip everything that isn't a digit; require 10 or 11 digits (US format,
  // with or without the leading 1). Permissive on input formatting since
  // users type these many different ways.
  const digits = s.replace(/\D/g, '');
  return digits.length === 10 || (digits.length === 11 && digits.startsWith('1'));
}

const app  = express();
const PORT = process.env.PORT || 3001;

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors({
  origin: [
    'https://santanpropertyinspections.com',
    'https://www.santanpropertyinspections.com',
    'http://localhost:3000',
    'http://localhost:5500',
    'http://127.0.0.1:5500',
  ],
  methods: ['GET','POST'],
  credentials: true,
}));

// ── POSTGRES ──────────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('railway')
    ? { rejectUnauthorized: false }
    : false,
});

async function initDb() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS pending_bookings (
        token       TEXT PRIMARY KEY,
        data        JSONB NOT NULL,
        created_at  TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS confirmed_bookings (
        id           SERIAL PRIMARY KEY,
        conf_id      TEXT UNIQUE NOT NULL,
        data         JSONB NOT NULL,
        confirmed_at TIMESTAMPTZ DEFAULT NOW(),
        paid_at      TIMESTAMPTZ DEFAULT NULL
      )
    `);
    // Add columns for existing deployments
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ DEFAULT NULL`);
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ DEFAULT NULL`);
    // Agreement signature columns
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS agreement_sent_at TIMESTAMPTZ DEFAULT NULL`);
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS agreement_signed_at TIMESTAMPTZ DEFAULT NULL`);
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS agreement_signature TEXT DEFAULT NULL`);
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS agreement_ip TEXT DEFAULT NULL`);
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS agreement_pdf_key TEXT DEFAULT NULL`);
    // Counter-signature: when Jaren reviews and counter-signs the executed agreement
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS counter_signed_at TIMESTAMPTZ DEFAULT NULL`);
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS counter_signed_by TEXT DEFAULT NULL`);
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS counter_signed_pdf_key TEXT DEFAULT NULL`);
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS payment_method TEXT DEFAULT NULL`);
    // Round-trip driving miles from OWNER_ADDRESS to inspection address.
    // Auto-computed at booking time via Distance Matrix; null if the API/env wasn't available.
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS miles NUMERIC(6,2) DEFAULT NULL`);
    // Operator: which inspector owns this job. 'jaren' (default) or 'jeff'.
    // Existing rows backfill to 'jaren' so nothing changes for the main business.
    await pool.query(`ALTER TABLE confirmed_bookings ADD COLUMN IF NOT EXISTS operator TEXT DEFAULT 'jaren'`);
    await pool.query(`ALTER TABLE pending_bookings   ADD COLUMN IF NOT EXISTS operator TEXT DEFAULT 'jaren'`);
    await pool.query(`UPDATE confirmed_bookings SET operator = 'jaren' WHERE operator IS NULL`);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS reschedule_requests (
        id           SERIAL PRIMARY KEY,
        conf_id      TEXT,
        name         TEXT,
        phone        TEXT,
        email        TEXT,
        message      TEXT,
        requested_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS discount_codes (
        code        TEXT PRIMARY KEY,
        pct         INTEGER NOT NULL,
        created_at  TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await pool.query(`
      INSERT INTO discount_codes (code, pct) VALUES ('SAVE10', 10), ('SAVE20', 20)
      ON CONFLICT (code) DO NOTHING
    `);
    await pool.query(`DELETE FROM pending_bookings WHERE created_at < NOW() - INTERVAL '48 hours'`);
    console.log('DB ready');
  } catch (e) {
    console.error('DB init error:', e.message);
  }
}

async function dbSet(token, data) {
  await pool.query(
    'INSERT INTO pending_bookings (token, data) VALUES ($1, $2) ON CONFLICT (token) DO UPDATE SET data = $2',
    [token, JSON.stringify(data)]
  );
}

async function dbGet(token) {
  const r = await pool.query('SELECT data FROM pending_bookings WHERE token = $1', [token]);
  return r.rows.length ? r.rows[0].data : null;
}

async function dbClaim(token) {
  // Atomic: deletes the token AND returns its data. If two requests race, only one returns data.
  const r = await pool.query('DELETE FROM pending_bookings WHERE token = $1 RETURNING data', [token]);
  return r.rows.length ? r.rows[0].data : null;
}

async function dbDelete(token) {
  await pool.query('DELETE FROM pending_bookings WHERE token = $1', [token]);
}

// ── GOOGLE CALENDAR ───────────────────────────────────────────
const oAuth2Client = new google.auth.OAuth2(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  process.env.GOOGLE_REDIRECT_URI
);
oAuth2Client.setCredentials({ refresh_token: process.env.GOOGLE_REFRESH_TOKEN });
const calendar   = google.calendar({ version: 'v3', auth: oAuth2Client });
const CALENDAR_ID       = process.env.GOOGLE_CALENDAR_ID || 'primary';
const BLOCK_CALENDAR_ID = '21fd9f285b32f1b290a601236f376d2495c6c0a363d4224bce7c0bc4aca7e65b@group.calendar.google.com';
const TIMEZONE    = 'America/Phoenix';
const ALL_SLOTS   = ['8:00 AM','8:30 AM','9:00 AM','9:30 AM','10:00 AM','10:30 AM','11:00 AM','11:30 AM','12:00 PM','12:30 PM','1:00 PM','1:30 PM','2:00 PM','2:30 PM','3:00 PM'];

function slotToMins(slot) {
  const [time, period] = slot.split(' ');
  let [h, m] = time.split(':').map(Number);
  if (period === 'PM' && h !== 12) h += 12;
  if (period === 'AM' && h === 12) h = 0;
  return h * 60 + m;
}

// ── OPERATORS ─────────────────────────────────────────────────
// Per-operator identity. San Tan is the shared brand; what varies per
// inspector is their name, license, reply-to inbox, notify inbox, the phone
// number printed on THEIR client-facing emails/agreements/reports, whether
// SMS fires for their jobs, and what they owe per inspection.
// Everything else (brand name, noreply@ from-address, review URL, agreement
// legal terms, pricing) is shared and lives outside this object.
const OPERATORS = {
  jaren: {
    inspectorName: 'Jaren Drummond',
    btrNumber:     '79346',
    replyTo:       process.env.OWNER_EMAIL || 'santanpropertyinspections@gmail.com',
    notifyEmails:  [process.env.OWNER_EMAIL || 'santanpropertyinspections@gmail.com'],
    phone:         '(480) 618-0805',
    sms:           true,
    payRate:       0,
  },
  jeff: {
    inspectorName: 'Jeff Thompson',
    btrNumber:     '79082',
    replyTo:       'JDThomeinspections@gmail.com',
    notifyEmails:  ['JDThomeinspections@gmail.com', process.env.OWNER_EMAIL || 'santanpropertyinspections@gmail.com'],
    phone:         '(480) 824-8048',
    sms:           false,
    payRate:       50,
  },
};

// Resolve an operator from any source (booking data, report row, query param).
// Unknown / missing / legacy → 'jaren' so all existing data and the public
// booking flow behave exactly as before.
function getOperator(id) {
  const key = String(id || '').toLowerCase().trim();
  return OPERATORS[key] ? key : 'jaren';
}
function operatorConfig(id) {
  return OPERATORS[getOperator(id)];
}

// ── QUO SMS ──────────────────────────────────────────────────
const QUO_API_KEY = process.env.QUO_API_KEY;
const QUO_FROM    = process.env.QUO_PHONE_NUMBER;  // E.164, e.g. +14806180805

function fmtPhone(raw) {
  const d = String(raw||'').replace(/\D/g,'');
  if (d.length === 10)               return '+1' + d;
  if (d.length === 11 && d[0]==='1') return '+' + d;
  return null;
}

async function sms(to, body) {
  const num = fmtPhone(to);
  if (!num) { console.warn('Bad phone, skipping SMS:', to); return; }
  if (!QUO_API_KEY || !QUO_FROM) {
    console.warn('Quo not configured (QUO_API_KEY or QUO_PHONE_NUMBER missing), skipping SMS to ' + num);
    return;
  }
  if (num === QUO_FROM) {
    console.warn('Refusing to send SMS to self (from === to ===', num + ')');
    return;
  }
  try {
    const res = await fetch('https://api.openphone.com/v1/messages', {
      method:  'POST',
      headers: {
        'Authorization': QUO_API_KEY,
        'Content-Type':  'application/json'
      },
      body: JSON.stringify({
        from:    QUO_FROM,
        to:      [num],
        content: body
      })
    });
    if (!res.ok) {
      const errText = await res.text().catch(function(){ return ''; });
      console.error('SMS error to ' + num + ': HTTP ' + res.status + ' ' + errText.slice(0, 200));
      return;
    }
    console.log('SMS sent to ' + num);
  } catch (e) {
    console.error('SMS error to ' + num + ': ' + e.message);
  }
}

// ── HTML ESCAPING ─────────────────────────────────────────────
// Used anywhere user-controlled data is interpolated into HTML/email/form output.
// Prevents stored XSS via booking fields (name, address, agent info, etc.).
function escapeHtml(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// ── INPUT LENGTH LIMITS (H4) ──────────────────────────────────
// Truncate user input on the server. Prevents 10MB payloads in `notes` from
// bloating the DB and slowing the admin page render. Always coerce to string
// so non-string inputs (numbers, objects) become safe values.
function clip(v, max) {
  if (v === null || v === undefined) return '';
  return String(v).slice(0, max);
}
const LEN = {
  name:      100,
  email:     200,
  phone:     30,
  address:   500,
  notes:     2000,
  message:   2000,
  brokerage: 200,
  code:      40,
};

// ── RATE LIMITERS (H1) ────────────────────────────────────────
// Protect public endpoints from bots and brute-force.
// Configured permissively to avoid blocking real users on shared NATs.
const bookingLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many booking attempts. Please wait a few minutes or call (480) 618-0805.' },
});
const adminAuthLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20, // Strict — brute-force defense on the basic-auth 401 challenge.
  standardHeaders: true,
  legacyHeaders: false,
  message: 'Too many admin requests — wait 15 minutes.',
});
// Once authed, admin browsing legitimately hits many endpoints (dashboard load,
// mark-paid, set-payment, csv export, etc). This is the per-IP cap on those
// actions — high enough to never block real use, low enough that a credential-
// stuffer who already cracked the password gets noticed.
const adminActionLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 200,
  standardHeaders: true,
  legacyHeaders: false,
  message: 'Too many admin requests — wait 15 minutes.',
});
const rescheduleLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 5,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many reschedule requests. Please wait a few minutes or call (480) 618-0805.' },
});
const agreementLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 20, // Agreement page can be opened/refreshed multiple times legitimately
  standardHeaders: true,
  legacyHeaders: false,
  message: 'Too many requests — please refresh in a minute or call (480) 618-0805.',
});

// ── PRICING (server-side, single source of truth) ─────────────
// Frontend computes the same values for display, but server ALWAYS recomputes
// from these tables before charging. Never trust client-sent totalPrice — a
// malicious user could submit totalPrice:1 and steal a $400 inspection.
//
// Tables MUST stay in sync with the BASE / ADDONS constants in website index.html.
// If you change pricing, change it BOTH places (or refactor to fetch from this server).
const PRICE_BASE = {
  1000: { p: 400, m: 90  }, 1500: { p: 425, m: 120 }, 2000: { p: 450, m: 150 },
  2500: { p: 475, m: 180 }, 3000: { p: 550, m: 195 }, 3500: { p: 600, m: 225 },
  4000: { p: 650, m: 240 }, 4500: { p: 675, m: 270 }, 9999: { p: 750, m: 300 },
};
const PRICE_ADDONS = {
  termite: { p: 85, m: 30, name: 'Termite Inspection (WDO)' },
  pool:    { p: 60, m: 30, name: 'Pool Inspection' },
  spa:     { p: 40, m: 20, name: 'Spa Inspection' },
  shed:    { p: 50, m: 30, name: 'Shed / Out Building' },
};

function sqftToTier(sqRaw) {
  const n = Number(sqRaw);
  if (!n || n <= 0) return null;
  if (n <= 1000) return 1000;
  if (n <= 1500) return 1500;
  if (n <= 2000) return 2000;
  if (n <= 2500) return 2500;
  if (n <= 3000) return 3000;
  if (n <= 3500) return 3500;
  if (n <= 4000) return 4000;
  if (n <= 4500) return 4500;
  return 9999;
}

// Returns { price, mins, breakdown } recomputed from authoritative server-side tables.
// addonsInput can be array of strings (legacy) or array of {id} or {name}.
function computePrice({ sqft, yearBuilt, addons, date, time }) {
  const tier = sqftToTier(sqft);
  if (!tier) return null; // Caller should reject the booking

  const base = PRICE_BASE[tier] || { p: 750, m: 300 };
  let p = base.p;
  let m = base.m;
  const breakdown = { tier, base: base.p, age: 0, addons: 0, heat: 0 };

  // Age surcharge
  const yr = Number(yearBuilt) || 0;
  if (yr > 0 && yr <= 1959)        { p += 80; m += 30; breakdown.age = 80; }
  else if (yr >= 1960 && yr <= 1980){ p += 50; m += 30; breakdown.age = 50; }

  // Addons — accept multiple shapes for safety
  const addonList = Array.isArray(addons) ? addons : [];
  for (const a of addonList) {
    let id = null;
    if (typeof a === 'string') {
      // Legacy / display string — match by name or id
      const lower = a.toLowerCase();
      for (const k of Object.keys(PRICE_ADDONS)) {
        if (lower === k || lower.indexOf(PRICE_ADDONS[k].name.toLowerCase()) !== -1) { id = k; break; }
      }
    } else if (a && typeof a === 'object') {
      if (a.id && PRICE_ADDONS[a.id]) id = a.id;
    }
    if (id && PRICE_ADDONS[id]) {
      p += PRICE_ADDONS[id].p;
      m += PRICE_ADDONS[id].m;
      breakdown.addons += PRICE_ADDONS[id].p;
    }
  }

  // Heat surcharge: June/July/August + afternoon (>= 12:00 PM)
  if (date && time) {
    const d = new Date(date + 'T00:00:00');
    const mo = d.getMonth(); // 0-indexed: 5=Jun, 6=Jul, 7=Aug
    const parts = String(time).split(' ');
    const hm = parts[0].split(':');
    let hr = parseInt(hm[0], 10);
    const period = parts[1];
    if (period === 'PM' && hr !== 12) hr += 12;
    if (period === 'AM' && hr === 12) hr = 0;
    if (mo >= 5 && mo <= 7 && hr >= 12) { p += 50; breakdown.heat = 50; }
  }

  return { price: p, mins: m, breakdown };
}

// ── GEO / TRIP CHARGE ─────────────────────────────────────────
// Trip charge applies when BOTH conditions are true:
//   1. Booking address is NOT in a service city (exact match on parsed city), AND
//   2. Driving distance from OWNER_ADDRESS to booking address is >= TRIP_CHARGE_MILES
//
// Owner address is geocoded once on first call and cached. Distance uses Google
// Distance Matrix (driving miles), matching the inspector app's mileage logic.
const SERVICE_CITIES = ['chandler','gilbert','mesa','tempe','queen creek','san tan valley','florence','apache junction'];
const TRIP_CHARGE_MILES = 50;
const TRIP_CHARGE_AMT   = 50;

// Cached geocode of OWNER_ADDRESS — populated lazily on first checkTripCharge call.
let _ownerCoords = null;
async function getOwnerCoords() {
  if (_ownerCoords) return _ownerCoords;
  if (!process.env.OWNER_ADDRESS || !process.env.GOOGLE_MAPS_API_KEY) return null;
  try {
    const url = 'https://maps.googleapis.com/maps/api/geocode/json?address='
      + encodeURIComponent(process.env.OWNER_ADDRESS)
      + '&key=' + process.env.GOOGLE_MAPS_API_KEY;
    const r = await fetch(url);
    const data = await r.json();
    if (data.status === 'OK' && data.results && data.results[0]) {
      _ownerCoords = data.results[0].geometry.location; // {lat, lng}
      console.log('Owner coords cached:', _ownerCoords);
      return _ownerCoords;
    }
    console.warn('Owner geocode failed:', data.status);
  } catch (e) {
    console.warn('Owner geocode threw:', e.message);
  }
  return null;
}

// Extract a city name from a freeform address by parsing comma-separated segments.
// Returns lowercase city if it exactly matches a SERVICE_CITY entry, else null.
// Handles: "1234 Main St, Gilbert, AZ 85234" → "gilbert"
//          "1234 Main St, Gilbert AZ 85234"  → "gilbert"
//          "1234 Florence Blvd, Phoenix, AZ" → null (Phoenix isn't service area; "florence" is in street, not city segment)
function parseServiceCity(address) {
  if (!address) return null;
  const segments = address.split(',').map(s => s.trim().toLowerCase());
  for (const seg of segments) {
    // Strip trailing state+zip ("gilbert az 85234" → "gilbert")
    const cityOnly = seg.replace(/\s+(az|arizona)\s*\d{5}.*$/i, '').replace(/\s+(az|arizona)$/i, '').trim();
    if (SERVICE_CITIES.indexOf(cityOnly) !== -1) return cityOnly;
  }
  return null;
}

// Returns { apply, miles, city }:
//   apply = whether the $50 trip charge applies (only for non-service-city + >=50mi)
//   miles = one-way driving miles from OWNER_ADDRESS to property (null on API failure or missing config)
//   city  = matched service city (lowercase) or null
// Always hits Distance Matrix when GOOGLE_MAPS_API_KEY + OWNER_ADDRESS are set,
// regardless of service city — we want the actual driving distance for mileage tracking.
async function checkTripCharge(address) {
  const matchedCity = parseServiceCity(address);

  if (!process.env.GOOGLE_MAPS_API_KEY || !process.env.OWNER_ADDRESS) {
    if (!process.env.GOOGLE_MAPS_API_KEY) console.warn('Trip charge: missing GOOGLE_MAPS_API_KEY');
    return { apply: false, miles: null, city: matchedCity };
  }

  let oneWayMiles = null;
  try {
    const owner = await getOwnerCoords();
    const originParam = owner
      ? owner.lat + ',' + owner.lng
      : encodeURIComponent(process.env.OWNER_ADDRESS || '');
    if (!originParam) return { apply: false, miles: null, city: matchedCity };

    const url = 'https://maps.googleapis.com/maps/api/distancematrix/json'
      + '?origins=' + originParam
      + '&destinations=' + encodeURIComponent(address)
      + '&units=imperial'
      + '&key=' + process.env.GOOGLE_MAPS_API_KEY;

    const controller = new AbortController();
    const timeout = setTimeout(function(){ controller.abort(); }, 4000);
    const r = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);
    const data = await r.json();

    const elem = data.rows && data.rows[0] && data.rows[0].elements && data.rows[0].elements[0];
    if (elem && elem.status === 'OK' && elem.distance) {
      oneWayMiles = elem.distance.value / 1609.344; // meters → miles
    }
  } catch (e) {
    console.warn('Trip charge distance lookup failed:', e.message);
  }

  // Trip charge only applies if NOT a service city AND distance >= threshold
  const apply = !matchedCity && oneWayMiles !== null && oneWayMiles >= TRIP_CHARGE_MILES;
  return {
    apply,
    miles: oneWayMiles !== null ? Math.round(oneWayMiles) : null,
    city: matchedCity,
  };
}

// ── EMAIL ─────────────────────────────────────────────────────
async function sendEmail(to, subject, html, attachments, replyTo, cc) {
  // Strip CRLF from subject to defang header injection via user-controlled name fields
  // (e.g. a booker named "Jane\r\nBcc: leak@evil.com" would otherwise inject a Bcc).
  const safeSubject = String(subject || '').replace(/[\r\n]+/g, ' ').slice(0, 500);
  try {
    const controller = new AbortController();
    // 15s — bumped from 8s because attachments make the request larger
    const timeout = setTimeout(function(){ controller.abort(); }, 15000);
    const body = {
      from: 'San Tan Property Inspections <noreply@santanpropertyinspections.com>',
      reply_to: replyTo || 'santanpropertyinspections@gmail.com',
      to: to,
      subject: safeSubject,
      html: html,
    };
    if (attachments && attachments.length) body.attachments = attachments;
    if (cc) body.cc = cc;
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      signal: controller.signal,
      headers: {
        'Authorization': 'Bearer ' + process.env.RESEND_API_KEY,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    clearTimeout(timeout);
    const data = await r.json();
    if (!r.ok) throw new Error(JSON.stringify(data));
    return data;
  } catch(e) { throw e; }
}

// ── EMAIL HELPERS ─────────────────────────────────────────────
const EMAIL_HEADER = '<div style="text-align:center;background:#0F1C35;padding:18px;margin-bottom:20px;border-radius:6px">'
  + '<div style="font-family:Georgia,serif;font-size:1.1rem;font-weight:700;color:#C9A84C;letter-spacing:2px">SAN TAN PROPERTY</div>'
  + '<div style="font-family:Georgia,serif;font-size:.75rem;color:#E8C97A;letter-spacing:4px">INSPECTIONS</div>'
  + '</div>';

const EMAIL_FOOTER = '<hr style="border:none;border-top:1px solid #E8DFC8;margin:20px 0"/>'
  + '<p style="color:#888;font-size:.8rem">San Tan Property Inspections &nbsp;&middot;&nbsp; San Tan Valley, AZ &nbsp;&middot;&nbsp; santanpropertyinspections.com</p>';

function emailWrap(content) {
  return '<div style="font-family:Georgia,serif;max-width:580px;margin:0 auto;border-top:4px solid #C9A84C;padding-top:20px">'
    + EMAIL_HEADER + content + EMAIL_FOOTER + '</div>';
}

// ── R2 UPLOAD ─────────────────────────────────────────────────
async function uploadToR2(key, buffer, contentType) {
  // Uses AWS-compatible S3 API that R2 supports
  const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
  const client = new S3Client({
    region: 'auto',
    endpoint: process.env.R2_ENDPOINT,
    credentials: {
      accessKeyId:     process.env.R2_ACCESS_KEY_ID,
      secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
    },
  });
  await client.send(new PutObjectCommand({
    Bucket: process.env.R2_BUCKET_NAME,
    Key:    key,
    Body:   buffer,
    ContentType: contentType,
  }));
  return key;
}

async function downloadFromR2(key) {
  const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
  const client = new S3Client({
    region: 'auto',
    endpoint: process.env.R2_ENDPOINT,
    credentials: {
      accessKeyId:     process.env.R2_ACCESS_KEY_ID,
      secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
    },
  });
  const result = await client.send(new GetObjectCommand({
    Bucket: process.env.R2_BUCKET_NAME,
    Key:    key,
  }));
  // Convert stream to Buffer
  const chunks = [];
  for await (const chunk of result.Body) chunks.push(chunk);
  return Buffer.concat(chunks);
}

// Download from the inspector's R2. Both apps may share an R2 account (same
// endpoint + credentials, possibly different bucket), or they may use entirely
// separate R2 accounts. Each piece is independently overridable:
//   INSPECTOR_R2_ENDPOINT          — falls back to R2_ENDPOINT
//   INSPECTOR_R2_ACCESS_KEY_ID     — falls back to R2_ACCESS_KEY_ID
//   INSPECTOR_R2_SECRET_ACCESS_KEY — falls back to R2_SECRET_ACCESS_KEY
//   INSPECTOR_R2_BUCKET_NAME       — falls back to R2_BUCKET_NAME
// In the common case where the two apps share one R2 account and one bucket,
// no env vars need to be set. If they share an R2 account but use different
// buckets, only INSPECTOR_R2_BUCKET_NAME needs to be set. If the inspector
// uses an entirely separate R2 account, set all four.
//
// Helper treats EMPTY STRINGS as unset. Without this, an env var set to "" in
// Railway (which is easy to do by accident — leaving the value field blank
// when adding a variable) would pass the `||` fallback as a truthy override
// and break credential resolution with a cryptic "Resolved credential object
// is not valid" error from the AWS SDK.
function pickEnv(primary, fallback) {
  const p = process.env[primary];
  if (p && p.trim() && p.trim() !== 'undefined') return p.trim();
  const f = process.env[fallback];
  return (f && f.trim()) ? f.trim() : null;
}

async function downloadFromInspectorR2(key) {
  const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
  const endpoint  = pickEnv('INSPECTOR_R2_ENDPOINT',          'R2_ENDPOINT');
  const accessKey = pickEnv('INSPECTOR_R2_ACCESS_KEY_ID',     'R2_ACCESS_KEY_ID');
  const secretKey = pickEnv('INSPECTOR_R2_SECRET_ACCESS_KEY', 'R2_SECRET_ACCESS_KEY');
  const bucket    = pickEnv('INSPECTOR_R2_BUCKET_NAME',       'R2_BUCKET_NAME');

  // Fail fast with a clear message rather than letting the SDK throw a
  // cryptic credential error. Each piece is required.
  if (!endpoint)  throw new Error('R2 endpoint not configured (set R2_ENDPOINT)');
  if (!accessKey) throw new Error('R2 access key not configured (set R2_ACCESS_KEY_ID)');
  if (!secretKey) throw new Error('R2 secret key not configured (set R2_SECRET_ACCESS_KEY)');
  if (!bucket)    throw new Error('R2 bucket not configured (set R2_BUCKET_NAME)');

  const client = new S3Client({
    region: 'auto',
    endpoint,
    credentials: { accessKeyId: accessKey, secretAccessKey: secretKey },
  });
  const result = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const chunks = [];
  for await (const chunk of result.Body) chunks.push(chunk);
  return Buffer.concat(chunks);
}

// ── AGREEMENT TEXT ────────────────────────────────────────────
// Versioned agreement text — stored with each signed record.
// Now a function so the inspector identity (name / BTR / phone) reflects the
// operator who owns the booking. The legal body is identical for all operators;
// only the header identity line and signature contact vary. San Tan remains the
// brand and business address on every agreement.
const AGREEMENT_VERSION = '2026-v2';
function agreementText(op) {
  const cfg = op || OPERATORS.jaren;
  return `SAN TAN PROPERTY INSPECTIONS
Certified Home Inspector — BTR #${cfg.btrNumber}
${cfg.inspectorName}
3850 E Gallatin Way, San Tan Valley, AZ 85143
${cfg.phone} | santanpropertyinspections@gmail.com | santanpropertyinspections.com

HOME INSPECTION AGREEMENT

1. SCOPE OF INSPECTION
The Inspector will perform a non-invasive visual inspection of the accessible systems and components of the property in accordance with the Standards of Professional Practice for Arizona Home Inspectors as adopted by the Arizona State Board of Technical Registration (available at btr.az.gov). The inspection will produce a written report identifying material defects observed at the time of inspection. The report will be delivered the same day as the inspection.

The standard inspection covers the following systems and components:

ROOF: Roof covering materials (shingles, tile, foam, membrane); flashings; roof drainage (gutters, downspouts, and extensions); skylights, chimneys, and roof penetrations; condition of visible roof structure from accessible areas.

EXTERIOR: Wall cladding, trim, and fascia; eaves, soffits, and exterior doors; windows and window operation from exterior; walkways, driveways, patios, and steps; grading and surface drainage at the foundation perimeter; fences and retaining walls where they affect the structure.

ELECTRICAL SYSTEM: Service entrance conductors and service equipment; main and sub-panel(s) including breakers and fuses; grounding and bonding; visible branch circuit wiring; outlets, switches, and fixtures (tested via spot checks); GFCI and AFCI protection where required; smoke and carbon monoxide detectors (presence observed, not tested).

PLUMBING SYSTEM: Main water supply, shut-off, and water meter location; interior water supply lines and visible distribution; drain, waste, and vent pipes; water heater(s) including capacity, condition, safety devices, and approximate age; sump pumps if present; functional flow and drainage at fixtures.

BASEMENT, FOUNDATION, AND STRUCTURE: Foundation type and visible condition; evidence of moisture intrusion, efflorescence, or staining; basement or crawl space access and conditions; visible structural framing including floor joists, beams, and columns where accessible; evidence of settlement, movement, or significant cracking.

GARAGE: Garage door(s), openers, and auto-reverse safety function; firewall and fire-rated door between garage and living space; vehicle door operation and weatherstripping; electrical and lighting within the garage.

HEATING AND COOLING: Heating equipment type, energy source, and approximate age; cooling (air conditioning) equipment type and approximate age; visible distribution systems (ductwork, registers); normal operating controls; flue pipes, chimneys, and venting for combustion appliances; filter condition observed.

DOORS, WINDOWS, AND INTERIOR: Interior doors and hardware; windows including sash operation, locking hardware, and glazing condition (visible fogging/seal failure noted); floors, walls, and ceilings for evidence of moisture, staining, or significant defects; stairways, railings, and guardrails; representative sample of accessible interior components.

INSULATION AND VENTILATION: Insulation type and approximate depth in accessible attic areas; vapor barriers where visible; attic ventilation (ridge, soffit, gable, or power vents); bathroom, kitchen, and laundry exhaust fan venting to the exterior where accessible; crawl space ventilation where applicable.

BUILT-IN KITCHEN APPLIANCES: Dishwasher (operated through a cycle); range/cooktop and oven (operated and tested for heating); built-in microwave (operated); garbage disposal (operated); exhaust fan/range hood (operated); refrigerator is not included unless specifically agreed to in writing.

Any add-on services (such as pool, fireplace, infrared camera inspection, or outbuilding) are performed in addition to the standard inspection and are subject to the same terms of this Agreement. Add-on services must be agreed upon in writing prior to the inspection and are reflected in the booking confirmation.

Only components with normal user controls and accessible under safe conditions will be operated. The inspection is a visual examination only and does not include destructive testing, dismantling of components, or moving of personal property.

2. EXCLUSIONS
Unless separately agreed to in writing, the following are NOT included in this inspection: Environmental hazards (radon, mold, asbestos, lead paint or pipes, soil contamination, or other pollutants); Code compliance, zoning compliance, or permit verification; Termites or wood-destroying organisms; Cosmetic or aesthetic conditions that do not affect function or safety; Auxiliary systems (alarm, solar, intercom, central vacuum, water softener, reverse osmosis systems, sprinkler or mister systems); The presence, condition, or certification of smoke detectors or carbon monoxide detectors beyond visual observation of presence; Product recalls; Areas inaccessible due to obstructions; Swimming pools or spas unless the pool add-on is purchased; Detached structures unless the outbuilding add-on is purchased; Fireplaces and chimneys unless the fireplace add-on is purchased.

The Inspector is a property generalist and does not act as a licensed engineer or specialist in any trade.

3. REPORT
The report is prepared for the sole use and benefit of the Client. The report is not a guarantee, warranty, or substitute for seller disclosure. The report will not be released until both: (1) this Agreement has been signed by the Client, and (2) full payment of the inspection fee has been received.

4. PAYMENT
The inspection fee is due and payable on the day of inspection. Accepted forms of payment: cash, Venmo, Zelle, or Square (credit/debit). The report will not be released until payment is received in full.

5. RE-INSPECTION AND CANCELLATION
A $125.00 re-inspection fee applies if utilities are not on and accessible at the time of the scheduled inspection. Please provide at least 24 hours notice for cancellations or rescheduling.

6. LIMITATION OF LIABILITY
The Inspector assumes no liability for the cost of repair or replacement of unreported defects. The Inspector's total liability is limited to a refund of the inspection fee paid. Client waives any claim for consequential, exemplary, special, or incidental damages or for loss of use of the property.

7. CLAIMS PROCEDURE
Client must: (1) provide written notification within 10 days of discovery; and (2) provide the Inspector with access for re-inspection before any repairs are made, except in cases of emergency where Client shall notify the Inspector in writing within 24 hours of such emergency repair. No legal action may be filed more than one (1) year after the date of inspection.

8. DISPUTE RESOLUTION
Any dispute not resolved by refund of the inspection fee shall be resolved by arbitration. At least one arbitrator must be an Arizona Certified Home Inspector with at least five years of experience. The prevailing party shall be awarded attorney's fees and arbitration costs. Client waives trial by jury.

9. ELECTRONIC SIGNATURE
This Agreement may be executed electronically in accordance with the federal E-SIGN Act (15 U.S.C. § 7001 et seq.) and the Arizona Electronic Transactions Act (A.R.S. § 44-7001 et seq.). An electronic signature shall be deemed valid and binding to the same extent as a handwritten signature.

10. GENERAL PROVISIONS
This Agreement constitutes the entire agreement between the parties. If any provision is found invalid, the remaining provisions remain in effect.

Agreement Version: ${AGREEMENT_VERSION}`;
}

// ── AGREEMENT PAGE ────────────────────────────────────────────
function buildAgreementPage(booking, token, opts = {}) {
  const { confId, address, date, time, dateFmt, fullName, buyer, addonsLine, finalPrice } = booking;
  const { signed = false, error = null } = opts;
  const agText = agreementText(operatorConfig(booking && booking.operator));
  const opPhone = operatorConfig(booking && booking.operator).phone;

  const addonsDisplay = addonsLine && addonsLine !== 'None' ? addonsLine : null;

  if (signed) {
    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Agreement Signed — San Tan Property Inspections</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0F1C35;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;}
.card{background:#fff;border-radius:16px;padding:40px 36px;max-width:500px;width:100%;text-align:center;box-shadow:0 28px 70px rgba(0,0,0,.4);}
.logo{background:#0F1C35;border-radius:10px;padding:16px;margin-bottom:28px;}
.logo-title{font-family:Georgia,serif;font-size:1rem;font-weight:700;color:#C9A84C;letter-spacing:2px;}
.logo-sub{font-family:Georgia,serif;font-size:.65rem;color:#E8C97A;letter-spacing:4px;margin-top:3px;}
.check{width:64px;height:64px;background:#e8f7ee;border-radius:50%;display:flex;align-items:center;justify-content:center;margin:0 auto 16px;font-size:2rem;}
h1{font-family:Georgia,serif;color:#1B2D52;margin-bottom:8px;}
p{color:#666;line-height:1.6;margin-bottom:12px;font-size:.9rem;}
.conf{background:#FAF7F0;border-radius:8px;padding:12px 16px;margin:16px 0;font-size:.85rem;color:#555;}
.conf strong{color:#1B2D52;}
</style>
</head>
<body>
<div class="card">
  <div class="logo">
    <div class="logo-title">SAN TAN PROPERTY</div>
    <div class="logo-sub">INSPECTIONS</div>
  </div>
  <div class="check">✅</div>
  <h1>Agreement Signed</h1>
  <p>Thank you, ${escapeHtml((buyer && buyer.firstName) || 'client')}. Your inspection agreement has been signed and recorded.</p>
  <div class="conf">
    <strong>Confirmation:</strong> ${escapeHtml(confId)}<br>
    <strong>Property:</strong> ${escapeHtml(address)}<br>
    <strong>Date:</strong> ${escapeHtml(dateFmt)} @ ${escapeHtml(time)}
  </div>
  <p>You are all set. We look forward to seeing you at the inspection.<br>Questions? Call or text <strong>${opPhone}</strong>.</p>
</div>
</body>
</html>`;
  }

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Inspection Agreement — San Tan Property Inspections</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0F1C35;min-height:100vh;padding:24px;}
.outer{max-width:700px;margin:0 auto;}
.logo-bar{background:#0F1C35;border-radius:10px;padding:16px;margin-bottom:16px;text-align:center;}
.logo-title{font-family:Georgia,serif;font-size:1rem;font-weight:700;color:#C9A84C;letter-spacing:2px;}
.logo-sub{font-family:Georgia,serif;font-size:.65rem;color:#E8C97A;letter-spacing:4px;margin-top:3px;}
.card{background:#fff;border-radius:16px;padding:36px;box-shadow:0 28px 70px rgba(0,0,0,.4);}
h1{font-family:Georgia,serif;color:#1B2D52;font-size:1.4rem;margin-bottom:6px;}
.subtitle{color:#888;font-size:.85rem;margin-bottom:24px;}
.info-box{background:#FAF7F0;border-radius:8px;padding:14px 16px;margin-bottom:24px;font-size:.85rem;line-height:1.8;}
.info-box strong{color:#1B2D52;}
.agreement-box{background:#F8F8F8;border:1px solid #E0E0E0;border-radius:8px;padding:20px;margin-bottom:24px;font-size:.78rem;line-height:1.7;color:#444;max-height:420px;overflow-y:auto;white-space:pre-wrap;font-family:monospace;}
.section-title{font-weight:700;color:#1B2D52;font-size:.9rem;margin-bottom:4px;}
label{display:flex;align-items:flex-start;gap:10px;font-size:.85rem;color:#444;margin-bottom:16px;cursor:pointer;}
label input{margin-top:3px;flex-shrink:0;width:16px;height:16px;}
.sig-field{width:100%;border:2px solid #E0D9CC;border-radius:8px;padding:12px;font-size:1rem;font-family:Georgia,serif;color:#1B2D52;outline:none;transition:border-color .2s;}
.sig-field:focus{border-color:#C9A84C;}
.sig-label{font-size:.82rem;color:#888;margin-bottom:6px;display:block;}
.btn{width:100%;background:#1B2D52;color:white;border:none;border-radius:10px;padding:16px;font-size:1rem;font-weight:700;cursor:pointer;margin-top:20px;transition:background .2s;}
.btn:hover{background:#243a6e;}
.btn:disabled{background:#aaa;cursor:not-allowed;}
.error{background:#fdecea;border:1px solid #f5c6c3;border-radius:8px;padding:12px 16px;color:#c0392b;font-size:.85rem;margin-bottom:16px;}
.legal{font-size:.75rem;color:#aaa;text-align:center;margin-top:12px;line-height:1.6;}
</style>
</head>
<body>
<div class="outer">
  <div class="logo-bar">
    <div class="logo-title">SAN TAN PROPERTY</div>
    <div class="logo-sub">INSPECTIONS</div>
  </div>
  <div class="card">
    <h1>Home Inspection Agreement</h1>
    <p class="subtitle">Please review and sign before your inspection</p>

    <div class="info-box">
      <strong>Client:</strong> ${escapeHtml(fullName)}<br>
      <strong>Property:</strong> ${escapeHtml(address)}<br>
      <strong>Inspection Date:</strong> ${escapeHtml(dateFmt)} @ ${escapeHtml(time)}<br>
      ${addonsDisplay ? '<strong>Add-Ons:</strong> ' + escapeHtml(addonsDisplay) + '<br>' : ''}
      <strong>Estimated Total:</strong> $${Number(finalPrice)||0}<br>
      <strong>Confirmation #:</strong> ${escapeHtml(confId)}
    </div>

    <div class="section-title">Agreement</div>
    <div class="agreement-box">${agText}</div>

    ${error ? '<div class="error">' + escapeHtml(error) + '</div>' : ''}

    <form method="POST" action="/agreement/${encodeURIComponent(token)}/sign?s=${encodeURIComponent(signToken(token))}" id="agreementForm">
      <label>
        <input type="checkbox" id="readCheck" required/>
        I have read and understand the full agreement above
      </label>
      <label>
        <input type="checkbox" id="agreeCheck" required/>
        I agree to be bound by all terms and conditions of this agreement
      </label>

      <span class="sig-label">Type your full legal name as your electronic signature</span>
      <input
        type="text"
        name="signature"
        class="sig-field"
        placeholder="Full legal name"
        required
        autocomplete="name"
      />

      <button type="submit" class="btn" id="submitBtn">Sign Agreement</button>
      <p class="legal">By signing, you acknowledge that your electronic signature is legally binding to the same extent as a handwritten signature.</p>
    </form>
  </div>
</div>
<script>
document.getElementById('agreementForm').addEventListener('submit', function(e) {
  const sig = document.querySelector('input[name="signature"]').value.trim();
  const r = document.getElementById('readCheck').checked;
  const a = document.getElementById('agreeCheck').checked;
  if (!sig || !r || !a) {
    e.preventDefault();
    alert('Please check both boxes and enter your full name before signing.');
    return;
  }
  document.getElementById('submitBtn').disabled = true;
  document.getElementById('submitBtn').textContent = 'Saving...';
});
</script>
</body>
</html>`;
}

// ── CUSTOMER HUB PAGE ─────────────────────────────────────────
// Single-page status hub for the client. Adapts to where the booking sits in
// its lifecycle (confirmed/signed/inspected/delivered/cancelled) and surfaces
// the right next-step CTA. Used for the /i/:token route.
//
// `row` is the raw confirmed_bookings row (paid_at, agreement_signed_at, etc.).
// `booking` is row.data (the JSONB payload — confId, dateFmt, address, etc.).
// `reportInfo` is the object returned by getReportInfoForConfId — {state, id, pdfKey, pdfFilename, version}.
// `hubToken` is the agreement token (also used as the hub token).
function buildHubPage(booking, row, reportInfo, hubToken) {
  const reportState = (reportInfo && reportInfo.state) || 'none';
  const { confId, address, dateFmt, time, endTime, svcLabel, addonsLine, finalPrice, fullName, buyer } = booking;
  const buyerFirst = (buyer && buyer.firstName) || (fullName ? String(fullName).split(' ')[0] : 'there');

  // Operator owns the booking; the hub shows their inspector name + phone so the
  // client reaches the right person. Falls back to OWNER_NAME / Jaren.
  const opCfg = operatorConfig(booking && booking.operator);
  const opPhone = opCfg.phone;
  const ownerFirst = (opCfg.inspectorName || process.env.OWNER_NAME || 'Jaren').split(' ')[0];

  const isCancelled    = !!(row && row.cancelled_at);
  const isSigned       = !!(row && row.agreement_signed_at);
  const isCounterSigned= !!(row && row.counter_signed_at);
  const isPaid         = !!(row && row.paid_at);

  // Compute "inspection has happened yet?" — we anchor on the END of the slot
  // so the hub doesn't flip to "completed" mid-inspection.
  let inspectionPast = false;
  try {
    if (booking.date && booking.time) {
      const sm = slotToMins(booking.time);
      const slotH = Math.floor(sm/60), slotM = sm%60;
      const startDT = new Date(`${booking.date}T${String(slotH).padStart(2,'0')}:${String(slotM).padStart(2,'0')}:00-07:00`);
      const endDT   = new Date(startDT.getTime() + (Number(booking.totalMins)||120)*60000);
      inspectionPast = Date.now() > endDT.getTime();
    }
  } catch(_) {}

  const agreementUrl = '/agreement/' + encodeURIComponent(hubToken) + '?s=' + encodeURIComponent(signToken(hubToken));

  // ── Build the status banner ────────────────────────────────
  // One-line state summary. Color-coded. This is the first thing the customer sees.
  let banner = '';
  if (isCancelled) {
    banner = '<div class="banner banner-bad"><strong>This inspection has been cancelled.</strong> Contact us at ' + opPhone + ' if you have questions.</div>';
  } else if (reportState === 'delivered') {
    banner = '<div class="banner banner-good"><strong>Your report has been delivered.</strong> Check your email — see the Report section below if you can\'t find it.</div>';
  } else if (inspectionPast && reportState === 'completed') {
    banner = '<div class="banner banner-info"><strong>Your inspection is complete.</strong> The report will be delivered to your email shortly.</div>';
  } else if (inspectionPast) {
    banner = '<div class="banner banner-info"><strong>Your inspection has been completed.</strong> Your report will be delivered to your email by end of day.</div>';
  } else if (!isSigned) {
    banner = '<div class="banner banner-action"><strong>Action needed:</strong> Please review and sign your inspection agreement before your appointment.</div>';
  } else {
    banner = '<div class="banner banner-good"><strong>You are all set.</strong> We look forward to seeing you on inspection day.</div>';
  }

  // ── Agreement section ──────────────────────────────────────
  let agreementSection = '';
  if (isCancelled) {
    agreementSection = '';
  } else if (isSigned) {
    const signedDate = row.agreement_signed_at
      ? new Date(row.agreement_signed_at).toLocaleString('en-US', { timeZone: TIMEZONE, dateStyle: 'medium', timeStyle: 'short' })
      : '';
    // Pick the most authoritative PDF we have: counter-signed (executed) beats
    // single-signed if both exist. The hub returns whichever is present.
    // The download URL is the same /agreement.pdf endpoint either way; it picks
    // the best PDF server-side.
    const hasAgreementPdf = !!(row.agreement_pdf_key || row.counter_signed_pdf_key);
    const dlUrl = hasAgreementPdf
      ? '/i/' + encodeURIComponent(hubToken) + '/agreement.pdf?s=' + encodeURIComponent(signToken(hubToken))
      : null;
    agreementSection = '<div class="section">'
      + '<h2>Agreement</h2>'
      + '<p class="muted">Signed ' + escapeHtml(signedDate) + ' (AZ)' + (isCounterSigned ? ' &middot; fully executed' : '') + '</p>'
      + '<div class="status-pill status-good">✓ Signed</div>'
      + (dlUrl ? '<div style="margin-top:14px"><a href="' + escapeHtml(dlUrl) + '" class="btn btn-secondary" target="_blank" rel="noopener">Download Signed Agreement (PDF)</a></div>' : '')
      + '</div>';
  } else {
    agreementSection = '<div class="section section-action">'
      + '<h2>Sign Your Inspection Agreement</h2>'
      + '<p class="muted">Your report cannot be released until your agreement is signed. This takes about a minute.</p>'
      + '<a href="' + escapeHtml(agreementUrl) + '" class="btn btn-primary">Review &amp; Sign Agreement</a>'
      + '</div>';
  }

  // ── Reschedule section ─────────────────────────────────────
  let rescheduleSection = '';
  if (!isCancelled && !inspectionPast) {
    rescheduleSection = '<div class="section">'
      + '<h2>Need to Reschedule?</h2>'
      + '<p class="muted">Fill out the form below and ' + escapeHtml(ownerFirst) + ' will reach out to find a new time. For same-day changes, please call ' + opPhone + '.</p>'
      + '<form id="rescheduleForm" method="POST" action="/api/reschedule">'
      + '<input type="hidden" name="confId" value="' + escapeHtml(confId) + '"/>'
      + '<input type="hidden" name="name" value="' + escapeHtml(fullName || '') + '"/>'
      + '<input type="hidden" name="phone" value="' + escapeHtml((buyer && buyer.phone) || '') + '"/>'
      + '<input type="hidden" name="email" value="' + escapeHtml((buyer && buyer.email) || '') + '"/>'
      + '<textarea name="message" rows="3" placeholder="Preferred dates/times or reason for rescheduling..." required></textarea>'
      + '<button type="submit" class="btn btn-secondary">Request Reschedule</button>'
      + '<div id="rescheduleResult" class="form-result"></div>'
      + '</form>'
      + '</div>';
  }

  // ── Report section ─────────────────────────────────────────
  let reportSection = '';
  if (!isCancelled) {
    if (reportState === 'delivered') {
      // Build a download URL only if we have a PDF key on file. Older reports
      // delivered before the inspector tracked pdf_r2_key won't have one — in
      // that case we just say "check your email" without a download button.
      const hasPdf = !!(reportInfo && reportInfo.pdfKey);
      const dlUrl = hasPdf
        ? '/i/' + encodeURIComponent(hubToken) + '/report.pdf?s=' + encodeURIComponent(signToken(hubToken))
        : null;
      const amendmentNote = (reportInfo && reportInfo.version && reportInfo.version > 1)
        ? '<p class="muted" style="margin-top:6px">This is the most recent version of your report (v' + reportInfo.version + ').</p>'
        : '';
      reportSection = '<div class="section section-good">'
        + '<h2>Your Report</h2>'
        + '<p>Your inspection report has been delivered to <strong>' + escapeHtml((buyer && buyer.email) || 'your email') + '</strong>.</p>'
        + (dlUrl
            ? '<a href="' + escapeHtml(dlUrl) + '" class="btn btn-primary" target="_blank" rel="noopener">Download Report (PDF)</a>'
            : '<p class="muted">If you can\'t find it, check your spam folder, or call/text ' + opPhone + ' and we will resend.</p>')
        + amendmentNote
        + '</div>';
    } else if (inspectionPast) {
      reportSection = '<div class="section">'
        + '<h2>Your Report</h2>'
        + '<p class="muted">Your report will be delivered to your email by end of day.</p>'
        + '</div>';
    } else {
      reportSection = '<div class="section">'
        + '<h2>Your Report</h2>'
        + '<p class="muted">Your report will be delivered to your email the <strong>same day</strong> as your inspection.</p>'
        + '</div>';
    }
  }

  // ── Cancellation policy note (always visible if not cancelled) ──
  const policyNote = isCancelled ? '' : '<p class="footnote">Need to change something? Call/text ' + opPhone + '. Cancellations within 24 hours of the scheduled time may incur a fee.</p>';

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Your Inspection — San Tan Property Inspections</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0F1C35;min-height:100vh;padding:24px 16px;color:#222;}
.outer{max-width:680px;margin:0 auto;}
.logo-bar{background:#0F1C35;border-radius:10px;padding:18px;margin-bottom:16px;text-align:center;}
.logo-title{font-family:Georgia,serif;font-size:1.05rem;font-weight:700;color:#C9A84C;letter-spacing:2px;}
.logo-sub{font-family:Georgia,serif;font-size:.7rem;color:#E8C97A;letter-spacing:4px;margin-top:3px;}
.card{background:#fff;border-radius:16px;padding:0;box-shadow:0 28px 70px rgba(0,0,0,.4);overflow:hidden;}
.head{padding:28px 32px 20px;border-bottom:1px solid #E8DFC8;}
.head h1{font-family:Georgia,serif;color:#1B2D52;font-size:1.5rem;margin-bottom:4px;}
.head .greet{color:#888;font-size:.92rem;}
.banner{padding:14px 20px;font-size:.9rem;line-height:1.5;}
.banner strong{display:inline-block;margin-right:4px;}
.banner-good{background:#e8f7ee;color:#0f5a32;border-bottom:1px solid #c8e6d2;}
.banner-info{background:#EAF3FB;color:#1B2D52;border-bottom:1px solid #c8dceb;}
.banner-action{background:#fff4e0;color:#7a4a00;border-bottom:1px solid #f0d99b;}
.banner-bad{background:#fdecea;color:#8c2520;border-bottom:1px solid #f3c5c0;}
.details{padding:24px 32px;background:#FAF7F0;border-bottom:1px solid #E8DFC8;}
.details h2{font-family:Georgia,serif;font-size:.78rem;color:#888;text-transform:uppercase;letter-spacing:2px;margin-bottom:14px;}
.detail-grid{display:grid;grid-template-columns:140px 1fr;gap:8px 16px;font-size:.92rem;line-height:1.5;}
.detail-grid .lbl{color:#888;}
.detail-grid .val{color:#1B2D52;font-weight:600;}
.price-line{color:#C9A84C;font-weight:700;}
.section{padding:24px 32px;border-bottom:1px solid #f0ebe0;}
.section:last-of-type{border-bottom:none;}
.section h2{font-family:Georgia,serif;color:#1B2D52;font-size:1.05rem;margin-bottom:10px;}
.section p{color:#555;font-size:.9rem;line-height:1.6;margin-bottom:12px;}
.section.section-action{background:#fff9ef;border-left:4px solid #C9A84C;}
.section.section-good{background:#f4faf6;}
.muted{color:#888 !important;font-size:.86rem !important;}
.status-pill{display:inline-block;font-size:.78rem;font-weight:700;padding:5px 12px;border-radius:14px;letter-spacing:.5px;}
.status-good{background:#e8f7ee;color:#0f5a32;}
.btn{display:inline-block;background:#1B2D52;color:white;border:none;border-radius:10px;padding:14px 28px;font-size:.95rem;font-weight:700;cursor:pointer;text-decoration:none;transition:background .2s;font-family:inherit;}
.btn:hover{background:#243a6e;}
.btn-primary{background:#1B2D52;}
.btn-secondary{background:#1B2D52;font-size:.88rem;padding:11px 22px;}
.btn:disabled{background:#aaa;cursor:not-allowed;}
textarea{width:100%;border:1.5px solid #E0D9CC;border-radius:8px;padding:10px 12px;font-size:.92rem;font-family:inherit;color:#1B2D52;outline:none;margin-bottom:12px;resize:vertical;min-height:72px;}
textarea:focus{border-color:#C9A84C;}
.form-result{margin-top:12px;font-size:.86rem;}
.form-result.ok{color:#0f5a32;}
.form-result.bad{color:#8c2520;}
.footnote{color:#888;font-size:.78rem;text-align:center;padding:18px 32px 24px;line-height:1.6;}
@media(max-width:540px){
  .head{padding:22px 22px 16px;}
  .details,.section{padding:20px 22px;}
  .detail-grid{grid-template-columns:1fr;gap:2px 0;}
  .detail-grid .lbl{margin-top:8px;font-size:.78rem;}
}
</style>
</head>
<body>
<div class="outer">
  <div class="logo-bar">
    <div class="logo-title">SAN TAN PROPERTY</div>
    <div class="logo-sub">INSPECTIONS</div>
  </div>
  <div class="card">
    <div class="head">
      <h1>Hi ${escapeHtml(buyerFirst)},</h1>
      <div class="greet">Here are the details for your inspection.</div>
    </div>
    ${banner}
    <div class="details">
      <h2>Inspection Details</h2>
      <div class="detail-grid">
        <span class="lbl">Service</span><span class="val">${escapeHtml(svcLabel || '')}${addonsLine && addonsLine !== 'None' ? ' + ' + escapeHtml(addonsLine) : ''}</span>
        <span class="lbl">Property</span><span class="val">${escapeHtml(address || '')}</span>
        <span class="lbl">Date</span><span class="val">${escapeHtml(dateFmt || '')}</span>
        <span class="lbl">Time</span><span class="val">${escapeHtml(time || '')}${endTime ? ' to ' + escapeHtml(endTime) : ''}</span>
        <span class="lbl">Estimated Total</span><span class="val price-line">$${Number(finalPrice)||0}${isPaid ? ' &middot; <span style="color:#1ab464;font-size:.82rem">Paid</span>' : ''}</span>
        <span class="lbl">Confirmation #</span><span class="val">${escapeHtml(confId || '')}</span>
      </div>
    </div>
    ${agreementSection}
    ${rescheduleSection}
    ${reportSection}
    ${policyNote}
  </div>
</div>
<script>
// Reschedule form submit handler — uses fetch so the page doesn't reload and lose context.
(function(){
  var form = document.getElementById('rescheduleForm');
  if (!form) return;
  form.addEventListener('submit', function(e) {
    e.preventDefault();
    var result = document.getElementById('rescheduleResult');
    var btn = form.querySelector('button[type="submit"]');
    result.textContent = '';
    result.className = 'form-result';
    btn.disabled = true;
    btn.textContent = 'Sending...';
    var fd = new FormData(form);
    var body = {};
    fd.forEach(function(v,k){ body[k] = v; });
    fetch('/api/reschedule', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    })
    .then(function(r){ return r.json().then(function(d){ return { ok: r.ok, data: d }; }); })
    .then(function(res){
      if (res.ok && res.data.success) {
        result.textContent = 'Request sent. ${ownerFirst.replace(/'/g, "\\\\'")} will reach out shortly.';
        result.className = 'form-result ok';
        form.querySelector('textarea').value = '';
        btn.textContent = 'Sent ✓';
        // Re-enable after a few seconds so they can send a second message if needed
        setTimeout(function(){ btn.disabled = false; btn.textContent = 'Request Reschedule'; }, 4000);
      } else {
        result.textContent = (res.data && res.data.error) || 'Could not send request. Please call (480) 618-0805.';
        result.className = 'form-result bad';
        btn.disabled = false;
        btn.textContent = 'Request Reschedule';
      }
    })
    .catch(function(){
      result.textContent = 'Network error. Please call (480) 618-0805.';
      result.className = 'form-result bad';
      btn.disabled = false;
      btn.textContent = 'Request Reschedule';
    });
  });
})();
</script>
</body>
</html>`;
}

// Look up report info for a given confirmation ID from the inspector app's
// `reports` table (shared Postgres). Returns:
//   {
//     state:        'none' | 'in_progress' | 'pending' | 'delivered',
//     id:           UUID of report row (or null)
//     pdfKey:       R2 key of latest PDF (or null)
//     pdfFilename:  filename to suggest on download (or null)
//     version:      report_version (1+, or null)
//   }
// State semantics:
//   'none'        — no report row exists for this confId
//   'in_progress' — row exists but inspector hasn't finalized it yet
//   'pending'     — row is 'complete' but report_sent_at is unset (rare)
//   'delivered'   — report_sent_at is present (the client got the report)
//
// Defensive: if the reports table doesn't exist or the query throws, returns
// {state:'none', ...}. The hub and admin row both handle missing reports
// gracefully (no buttons render), so a schema mismatch degrades politely.
async function getReportInfoForConfId(confId) {
  if (!confId) return { state: 'none', id: null, pdfKey: null, pdfFilename: null, version: null };
  try {
    // Order by report_version DESC so we pick the latest amendment, then by
    // created_at as a tiebreaker. NULLS LAST keeps very-old rows that pre-date
    // the report_version column from outranking newer versioned rows.
    const r = await pool.query(
      `SELECT id, status, report_version,
              report_data->>'report_sent_at' AS sent_at,
              report_data->>'pdf_r2_key' AS pdf_key,
              report_data->>'pdf_filename' AS pdf_filename
         FROM reports
        WHERE report_data->>'confId' = $1
        ORDER BY report_version DESC NULLS LAST, created_at DESC
        LIMIT 1`,
      [confId]
    );
    if (!r.rows.length) return { state: 'none', id: null, pdfKey: null, pdfFilename: null, version: null };
    const row = r.rows[0];
    let state;
    if (row.sent_at) state = 'delivered';
    else if (row.status === 'complete') state = 'pending';
    else state = 'in_progress';
    return {
      state,
      id: row.id,
      pdfKey: row.pdf_key || null,
      pdfFilename: row.pdf_filename || null,
      version: row.report_version || null,
    };
  } catch(e) {
    // Table may not exist on a fresh deploy, or column shapes may differ.
    // Don't spam the log — just return 'none' so the UI degrades gracefully.
    return { state: 'none', id: null, pdfKey: null, pdfFilename: null, version: null };
  }
}

// ── AGREEMENT PDF GENERATION ──────────────────────────────────
async function generateAgreementPdf(booking, signedAt, signature, ip) {
  // Use puppeteer (already available in santan-inspector) to generate PDF
  // We build a simple HTML page and render it
  const { confId, address, dateFmt, time, fullName, buyer, finalPrice, addonsLine } = booking;
  const opCfg = operatorConfig(booking && booking.operator);
  const signedDate = new Date(signedAt).toLocaleString('en-US', { timeZone: 'America/Phoenix', dateStyle: 'full', timeStyle: 'short' });

  const html = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8"/>
<style>
body{font-family:Arial,sans-serif;font-size:10pt;color:#222;margin:48px;line-height:1.6;}
h1{font-size:14pt;color:#1B2D52;margin-bottom:4px;}
h2{font-size:11pt;color:#1B2D52;margin:18px 0 4px;}
.header{text-align:center;border-bottom:2px solid #C9A84C;padding-bottom:12px;margin-bottom:20px;}
.header .biz{font-size:16pt;font-weight:700;color:#1B2D52;}
.header .sub{font-size:9pt;color:#666;margin-top:4px;}
.info-grid{display:grid;grid-template-columns:120px 1fr;gap:4px 12px;margin-bottom:20px;font-size:9.5pt;}
.info-grid .lbl{color:#666;}
.info-grid .val{font-weight:600;}
.agreement-text{font-size:8.5pt;line-height:1.65;white-space:pre-wrap;background:#f8f8f8;border:1px solid #ddd;padding:14px;border-radius:4px;margin-bottom:20px;}
.sig-block{border-top:2px solid #1B2D52;padding-top:16px;margin-top:20px;}
.sig-row{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin-top:12px;font-size:9pt;}
.sig-row .lbl{color:#666;font-size:8pt;}
.sig-row .val{font-size:10pt;font-weight:600;border-bottom:1px solid #333;padding-bottom:4px;margin-top:4px;}
.record{background:#EAF3FB;border:1px solid #1B2D52;border-radius:4px;padding:10px 14px;font-size:8pt;color:#444;margin-top:16px;}
.record strong{color:#1B2D52;}
</style>
</head>
<body>
<div class="header">
  <div class="biz">SAN TAN PROPERTY INSPECTIONS</div>
  <div class="sub">Certified Home Inspector — BTR #${opCfg.btrNumber} &nbsp;|&nbsp; ${opCfg.inspectorName}<br>
  3850 E Gallatin Way, San Tan Valley, AZ 85143 &nbsp;|&nbsp; ${opCfg.phone} &nbsp;|&nbsp; santanpropertyinspections.com</div>
</div>

<h1>HOME INSPECTION AGREEMENT</h1>

<div class="info-grid">
  <span class="lbl">Client</span><span class="val">${escapeHtml(fullName)}</span>
  <span class="lbl">Property</span><span class="val">${escapeHtml(address)}</span>
  <span class="lbl">Inspection Date</span><span class="val">${escapeHtml(dateFmt)} @ ${escapeHtml(time)}</span>
  <span class="lbl">Add-On Services</span><span class="val">${escapeHtml(addonsLine || 'None')}</span>
  <span class="lbl">Est. Total</span><span class="val">$${Number(finalPrice)||0}</span>
  <span class="lbl">Confirmation #</span><span class="val">${escapeHtml(confId)}</span>
</div>

<div class="agreement-text">${escapeHtml(agreementText(opCfg))}</div>

<div class="sig-block">
  <h2>Electronic Signature Record</h2>
  <div class="sig-row">
    <div>
      <div class="lbl">Client Printed Name</div>
      <div class="val">${escapeHtml(fullName)}</div>
    </div>
    <div>
      <div class="lbl">Electronic Signature</div>
      <div class="val">${escapeHtml(signature)}</div>
    </div>
    <div>
      <div class="lbl">Date Signed</div>
      <div class="val">${escapeHtml(signedDate)} (AZ)</div>
    </div>
  </div>
  <div class="record">
    <strong>Signature Record:</strong> This agreement was electronically signed on ${escapeHtml(signedDate)} (Arizona Time).
    IP Address: ${escapeHtml(ip)} &nbsp;|&nbsp; Agreement Version: ${escapeHtml(AGREEMENT_VERSION)} &nbsp;|&nbsp; Confirmation: ${escapeHtml(confId)}<br>
    The electronic signature above is legally binding pursuant to the terms of Section 9 of this Agreement.
  </div>
</div>
</body>
</html>`;

  try {
    const puppeteer = require('puppeteer');
    const browser = await puppeteer.launch({
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium',
      args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage'],
    });
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: 'networkidle0' });
    const pdf = await page.pdf({ format: 'Letter', printBackground: true, margin: { top:'0.5in', right:'0.5in', bottom:'0.5in', left:'0.5in' } });
    await browser.close();
    return pdf;
  } catch(e) {
    console.error('Agreement PDF generation failed:', e.message);
    return null;
  }
}

// ── EXECUTED (COUNTER-SIGNED) AGREEMENT PDF ───────────────────
// Generates the fully-executed agreement: client signature + inspector counter-signature.
// Called after Jaren reviews and counter-signs from the admin page.
async function generateExecutedAgreementPdf(booking, signedAt, signature, ip, counterSignedAt, counterSignedBy) {
  const { confId, address, dateFmt, time, fullName, finalPrice, addonsLine } = booking;
  const opCfg = operatorConfig(booking && booking.operator);
  const signedDate        = new Date(signedAt).toLocaleString('en-US', { timeZone: 'America/Phoenix', dateStyle: 'full', timeStyle: 'short' });
  const counterSignedDate = new Date(counterSignedAt).toLocaleString('en-US', { timeZone: 'America/Phoenix', dateStyle: 'full', timeStyle: 'short' });

  const html = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8"/>
<style>
body{font-family:Arial,sans-serif;font-size:10pt;color:#222;margin:48px;line-height:1.6;}
h1{font-size:14pt;color:#1B2D52;margin-bottom:4px;}
h2{font-size:11pt;color:#1B2D52;margin:18px 0 4px;}
.header{text-align:center;border-bottom:2px solid #C9A84C;padding-bottom:12px;margin-bottom:20px;position:relative;}
.header .biz{font-size:16pt;font-weight:700;color:#1B2D52;}
.header .sub{font-size:9pt;color:#666;margin-top:4px;}
.executed-badge{position:absolute;top:0;right:0;background:#1B2D52;color:#C9A84C;padding:6px 12px;font-size:9pt;font-weight:700;letter-spacing:1.5px;border-radius:3px;}
.info-grid{display:grid;grid-template-columns:120px 1fr;gap:4px 12px;margin-bottom:20px;font-size:9.5pt;}
.info-grid .lbl{color:#666;}
.info-grid .val{font-weight:600;}
.agreement-text{font-size:8.5pt;line-height:1.65;white-space:pre-wrap;background:#f8f8f8;border:1px solid #ddd;padding:14px;border-radius:4px;margin-bottom:20px;}
.sig-block{border-top:2px solid #1B2D52;padding-top:16px;margin-top:20px;}
.parties{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-top:16px;}
.party{border:1px solid #ddd;border-radius:6px;padding:14px;background:#fafafa;}
.party h3{margin:0 0 10px 0;font-size:9.5pt;color:#1B2D52;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #ddd;padding-bottom:6px;}
.party .row{margin-bottom:10px;}
.party .lbl{color:#666;font-size:8pt;display:block;margin-bottom:2px;}
.party .val{font-size:11pt;font-weight:600;border-bottom:1.5px solid #333;padding-bottom:3px;font-family:'Brush Script MT',cursive;}
.party .meta{font-size:8.5pt;color:#444;font-weight:400;font-family:Arial,sans-serif;border:none;padding:0;}
.record{background:#EAF3FB;border:1px solid #1B2D52;border-radius:4px;padding:10px 14px;font-size:8pt;color:#444;margin-top:16px;}
.record strong{color:#1B2D52;}
</style>
</head>
<body>
<div class="header">
  <div class="executed-badge">FULLY EXECUTED</div>
  <div class="biz">SAN TAN PROPERTY INSPECTIONS</div>
  <div class="sub">Certified Home Inspector — BTR #${opCfg.btrNumber} &nbsp;|&nbsp; ${opCfg.inspectorName}<br>
  3850 E Gallatin Way, San Tan Valley, AZ 85143 &nbsp;|&nbsp; ${opCfg.phone} &nbsp;|&nbsp; santanpropertyinspections.com</div>
</div>

<h1>HOME INSPECTION AGREEMENT</h1>

<div class="info-grid">
  <span class="lbl">Client</span><span class="val">${escapeHtml(fullName)}</span>
  <span class="lbl">Property</span><span class="val">${escapeHtml(address)}</span>
  <span class="lbl">Inspection Date</span><span class="val">${escapeHtml(dateFmt)} @ ${escapeHtml(time)}</span>
  <span class="lbl">Add-On Services</span><span class="val">${escapeHtml(addonsLine || 'None')}</span>
  <span class="lbl">Est. Total</span><span class="val">$${Number(finalPrice)||0}</span>
  <span class="lbl">Confirmation #</span><span class="val">${escapeHtml(confId)}</span>
</div>

<div class="agreement-text">${escapeHtml(agreementText(opCfg))}</div>

<div class="sig-block">
  <h2>Signatures of the Parties</h2>
  <div class="parties">
    <div class="party">
      <h3>Client</h3>
      <div class="row">
        <div class="lbl">Electronic Signature</div>
        <div class="val">${escapeHtml(signature)}</div>
      </div>
      <div class="row">
        <div class="lbl">Printed Name</div>
        <div class="meta">${escapeHtml(fullName)}</div>
      </div>
      <div class="row">
        <div class="lbl">Date Signed</div>
        <div class="meta">${escapeHtml(signedDate)} (AZ)</div>
      </div>
      <div class="row">
        <div class="lbl">IP Address</div>
        <div class="meta">${escapeHtml(ip)}</div>
      </div>
    </div>
    <div class="party">
      <h3>Inspector (Counter-Signature)</h3>
      <div class="row">
        <div class="lbl">Electronic Signature</div>
        <div class="val">${escapeHtml(counterSignedBy)}</div>
      </div>
      <div class="row">
        <div class="lbl">Printed Name</div>
        <div class="meta">${escapeHtml(counterSignedBy)}</div>
      </div>
      <div class="row">
        <div class="lbl">Date Counter-Signed</div>
        <div class="meta">${escapeHtml(counterSignedDate)} (AZ)</div>
      </div>
      <div class="row">
        <div class="lbl">License</div>
        <div class="meta">AZ BTR #79346</div>
      </div>
    </div>
  </div>
  <div class="record">
    <strong>Execution Record:</strong> This agreement is fully executed.
    Client electronically signed on ${escapeHtml(signedDate)}. Inspector counter-signed on ${escapeHtml(counterSignedDate)}.
    Both signatures are legally binding pursuant to the terms of Section 9 of this Agreement.<br>
    Agreement Version: ${escapeHtml(AGREEMENT_VERSION)} &nbsp;|&nbsp; Confirmation: ${escapeHtml(confId)} &nbsp;|&nbsp; Client IP: ${escapeHtml(ip)}
  </div>
</div>
</body>
</html>`;

  try {
    const puppeteer = require('puppeteer');
    const browser = await puppeteer.launch({
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium',
      args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage'],
    });
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: 'networkidle0' });
    const pdf = await page.pdf({ format: 'Letter', printBackground: true, margin: { top:'0.5in', right:'0.5in', bottom:'0.5in', left:'0.5in' } });
    await browser.close();
    return pdf;
  } catch(e) {
    console.error('Executed agreement PDF generation failed:', e.message);
    return null;
  }
}

// ── ROUTES ────────────────────────────────────────────────────
app.get('/api/health', function(req, res){ res.json({ status:'ok', ts: new Date().toISOString() }); });

app.get('/api/availability', async function(req, res) {
  const date = req.query.date;
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date))
    return res.status(400).json({ error: 'Use YYYY-MM-DD' });

  const inspDuration   = parseInt(req.query.duration) || 360;
  const BUFFER_MINS    = 120;
  const totalBlockMins = inspDuration + BUFFER_MINS;

  // Pull DB bookings for this date — both pending (awaiting owner confirm) and confirmed (not cancelled).
  // This locks slots immediately on customer submit, before calendar event creation.
  // Cancelled bookings (cancelled_at IS NOT NULL) and pending bookings older than 48h are excluded automatically.
  let dbBookings = [];
  try {
    const dbRes = await pool.query(`
      SELECT data->>'time' AS time, data->>'totalMins' AS mins, 'pending' AS source
        FROM pending_bookings
       WHERE data->>'date' = $1
         AND created_at > NOW() - INTERVAL '48 hours'
      UNION ALL
      SELECT data->>'time' AS time, data->>'totalMins' AS mins, 'confirmed' AS source
        FROM confirmed_bookings
       WHERE data->>'date' = $1
         AND cancelled_at IS NULL
    `, [date]);
    dbBookings = dbRes.rows;
  } catch(e) {
    console.warn('Availability DB query failed (using calendar only):', e.message);
  }

  try {
    const timeMin = `${date}T00:00:00-07:00`;
    const timeMax = `${date}T23:59:59-07:00`;
    const [mainResp, blockResp] = await Promise.all([
      calendar.events.list({ calendarId: CALENDAR_ID, timeMin, timeMax, singleEvents: true, orderBy: 'startTime' }).catch(function(e){
        console.warn('Main calendar fetch failed:', e.message);
        return { data: { items: [] } };
      }),
      calendar.events.list({ calendarId: BLOCK_CALENDAR_ID, timeMin, timeMax, singleEvents: true, orderBy: 'startTime' }).catch(function(e){
        console.warn('Block calendar fetch failed:', e.message);
        return { data: { items: [] } };
      }),
    ]);
    const allItems = [...(mainResp.data.items||[]), ...(blockResp.data.items||[])];
    const dayBlocked = allItems.some(function(ev){ return ev.start && ev.start.date && !ev.start.dateTime; });
    if (dayBlocked) return res.json({ date, booked: ALL_SLOTS, available: [], dayBlocked: true });

    // Convert calendar events into the same shape as DB bookings (start time + end time).
    const calendarEvents = allItems.filter(function(ev){ return ev.start && ev.start.dateTime; }).map(function(ev){
      const evStart = new Date(ev.start.dateTime);
      const evEnd   = ev.end && ev.end.dateTime ? new Date(ev.end.dateTime) : new Date(evStart.getTime() + 60*60000);
      return { start: evStart, end: evEnd };
    });

    // Convert DB bookings into the same shape (start/end Date objects).
    const dbEvents = dbBookings.filter(function(b){ return b.time; }).map(function(b){
      const bMins = slotToMins(b.time);
      const bH = Math.floor(bMins/60), bM = bMins%60;
      const bStart = new Date(`${date}T${String(bH).padStart(2,'0')}:${String(bM).padStart(2,'0')}:00-07:00`);
      const bEnd   = new Date(bStart.getTime() + (parseInt(b.mins)||120) * 60000);
      return { start: bStart, end: bEnd };
    });

    const allEvents = [...calendarEvents, ...dbEvents];

    const booked = [];
    for (let s = 0; s < ALL_SLOTS.length; s++) {
      const slot    = ALL_SLOTS[s];
      const slotMins = slotToMins(slot);
      const slotH   = Math.floor(slotMins/60), slotM = slotMins%60;
      const slotStart = new Date(`${date}T${String(slotH).padStart(2,'0')}:${String(slotM).padStart(2,'0')}:00-07:00`);
      const slotEnd   = new Date(slotStart.getTime() + inspDuration * 60000);

      for (let i = 0; i < allEvents.length; i++) {
        const ev = allEvents[i];
        const evBlockEnd = new Date(ev.end.getTime() + BUFFER_MINS*60000);
        const overlapsEvent = slotStart < ev.end && slotEnd > ev.start;
        const inBufferWindow = slotStart >= ev.end && slotStart <= evBlockEnd;
        if ((overlapsEvent || inBufferWindow) && !booked.includes(slot)) {
          booked.push(slot); break;
        }
      }
    }
    res.json({ date, booked, available: ALL_SLOTS.filter(function(s){ return !booked.includes(s); }) });
  } catch (e) {
    console.error('Availability:', e.message);
    res.json({ date, booked: [], available: ALL_SLOTS, warning: 'Calendar unavailable' });
  }
});

app.post('/api/book', bookingLimiter, async function(req, res) {
  const bookingTimeout = setTimeout(function() {
    if (!res.headersSent) {
      res.status(504).json({ error: 'Request timed out. Please try again or call (480) 618-0805.' });
    }
  }, 20000);
  const originalJson = res.json.bind(res);
  const originalStatus = res.status.bind(res);
  res.json = function(data) { clearTimeout(bookingTimeout); return originalJson(data); };
  res.status = function(code) {
    const s = originalStatus(code);
    s.json = function(data) { clearTimeout(bookingTimeout); return originalJson.call(res, data); };
    return s;
  };

  const b = req.body;
  // Which inspector this request is for. Public bookings omit it → 'jaren'.
  // Jeff's intake form posts operator:'jeff'. getOperator() rejects anything unknown.
  const opId  = getOperator(b.operator);
  const opCfg = OPERATORS[opId];
  let   { address, sqft, yearBuilt, inspType, totalMins, date, time, endTime, buyer, buyerAgent, sellerAgent, notes } = b;
  const addons         = b.addons || [];
  const extraEmails    = (b.extraEmails || []).filter(function(e){ return e && e.trim(); }).slice(0, 5).map(e => clip(e, LEN.email));
  const discountCode   = b.discountCode ? clip(b.discountCode, LEN.code) : null;
  const discountPct    = b.discountPct   || null;
  // discountAmount is recomputed below from server price + verified discountPct;
  // never trust the client value (otherwise you can buy $400 jobs for $1).
  let   discountAmount = null;

  // ── INPUT LENGTH CLIPPING (H4) ───────────────────────────────
  // Prevents DB bloat and admin-page slowness from oversized fields.
  address = clip(address, LEN.address);
  notes   = clip(notes,   LEN.notes);
  if (buyer && typeof buyer === 'object') {
    buyer.firstName = clip(buyer.firstName, LEN.name);
    buyer.lastName  = clip(buyer.lastName,  LEN.name);
    buyer.email     = clip(buyer.email,     LEN.email);
    buyer.phone     = clip(buyer.phone,     LEN.phone);
  }
  if (buyerAgent && typeof buyerAgent === 'object') {
    buyerAgent.name      = clip(buyerAgent.name,      LEN.name);
    buyerAgent.email     = clip(buyerAgent.email,     LEN.email);
    buyerAgent.phone     = clip(buyerAgent.phone,     LEN.phone);
    buyerAgent.brokerage = clip(buyerAgent.brokerage, LEN.brokerage);
  }
  if (sellerAgent && typeof sellerAgent === 'object') {
    sellerAgent.name      = clip(sellerAgent.name,      LEN.name);
    sellerAgent.email     = clip(sellerAgent.email,     LEN.email);
    sellerAgent.phone     = clip(sellerAgent.phone,     LEN.phone);
    sellerAgent.brokerage = clip(sellerAgent.brokerage, LEN.brokerage);
  }

  const miss=[];
  if(!address) miss.push('address');
  if(!sqft) miss.push('sqft');
  if(!inspType) miss.push('inspType');
  if(!date) miss.push('date');
  if(!time) miss.push('time');
  if(!buyer||!buyer.firstName) miss.push('buyer.firstName');
  if(!buyer||!buyer.phone) miss.push('buyer.phone');
  if(!buyer||!buyer.email) miss.push('buyer.email');
  // Buyer's agent is OPTIONAL — no validation
  if(miss.length) return res.status(400).json({ error:'Missing: '+miss.join(', ') });

  // Format validation — server-side regex on email + phone fields. Browser
  // already validates these, but the server takes whatever it's given.
  if (!isValidEmail(buyer.email)) return res.status(400).json({ error: 'Please enter a valid email address.' });
  if (!isValidPhone(buyer.phone)) return res.status(400).json({ error: 'Please enter a valid phone number.' });
  if (buyerAgent && buyerAgent.email && !isValidEmail(buyerAgent.email))   return res.status(400).json({ error: "Buyer's agent email looks invalid." });
  if (buyerAgent && buyerAgent.phone && !isValidPhone(buyerAgent.phone))   return res.status(400).json({ error: "Buyer's agent phone looks invalid." });
  if (sellerAgent && sellerAgent.email && !isValidEmail(sellerAgent.email)) return res.status(400).json({ error: "Seller's agent email looks invalid." });
  if (sellerAgent && sellerAgent.phone && !isValidPhone(sellerAgent.phone)) return res.status(400).json({ error: "Seller's agent phone looks invalid." });
  for (const e of extraEmails) {
    if (!isValidEmail(e)) return res.status(400).json({ error: 'Extra recipient "' + e + '" is not a valid email.' });
  }

  // ── SERVER-SIDE PRICING (authoritative) ──────────────────────
  // Recompute from authoritative tables. Ignore client-sent totalPrice entirely.
  const priced = computePrice({ sqft, yearBuilt, addons, date, time });
  if (!priced) return res.status(400).json({ error: 'Invalid square footage' });
  let totalPrice = priced.price;

  // If client sent a discount code, verify it server-side and apply % off the recomputed total
  if (discountCode) {
    try {
      const codeRow = await pool.query(
        'SELECT pct FROM discount_codes WHERE UPPER(code) = UPPER($1) LIMIT 1',
        [discountCode]
      );
      if (codeRow.rows.length) {
        const pct = Math.max(0, Math.min(100, Number(codeRow.rows[0].pct) || 0));
        discountAmount = Math.round(totalPrice * pct / 100);
        totalPrice    = Math.max(0, totalPrice - discountAmount);
      }
    } catch (e) {
      console.warn('Discount code verify failed:', e.message);
    }
  }

  // Sanity: log if client value drastically diverged — could indicate a UI bug or tampering attempt
  if (b.totalPrice && Math.abs(Number(b.totalPrice) - priced.price) > 1) {
    console.warn('Price mismatch — client sent $' + b.totalPrice + ', server computed $' + priced.price + ' for', { sqft, yearBuilt, addons, date, time });
  }

  // Server-side dup-guard: reject if same buyer email already booked the same date+time slot
  // within the last 5 minutes. Catches accidental double-submits from frantic clickers.
  try {
    const dupCheck = await pool.query(
      `SELECT conf_id FROM confirmed_bookings
       WHERE cancelled_at IS NULL
         AND data->'buyer'->>'email' = $1
         AND data->>'date' = $2
         AND data->>'time' = $3
         AND confirmed_at > NOW() - INTERVAL '5 minutes'
       LIMIT 1`,
      [buyer.email, date, time]
    );
    if (dupCheck.rows.length) {
      return res.status(409).json({
        error: 'You already have a booking for this date and time. Confirmation # ' + dupCheck.rows[0].conf_id + '. Check your email or call (480) 618-0805 if you didn\'t receive it.'
      });
    }
    // Also check pending_bookings for very-recent duplicates not yet confirmed
    const pendDup = await pool.query(
      `SELECT token FROM pending_bookings
       WHERE data->'buyer'->>'email' = $1
         AND data->>'date' = $2
         AND data->>'time' = $3
         AND created_at > NOW() - INTERVAL '5 minutes'
       LIMIT 1`,
      [buyer.email, date, time]
    );
    if (pendDup.rows.length) {
      return res.status(409).json({
        error: 'You\'ve already submitted a booking for this date and time — please check your email for confirmation. Call (480) 618-0805 if anything looks wrong.'
      });
    }
  } catch(e) {
    console.error('Dup-check failed (allowing booking through):', e.message);
    // Don't block the booking on a dup-check DB hiccup — fall through
  }

  // Normalize buyerAgent to safe object so downstream code can read .name/.phone/.email/.brokerage without crashing
  const ba = (buyerAgent && typeof buyerAgent === 'object') ? buyerAgent : {};
  const baName  = ba.name  || '';
  const baPhone = ba.phone || '';
  const baEmail = ba.email || '';
  const baBrok  = ba.brokerage || '';
  const hasBA   = !!(baName || baPhone || baEmail);

  const confId   = 'STH-' + uuidv4().slice(0,8).toUpperCase();
  const fullName = buyer.firstName + ' ' + buyer.lastName;
  const sm       = slotToMins(time);
  const slotH    = Math.floor(sm/60), slotM = sm%60;
  const startDT  = new Date(`${date}T${String(slotH).padStart(2,'0')}:${String(slotM).padStart(2,'0')}:00-07:00`);
  const endDT    = new Date(startDT.getTime() + (totalMins||120)*60000);

  const SVC = {
    'pre-purchase':'Pre-Purchase Inspection','pre-listing':'Pre-Listing Inspection',
    'new-construction':'New Construction Inspection','warranty':'Pre-One Year Warranty Inspection','reinspection':'Re-Inspection',
  };
  const svcLabel   = SVC[inspType] || inspType;
  const addonsLine = addons.length ? addons.join(', ') : 'None';
  const dateFmt    = startDT.toLocaleDateString('en-US',{ weekday:'long', year:'numeric', month:'long', day:'numeric', timeZone: TIMEZONE });

  try {
    // Check both the main calendar and the block calendar (manual time-off blocks).
    const [chk, blockChk] = await Promise.all([
      calendar.events.list({ calendarId: CALENDAR_ID, timeMin: startDT.toISOString(), timeMax: endDT.toISOString(), singleEvents: true }),
      calendar.events.list({ calendarId: BLOCK_CALENDAR_ID, timeMin: startDT.toISOString(), timeMax: endDT.toISOString(), singleEvents: true }).catch(function(){ return { data: { items: [] } }; }),
    ]);
    if ((chk.data.items||[]).length || (blockChk.data.items||[]).length)
      return res.status(409).json({ error:'That slot was just booked — please choose another.' });
  } catch(e) { console.warn('Slot check failed:', e.message); }

  const token      = uuidv4();
  const trip       = await checkTripCharge(address);
  const finalPrice = trip.apply ? totalPrice + TRIP_CHARGE_AMT : totalPrice;
  // Round-trip driving miles for mileage tracking (tax / per-job analysis).
  // Reuses trip.miles (one-way) so we only hit Distance Matrix once per booking.
  const miles      = (trip.miles !== null && trip.miles !== undefined)
    ? Math.round(trip.miles * 2 * 100) / 100
    : null;

  const bookingData = { confId, address, sqft, yearBuilt, inspType, svcLabel, addons, addonsLine, totalPrice, finalPrice, totalMins, date, time, endTime, dateFmt, fullName, buyer, buyerAgent, sellerAgent, notes, extraEmails, discountCode, discountPct, discountAmount, tripCharge: trip, miles, operator: opId, createdAt: Date.now() };

  try {
    await dbSet(token, bookingData);
    // Stamp the operator column too (dbSet only writes the data JSONB).
    await pool.query('UPDATE pending_bookings SET operator = $1 WHERE token = $2', [opId, token]);
  } catch(e) {
    console.error('DB write failed:', e.message);
    return res.status(500).json({ error: 'Could not save booking. Please try again.' });
  }

  const BASE_URL   = process.env.RAILWAY_URL || 'https://santanproperty-backend-production.up.railway.app';
  const confirmUrl = withSig(BASE_URL + '/confirm/' + token, token);
  const cancelUrl  = withSig(BASE_URL + '/cancel/'  + token, token);

  const sellerLineOwner    = sellerAgent && sellerAgent.name ? '<p><b>Seller Agent:</b> ' + escapeHtml(sellerAgent.name) + (sellerAgent.brokerage ? ' — ' + escapeHtml(sellerAgent.brokerage) : '') + '<br>Phone: ' + escapeHtml(sellerAgent.phone||'—') + '<br>Email: ' + (sellerAgent.email ? escapeHtml(sellerAgent.email) : '<span style="color:#C0392B">not provided — no email will be sent</span>') + '</p>' : '';
  const tripLineOwner      = trip.apply ? '<p style="background:#FFF3CD;padding:10px;border-radius:6px">Trip charge: $' + TRIP_CHARGE_AMT + ' (' + Number(trip.miles||0) + ' miles)</p>' : '';
  const notesLineOwner     = notes ? '<p><b>Notes:</b> ' + escapeHtml(notes) + '</p>' : '';
  const extraEmailsLineOwner = extraEmails.length ? '<p><b>Extra Report Recipients:</b> ' + escapeHtml(extraEmails.join(', ')) + '</p>' : '';
  const discountLineOwner  = discountCode ? '<p style="background:#e8f7ee;padding:10px;border-radius:6px"><b>Discount Code:</b> ' + escapeHtml(discountCode) + ' (' + (Number(discountPct)||0) + '% off — −$' + (Number(discountAmount)||0) + ')</p>' : '';

  const ownerHtml = '<div style="font-family:Arial,sans-serif;max-width:560px">'
    + '<h2>New Booking Request — ' + escapeHtml(confId) + '</h2>'
    + '<p><b>Service:</b> ' + escapeHtml(svcLabel) + '<br><b>Add-ons:</b> ' + escapeHtml(addonsLine) + '</p>'
    + '<p><b>Date/Time:</b> ' + escapeHtml(dateFmt) + ' @ ' + escapeHtml(time) + (endTime ? ' to ' + escapeHtml(endTime) : '') + '</p>'
    + '<p><b>Address:</b> ' + escapeHtml(address) + '<br><b>Sq Ft:</b> ' + escapeHtml(String(sqft)) + ' / <b>Year:</b> ' + escapeHtml(String(yearBuilt||'')) + '</p>'
    + '<p><b>Est. Total:</b> $' + (Number(finalPrice)||0) + (trip.apply ? ' (incl. $' + TRIP_CHARGE_AMT + ' trip charge)' : '') + '</p>'
    + '<hr/>'
    + '<p><b>Buyer:</b> ' + escapeHtml(fullName) + '<br>Phone: ' + escapeHtml(buyer.phone) + '<br>Email: ' + escapeHtml(buyer.email) + '</p>'
    + (hasBA ? '<p><b>Buyer Agent:</b> ' + escapeHtml(baName) + (baBrok ? ' — ' + escapeHtml(baBrok) : '') + (baPhone ? '<br>Phone: ' + escapeHtml(baPhone) : '') + (baEmail ? '<br>Email: ' + escapeHtml(baEmail) : '') + '</p>' : '<p><b>Buyer Agent:</b> <i>None provided</i></p>')
    + sellerLineOwner + notesLineOwner + extraEmailsLineOwner + discountLineOwner + tripLineOwner
    + '<div style="margin:28px 0">'
    + '<a href="' + confirmUrl + '" style="background:#1B2D52;color:white;padding:14px 28px;border-radius:6px;text-decoration:none;font-weight:700">' + (opCfg.sms ? 'CONFIRM AND SEND TEXTS' : 'CONFIRM AND SEND EMAILS') + '</a>'
    + '&nbsp;&nbsp;'
    + '<a href="' + cancelUrl + '" style="background:#C0392B;color:white;padding:14px 28px;border-radius:6px;text-decoration:none;font-weight:700">CANCEL</a>'
    + '</div>'
    + '<p style="color:#888;font-size:.8rem">' + (opCfg.sms ? 'Texts' : 'Emails') + ' will NOT go out until you tap Confirm.</p>'
    + '</div>';

  // Notify the operator(s). For jaren: email to OWNER_EMAIL + owner SMS.
  // For jeff: email to BOTH Jeff and Jaren (per setup), and NO SMS.
  const notifyList = (opCfg.notifyEmails && opCfg.notifyEmails.length)
    ? opCfg.notifyEmails
    : [process.env.OWNER_EMAIL];
  for (const recip of notifyList) {
    if (!recip) continue;
    sendEmail(recip, 'PENDING BOOKING' + (opId !== 'jaren' ? ' [' + opCfg.inspectorName + ']' : '') + ': ' + fullName + ' — ' + dateFmt + ' @ ' + time, ownerHtml)
      .then(function(){ console.log('Owner alert sent for ' + confId + ' → ' + recip); })
      .catch(function(e){ console.error('Owner alert email:', e.message); });
  }

  res.json({ success: true, confirmationId: confId, message: 'Request received! You will be confirmed shortly.' });

  // Owner SMS only fires for operators with sms:true (jaren). Jeff is email-only.
  if (opCfg.sms) {
    const ownerSmsBody = 'NEW BOOKING — ' + confId + '\n' + fullName + '\n' + address + '\n' + dateFmt + ' @ ' + time + '\n' + svcLabel + '\n$' + finalPrice + (trip.apply ? ' (incl. trip charge)' : '') + '\n\nCONFIRM:\n' + confirmUrl + '\n\nCANCEL:\n' + cancelUrl;
    sms(process.env.OWNER_PHONE, ownerSmsBody).catch(function(e){ console.error('Owner SMS:', e.message); });
  }
});

// ── JEFF INTAKE FORM ──────────────────────────────────────────
// Public request form Jeff hands to his realtors. Mirrors the main booking
// form but is intentionally a *request*: it posts to /api/book with
// operator:'jeff', creating a pending row Jeff reviews and confirms manually.
// Pricing shown is the same San Tan pricing (estimate); Jeff can adjust on
// confirm. No calendar, no auto-confirm, email-only downstream.
function renderJeffIntakePage() {
  return `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Request an Inspection — San Tan Property Inspections</title>
<style>
  *{box-sizing:border-box}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#F4F1EA;color:#1a1a1a;margin:0;padding:0;line-height:1.5}
  .wrap{max-width:640px;margin:0 auto;padding:24px 18px 60px}
  .head{text-align:center;background:#0F1C35;padding:26px 18px;border-radius:10px;margin-bottom:6px}
  .brand{font-family:Georgia,serif;font-size:1.3rem;font-weight:700;color:#C9A84C;letter-spacing:2px}
  .brand-sub{font-family:Georgia,serif;font-size:.8rem;color:#E8C97A;letter-spacing:4px;margin-top:2px}
  .insp{color:#fff;font-size:.85rem;margin-top:10px;opacity:.85}
  h1{font-size:1.25rem;color:#0F1C35;margin:24px 0 4px}
  .lead{color:#5a5a5a;font-size:.92rem;margin:0 0 20px}
  .card{background:#fff;border-radius:10px;padding:20px 18px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.06)}
  .card h2{font-size:.95rem;text-transform:uppercase;letter-spacing:.5px;color:#1B2D52;margin:0 0 14px}
  label{display:block;font-size:.82rem;font-weight:600;color:#445;margin:12px 0 4px}
  input,select,textarea{width:100%;padding:11px 12px;border:1px solid #d4cdbf;border-radius:7px;font-size:1rem;background:#fff;font-family:inherit}
  textarea{min-height:70px;resize:vertical}
  .row{display:flex;gap:12px}.row>div{flex:1}
  .addons{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:6px}
  .addon{display:flex;align-items:center;gap:8px;background:#F8F6F0;border:1px solid #e4ddcd;border-radius:7px;padding:10px;font-size:.9rem;cursor:pointer}
  .addon input{width:auto}
  .price{background:#0F1C35;color:#fff;border-radius:10px;padding:18px;text-align:center;margin-bottom:16px}
  .price .amt{font-size:2rem;font-weight:700;color:#C9A84C}
  .price .note{font-size:.78rem;opacity:.8;margin-top:6px}
  .btn{width:100%;background:#C9A84C;color:#0F1C35;font-weight:700;font-size:1.05rem;padding:15px;border:none;border-radius:8px;cursor:pointer;margin-top:8px}
  .btn:disabled{opacity:.5;cursor:not-allowed}
  .msg{padding:14px;border-radius:8px;margin-bottom:14px;font-size:.92rem;display:none}
  .msg.err{background:#FDECEA;color:#922;border:1px solid #f5c6cb;display:block}
  .msg.ok{background:#E9F7EF;color:#1c6b3f;border:1px solid #bfe3cd;display:block}
  .req{color:#C0392B}
</style></head>
<body><div class="wrap">
  <div class="head">
    <div class="brand">SAN TAN PROPERTY</div>
    <div class="brand-sub">INSPECTIONS</div>
    <div class="insp">Inspection by Jeff Thompson · AZ BTR #79082</div>
  </div>
  <h1>Request an Inspection</h1>
  <p class="lead">Fill out the details below and Jeff will follow up by email to confirm your inspection. The price shown is an estimate.</p>

  <div id="msg" class="msg"></div>

  <div class="card">
    <h2>Property</h2>
    <label>Property Address <span class="req">*</span></label>
    <input id="address" autocomplete="off" placeholder="123 E Main St, Gilbert, AZ 85296">
    <div class="row">
      <div><label>Square Footage <span class="req">*</span></label><input id="sqft" inputmode="numeric" placeholder="2000"></div>
      <div><label>Year Built</label><input id="yearBuilt" inputmode="numeric" placeholder="2005"></div>
    </div>
    <label>Inspection Type <span class="req">*</span></label>
    <select id="inspType">
      <option value="pre-purchase">Pre-Purchase Inspection</option>
      <option value="pre-listing">Pre-Listing Inspection</option>
      <option value="new-construction">New Construction Inspection</option>
      <option value="warranty">Pre-One-Year Warranty Inspection</option>
      <option value="reinspection">Re-Inspection</option>
    </select>
  </div>

  <div class="card">
    <h2>Add-Ons (optional)</h2>
    <div class="addons">
      <label class="addon"><input type="checkbox" class="addon-cb" value="termite"> Termite / WDO ($85)</label>
      <label class="addon"><input type="checkbox" class="addon-cb" value="pool"> Pool ($60)</label>
      <label class="addon"><input type="checkbox" class="addon-cb" value="spa"> Spa ($40)</label>
      <label class="addon"><input type="checkbox" class="addon-cb" value="shed"> Shed / Outbuilding ($50)</label>
    </div>
  </div>

  <div class="card">
    <h2>Preferred Schedule</h2>
    <div class="row">
      <div><label>Preferred Date <span class="req">*</span></label><input id="date" type="date"></div>
      <div><label>Preferred Time <span class="req">*</span></label>
        <select id="time">
          <option value="8:00 AM">8:00 AM</option><option value="9:00 AM">9:00 AM</option>
          <option value="10:00 AM">10:00 AM</option><option value="11:00 AM">11:00 AM</option>
          <option value="12:00 PM">12:00 PM</option><option value="1:00 PM">1:00 PM</option>
          <option value="2:00 PM">2:00 PM</option><option value="3:00 PM">3:00 PM</option>
        </select>
      </div>
    </div>
  </div>

  <div class="card">
    <h2>Buyer / Client</h2>
    <div class="row">
      <div><label>First Name <span class="req">*</span></label><input id="bFirst" autocomplete="off"></div>
      <div><label>Last Name</label><input id="bLast" autocomplete="off"></div>
    </div>
    <label>Email <span class="req">*</span></label><input id="bEmail" type="email" autocomplete="off">
    <label>Phone <span class="req">*</span></label><input id="bPhone" inputmode="tel" autocomplete="off">
  </div>

  <div class="card">
    <h2>Buyer's Agent (optional)</h2>
    <label>Name</label><input id="aName" autocomplete="off">
    <div class="row">
      <div><label>Email</label><input id="aEmail" type="email" autocomplete="off"></div>
      <div><label>Phone</label><input id="aPhone" inputmode="tel" autocomplete="off"></div>
    </div>
    <label>Brokerage</label><input id="aBrok" autocomplete="off">
  </div>

  <div class="card">
    <h2>Listing Agent (optional)</h2>
    <p style="margin:-6px 0 10px;color:#6a6a6a;font-size:.82rem">This is who Jeff coordinates with for access (lockbox/CBS code, utilities, etc.).</p>
    <label>Name</label><input id="sName" autocomplete="off">
    <div class="row">
      <div><label>Email</label><input id="sEmail" type="email" autocomplete="off"></div>
      <div><label>Phone</label><input id="sPhone" inputmode="tel" autocomplete="off"></div>
    </div>
    <label>Brokerage</label><input id="sBrok" autocomplete="off">
    <label>Notes for Jeff</label><textarea id="notes" placeholder="Gate code, lockbox/CBS code, special requests, etc."></textarea>
  </div>

  <div class="price">
    <div style="font-size:.8rem;opacity:.8;text-transform:uppercase;letter-spacing:1px">Estimated Total</div>
    <div class="amt" id="priceAmt">&mdash;</div>
    <div class="note">Estimate based on standard pricing. Jeff confirms the final total.</div>
  </div>

  <button class="btn" id="submitBtn">Request Inspection</button>
</div>

<script>
// Google Places address autocomplete — same key/library as the main site.
// Called by the Maps script via &callback=initAutocomplete. Must be global.
function initAutocomplete(){
  try {
    var input = document.getElementById('address');
    if (!input || !window.google || !google.maps || !google.maps.places) return;
    var ac = new google.maps.places.Autocomplete(input, {
      types: ['address'],
      componentRestrictions: { country: 'us' },
      fields: ['formatted_address']
    });
    ac.addListener('place_changed', function(){
      var place = ac.getPlace();
      if (place && place.formatted_address) input.value = place.formatted_address;
    });
    input.addEventListener('keydown', function(e){ if (e.key === 'Enter') e.preventDefault(); });
  } catch(e) { /* autocomplete is a nice-to-have; never block the form */ }
}
(function(){
  var PRICE_BASE = {1000:400,1500:425,2000:450,2500:475,3000:550,3500:600,4000:650,4500:675,9999:750};
  var ADDON_P = {termite:85,pool:60,spa:40,shed:50};
  function tier(n){n=Number(n);if(!n||n<=0)return null;if(n<=1000)return 1000;if(n<=1500)return 1500;if(n<=2000)return 2000;if(n<=2500)return 2500;if(n<=3000)return 3000;if(n<=3500)return 3500;if(n<=4000)return 4000;if(n<=4500)return 4500;return 9999;}
  function calc(){
    var t=tier(document.getElementById('sqft').value);
    if(!t){document.getElementById('priceAmt').innerHTML='&mdash;';return;}
    var p=PRICE_BASE[t];
    var yr=Number(document.getElementById('yearBuilt').value)||0;
    if(yr>0&&yr<=1959)p+=80;else if(yr>=1960&&yr<=1980)p+=50;
    document.querySelectorAll('.addon-cb').forEach(function(cb){if(cb.checked)p+=ADDON_P[cb.value]||0;});
    document.getElementById('priceAmt').textContent='$'+p;
  }
  document.getElementById('sqft').addEventListener('input',calc);
  document.getElementById('yearBuilt').addEventListener('input',calc);
  document.querySelectorAll('.addon-cb').forEach(function(cb){cb.addEventListener('change',calc);});

  function showMsg(text,kind){var m=document.getElementById('msg');m.textContent=text;m.className='msg '+kind;window.scrollTo({top:0,behavior:'smooth'});}

  document.getElementById('submitBtn').addEventListener('click',async function(){
    var btn=this;
    var addons=[];document.querySelectorAll('.addon-cb').forEach(function(cb){if(cb.checked)addons.push(cb.value);});
    var payload={
      operator:'jeff',
      address:document.getElementById('address').value.trim(),
      sqft:document.getElementById('sqft').value.trim(),
      yearBuilt:document.getElementById('yearBuilt').value.trim(),
      inspType:document.getElementById('inspType').value,
      addons:addons,
      date:document.getElementById('date').value,
      time:document.getElementById('time').value,
      totalMins:120,
      buyer:{firstName:document.getElementById('bFirst').value.trim(),lastName:document.getElementById('bLast').value.trim(),email:document.getElementById('bEmail').value.trim(),phone:document.getElementById('bPhone').value.trim()},
      buyerAgent:{name:document.getElementById('aName').value.trim(),email:document.getElementById('aEmail').value.trim(),phone:document.getElementById('aPhone').value.trim(),brokerage:document.getElementById('aBrok').value.trim()},
      sellerAgent:{name:document.getElementById('sName').value.trim(),email:document.getElementById('sEmail').value.trim(),phone:document.getElementById('sPhone').value.trim(),brokerage:document.getElementById('sBrok').value.trim()},
      notes:document.getElementById('notes').value.trim()
    };
    if(!payload.address||!payload.sqft||!payload.date||!payload.time||!payload.buyer.firstName||!payload.buyer.email||!payload.buyer.phone){
      showMsg('Please fill out all required fields (marked with *).','err');return;
    }
    btn.disabled=true;btn.textContent='Sending…';
    try{
      var r=await fetch('/api/book',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
      var data=await r.json();
      if(!r.ok){showMsg(data.error||'Something went wrong. Please try again.','err');btn.disabled=false;btn.textContent='Request Inspection';return;}
      showMsg('Request received! Jeff will email you shortly to confirm. Confirmation #'+data.confirmationId,'ok');
      btn.textContent='Request Sent ✓';
    }catch(e){
      showMsg('Network error. Please try again or contact Jeff directly.','err');
      btn.disabled=false;btn.textContent='Request Inspection';
    }
  });
})();
</script>
<script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBCNujdEjf3Gw51CJF1gPuANtaS_2_LOdI&libraries=places&callback=initAutocomplete" async defer></script>
</body></html>`;
}

app.get(['/book-jeff', '/jeff'], function(req, res) {
  res.set('Content-Type', 'text/html; charset=utf-8');
  res.send(renderJeffIntakePage());
});

// ── SUBDOMAIN ROOT ROUTING ────────────────────────────────────
// Clean subdomains so links on business cards have nothing trailing:
//   jeff.santanpropertyinspections.com  → Jeff's intake form (served here)
//   book.santanpropertyinspections.com  → redirect to the main public site
// Detection is by the Host header's first label. Falls through harmlessly on
// the raw Railway URL or any other host (404 as before — backend has no other
// root page; the public marketing site is hosted separately).
app.get('/', function(req, res) {
  const host = String(req.headers.host || '').toLowerCase();
  const sub = host.split(':')[0].split('.')[0];
  if (sub === 'jeff') {
    res.set('Content-Type', 'text/html; charset=utf-8');
    return res.send(renderJeffIntakePage());
  }
  if (sub === 'book') {
    return res.redirect(302, 'https://santanpropertyinspections.com');
  }
  if (sub === 'admin') {
    // Bare admin subdomain → send to the login page (or dashboard if already in).
    return res.redirect(302, '/admin');
  }
  return res.status(404).send('Not found.');
});

// ── CONFIRM BOOKING ───────────────────────────────────────────
app.get('/confirm/:token', async function(req, res) {
  // Verify HMAC signature before doing any DB work. Owner-facing endpoint so
  // we can be terse on failure.
  const sigCheck = verifySignedToken(req.params.token, req.query.s);
  if (!sigCheck.ok) {
    console.warn('Confirm link rejected: ' + sigCheck.reason);
    return res.status(403).send('<h2>Invalid or expired link. Check your email for the original confirmation message, or call (480) 618-0805.</h2>');
  }
  if (sigCheck.legacy) console.warn('Confirm: accepting legacy unsigned token (pre-HMAC migration)');

  let booking;
  try {
    // Atomically claim the booking — if a previous click already claimed it, this returns null.
    booking = await dbClaim(req.params.token);
  } catch(e) {
    console.error('DB claim error:', e.message);
    return res.send('<h2>Database error. Please try again or call (480) 618-0805.</h2>');
  }
  if (!booking) return res.send('<h2>This booking has already been confirmed. Check your inbox for confirmation details, or call (480) 618-0805.</h2>');

  const { confId, address, sqft, yearBuilt, svcLabel, addons, addonsLine, finalPrice, totalMins, date, time, endTime, dateFmt, fullName, buyer, buyerAgent, sellerAgent, notes, extraEmails, discountCode, discountPct, discountAmount } = booking;
  const tripCharge = booking.tripCharge || { apply: false, miles: 0 };
  // Which inspector owns this booking (defaults to jaren). Drives reply-to,
  // the phone shown to the client, whether SMS fires, and notify recipients.
  const opId  = getOperator(booking.operator);
  const opCfg = OPERATORS[opId];

  // Normalize buyerAgent fields locally — these were created in /api/book but don't survive the round-trip through pending_bookings
  const ba      = (buyerAgent && typeof buyerAgent === 'object') ? buyerAgent : {};
  const baName  = ba.name  || '';
  const baPhone = ba.phone || '';
  const baEmail = ba.email || '';
  const baBrok  = ba.brokerage || '';
  const hasBA   = !!(baName || baPhone || baEmail);

  // Token already removed by dbClaim — no separate dbDelete needed

  const sm2     = slotToMins(time);
  const slotH2  = Math.floor(sm2/60), slotM2 = sm2%60;
  const startDT2 = new Date(`${date}T${String(slotH2).padStart(2,'0')}:${String(slotM2).padStart(2,'0')}:00-07:00`);
  const endDT2   = new Date(startDT2.getTime() + (totalMins||120)*60000);

  let calId = null;
  // Calendar integration for BOTH operators.
  //  - jaren  → CALENDAR_ID (his calendar), buyer invited as attendee.
  //  - jeff   → CALENDAR_ID_JEFF (separate calendar Jaren owns + shares to Jeff);
  //             Jeff's Gmail is invited so the job lands on his phone calendar.
  // If an operator has no calendar ID configured, event creation is skipped
  // gracefully (no crash) — so jeff's bookings still work even before the
  // CALENDAR_ID_JEFF env var is set.
  const targetCalId = (opId === 'jeff') ? (process.env.CALENDAR_ID_JEFF || null) : CALENDAR_ID;
  if (targetCalId) {
   try {
    // Blocked, labeled layout (clean style). Only fields actually collected
    // are shown; missing optional fields are omitted rather than left blank.
    const descLines = [
      'BUYER',
      '  ' + fullName + (buyer.phone ? '  |  ' + buyer.phone : '') + (buyer.email ? '  |  ' + buyer.email : ''),
      '',
      hasBA
        ? 'BUYER\u2019S AGENT\n  ' + baName + (baBrok ? '  \u2014  ' + baBrok : '') + (baPhone ? '  |  ' + baPhone : '') + (baEmail ? '  |  ' + baEmail : '')
        : 'BUYER\u2019S AGENT\n  None provided',
      '',
      (sellerAgent && sellerAgent.name)
        ? 'LISTING AGENT\n  ' + sellerAgent.name + (sellerAgent.brokerage ? '  \u2014  ' + sellerAgent.brokerage : '') + (sellerAgent.phone ? '  |  ' + sellerAgent.phone : '') + '\n'
        : null,
      'SERVICES (Total: $' + finalPrice + (tripCharge.apply ? ', incl. trip charge' : '') + ')',
      '  ' + svcLabel + (addons.length ? '  +  ' + addonsLine : ''),
      '',
      'DETAILS',
      '  Conf #: ' + confId,
      '  Year Built: ' + (yearBuilt || '\u2014'),
      '  Square Footage: ' + (sqft || '\u2014'),
      discountCode ? '  Discount: ' + discountCode + ' (' + discountPct + '% off \u2212 \u2212$' + discountAmount + ')' : null,
      (notes) ? '  Notes: ' + notes : null,
      extraEmails && extraEmails.length ? '  Extra report recipients: ' + extraEmails.join(', ') : null,
    ].filter(function(x){ return x !== null && x !== undefined; }).join('\n');

    // Attendees: always the buyer; for Jeff's bookings also invite Jeff's Gmail
    // so the event appears on his own calendar.
    const attendees = [{ email: buyer.email, displayName: fullName }];
    if (opId === 'jeff') {
      attendees.push({ email: opCfg.replyTo, displayName: opCfg.inspectorName });
    }

    const ev = {
      summary: svcLabel + ' \u2014 ' + fullName, location: address, description: descLines,
      start: { dateTime: startDT2.toISOString(), timeZone: TIMEZONE },
      end:   { dateTime: endDT2.toISOString(),   timeZone: TIMEZONE },
      colorId: '5',
      attendees: attendees,
      reminders: { useDefault: false, overrides: [{ method:'email', minutes:24*60 },{ method:'popup', minutes:60 }] },
    };
    const r = await calendar.events.insert({ calendarId: targetCalId, resource: ev, sendUpdates:'all' });
    calId = r.data.id;
    console.log('Calendar event created (' + opId + '): ' + calId);
   } catch(e) { console.error('Calendar:', e.message); }
  }

  // Save to confirmed_bookings — runs regardless of Calendar success/failure
  try {
    await pool.query(
      'INSERT INTO confirmed_bookings (conf_id, data, miles, operator) VALUES ($1, $2, $3, $4) ON CONFLICT (conf_id) DO NOTHING',
      [confId, JSON.stringify({ ...booking, calId, confirmedAt: new Date().toISOString() }), (booking && booking.miles != null) ? booking.miles : null, opId]
    );
    console.log('Confirmed booking saved to DB: ' + confId);
  } catch(e) { console.error('DB confirmed save:', e.message); }

  // Generate agreement token for this confirmed booking
  // Used for BOTH the agreement page and the customer hub — same token, same HMAC sig.
  const agreeToken = uuidv4();
  const BASE_URL = process.env.RAILWAY_URL || 'https://santanproperty-backend-production.up.railway.app';
  const agreementUrl = withSig(BASE_URL + '/agreement/' + agreeToken, agreeToken);
  const hubUrl       = withSig(BASE_URL + '/i/'         + agreeToken, agreeToken);

  // Store agreement token temporarily so we can look up booking from it
  try {
    await pool.query(
      'UPDATE confirmed_bookings SET agreement_sent_at = NOW(), data = data || $1::jsonb WHERE conf_id = $2',
      [JSON.stringify({ agreementToken: agreeToken }), confId]
    );
    // Also store in pending_bookings temporarily for agreement lookup
    await dbSet('agree_' + agreeToken, { ...booking, calId, confirmedAt: new Date().toISOString(), agreeToken });
  } catch(e) { console.error('Agreement token store error:', e.message); }

  // SMS — buyer, buyer agent, listing agent.
  // Gated on the operator's sms flag: inspectors with sms:false (e.g. Jeff)
  // run email-only, so none of these texts fire for their bookings.
  if (opCfg.sms) {
    // SMS - buyer
    // Sends the hub URL — single link that opens the status page where the
    // client can sign the agreement, reschedule, or check report status.
    await sms(buyer.phone,
      'Hi ' + buyer.firstName + '! Your inspection is confirmed.\n\nAddress: ' + address + '\nDate: ' + dateFmt + '\nTime: ' + time + (endTime ? ' to ' + endTime : '') + '\nService: ' + svcLabel + (addons.length ? '\nAdd-ons: ' + addonsLine : '') + '\nEst. Total: $' + finalPrice + ' (pay day-of)' + (tripCharge.apply ? ' incl. $' + TRIP_CHARGE_AMT + ' trip charge' : '') + '\nConf #: ' + confId + (baPhone ? '\n\nYour agent ' + (baName ? baName.split(\' \')[0] : 'your agent') + ' has also been notified.' : '') + '\n\nOpen your inspection hub (sign agreement, reschedule, report status):\n' + hubUrl + '\n\nQuestions? ' + opCfg.phone + ' | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
    );

    // SMS - buyer agent (only if phone provided)
    if (baPhone) {
      await sms(baPhone,
        'Hi ' + (baName ? baName.split(' ')[0] : 'there') + '! Inspection scheduled for your buyer.\n\nAddress: ' + address + '\nBuyer: ' + fullName + '\nDate: ' + dateFmt + ' @ ' + time + '\nService: ' + svcLabel + '\nConf #: ' + confId + '\n\nACTION NEEDED — Confirm with listing agent:\n- Listing agent aware of date & time\n- GAS on & accessible\n- WATER on & accessible\n- ELECTRICAL on & accessible\n- ATTIC ACCESS clear & accessible\n\nQuestions? ' + opCfg.phone + ' | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
      );
    }

    if (sellerAgent && sellerAgent.phone) {
      await sms(sellerAgent.phone,
        'Hi ' + (sellerAgent.name ? sellerAgent.name.split(' ')[0] : 'there') + '! Inspection scheduled at your listing.\n\nAddress: ' + address + '\nDate: ' + dateFmt + ' @ ' + time + '\nService: ' + svcLabel + '\n\nPlease ensure by inspection day:\n- GAS on & accessible\n- WATER on & accessible\n- ELECTRICAL on & accessible\n- ATTIC ACCESS clear & accessible\n\nIMPORTANT: Please send the CBS code so I can access the home, and reply to this message to confirm the inspection.\n\nWARNING: If utilities are NOT on, a $125 re-inspection fee will apply.\n\nQuestions? ' + opCfg.phone + ' | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
      );
    }
  }

  const tripLineBuyer = tripCharge.apply ? ' (incl. $' + TRIP_CHARGE_AMT + ' trip charge)' : '';

  // Buyer confirmation email — links to the customer hub (single page with
  // agreement signing, reschedule form, and report status).
  const buyerHtml = emailWrap(
    '<h2 style="color:#0F1C35">Inspection Confirmed</h2>'
    + '<p>Hi ' + escapeHtml(buyer.firstName) + ', here are your booking details:</p>'
    + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
    + '<tr><td style="padding:6px 0;color:#888;width:130px">Service</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(svcLabel) + (addons.length ? ' + ' + escapeHtml(addonsLine) : '') + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(dateFmt) + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(time) + (endTime ? ' to ' + escapeHtml(endTime) : '') + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Property</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(address) + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Est. Total</td><td style="color:#C9A84C;font-weight:700">$' + (Number(finalPrice)||0) + escapeHtml(tripLineBuyer) + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Confirmation</td><td style="color:#C9A84C;font-weight:700">' + escapeHtml(confId) + '</td></tr>'
    + '</table>'
    + '<p>Payment can be made on inspection day. We accept cash, Venmo, Zelle, or credit/debit card.</p>'
    + '<div style="background:#EAF3FB;border-left:4px solid #1B2D52;padding:18px 20px;margin:24px 0;border-radius:0 8px 8px 0">'
    + '<p style="margin:0 0 6px;font-size:.95rem;font-weight:700;color:#1B2D52">Your Inspection Hub</p>'
    + '<p style="margin:0 0 14px;font-size:.86rem;color:#555;line-height:1.6">Open the link below to sign your agreement, request a reschedule, or check the status of your report. Bookmark it — this is your one-stop page for everything.</p>'
    + '<a href="' + hubUrl + '" style="display:inline-block;background:#1B2D52;color:white;padding:13px 26px;border-radius:6px;text-decoration:none;font-weight:700;font-size:.9rem">Open Inspection Hub</a>'
    + '<p style="margin:14px 0 0;font-size:.78rem;color:#888;line-height:1.5"><strong style="color:#7a4a00">Action needed:</strong> your inspection agreement must be signed before the report can be released. The hub above will walk you through it.</p>'
    + '</div>'
    + '<p>Your report will be delivered the <strong>same day</strong> as your inspection.</p>'
    + '<p>Questions? Call/text <strong>' + opCfg.phone + '</strong></p>'
  );

  try {
    await sendEmail(buyer.email, 'Inspection Confirmed — ' + dateFmt + ' @ ' + time + ' [' + confId + ']', buyerHtml, null, opCfg.replyTo, baEmail || null);
  } catch(e) { console.error('Buyer email:', e.message); }

  // Extra recipients email
  if (extraEmails && extraEmails.length) {
    const extraHtml = emailWrap(
      '<h2 style="color:#0F1C35">Inspection Confirmed</h2>'
      + '<p>You have been added as a report recipient for the following inspection:</p>'
      + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
      + '<tr><td style="padding:6px 0;color:#888;width:130px">Buyer</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(fullName) + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Service</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(svcLabel) + (addons.length ? ' + ' + escapeHtml(addonsLine) : '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(dateFmt) + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(time) + (endTime ? ' to ' + escapeHtml(endTime) : '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Property</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(address) + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Confirmation</td><td style="color:#C9A84C;font-weight:700">' + escapeHtml(confId) + '</td></tr>'
      + '</table>'
      + '<p>The inspection report will be delivered the <strong>same day</strong> as the inspection.</p>'
      + '<p>Questions? Call/text <strong>' + opCfg.phone + '</strong></p>'
    );
    for (const email of extraEmails) {
      try {
        await sendEmail(email, 'Inspection Confirmed — ' + dateFmt + ' @ ' + time + ' [' + confId + ']', extraHtml, null, opCfg.replyTo);
      } catch(e) { console.error('Extra recipient email to ' + email + ':', e.message); }
    }
  }

  // Buyer agent email
  if (baEmail) {
    const baHtml = emailWrap(
      '<h2 style="color:#0F1C35">Inspection Confirmed for Your Buyer</h2>'
      + '<p>Hi ' + escapeHtml(baName || 'there') + ',</p>'
      + '<p>The inspection for your buyer has been confirmed. Details below:</p>'
      + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
      + '<tr><td style="padding:6px 0;color:#888;width:130px">Buyer</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(fullName) + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Property</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(address) + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(dateFmt) + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(time) + (endTime ? ' to ' + escapeHtml(endTime) : '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Service</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(svcLabel) + (addons.length ? ' + ' + escapeHtml(addonsLine) : '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Confirmation</td><td style="color:#C9A84C;font-weight:700">' + escapeHtml(confId) + '</td></tr>'
      + '</table>'
      + '<p>Please confirm the listing agent is aware of the inspection date and that <strong>gas, water, electrical, and attic access are on &amp; accessible</strong>.</p>'
      + '<p>Questions? Call/text <strong>' + opCfg.phone + '</strong></p>'
    );
    try {
      await sendEmail(baEmail, 'Inspection Confirmed — ' + fullName + ' — ' + dateFmt + ' @ ' + time, baHtml, null, opCfg.replyTo);
    } catch(e) { console.error('Buyer agent email:', e.message); }
  }

  // Seller agent email
  if (sellerAgent && sellerAgent.email) {
    const isNewConstruction = inspType === 'new-construction';
    const sellerRoleLabel = isNewConstruction ? 'Construction Representative' : 'Listing Agent';
    const sellerHeading   = isNewConstruction ? 'Inspection Scheduled — New Construction' : 'Inspection Scheduled at Your Listing';
    const sellerIntro     = isNewConstruction
      ? 'A new construction inspection has been scheduled. Please ensure the following are ready by inspection day:'
      : 'A home inspection has been scheduled at your listing. Please ensure the following are ready by inspection day:';
    const sellerHtml = isNewConstruction ? emailWrap(
      '<h2 style="color:#0F1C35">New Construction Inspection Scheduled</h2>'
      + '<p>Hi ' + escapeHtml(sellerAgent.name || 'there') + ',</p>'
      + '<p>A new construction inspection has been scheduled at the following property. I need a few things from you before inspection day:</p>'
      + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
      + '<tr><td style="padding:6px 0;color:#888;width:130px">Property</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(address) + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(dateFmt) + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(time) + (endTime ? ' to ' + escapeHtml(endTime) : '') + '</td></tr>'
      + '</table>'
      + '<p style="font-weight:600;color:#1B2D52;margin:20px 0 8px">Please reply with the following:</p>'
      + '<ol style="margin:0 0 16px 20px;line-height:2">'
      + '<li>Are <strong>gas, water, and electrical</strong> on and accessible at the time of inspection?</li>'
      + '<li>Are there any areas or systems that are <strong>not yet complete or not accessible</strong> for inspection?</li>'
      + '<li>How will I <strong>gain access</strong> to the home? (key, lockbox, someone on site, etc.)</li>'
      + '<li>Is there anything you need from me prior to the inspection? (e.g. proof of insurance, BTR license number, or any other documentation)</li>'
      + '<li>Is there any other information I should have before arriving?</li>'
      + '</ol>'
      + '<p>Questions? Call/text <strong>' + opCfg.phone + '</strong></p>'
    ) : emailWrap(
      '<h2 style="color:#0F1C35">Inspection Scheduled at Your Listing</h2>'
      + '<p>Hi ' + escapeHtml(sellerAgent.name || 'there') + ',</p>'
      + '<p>A home inspection has been scheduled at your listing. Please ensure the following are ready by inspection day:</p>'
      + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
      + '<tr><td style="padding:6px 0;color:#888;width:130px">Property</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(address) + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(dateFmt) + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(time) + (endTime ? ' to ' + escapeHtml(endTime) : '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Service</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(svcLabel) + '</td></tr>'
      + '</table>'
      + '<ul style="margin:12px 0 12px 20px"><li>Gas on &amp; accessible</li><li>Water on &amp; accessible</li><li>Electrical on &amp; accessible</li><li>Attic access clear &amp; accessible</li></ul>'
      + '<div style="background:#EAF3FB;border-left:4px solid #1B2D52;padding:12px 16px;border-radius:0 8px 8px 0;margin:14px 0"><p style="margin:0;font-size:.92rem"><strong style="color:#1B2D52">Important:</strong> Please send the <strong>CBS code</strong> so I can access the home, and reply to confirm the inspection.</p></div>'
      + '<p style="background:#FFF3CD;padding:10px;border-radius:6px"><strong>Note:</strong> If utilities are not on at the time of inspection, a $125 re-inspection fee will apply.</p>'
      + '<p>Questions? Call/text <strong>' + opCfg.phone + '</strong></p>'
    );
    try {
      await sendEmail(sellerAgent.email, (isNewConstruction ? 'New Construction Inspection — ' : 'Inspection Scheduled — ') + address + ' on ' + dateFmt, sellerHtml, null, opCfg.replyTo);
    } catch(e) { console.error('Listing agent email:', e.message); }
  }

  // Owner confirmation page
  const tripConfirmLine = tripCharge.apply ? '<tr><td style="padding:6px 0;color:#888;width:130px">Trip Charge</td><td style="color:#C9A84C;font-weight:600">+$' + TRIP_CHARGE_AMT + ' (' + (Number(tripCharge.miles)||0) + ' miles)</td></tr>' : '';
  const discountConfirmLine = discountCode ? '<tr><td style="padding:6px 0;color:#888">Discount</td><td style="color:#1ab464;font-weight:600">' + escapeHtml(discountCode) + ' (-$' + (Number(discountAmount)||0) + ')</td></tr>' : '';

  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Booking Confirmed — San Tan Property Inspections</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0F1C35;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;position:relative;overflow:hidden;}
body::before{content:'';position:absolute;inset:0;background:url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='%23C9A84C' fill-opacity='0.04'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/svg%3E");pointer-events:none;}
.card{background:#fff;border-radius:16px;padding:40px 36px;max-width:520px;width:100%;position:relative;z-index:1;box-shadow:0 28px 70px rgba(0,0,0,.4);}
.logo{text-align:center;background:#0F1C35;border-radius:10px;padding:16px;margin-bottom:28px;}
.logo-title{font-family:Georgia,serif;font-size:1rem;font-weight:700;color:#C9A84C;letter-spacing:2px;}
.logo-sub{font-family:Georgia,serif;font-size:.65rem;color:#E8C97A;letter-spacing:4px;margin-top:3px;}
.check{width:60px;height:60px;background:#e8f7ee;border-radius:50%;display:flex;align-items:center;justify-content:center;margin:0 auto 16px;font-size:1.8rem;}
h1{font-family:Georgia,serif;font-size:1.5rem;color:#1B2D52;text-align:center;margin-bottom:6px;}
.sub{text-align:center;color:#8C7B6B;font-size:.88rem;margin-bottom:28px;line-height:1.6;}
.conf-badge{background:#1B2D52;color:#C9A84C;font-weight:700;font-size:.82rem;letter-spacing:1px;padding:8px 18px;border-radius:20px;display:block;text-align:center;margin-bottom:24px;}
table{width:100%;border-collapse:collapse;margin-bottom:24px;font-size:.88rem;}
td{padding:8px 0;vertical-align:top;border-bottom:1px solid #F0EBE0;}
tr:last-child td{border-bottom:none;}
td:first-child{color:#8C7B6B;width:130px;}
td:last-child{color:#2C2C2C;font-weight:600;}
.total-row td{padding-top:14px;font-size:1rem;}
.total-row td:first-child{font-weight:700;color:#1B2D52;}
.total-row td:last-child{color:#C9A84C;font-size:1.2rem;}
.notice{background:#FAF7F0;border-radius:8px;padding:14px 16px;font-size:.82rem;color:#8C7B6B;line-height:1.7;margin-bottom:16px;}
.notice strong{color:#1B2D52;}
.agree-notice{background:#EAF3FB;border-left:4px solid #1B2D52;border-radius:0 8px 8px 0;padding:12px 16px;font-size:.82rem;color:#555;line-height:1.6;margin-bottom:20px;}
.footer{text-align:center;font-size:.78rem;color:#B0A898;border-top:1px solid #F0EBE0;padding-top:16px;}
.footer a{color:#C9A84C;text-decoration:none;}
</style>
</head>
<body>
<div class="card">
  <div class="logo">
    <div class="logo-title">SAN TAN PROPERTY</div>
    <div class="logo-sub">INSPECTIONS</div>
  </div>
  <div class="check">&#10003;</div>
  <h1>Booking Confirmed!</h1>
  <p class="sub">Confirmation ${opCfg.sms ? 'texts and emails have' : 'emails have'} been sent to the buyer and agents.</p>
  <div class="conf-badge">Confirmation # ${escapeHtml(confId)}</div>
  <table>
    <tr><td>Buyer</td><td>${escapeHtml(fullName)}</td></tr>
    <tr><td>Property</td><td>${escapeHtml(address)}</td></tr>
    <tr><td>Service</td><td>${escapeHtml(svcLabel)}${addons.length ? ' + ' + escapeHtml(addonsLine) : ''}</td></tr>
    <tr><td>Date</td><td>${escapeHtml(dateFmt)}</td></tr>
    <tr><td>Time</td><td>${escapeHtml(time)}${endTime ? ' to ' + escapeHtml(endTime) : ''}</td></tr>
    ${tripConfirmLine}${discountConfirmLine}
    <tr class="total-row"><td>Total</td><td>$${Number(finalPrice)||0}</td></tr>
  </table>
  <div class="agree-notice">
    <strong>Agreement link sent to buyer.</strong> The report will be locked until the agreement is signed. You can check signature status in the admin dashboard.
  </div>
  <div class="notice">
    ${opCfg.sms ? '<strong>Texts sent to:</strong> ' + escapeHtml(fullName) + (baPhone ? ' &middot; ' + escapeHtml(baName) : '') + (sellerAgent && sellerAgent.name ? ' &middot; ' + escapeHtml(sellerAgent.name) : '') + '<br>' : ''}
    <strong>Emails sent to:</strong> ${escapeHtml(buyer.email)}${baEmail ? ' &middot; ' + escapeHtml(baEmail) : ''}
  </div>
  <div class="footer">
    <a href="https://santanpropertyinspections.com">santanpropertyinspections.com</a> &nbsp;&middot;&nbsp; ${opCfg.phone} &nbsp;&middot;&nbsp; BTR #${opCfg.btrNumber}
  </div>
</div>
</body>
</html>`);
});

// ── AGREEMENT ROUTES ──────────────────────────────────────────

// Show agreement page
app.get('/agreement/:token', agreementLimiter, async function(req, res) {
  const token = req.params.token;
  const sigCheck = verifySignedToken(token, req.query.s);
  if (!sigCheck.ok) {
    console.warn('Agreement GET rejected: ' + sigCheck.reason);
    return res.status(403).send('<h2>Invalid or expired link. Please call (480) 618-0805 if you need to sign your agreement.</h2>');
  }
  if (sigCheck.legacy) console.warn('Agreement GET: accepting legacy unsigned token');

  let booking;
  try {
    booking = await dbGet('agree_' + token);
  } catch(e) {
    console.error('Agreement DB read error:', e.message);
    return res.status(500).send('<h2>Database error. Please call (480) 618-0805.</h2>');
  }

  if (!booking) {
    return res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Agreement — San Tan Property Inspections</title></head>
<body style="font-family:sans-serif;text-align:center;padding:60px 24px;background:#0F1C35;color:#fff">
<h2>This agreement link has expired or already been signed.</h2>
<p style="margin-top:12px;color:#aaa">If you need to sign your agreement, please contact us at (480) 618-0805.</p>
</body></html>`);
  }

  // Check if already signed
  try {
    const r = await pool.query('SELECT agreement_signed_at FROM confirmed_bookings WHERE conf_id = $1', [booking.confId]);
    if (r.rows.length && r.rows[0].agreement_signed_at) {
      return res.send(buildAgreementPage(booking, token, { signed: true }));
    }
  } catch(e) { console.error('Agreement status check:', e.message); }

  res.send(buildAgreementPage(booking, token));
});

// Process signature
app.post('/agreement/:token/sign', agreementLimiter, async function(req, res) {
  const token = req.params.token;

  // HMAC check first — block forged sign-as posts before any work or DB read.
  const sigCheck = verifySignedToken(token, req.query.s);
  if (!sigCheck.ok) {
    console.warn('Agreement POST rejected: ' + sigCheck.reason);
    return res.status(403).send('<h2>Invalid or expired link. Please call (480) 618-0805 if you need to sign your agreement.</h2>');
  }
  if (sigCheck.legacy) console.warn('Agreement POST: accepting legacy unsigned token');

  const signature = (req.body.signature || '').trim();

  if (!signature) {
    let booking;
    try { booking = await dbGet('agree_' + token); } catch(e) { booking = null; }
    return res.send(buildAgreementPage(booking || {}, token, { error: 'Please enter your full legal name to sign.' }));
  }

  let booking;
  try {
    booking = await dbGet('agree_' + token);
  } catch(e) {
    return res.status(500).send('<h2>Database error. Please call (480) 618-0805.</h2>');
  }

  if (!booking) {
    return res.send('<h2>This agreement link has expired. Please contact us at (480) 618-0805.</h2>');
  }

  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
  const signedAt = new Date().toISOString();

  // Store signature in DB
  try {
    await pool.query(
      `UPDATE confirmed_bookings
       SET agreement_signed_at = $1, agreement_signature = $2, agreement_ip = $3
       WHERE conf_id = $4`,
      [signedAt, signature, ip, booking.confId]
    );
  } catch(e) {
    console.error('Signature DB write error:', e.message);
    return res.status(500).send('<h2>Could not save signature. Please call (480) 618-0805.</h2>');
  }

  // Generate and store signed PDF in background — dont block the response
  res.send(buildAgreementPage(booking, token, { signed: true }));

  // Background: generate PDF, upload to R2, send confirmation email
  setImmediate(async function() {
    const opCfg = operatorConfig(booking && booking.operator);
    const notifyList = (opCfg.notifyEmails && opCfg.notifyEmails.length) ? opCfg.notifyEmails : [process.env.OWNER_EMAIL];
    try {
      const pdf = await generateAgreementPdf(booking, signedAt, signature, ip);
      if (pdf) {
        const pdfKey = 'agreements/' + booking.confId + '-agreement.pdf';
        try {
          await uploadToR2(pdfKey, pdf, 'application/pdf');
          await pool.query(
            'UPDATE confirmed_bookings SET agreement_pdf_key = $1 WHERE conf_id = $2',
            [pdfKey, booking.confId]
          );
          console.log('Agreement PDF saved to R2: ' + pdfKey);
        } catch(e) { console.error('R2 agreement upload error:', e.message); }
      }
    } catch(e) { console.error('Agreement PDF background error:', e.message); }

    // Send owner notification
    try {
      const signedDate = new Date(signedAt).toLocaleString('en-US', { timeZone: 'America/Phoenix', dateStyle: 'medium', timeStyle: 'short' });
      const ownerHtml = '<div style="font-family:Arial,sans-serif;max-width:520px">'
        + '<h2 style="color:#1B2D52">Agreement Signed</h2>'
        + '<p><b>' + escapeHtml(booking.fullName) + '</b> has signed the inspection agreement.</p>'
        + '<p><b>Conf #:</b> ' + escapeHtml(booking.confId) + '<br>'
        + '<b>Property:</b> ' + escapeHtml(booking.address) + '<br>'
        + '<b>Inspection:</b> ' + escapeHtml(booking.dateFmt) + ' @ ' + escapeHtml(booking.time) + '<br>'
        + '<b>Signed:</b> ' + escapeHtml(signedDate) + ' (AZ)<br>'
        + '<b>Signature:</b> ' + escapeHtml(signature) + '<br>'
        + '<b>IP:</b> ' + escapeHtml(ip) + '</p>'
        + '</div>';
      for (const recip of notifyList) {
        if (!recip) continue;
        await sendEmail(recip, 'AGREEMENT SIGNED' + (opCfg.inspectorName !== 'Jaren Drummond' ? ' [' + opCfg.inspectorName + ']' : '') + ': ' + booking.fullName + ' [' + booking.confId + ']', ownerHtml);
      }
    } catch(e) { console.error('Owner agreement notification email:', e.message); }

    // Send client confirmation email
    try {
      const clientHtml = emailWrap(
        '<h2 style="color:#0F1C35">Agreement Signed</h2>'
        + '<p>Hi ' + escapeHtml((booking.buyer && booking.buyer.firstName) || 'there') + ', this confirms that you have signed the inspection agreement for:</p>'
        + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
        + '<tr><td style="padding:6px 0;color:#888;width:130px">Property</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(booking.address) + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Inspection Date</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(booking.dateFmt) + ' @ ' + escapeHtml(booking.time) + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Confirmation #</td><td style="color:#C9A84C;font-weight:700">' + escapeHtml(booking.confId) + '</td></tr>'
        + '</table>'
        + '<p>Your agreement has been recorded. You are all set for your inspection.</p>'
        + '<p>Questions? Call or text <strong>' + opCfg.phone + '</strong></p>'
      );
      await sendEmail(booking.buyer.email, 'Agreement Signed — ' + booking.confId, clientHtml, null, opCfg.replyTo);
    } catch(e) { console.error('Client agreement confirmation email:', e.message); }
  });
});

// API for inspector app to check agreement status before sending report
// Requires x-internal-key header matching INTERNAL_API_KEY env variable
app.get('/api/agreement-status/:confId', async function(req, res) {
  const key = req.headers['x-internal-key'];
  if (!key || key !== process.env.INTERNAL_API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const r = await pool.query(
      'SELECT agreement_signed_at, agreement_signature, agreement_pdf_key FROM confirmed_bookings WHERE conf_id = $1',
      [req.params.confId]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'Booking not found' });
    const row = r.rows[0];
    res.json({
      signed: !!row.agreement_signed_at,
      signedAt: row.agreement_signed_at || null,
      signature: row.agreement_signature || null,
      pdfKey: row.agreement_pdf_key || null,
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── CUSTOMER HUB ──────────────────────────────────────────────
// Single status page for the client. Linked from the booking confirmation
// email; replaces having to dig through separate emails for the agreement,
// reschedule form, and report. URL: /i/:token?s=:sig where token is the
// agreement token stored on confirmed_bookings.data.agreementToken.
app.get('/i/:token', agreementLimiter, async function(req, res) {
  const token = req.params.token;
  const sigCheck = verifySignedToken(token, req.query.s);
  if (!sigCheck.ok) {
    console.warn('Hub link rejected: ' + sigCheck.reason);
    return res.status(403).send('<h2 style="font-family:sans-serif;text-align:center;padding:60px 24px">Invalid or expired link. Check your email for the original message, or call (480) 618-0805.</h2>');
  }
  if (sigCheck.legacy) console.warn('Hub: accepting legacy unsigned token');

  let row;
  try {
    const r = await pool.query(
      `SELECT * FROM confirmed_bookings WHERE data->>'agreementToken' = $1 LIMIT 1`,
      [token]
    );
    row = r.rows[0];
  } catch(e) {
    console.error('Hub DB read error:', e.message);
    return res.status(500).send('<h2 style="font-family:sans-serif;text-align:center;padding:60px 24px">Database error. Please call (480) 618-0805.</h2>');
  }

  if (!row) {
    return res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Inspection — San Tan Property Inspections</title></head>
<body style="font-family:sans-serif;text-align:center;padding:60px 24px;background:#0F1C35;color:#fff">
<h2>This link has expired or is no longer valid.</h2>
<p style="margin-top:12px;color:#aaa">If you need to check on your inspection, please call (480) 618-0805.</p>
</body></html>`);
  }

  const booking = row.data || {};
  const reportInfo = await getReportInfoForConfId(booking.confId);

  res.send(buildHubPage(booking, row, reportInfo, token));
});

// ── CUSTOMER HUB: AGREEMENT PDF DOWNLOAD ──────────────────────
// Token-authed download of the signed inspection agreement. Same HMAC sig as
// the hub page. Returns the counter-signed (executed) PDF if available,
// otherwise the single-signed PDF. 404 if no agreement is on file.
//
// Why this exists: clients sometimes lose their original confirmation email
// and need their agreement again. Without this route they'd have to email
// Jaren to ask for a copy. The hub now self-serves it.
app.get('/i/:token/agreement.pdf', agreementLimiter, async function(req, res) {
  const token = req.params.token;
  const sigCheck = verifySignedToken(token, req.query.s);
  if (!sigCheck.ok) {
    console.warn('Hub agreement PDF link rejected: ' + sigCheck.reason);
    return res.status(403).send('Invalid or expired link.');
  }

  let row;
  try {
    const r = await pool.query(
      `SELECT conf_id, agreement_pdf_key, counter_signed_pdf_key, cancelled_at
         FROM confirmed_bookings WHERE data->>'agreementToken' = $1 LIMIT 1`,
      [token]
    );
    row = r.rows[0];
  } catch(e) {
    return res.status(500).send('Database error.');
  }
  if (!row || row.cancelled_at) return res.status(404).send('Not found.');

  // Prefer the counter-signed (executed) PDF — it's the most authoritative
  // version. Falls back to the buyer-signed-only PDF if counter-sign hasn't
  // happened yet.
  const key = row.counter_signed_pdf_key || row.agreement_pdf_key;
  const isExecuted = !!row.counter_signed_pdf_key;
  if (!key) return res.status(404).send('No signed agreement on file yet.');

  try {
    const pdf = await downloadFromR2(key);
    res.set('Content-Type', 'application/pdf');
    res.set('Content-Disposition', 'inline; filename="' + row.conf_id + '-' + (isExecuted ? 'executed-' : '') + 'agreement.pdf"');
    res.send(pdf);
  } catch(e) {
    console.error('Hub agreement PDF download error:', e.message);
    return res.status(500).send('Could not retrieve agreement. Please call (480) 618-0805.');
  }
});

// ── CUSTOMER HUB: REPORT PDF DOWNLOAD ─────────────────────────
// Token-authed download of the latest report PDF. Same token + sig as the
// hub page itself. Returns 404 if no report exists yet or it isn't delivered —
// we don't leak the existence of in-progress reports to the client side.
app.get('/i/:token/report.pdf', agreementLimiter, async function(req, res) {
  const token = req.params.token;
  const sigCheck = verifySignedToken(token, req.query.s);
  if (!sigCheck.ok) {
    console.warn('Hub PDF link rejected: ' + sigCheck.reason);
    return res.status(403).send('Invalid or expired link.');
  }

  let row;
  try {
    const r = await pool.query(
      `SELECT data, cancelled_at FROM confirmed_bookings WHERE data->>'agreementToken' = $1 LIMIT 1`,
      [token]
    );
    row = r.rows[0];
  } catch(e) {
    return res.status(500).send('Database error.');
  }
  if (!row || row.cancelled_at) return res.status(404).send('Not found.');

  const confId = row.data && row.data.confId;
  const info   = await getReportInfoForConfId(confId);

  // Only allow client downloads of *delivered* reports — keeps in-progress
  // and pending-but-unsent reports private to the inspector.
  if (info.state !== 'delivered' || !info.pdfKey) return res.status(404).send('Report not yet available.');

  try {
    const pdf = await downloadFromInspectorR2(info.pdfKey);
    res.set('Content-Type', 'application/pdf');
    res.set('Content-Disposition', 'inline; filename="' + (info.pdfFilename || 'report.pdf').replace(/[^a-zA-Z0-9._-]/g,'_') + '"');
    res.send(pdf);
  } catch(e) {
    console.error('Hub PDF download error:', e.message);
    // Generic user-facing message — clients shouldn't see internal config details.
    return res.status(500).send('Could not retrieve report. Please call (480) 618-0805.');
  }
});

// ── ADMIN: REPORT PDF STREAM ──────────────────────────────────
// Stream the latest report PDF for a confirmation ID. Used by the admin
// dashboard "View Report" button. Works for any report state — admin can
// peek at in-progress reports too.
app.get('/admin/report-pdf/:confId', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) {
    return res.redirect('/admin/login');
  }
  if (!(await roleCanTouchBooking(adminRole(req), req.params.confId))) return res.status(403).send('Forbidden');
  const info = await getReportInfoForConfId(req.params.confId);
  if (!info.id || !info.pdfKey) return res.status(404).send('No report PDF on file for ' + escapeHtml(req.params.confId) + '. The inspector may not have generated one yet.');
  try {
    const pdf = await downloadFromInspectorR2(info.pdfKey);
    res.set('Content-Type', 'application/pdf');
    res.set('Content-Disposition', 'inline; filename="' + (info.pdfFilename || 'report.pdf').replace(/[^a-zA-Z0-9._-]/g,'_') + '"');
    res.send(pdf);
  } catch(e) {
    console.error('Admin report PDF download error:', e.message);
    // Common failure modes — give the user actionable language rather than
    // pointing at the bucket variable, which is rarely the actual cause.
    let hint = '';
    const m = (e.message || '').toLowerCase();
    if (m.includes('credential')) {
      hint = 'R2 credentials issue. Check that R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY are set correctly on the website Railway service. If you set any INSPECTOR_R2_* override variables, try deleting them — the website and inspector usually share the same R2 account.';
    } else if (m.includes('no such key') || m.includes('notfound')) {
      hint = 'The PDF was not found in R2. The report may have been generated against a different bucket. If the inspector uses a different bucket, set INSPECTOR_R2_BUCKET_NAME on the website service.';
    } else if (m.includes('bucket')) {
      hint = 'Bucket access issue. If the inspector uses a different bucket than the website, set INSPECTOR_R2_BUCKET_NAME on the website service.';
    } else {
      hint = 'See the website service logs in Railway for the full error.';
    }
    return res.status(500).send('R2 fetch failed: ' + escapeHtml(e.message) + '. ' + hint);
  }
});

// ── ADMIN: REPORT INFO LOOKUP ─────────────────────────────────
// Lightweight JSON endpoint used by the admin dashboard JS to decide which
// "View Report" / "Edit in Inspector" buttons to render per row. We don't
// want to widen /admin/data with a per-row LEFT JOIN on reports (it would
// run on every dashboard refresh), so the dashboard fetches this once and
// caches the result for the session.
app.get('/admin/reports-map', adminActionLimiter, async function(req, res) {
  const role = adminRole(req);
  if (!role) return res.status(401).json({ error: 'Unauthorized' });
  const scoped = role !== 'jaren';
  try {
    const r = await pool.query(
      `SELECT id,
              report_data->>'confId' AS conf_id,
              report_data->>'pdf_r2_key' AS pdf_key,
              report_data->>'report_sent_at' AS sent_at,
              status,
              report_version
         FROM reports
        WHERE report_data->>'confId' IS NOT NULL` + (scoped ? " AND operator = 'jeff'" : '')
    );
    // Build a confId-keyed map. If a confId somehow has multiple report rows
    // (shouldn't happen but defensive), keep the latest by version then status.
    const map = {};
    for (const row of r.rows) {
      const existing = map[row.conf_id];
      const candidate = {
        id: row.id,
        hasPdf: !!row.pdf_key,
        delivered: !!row.sent_at,
        status: row.status,
        version: row.report_version || 1,
      };
      if (!existing || candidate.version > existing.version) {
        map[row.conf_id] = candidate;
      }
    }
    res.json({ reports: map });
  } catch(e) {
    // Table may not exist (fresh deploy before inspector schema is set up).
    // Return empty so the dashboard renders fine with no report buttons.
    res.json({ reports: {} });
  }
});

// ── CANCEL BOOKING ────────────────────────────────────────────
app.get('/cancel/:token', async function(req, res) {
  const sigCheck = verifySignedToken(req.params.token, req.query.s);
  if (!sigCheck.ok) {
    console.warn('Cancel link rejected: ' + sigCheck.reason);
    return res.status(403).send('<h2>Invalid or expired link. Check your email for the original confirmation message, or call (480) 618-0805.</h2>');
  }
  if (sigCheck.legacy) console.warn('Cancel: accepting legacy unsigned token (pre-HMAC migration)');

  let booking;
  try {
    booking = await dbGet(req.params.token);
  } catch(e) {
    return res.send('<h2>Database error. Please try again or call (480) 618-0805.</h2>');
  }
  if (!booking) return res.send('<h2>This link has expired or already been used.</h2>');
  const { confId, fullName, dateFmt, time } = booking;
  try { await dbDelete(req.params.token); } catch(e) { console.error('DB delete error:', e.message); }
  console.log('Booking cancelled: ' + confId);
  res.send('<div style="font-family:Arial,sans-serif;max-width:500px;margin:60px auto;text-align:center;padding:40px;border-top:4px solid #C0392B">'
    + '<h2 style="color:#C0392B">Booking Cancelled</h2>'
    + '<p>The booking for <b>' + fullName + '</b> on ' + dateFmt + ' @ ' + time + ' has been cancelled.</p>'
    + '<p>No texts were sent.</p>'
    + '</div>');
});

// ── GOOGLE AUTH ────────────────────────────────────────────────
app.get('/auth/google', function(req, res) {
  res.redirect(oAuth2Client.generateAuthUrl({ access_type:'offline', scope:['https://www.googleapis.com/auth/calendar'], prompt:'consent' }));
});
app.get('/auth/google/callback', async function(req, res) {
  const result = await oAuth2Client.getToken(req.query.code);
  console.log('REFRESH TOKEN:', result.tokens.refresh_token);
  res.send('<pre>Add to Railway as GOOGLE_REFRESH_TOKEN:\n\n' + result.tokens.refresh_token + '</pre>');
});

// ── CONTACT FORM ──────────────────────────────────────────────
app.post('/api/contact', rescheduleLimiter, async function(req, res) {
  let { name, phone, email, role, message } = req.body;
  if (!name || !email || !message) return res.status(400).json({ error: 'Name, email, and message are required.' });
  if (!isValidEmail(email)) return res.status(400).json({ error: 'Please enter a valid email address.' });
  if (phone && !isValidPhone(phone)) return res.status(400).json({ error: 'Please enter a valid phone number (or leave it blank).' });

  // Clip lengths
  name    = clip(name,    LEN.name);
  phone   = clip(phone,   LEN.phone);
  email   = clip(email,   LEN.email);
  message = clip(message, LEN.message);

  const roleLabel = {
    'buyer': 'Buyer / Homeowner',
    'agent': "Buyer's Agent",
    'seller': 'Seller / Listing Agent',
    'other': 'Other'
  }[role] || role || 'Not specified';

  const html = '<div style="font-family:Arial,sans-serif;max-width:520px">'
    + '<h2 style="color:#1B2D52">New Contact Form Submission</h2>'
    + '<p><b>Name:</b> ' + escapeHtml(name) + '</p>'
    + '<p><b>Role:</b> ' + escapeHtml(roleLabel) + '</p>'
    + (phone ? '<p><b>Phone:</b> ' + escapeHtml(phone) + '</p>' : '')
    + '<p><b>Email:</b> ' + escapeHtml(email) + '</p>'
    + '<p><b>Message:</b></p><p style="background:#FAF7F0;padding:12px;border-radius:6px;border-left:4px solid #C9A84C">' + escapeHtml(message) + '</p>'
    + '<p style="color:#888;font-size:.85rem;margin-top:16px">Reply directly to this email to respond to ' + escapeHtml(name) + '.</p>'
    + '</div>';

  try {
    await sendEmail(process.env.OWNER_EMAIL, 'CONTACT: ' + name + ' (' + roleLabel + ')', html);
  } catch(e) {
    console.error('Contact form email:', e.message);
    return res.status(500).json({ error: 'Could not send message. Please call or email directly.' });
  }

  sms(process.env.OWNER_PHONE, 'NEW CONTACT FORM\n' + name + ' (' + roleLabel + ')' + (phone ? '\n' + phone : '') + '\n' + email + '\n\n' + message).catch(function(e){ console.error('Contact SMS:', e.message); });

  res.json({ success: true });
});

// ── RESCHEDULE REQUEST ────────────────────────────────────────
app.post('/api/reschedule', rescheduleLimiter, async function(req, res) {
  let { confId, name, phone, email, message } = req.body;
  if (!name || !email) return res.status(400).json({ error: 'Name and email required.' });
  if (!isValidEmail(email)) return res.status(400).json({ error: 'Please enter a valid email address.' });
  if (phone && !isValidPhone(phone)) return res.status(400).json({ error: 'Please enter a valid phone number (or leave it blank).' });

  // Clip lengths to prevent DB bloat / oversized emails
  confId  = clip(confId,  LEN.code);
  name    = clip(name,    LEN.name);
  phone   = clip(phone,   LEN.phone);
  email   = clip(email,   LEN.email);
  message = clip(message, LEN.message);

  try {
    await pool.query(
      'INSERT INTO reschedule_requests (conf_id, name, phone, email, message) VALUES ($1,$2,$3,$4,$5)',
      [confId||null, name, phone||null, email, message||null]
    );
  } catch(e) { console.error('Reschedule DB:', e.message); }

  const rHtml = '<div style="font-family:Arial,sans-serif;max-width:520px">'
    + '<h2 style="color:#1B2D52">Reschedule Request</h2>'
    + '<p><b>From:</b> ' + escapeHtml(name) + '</p>'
    + (confId ? '<p><b>Conf #:</b> ' + escapeHtml(confId) + '</p>' : '')
    + (phone ? '<p><b>Phone:</b> ' + escapeHtml(phone) + '</p>' : '')
    + '<p><b>Email:</b> ' + escapeHtml(email) + '</p>'
    + (message ? '<p><b>Message:</b> ' + escapeHtml(message) + '</p>' : '')
    + '</div>';

  try {
    await sendEmail(process.env.OWNER_EMAIL, 'RESCHEDULE REQUEST: ' + name + (confId ? ' [' + confId + ']' : ''), rHtml);
  } catch(e) { console.error('Reschedule email:', e.message); }

  res.json({ success: true });
});

// ── ADMIN DASHBOARD ───────────────────────────────────────────
// Read from env. Fallback to legacy literal so existing deploys don't lock out
// before the env var is set. After confirming ADMIN_PASSWORD is set in Railway,
// remove the fallback in a follow-up commit (and rotate the password).
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'monroe';
if (!process.env.ADMIN_PASSWORD) {
  console.warn('⚠️  ADMIN_PASSWORD env var not set — using legacy fallback. Set the env var and remove the fallback ASAP.');
}
// Jeff's separate admin password. When set, logging in with it scopes the admin
// to operator='jeff' only. If unset, Jeff simply has no login (Jaren still sees all).
const ADMIN_PASSWORD_JEFF = process.env.ADMIN_PASSWORD_JEFF || null;

// Constant-time string compare (equal-length guard + timingSafeEqual).
function safeEq(input, secret) {
  if (input == null || secret == null) return false;
  const a = Buffer.from(input);
  const b = Buffer.from(secret);
  if (a.length !== b.length) return false;
  try { return require('crypto').timingSafeEqual(a, b); } catch (_) { return false; }
}

// Returns the operator role of the authenticated admin: 'jaren' (full access,
// sees every operator), 'jeff' (scoped to his own data), or null (unauthorized).
// Reads the signed session cookie set at login. (Replaces the old Basic Auth
// header approach — the styled /admin/login page issues the cookie.)
function adminRole(req) {
  return readAdminCookie(getCookie(req, ADMIN_COOKIE));
}
// Given a submitted password, return the operator it authenticates as, or null.
function passwordRole(pass) {
  if (pass == null) return null;
  if (safeEq(pass, ADMIN_PASSWORD)) return 'jaren';
  if (ADMIN_PASSWORD_JEFF && safeEq(pass, ADMIN_PASSWORD_JEFF)) return 'jeff';
  return null;
}

function checkAdmin(req) {
  return adminRole(req) !== null;
}

// Authorize a role to act on a specific booking. Jaren may touch any booking;
// Jeff may only touch bookings where operator='jeff'. Returns true/false.
async function roleCanTouchBooking(role, confId) {
  if (role === 'jaren') return true;
  if (role !== 'jeff') return false;
  try {
    const r = await pool.query('SELECT operator FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    if (!r.rows.length) return false;
    return getOperator(r.rows[0].operator) === 'jeff';
  } catch (_) { return false; }
}

// ── ADMIN LOGIN (styled, session-cookie based) ────────────────
// Replaces the old browser Basic Auth popup with a real login page.
// Same password(s) as before: ADMIN_PASSWORD → jaren (sees all),
// ADMIN_PASSWORD_JEFF → jeff (scoped). On success we set a signed cookie.
app.get('/admin/login', adminAuthLimiter, function(req, res) {
  // Already logged in? Skip straight to the dashboard.
  if (adminRole(req)) return res.redirect('/admin');
  const err = req.query.e ? '<div class="err">Incorrect password. Please try again.</div>' : '';
  res.set('Content-Type', 'text/html; charset=utf-8');
  res.send(`<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>San Tan Admin — Sign In</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    background:radial-gradient(900px 500px at 50% -10%,#1a2f56 0%,#0F1C35 60%,#0a1426 100%);
    min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px}
  .box{width:100%;max-width:360px;background:#fffdf8;border-radius:16px;padding:32px 28px;
    box-shadow:0 24px 60px -18px rgba(0,0,0,.6);text-align:center}
  .brand{font-family:Georgia,serif;font-weight:700;color:#C9A84C;letter-spacing:.18em;font-size:1rem;text-transform:uppercase}
  .sub{color:#8a8678;font-size:.62rem;letter-spacing:.4em;text-transform:uppercase;margin-top:3px;margin-bottom:24px}
  h1{font-size:1.15rem;color:#0F1C35;margin-bottom:4px}
  p.note{color:#7a7a7a;font-size:.82rem;margin-bottom:20px}
  label{display:block;text-align:left;font-size:.78rem;font-weight:600;color:#445;margin-bottom:6px}
  input{width:100%;padding:12px 13px;border:1px solid #d4cdbf;border-radius:9px;font-size:1rem;background:#fff}
  input:focus{outline:none;border-color:#C9A84C;box-shadow:0 0 0 3px rgba(201,168,76,.18)}
  button{width:100%;margin-top:16px;background:linear-gradient(180deg,#E8C97A,#C9A84C);color:#0F1C35;
    font-weight:700;font-size:1rem;padding:13px;border:none;border-radius:9px;cursor:pointer}
  button:hover{filter:brightness(1.04)}
  .err{background:#FDECEA;color:#922;border:1px solid #f5c6cb;border-radius:8px;padding:10px;font-size:.85rem;margin-bottom:16px}
</style></head>
<body>
  <form class="box" method="POST" action="/admin/login">
    <div class="brand">San Tan Property</div>
    <div class="sub">Inspections</div>
    <h1>Admin Sign In</h1>
    <p class="note">Enter your password to continue.</p>
    ${err}
    <label for="pw">Password</label>
    <input id="pw" name="password" type="password" autocomplete="current-password" autofocus required>
    <button type="submit">Sign In</button>
  </form>
</body></html>`);
});

app.post('/admin/login', adminAuthLimiter, function(req, res) {
  const role = passwordRole((req.body && req.body.password) || '');
  if (!role) return res.redirect('/admin/login?e=1');
  res.cookie(ADMIN_COOKIE, makeAdminCookie(role), {
    httpOnly: true,
    secure: true,
    sameSite: 'lax',
    maxAge: ADMIN_SESSION_MS,
    path: '/',
  });
  res.redirect('/admin');
});

app.get('/admin/logout', function(req, res) {
  res.clearCookie(ADMIN_COOKIE, { path: '/' });
  res.redirect('/admin/login');
});

app.get('/admin', adminAuthLimiter, function(req, res) {
  if (!checkAdmin(req)) return res.redirect('/admin/login');

  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>San Tan Admin</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0F1C35;color:#BEC8D8;min-height:100vh;}
nav{background:#0a1428;border-bottom:2px solid #C9A84C;padding:14px 28px;display:flex;align-items:center;justify-content:space-between;}
nav h1{font-size:1rem;font-weight:700;color:#C9A84C;letter-spacing:1px;text-transform:uppercase;}
nav span{font-size:.78rem;color:#4A5A7A;}
.wrap{max-width:1100px;margin:0 auto;padding:28px 20px;}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:28px;}
.stat{background:#1B2D52;border-radius:10px;padding:18px 20px;}
.stat .lbl{font-size:.72rem;text-transform:uppercase;letter-spacing:1px;color:#4A5A7A;margin-bottom:6px;}
.stat .val{font-size:1.7rem;font-weight:700;color:#C9A84C;}
.stat .sub{font-size:.75rem;color:#4A5A7A;margin-top:3px;}
.card{background:#1B2D52;border-radius:10px;overflow:hidden;margin-bottom:20px;}
.card-hd{padding:14px 20px;border-bottom:1px solid #243660;display:flex;align-items:center;justify-content:space-between;}
.card-hd h2{font-size:.85rem;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#C9A84C;}
.card-hd span{font-size:.75rem;color:#4A5A7A;}
table{width:100%;border-collapse:collapse;}
th{padding:10px 16px;text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:1px;color:#4A5A7A;border-bottom:1px solid #243660;white-space:nowrap;}
td{padding:12px 16px;font-size:.83rem;border-bottom:1px solid #162240;vertical-align:top;}
tr:last-child td{border-bottom:none;}
tr:hover td{background:rgba(201,168,76,.04);}
.conf{color:#C9A84C;font-weight:700;font-size:.75rem;}
.name{color:#fff;font-weight:600;}
.addr{color:#8A9AB5;font-size:.78rem;margin-top:2px;}
.svc{font-size:.75rem;color:#8A9AB5;}
.price{color:#C9A84C;font-weight:700;}
.agent{font-size:.78rem;color:#8A9AB5;}
.empty{padding:32px;text-align:center;color:#4A5A7A;font-size:.85rem;}
.badge{display:inline-block;font-size:.65rem;font-weight:700;padding:2px 7px;border-radius:10px;text-transform:uppercase;letter-spacing:.5px;}
.badge-disc{background:rgba(26,180,100,.15);color:#1ab464;}
.badge-trip{background:rgba(201,168,76,.15);color:#C9A84C;}
.badge-signed{background:rgba(26,180,100,.15);color:#1ab464;}
.badge-unsigned{background:rgba(192,57,43,.15);color:#e8a87c;}
.resc-msg{font-size:.78rem;color:#8A9AB5;margin-top:3px;font-style:italic;}
@media(max-width:600px){th:nth-child(4),td:nth-child(4),th:nth-child(5),td:nth-child(5){display:none;}}
</style>
</head>
<body>
<nav>
  <h1>San Tan Property Inspections — Admin</h1>
  <span style="display:flex;align-items:center;gap:14px"><span id="lastRefresh"></span><a href="/admin/logout" style="color:#C9A84C;font-size:.78rem;text-decoration:none;border:1px solid #C9A84C;padding:5px 12px;border-radius:6px">Sign Out</a></span>
</nav>
<div class="wrap">
  <div class="stats" id="stats"><div class="stat"><div class="lbl">Loading...</div><div class="val">—</div></div></div>
  <div style="display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap;">
    <a href="/admin/csv" style="background:#C9A84C;color:#0F1C35;font-weight:700;font-size:.82rem;padding:10px 20px;border-radius:8px;text-decoration:none;display:inline-block;">Export CSV</a>
  </div>

  <div class="card" style="margin-bottom:20px;border-left:4px solid #C9A84C">
    <div class="card-hd"><h2>Business Health</h2><span style="font-size:.7rem;font-weight:400;color:#8A9AB5;text-transform:none;letter-spacing:0">Last 30 days</span></div>
    <div id="healthBody" style="padding:18px 20px"><div class="empty">Loading...</div></div>
  </div>

  <div class="card" style="margin-bottom:20px" id="codesCard">
    <div class="card-hd"><h2>Discount Codes</h2></div>
    <div style="padding:16px 20px;border-bottom:1px solid #243660;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <input id="newCode" placeholder="Code (e.g. AGENT15)" style="background:#243660;border:1px solid #344870;color:#fff;padding:8px 12px;border-radius:6px;font-size:.83rem;width:160px;outline:none"/>
      <input id="newPct" placeholder="% off" type="number" min="1" max="100" style="background:#243660;border:1px solid #344870;color:#fff;padding:8px 12px;border-radius:6px;font-size:.83rem;width:80px;outline:none"/>
      <button onclick="addCode()" style="background:#C9A84C;color:#0F1C35;border:none;border-radius:6px;padding:8px 18px;font-weight:700;font-size:.83rem;cursor:pointer">Add Code</button>
    </div>
    <div id="codeList"><div class="empty">Loading...</div></div>
  </div>

  <div class="card" style="border-left:4px solid #e8a87c">
    <div class="card-hd">
      <h2>Pending Bookings <span style="font-size:.7rem;font-weight:400;color:#8A9AB5;text-transform:none;letter-spacing:0">(awaiting your CONFIRM tap)</span></h2>
      <div style="display:flex;gap:8px;align-items:center"><span id="pendingCount">—</span><button onclick="clearAllPending()" style="background:#C0392B;color:#fff;border:none;border-radius:5px;padding:4px 10px;font-size:.7rem;cursor:pointer">Clear All</button></div>
    </div>
    <div id="pendingTable"><div class="empty">Loading...</div></div>
  </div>

  <div class="card">
    <div class="card-hd"><h2>Confirmed Bookings</h2><span id="bookingCount">—</span></div>
    <div id="bookingTable"><div class="empty">Loading...</div></div>
  </div>
  <div class="card">
    <div class="card-hd"><h2>Reschedule Requests</h2><span id="rescheduleCount">—</span></div>
    <div id="rescheduleTable"><div class="empty">Loading...</div></div>
  </div>
</div>
<script>
// XSS guard — wraps any user-controlled string before it goes into innerHTML.
// Booking fields (name, address, agent info, notes, etc.) come from the public
// form and could contain malicious HTML. Always pass through esc() before injecting.
function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function load() {
  try {
    // Fetch dashboard data + reports map in parallel. The reports map is keyed
    // by confId and tells us which bookings have a report (and which are delivered),
    // so we can render View Report / Edit in Inspector buttons accordingly.
    const [r, rm] = await Promise.all([
      fetch('/admin/data'),
      fetch('/admin/reports-map').catch(function(){ return null; }),
    ]);
    // Session expired or not logged in → bounce to the login page.
    if (r.status === 401) { window.location.href = '/admin/login'; return; }
    const d = await r.json();
    // Scoped-view indicator: when Jeff is logged in, make clear he's seeing only his data.
    if (d.role && d.role !== 'jaren') {
      var navH1 = document.querySelector('nav h1');
      if (navH1) navH1.textContent = 'San Tan — ' + (d.role.charAt(0).toUpperCase() + d.role.slice(1)) + "'s Inspections";
      var cc = document.getElementById('codesCard');
      if (cc) cc.style.display = 'none';
    }
    const reportsMap = (rm && rm.ok) ? (await rm.json()).reports || {} : {};
    const INSPECTOR_URL = ${JSON.stringify(process.env.INSPECTOR_URL || '')};
    // Default empty = link goes straight to INSPECTOR_URL home. Set to e.g.
    // "/?edit={id}" or "/#/reports/{id}" later if the inspector adds a deep-link handler.
    const INSPECTOR_EDIT_PATH_TPL = ${JSON.stringify(process.env.INSPECTOR_EDIT_PATH_TPL || '')};

    const totalRev = d.bookings.filter(function(b){ return b.paid_at; }).reduce(function(s,b){ return s + (b.data.finalPrice||0); }, 0);
    const thisMonth = new Date(); thisMonth.setDate(1); thisMonth.setHours(0,0,0,0);
    const monthJobs = d.bookings.filter(function(b){ return new Date(b.confirmed_at) >= thisMonth; }).length;
    const monthRev  = d.bookings.filter(function(b){ return b.paid_at && new Date(b.confirmed_at) >= thisMonth; }).reduce(function(s,b){ return s+(b.data.finalPrice||0);},0);
    const agentCount = {};
    d.bookings.forEach(function(b){ const n=b.data.buyerAgent&&b.data.buyerAgent.name?b.data.buyerAgent.name:'Unknown'; agentCount[n]=(agentCount[n]||0)+1; });
    const topAgent = Object.entries(agentCount).sort(function(a,b){return b[1]-a[1];})[0];
    const paidRev = d.bookings.filter(function(b){ return b.paid_at; }).reduce(function(s,b){ return s+(b.data.finalPrice||0);},0);
    const unpaidCount = d.bookings.filter(function(b){ return !b.paid_at && !b.cancelled_at; }).length;
    const unsignedCount = d.bookings.filter(function(b){ return !b.agreement_signed_at && !b.cancelled_at; }).length;

    document.getElementById('stats').innerHTML =
      '<div class="stat"><div class="lbl">Total Jobs</div><div class="val">'+d.bookings.length+'</div><div class="sub">all time</div></div>' +
      '<div class="stat"><div class="lbl">Total Collected</div><div class="val">$'+totalRev.toLocaleString()+'</div><div class="sub">paid jobs only</div></div>' +
      '<div class="stat"><div class="lbl">Collected</div><div class="val" style="color:#1ab464">$'+paidRev.toLocaleString()+'</div><div class="sub">'+d.bookings.filter(function(b){return b.paid_at;}).length+' jobs paid</div></div>' +
      '<div class="stat"><div class="lbl">This Month</div><div class="val">'+monthJobs+'</div><div class="sub">$'+monthRev.toLocaleString()+' collected</div></div>' +
      '<div class="stat"><div class="lbl">Top Agent</div><div class="val" style="font-size:1rem;padding-top:4px">'+(topAgent?topAgent[0]:'—')+'</div><div class="sub">'+(topAgent?topAgent[1]+' booking'+(topAgent[1]>1?'s':''):'')+'</div></div>' +
      '<div class="stat"><div class="lbl">Awaiting Payment</div><div class="val" style="color:'+(unpaidCount>0?'#e8a87c':'#C9A84C')+'">'+unpaidCount+'</div><div class="sub">unconfirmed</div></div>' +
      '<div class="stat"><div class="lbl">Unsigned Agreements</div><div class="val" style="color:'+(unsignedCount>0?'#e8a87c':'#1ab464')+'">'+unsignedCount+'</div><div class="sub">pending signature</div></div>' +
      '<div class="stat"><div class="lbl">Reschedule Requests</div><div class="val" style="color:'+(d.reschedules.length>0?'#e8a87c':'#C9A84C')+'">'+d.reschedules.length+'</div><div class="sub">open requests</div></div>' +
      '<div class="stat"><div class="lbl">Miles This Month</div><div class="val">'+(d.mileage&&d.mileage.monthMiles?Number(d.mileage.monthMiles).toFixed(1):'0')+'</div><div class="sub">YTD '+(d.mileage&&d.mileage.ytdMiles?Number(d.mileage.ytdMiles).toFixed(0):'0')+' &nbsp;<a href="/admin/mileage-csv?from='+(new Date().getFullYear())+'-01-01&to='+(new Date().getFullYear())+'-12-31" style="color:#C9A84C;text-decoration:underline;font-size:.7rem">CSV</a></div></div>' +
      ((d.role === 'jaren' && d.jeffOwes && d.jeffOwes.count > 0)
        ? '<div class="stat"><div class="lbl">Jeff Owes</div><div class="val" style="color:#C9A84C">$'+d.jeffOwes.total.toLocaleString()+'</div><div class="sub">'+d.jeffOwes.count+' inspection'+(d.jeffOwes.count!==1?'s':'')+' &times; $'+d.jeffOwes.rate+'</div></div>'
        : '');

    renderCodes(d.codes || []);
    renderPending(d.pending || []);
    renderHealth(d.bookings || [], d.reschedules || []);

    document.getElementById('bookingCount').textContent = d.bookings.length + ' total';
    if (!d.bookings.length) {
      document.getElementById('bookingTable').innerHTML = '<div class="empty">No confirmed bookings yet.</div>';
    } else {
      const rows = d.bookings.slice().reverse().map(function(b) {
        const bd = b.data;
        const dt = new Date(b.confirmed_at).toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'});
        const discBadge = bd.discountCode ? '<span class="badge badge-disc">'+bd.discountCode+'</span> ' : '';
        const tripBadge = bd.tripCharge&&bd.tripCharge.apply ? '<span class="badge badge-trip">trip</span> ' : '';
        const addons = bd.addons&&bd.addons.length ? bd.addons.join(', ') : '—';
        const isPaid = !!b.paid_at;
        const isCancelled = !!b.cancelled_at;
        const isSigned = !!b.agreement_signed_at;
        const isCounterSigned = !!b.counter_signed_at;
        const payMethod = b.payment_method || '';
        const signedBadge = isCancelled ? '' : (isSigned
          ? (isCounterSigned
              ? '<span class="badge badge-signed" style="background:#1a8c52">Executed</span>'
              : '<span class="badge badge-signed">Signed</span>')
          : '<span class="badge badge-unsigned">Unsigned</span>');
        const pdfLink = (isSigned && b.agreement_pdf_key)
          ? '<a href="/admin/agreement-pdf/'+bd.confId+'" target="_blank" style="display:inline-block;background:#243660;color:#C9A84C;border:1px solid #344870;border-radius:5px;padding:4px 10px;font-size:.72rem;text-decoration:none;margin-left:4px">View PDF</a>'
          : '';
        // Counter-sign UI: button if signed-not-yet-countersigned, link if already countersigned
        const counterSignUi = isCancelled ? '' : (
          isSigned && !isCounterSigned
            ? '<button data-action="countersign" data-id="'+bd.confId+'" style="display:inline-block;background:#C9A84C;color:#1B2D52;border:1px solid #C9A84C;border-radius:5px;padding:4px 10px;font-size:.72rem;font-weight:700;cursor:pointer;margin-left:4px">Counter-Sign</button>'
            : (isCounterSigned
                ? '<a href="/admin/executed-pdf/'+bd.confId+'" target="_blank" style="display:inline-block;background:#1a8c52;color:white;border:1px solid #1a8c52;border-radius:5px;padding:4px 10px;font-size:.72rem;text-decoration:none;margin-left:4px">View Executed</a>'
                : '')
        );
        // Payment method dropdown — selecting a method auto-marks paid
        const payOptions = ['cash','card','venmo','zelle']
          .map(function(m){ return '<option value="'+m+'"'+(payMethod===m?' selected':'')+'>'+m.charAt(0).toUpperCase()+m.slice(1)+'</option>'; })
          .join('');
        const payDropdown = isCancelled
          ? '<span style="color:#C0392B;font-size:.72rem;font-weight:700">CANCELLED</span>'
          : '<select data-action="setpay" data-id="'+bd.confId+'" style="background:'+(isPaid?'#1ab464':'#243660')+';color:'+(isPaid?'white':'#8A9AB5')+';border:1px solid '+(isPaid?'#1ab464':'#344870')+';border-radius:5px;padding:4px 8px;font-size:.72rem;margin-top:4px;cursor:pointer;font-weight:'+(isPaid?'700':'400')+'"><option value="">'+(isPaid?'Paid — change?':'Mark paid…')+'</option>'+payOptions+(isPaid?'<option value="__unpaid">— Unpaid</option>':'')+'</select>';
        const cancelBtn = isCancelled
          ? '<button data-action="hard-delete" data-id="'+bd.confId+'" style="background:none;color:#C0392B;border:1px solid #C0392B;border-radius:5px;padding:4px 10px;cursor:pointer;font-size:.72rem;margin-top:4px;margin-left:4px">Delete</button>'
          : '<button data-action="cancel" data-id="'+bd.confId+'" style="background:none;color:#C0392B;border:1px solid #C0392B;border-radius:5px;padding:4px 10px;cursor:pointer;font-size:.72rem;margin-top:4px;margin-left:4px">Cancel</button>';
        const editLink = '<a href="/admin/booking/'+encodeURIComponent(bd.confId)+'" target="_blank" style="background:none;color:#C9A84C;border:1px solid #C9A84C;border-radius:5px;padding:4px 10px;cursor:pointer;font-size:.72rem;margin-top:4px;margin-left:4px;text-decoration:none;display:inline-block">Edit</a>';

        // Report-related buttons. These only render when reportsMap has an
        // entry for this confId — i.e. the inspector has at least started a
        // report. View Report needs a PDF on file; Edit in Inspector needs
        // INSPECTOR_URL configured. Each button is mutually independent.
        const ri = reportsMap[bd.confId];
        let reportBtns = '';
        if (ri) {
          if (ri.hasPdf) {
            const viewLabel = ri.delivered
              ? 'View Report' + (ri.version > 1 ? ' v' + ri.version : '')
              : 'Preview Report (Draft)';
            reportBtns += '<a href="/admin/report-pdf/'+encodeURIComponent(bd.confId)+'" target="_blank" style="background:'+(ri.delivered?'#1ab464':'#243660')+';color:'+(ri.delivered?'#fff':'#C9A84C')+';border:1px solid '+(ri.delivered?'#1ab464':'#C9A84C')+';border-radius:5px;padding:4px 10px;cursor:pointer;font-size:.72rem;margin-top:4px;margin-left:4px;text-decoration:none;display:inline-block;font-weight:'+(ri.delivered?'700':'400')+'">'+viewLabel+'</a>';
          }
          if (INSPECTOR_URL && ri.id) {
            const editUrl = INSPECTOR_URL.replace(/\\/$/, '') + INSPECTOR_EDIT_PATH_TPL.replace('{id}', encodeURIComponent(ri.id));
            reportBtns += '<a href="'+editUrl+'" target="_blank" style="background:none;color:#8A9AB5;border:1px solid #344870;border-radius:5px;padding:4px 10px;cursor:pointer;font-size:.72rem;margin-top:4px;margin-left:4px;text-decoration:none;display:inline-block">Edit in Inspector</a>';
          }
        }
        return '<tr>' +
          '<td><div class="conf">'+esc(bd.confId)+'</div><div style="font-size:.72rem;color:#4A5A7A;margin-top:2px">'+dt+'</div></td>' +
          '<td><div class="name">'+esc(bd.fullName||'')+'</div><div class="addr">'+esc(bd.address||'')+'</div></td>' +
          '<td><div class="svc">'+esc(bd.svcLabel||'')+'</div><div class="svc" style="margin-top:2px">'+esc(addons)+'</div></td>' +
          '<td><div class="agent">'+esc(bd.buyerAgent&&bd.buyerAgent.name?bd.buyerAgent.name:'—')+'</div><div class="svc">'+esc(bd.buyerAgent&&bd.buyerAgent.brokerage?bd.buyerAgent.brokerage:'')+'</div></td>' +
          '<td><div class="price">'+discBadge+tripBadge+'$'+(bd.finalPrice||'—')+(b.miles!=null?' <span class="badge" style="background:#243660;color:#8A9AB5;border:1px solid #344870;font-weight:600">↔ '+Number(b.miles).toFixed(1)+' mi</span>':'')+'</div><div style="font-size:.72rem;color:#4A5A7A">'+esc(bd.dateFmt||'')+' @ '+esc(bd.time||'')+'</div><div style="margin-top:4px">'+signedBadge+pdfLink+counterSignUi+'</div><div>'+payDropdown+editLink+cancelBtn+'</div>'+(reportBtns?'<div style="margin-top:2px">'+reportBtns+'</div>':'')+'</td>' +
          '</tr>';
      }).join('');
      document.getElementById('bookingTable').innerHTML = '<table><thead><tr><th>Conf #</th><th>Buyer / Address</th><th>Service</th><th>Agent</th><th>Total / Date / Status</th></tr></thead><tbody>'+rows+'</tbody></table>';
    }

    document.getElementById('rescheduleCount').textContent = d.reschedules.length + ' total';
    if (!d.reschedules.length) {
      document.getElementById('rescheduleTable').innerHTML = '<div class="empty">No reschedule requests.</div>';
    } else {
      const rrows = d.reschedules.slice().reverse().map(function(r) {
        const dt = new Date(r.requested_at).toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric',hour:'numeric',minute:'2-digit'});
        return '<tr>' +
          '<td><div class="name">'+esc(r.name)+'</div><div class="svc">'+dt+'</div></td>' +
          '<td>'+(r.conf_id?'<span class="conf">'+esc(r.conf_id)+'</span>':'—')+'</td>' +
          '<td><div class="svc">'+esc(r.email||'—')+'</div><div class="svc">'+esc(r.phone||'—')+'</div></td>' +
          '<td><div class="resc-msg">'+esc(r.message||'No message provided.')+'</div></td>' +
          '</tr>';
      }).join('');
      document.getElementById('rescheduleTable').innerHTML = '<table><thead><tr><th>Name</th><th>Conf #</th><th>Contact</th><th>Message</th></tr></thead><tbody>'+rrows+'</tbody></table>';
    }

    document.getElementById('lastRefresh').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch(e) { console.error(e); }
}

async function cancelBooking(confId) {
  if (!confirm('Cancel booking ' + confId + '? This will delete the calendar event.')) return;
  // Second prompt: notify clients or silent cancel?
  //   OK     → send cancellation emails to buyer + buyer's agent
  //   Cancel → silent cancel, no emails, no SMS, no notifications
  // Use this silent option for test bookings, mistakes you made, or
  // cancellations the client already knows about through another channel.
  const notify = confirm('Notify the buyer and agent by email?\\n\\nOK = send cancellation emails\\nCancel = silent cancel (no email)');
  const r = await fetch('/admin/cancel-booking', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({confId, silent: !notify})
  });
  const data = await r.json();
  if (data.success) {
    alert(notify ? 'Booking cancelled. Cancellation emails sent.' : 'Booking cancelled silently — no notifications were sent.');
    load();
  } else {
    alert('Error: ' + (data.error||'Unknown error'));
  }
}

async function markPaid(confId) {
  await fetch('/admin/mark-paid', { method:'POST', headers:{'Content-Type':'application/json'/* basic auth auto-sent by browser */}, body: JSON.stringify({confId}) });
  load();
}
async function markUnpaid(confId) {
  if (!confirm('Mark as unpaid?')) return;
  await fetch('/admin/mark-unpaid', { method:'POST', headers:{'Content-Type':'application/json'/* basic auth auto-sent by browser */}, body: JSON.stringify({confId}) });
  load();
}

function renderCodes(codes) {
  const el = document.getElementById('codeList');
  if (!codes.length) { el.innerHTML = '<div class="empty">No active codes.</div>'; return; }
  el.innerHTML = '<table><thead><tr><th>Code</th><th>Discount</th><th>Action</th></tr></thead><tbody>' +
    codes.map(function(c) {
      return '<tr><td><span class="conf">'+c.code+'</span></td><td class="price">'+c.pct+'% off</td>' +
        '<td><button data-action="deletecode" data-code="'+c.code+'" style="background:#C0392B;color:white;border:none;border-radius:5px;padding:5px 12px;cursor:pointer;font-size:.75rem">Remove</button></td></tr>';
    }).join('') + '</tbody></table>';
}

async function addCode() {
  const code = document.getElementById('newCode').value.trim().toUpperCase();
  const pct  = document.getElementById('newPct').value.trim();
  if (!code || !pct) { alert('Enter both a code and a percentage.'); return; }
  await fetch('/admin/codes/add', { method:'POST', headers:{'Content-Type':'application/json'/* basic auth auto-sent by browser */}, body: JSON.stringify({code,pct}) });
  document.getElementById('newCode').value='';
  document.getElementById('newPct').value='';
  load();
}

async function deleteCode(code) {
  if (!confirm('Remove code ' + code + '?')) return;
  await fetch('/admin/codes/delete', { method:'POST', headers:{'Content-Type':'application/json'/* basic auth auto-sent by browser */}, body: JSON.stringify({code}) });
  load();
}

// ─── BUSINESS HEALTH DASHBOARD ─────────────────────────────────
// Computes rolling 30-day metrics live from booking + reschedule data.
// All math is client-side so this stays in sync with the existing /admin/data endpoint.
function renderHealth(bookings, reschedules) {
  const el = document.getElementById('healthBody');
  if (!el) return;

  const NOW = Date.now();
  const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;
  const SIXTY_DAYS_MS  = 60 * 24 * 60 * 60 * 1000;

  const recent = bookings.filter(function(b){ return new Date(b.confirmed_at).getTime() >= NOW - THIRTY_DAYS_MS; });
  const prior  = bookings.filter(function(b){
    const t = new Date(b.confirmed_at).getTime();
    return t >= NOW - SIXTY_DAYS_MS && t < NOW - THIRTY_DAYS_MS;
  });

  // Drop cancelled from "active" math but include them in cancellation rate
  const recentActive    = recent.filter(function(b){ return !b.cancelled_at; });
  const recentCancelled = recent.filter(function(b){ return  b.cancelled_at; });

  const revenue       = recentActive.reduce(function(s,b){ return s + (b.data.finalPrice||0); }, 0);
  const collected     = recentActive.filter(function(b){return b.paid_at;}).reduce(function(s,b){return s+(b.data.finalPrice||0);},0);
  const avgTicket     = recentActive.length ? Math.round(revenue / recentActive.length) : 0;
  const cancelRate    = recent.length ? Math.round((recentCancelled.length / recent.length) * 100) : 0;
  const signedCount   = recentActive.filter(function(b){ return b.agreement_signed_at; }).length;
  const signedRate    = recentActive.length ? Math.round((signedCount / recentActive.length) * 100) : 0;
  const counterCount  = recentActive.filter(function(b){ return b.counter_signed_at; }).length;

  // Compare to prior 30d to show trend
  const priorActive   = prior.filter(function(b){ return !b.cancelled_at; });
  const trend = recentActive.length - priorActive.length;
  const trendStr = trend === 0 ? '' : (trend > 0 ? ' ▲' + trend + ' vs prior 30d' : ' ▼' + Math.abs(trend) + ' vs prior 30d');
  const trendColor = trend > 0 ? '#1ab464' : (trend < 0 ? '#e8a87c' : '#8A9AB5');

  // Average lead time (days from booking confirmation to inspection date)
  const leadTimes = recentActive.map(function(b){
    if (!b.data.date) return null;
    const inspDate = new Date(b.data.date + 'T12:00:00');
    const confDate = new Date(b.confirmed_at);
    return Math.round((inspDate.getTime() - confDate.getTime()) / (24 * 60 * 60 * 1000));
  }).filter(function(n){ return n !== null && n >= 0; });
  const avgLead = leadTimes.length ? Math.round(leadTimes.reduce(function(s,n){return s+n;}, 0) / leadTimes.length) : 0;

  // Service type breakdown
  const svcCount = {};
  recentActive.forEach(function(b){
    const svc = (b.data.svcLabel || 'Unknown').replace(/ Inspection$/, '');
    svcCount[svc] = (svcCount[svc] || 0) + 1;
  });
  const svcEntries = Object.entries(svcCount).sort(function(a,b){return b[1]-a[1];});

  // Top 5 referring agents
  const agentCount = {};
  recentActive.forEach(function(b){
    const ba = b.data.buyerAgent;
    const name = (ba && ba.name) ? ba.name : '(no agent)';
    if (!agentCount[name]) agentCount[name] = { count: 0, revenue: 0 };
    agentCount[name].count++;
    agentCount[name].revenue += (b.data.finalPrice || 0);
  });
  const topAgents = Object.entries(agentCount)
    .sort(function(a,b){ return b[1].count - a[1].count; })
    .slice(0, 5);

  // Build the markup. Two columns of stats on top, then service + agent breakdowns below.
  const recentReschedules = reschedules.filter(function(r){ return new Date(r.requested_at).getTime() >= NOW - THIRTY_DAYS_MS; }).length;

  const cardCss = 'background:#1a2541;border:1px solid #243660;border-radius:8px;padding:14px 16px;';
  const lblCss  = 'font-size:.7rem;color:#8A9AB5;text-transform:uppercase;letter-spacing:1.2px;margin-bottom:6px;';
  const valCss  = 'font-size:1.6rem;font-weight:700;color:#E8DEC4;';
  const subCss  = 'font-size:.74rem;color:#8A9AB5;margin-top:4px;';

  let html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:18px">';
  html += '<div style="'+cardCss+'"><div style="'+lblCss+'">Inspections</div><div style="'+valCss+'">'+recentActive.length+'</div><div style="'+subCss+';color:'+trendColor+'">'+trendStr+'</div></div>';
  html += '<div style="'+cardCss+'"><div style="'+lblCss+'">Revenue Booked</div><div style="'+valCss+';color:#C9A84C">$'+revenue.toLocaleString()+'</div><div style="'+subCss+'">$'+collected.toLocaleString()+' collected</div></div>';
  html += '<div style="'+cardCss+'"><div style="'+lblCss+'">Avg Ticket</div><div style="'+valCss+'">$'+avgTicket.toLocaleString()+'</div><div style="'+subCss+'">per inspection</div></div>';
  html += '<div style="'+cardCss+'"><div style="'+lblCss+'">Avg Lead Time</div><div style="'+valCss+'">'+avgLead+'<span style="font-size:.9rem;color:#8A9AB5;margin-left:4px">days</span></div><div style="'+subCss+'">booking → inspection</div></div>';
  html += '<div style="'+cardCss+'"><div style="'+lblCss+'">Signed Rate</div><div style="'+valCss+';color:'+(signedRate>=80?'#1ab464':signedRate>=60?'#C9A84C':'#e8a87c')+'">'+signedRate+'%</div><div style="'+subCss+'">'+signedCount+' of '+recentActive.length+' agreements</div></div>';
  html += '<div style="'+cardCss+'"><div style="'+lblCss+'">Counter-Signed</div><div style="'+valCss+';color:'+(counterCount===signedCount?'#1ab464':'#e8a87c')+'">'+counterCount+'</div><div style="'+subCss+'">of '+signedCount+' signed</div></div>';
  html += '<div style="'+cardCss+'"><div style="'+lblCss+'">Cancellation Rate</div><div style="'+valCss+';color:'+(cancelRate>10?'#e8a87c':'#1ab464')+'">'+cancelRate+'%</div><div style="'+subCss+'">'+recentCancelled.length+' cancelled</div></div>';
  html += '<div style="'+cardCss+'"><div style="'+lblCss+'">Reschedule Requests</div><div style="'+valCss+'">'+recentReschedules+'</div><div style="'+subCss+'">last 30 days</div></div>';
  html += '</div>';

  // Two-column section: services & agents
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">';

  // Services
  html += '<div style="'+cardCss+'"><div style="'+lblCss+';margin-bottom:12px">Service Mix</div>';
  if (!svcEntries.length) {
    html += '<div style="color:#8A9AB5;font-size:.85rem">No data.</div>';
  } else {
    svcEntries.forEach(function(entry){
      const name = entry[0];
      const count = entry[1];
      const pct = Math.round((count / recentActive.length) * 100);
      html += '<div style="margin-bottom:10px"><div style="display:flex;justify-content:space-between;font-size:.82rem;color:#E8DEC4;margin-bottom:3px"><span>'+name+'</span><span style="color:#8A9AB5">'+count+' &middot; '+pct+'%</span></div>';
      html += '<div style="height:6px;background:#0F1C35;border-radius:3px;overflow:hidden"><div style="width:'+pct+'%;height:100%;background:#C9A84C"></div></div></div>';
    });
  }
  html += '</div>';

  // Top agents
  html += '<div style="'+cardCss+'"><div style="'+lblCss+';margin-bottom:12px">Top Referring Agents</div>';
  if (!topAgents.length) {
    html += '<div style="color:#8A9AB5;font-size:.85rem">No data.</div>';
  } else {
    topAgents.forEach(function(entry, idx){
      const name = entry[0];
      const stats = entry[1];
      html += '<div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;'+(idx<topAgents.length-1?'border-bottom:1px solid #243660':'')+'">';
      html += '<div><div style="font-size:.85rem;color:#E8DEC4;font-weight:600">'+name+'</div><div style="font-size:.72rem;color:#8A9AB5">'+stats.count+' booking'+(stats.count>1?'s':'')+'</div></div>';
      html += '<div style="font-size:.85rem;color:#C9A84C;font-weight:700">$'+stats.revenue.toLocaleString()+'</div>';
      html += '</div>';
    });
  }
  html += '</div>';

  html += '</div>';

  // Empty state if absolutely no recent bookings
  if (!recent.length) {
    html = '<div class="empty">No bookings in the last 30 days.</div>';
  }

  el.innerHTML = html;
}

function renderPending(rows) {
  const el = document.getElementById('pendingTable');
  document.getElementById('pendingCount').textContent = rows.length + ' pending';
  if (!rows.length) { el.innerHTML = '<div class="empty">No pending bookings.</div>'; return; }
  // Build a simple table — pending bookings don't need all the columns confirmed ones do
  const rowsHtml = rows.map(function(r) {
    const d = r.data || {};
    const created = new Date(r.created_at);
    const ageMins = Math.floor((Date.now() - created.getTime()) / 60000);
    const ageStr = ageMins < 60 ? ageMins + 'm ago' : ageMins < 1440 ? Math.floor(ageMins/60) + 'h ago' : Math.floor(ageMins/1440) + 'd ago';
    return '<tr>' +
      '<td><div class="conf">' + esc(d.confId || '—') + '</div><div style="font-size:.72rem;color:#4A5A7A;margin-top:2px">' + ageStr + '</div></td>' +
      '<td><div class="name">' + esc(d.fullName || '') + '</div><div class="addr">' + esc(d.address || '') + '</div></td>' +
      '<td><div class="svc">' + esc(d.svcLabel || '') + '</div></td>' +
      '<td><div style="font-size:.85rem;color:#E8DEC4">' + esc(d.dateFmt || '') + '</div><div style="font-size:.72rem;color:#4A5A7A">@ ' + esc(d.time || '') + '</div></td>' +
      '<td><div class="price">$' + (d.finalPrice || '—') + '</div><button data-action="delete-pending" data-token="' + esc(r.token) + '" style="background:none;color:#C0392B;border:1px solid #C0392B;border-radius:5px;padding:4px 10px;cursor:pointer;font-size:.72rem;margin-top:4px">Delete</button></td>' +
      '</tr>';
  }).join('');
  el.innerHTML = '<table><thead><tr><th>Conf #</th><th>Customer / Address</th><th>Service</th><th>Date / Time</th><th>Price</th></tr></thead><tbody>' + rowsHtml + '</tbody></table>';
}

async function deletePending(token) {
  if (!confirm('Delete this pending booking? This frees the slot for other customers.')) return;
  const r = await fetch('/admin/delete-pending', { method:'POST', headers:{'Content-Type':'application/json'/* basic auth auto-sent by browser */}, body: JSON.stringify({token}) });
  const data = await r.json();
  if (data.success) { load(); }
  else { alert('Error: ' + (data.error || 'Unknown')); }
}

async function clearAllPending() {
  if (!confirm('Clear ALL pending bookings? This frees every locked slot from bookings that you never tapped CONFIRM on. Use this for cleaning up after testing.')) return;
  const r = await fetch('/admin/clear-all-pending', { method:'POST'/* basic auth auto-sent by browser */ });
  const data = await r.json();
  if (data.success) { alert('Cleared ' + data.deleted + ' pending booking(s).'); load(); }
  else { alert('Error: ' + (data.error || 'Unknown')); }
}

async function setPayment(confId, method) {
  // method: '' = unpaid, 'cash'|'card'|'venmo'|'zelle' = paid w/ method, '__unpaid' = unpaid
  const r = await fetch('/admin/set-payment', { method:'POST', headers:{'Content-Type':'application/json'/* basic auth auto-sent by browser */}, body: JSON.stringify({confId, method}) });
  load();
}

async function hardDelete(confId) {
  if (!confirm('PERMANENTLY DELETE booking ' + confId + '? This removes it from the database. Use this for test bookings only.')) return;
  const r = await fetch('/admin/hard-delete-booking', { method:'POST', headers:{'Content-Type':'application/json'/* basic auth auto-sent by browser */}, body: JSON.stringify({confId}) });
  const data = await r.json();
  if (data.success) { load(); }
  else { alert('Error: ' + (data.error||'Unknown error')); }
}

// Event delegation for booking action buttons
document.addEventListener('click', function(e) {
  const btn = e.target.closest('button[data-action]');
  if (!btn) return;
  const action = btn.dataset.action;
  const id = btn.dataset.id;
  const code = btn.dataset.code;
  const token = btn.dataset.token;
  if (action === 'cancel') cancelBooking(id);
  if (action === 'hard-delete') hardDelete(id);
  if (action === 'deletecode') deleteCode(code);
  if (action === 'delete-pending') deletePending(token);
  if (action === 'countersign') counterSign(id, btn);
});

async function counterSign(confId, btn) {
  if (!confirm('Counter-sign agreement for ' + confId + '?\\n\\nThis will:\\n  • Generate a fully-executed PDF with both signatures\\n  • Email the executed PDF to the client\\n  • Send you a copy in your email\\n\\nThis action cannot be undone.')) return;
  const original = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Working...';
  try {
    const r = await fetch('/admin/counter-sign', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', /* Authorization auto-sent by browser */ },
      body: JSON.stringify({ confId })
    });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || ('Status ' + r.status));
    alert('✓ Counter-signed and emailed successfully.');
    load();  // refresh booking table to show new state
  } catch(e) {
    alert('Counter-sign failed: ' + e.message);
    btn.disabled = false;
    btn.textContent = original;
  }
}

// Event delegation for payment dropdown changes
document.addEventListener('change', function(e) {
  const sel = e.target.closest('select[data-action="setpay"]');
  if (!sel) return;
  const id = sel.dataset.id;
  const method = sel.value;
  if (method === '') return;  // ignore the placeholder option
  setPayment(id, method === '__unpaid' ? '' : method);
});

load();
setInterval(load, 60000);
</script>
</body>
</html>`);
});

// ── ADMIN API ROUTES ──────────────────────────────────────────
app.post('/admin/cancel-booking', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId, silent } = req.body;
  if (!confId) return res.status(400).json({ error: 'No confId' });
  if (!(await roleCanTouchBooking(adminRole(req), confId))) return res.status(403).json({ error: 'Forbidden' });

  let booking;
  try {
    const r = await pool.query('SELECT * FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    if (!r.rows.length) return res.status(404).json({ error: 'Booking not found' });
    booking = r.rows[0];
  } catch(e) { return res.status(500).json({ error: e.message }); }

  const d = booking.data;
  const opCfg = operatorConfig(booking.operator || (d && d.operator));

  // Calendar event is always deleted regardless of silent mode — keeping it
  // on the calendar after cancellation would create real-world confusion.
  if (d.calId) {
    const delCalId = (getOperator(booking.operator || d.operator) === 'jeff') ? (process.env.CALENDAR_ID_JEFF || CALENDAR_ID) : CALENDAR_ID;
    try {
      await calendar.events.delete({ calendarId: delCalId, eventId: d.calId, sendUpdates: 'none' });
    } catch(e) { console.warn('Calendar delete failed:', e.message); }
  }

  try {
    await pool.query('UPDATE confirmed_bookings SET cancelled_at = NOW() WHERE conf_id = $1', [confId]);
  } catch(e) { console.error('DB cancel update:', e.message); }

  // Silent mode: log it and return without sending emails. Used when you're
  // cancelling a test booking, an internal mistake, or a booking the client
  // already knows about through another channel.
  if (silent) {
    console.log('Booking cancelled SILENTLY (no client emails): ' + confId);
    return res.json({ success: true, silent: true });
  }

  const buyerHtml = emailWrap(
    '<h2 style="color:#C0392B">Inspection Cancelled</h2>'
    + '<p>Hi ' + (d.buyer && d.buyer.firstName ? d.buyer.firstName : 'there') + ',</p>'
    + '<p>Your inspection has been cancelled. We apologize for any inconvenience.</p>'
    + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
    + '<tr><td style="padding:6px 0;color:#888;width:130px">Confirmation</td><td style="color:#2C2C2C;font-weight:600">' + confId + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Property</td><td style="color:#2C2C2C;font-weight:600">' + (d.address||'') + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + (d.dateFmt||'') + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + (d.time||'') + '</td></tr>'
    + '</table>'
    + '<p>If you would like to reschedule, please contact us:</p>'
    + '<p>&#128222; <strong>' + opCfg.phone + '</strong><br>&#9993; <strong>santanpropertyinspections@gmail.com</strong></p>'
  );

  try {
    if (d.buyer && d.buyer.email) await sendEmail(d.buyer.email, 'Inspection Cancelled — ' + confId, buyerHtml, null, opCfg.replyTo);
  } catch(e) { console.error('Cancel buyer email:', e.message); }

  if (d.buyerAgent && d.buyerAgent.email) {
    const agentHtml = emailWrap(
      '<h2 style="color:#C0392B">Inspection Cancelled</h2>'
      + '<p>Hi ' + d.buyerAgent.name + ',</p>'
      + '<p>The inspection for your buyer has been cancelled.</p>'
      + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
      + '<tr><td style="padding:6px 0;color:#888;width:130px">Buyer</td><td style="color:#2C2C2C;font-weight:600">' + (d.fullName||'') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Property</td><td style="color:#2C2C2C;font-weight:600">' + (d.address||'') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + (d.dateFmt||'') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Confirmation</td><td style="color:#2C2C2C;font-weight:600">' + confId + '</td></tr>'
      + '</table>'
      + '<p>Questions? Call/text <strong>' + opCfg.phone + '</strong></p>'
    );
    try { await sendEmail(d.buyerAgent.email, 'Inspection Cancelled — ' + (d.fullName||'') + ' [' + confId + ']', agentHtml, null, opCfg.replyTo); } catch(e) {}
  }

  res.json({ success: true });
});

app.post('/admin/mark-paid', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId } = req.body;
  if (!confId) return res.status(400).json({ error: 'No confId' });
  if (!(await roleCanTouchBooking(adminRole(req), confId))) return res.status(403).json({ error: 'Forbidden' });
  try {
    await pool.query('UPDATE confirmed_bookings SET paid_at = NOW() WHERE conf_id = $1', [confId]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/admin/mark-unpaid', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId } = req.body;
  if (!confId) return res.status(400).json({ error: 'No confId' });
  if (!(await roleCanTouchBooking(adminRole(req), confId))) return res.status(403).json({ error: 'Forbidden' });
  try {
    await pool.query('UPDATE confirmed_bookings SET paid_at = NULL WHERE conf_id = $1', [confId]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Set payment method + auto-mark paid (or unpaid if method blank)
app.post('/admin/set-payment', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId, method } = req.body;
  if (!confId) return res.status(400).json({ error: 'No confId' });
  if (!(await roleCanTouchBooking(adminRole(req), confId))) return res.status(403).json({ error: 'Forbidden' });
  const allowed = ['cash', 'card', 'venmo', 'zelle', ''];
  if (!allowed.includes(method)) return res.status(400).json({ error: 'Invalid method' });
  try {
    if (method) {
      // Method provided → mark paid + record method
      await pool.query(
        'UPDATE confirmed_bookings SET paid_at = COALESCE(paid_at, NOW()), payment_method = $1 WHERE conf_id = $2',
        [method, confId]
      );
    } else {
      // Empty → unpaid (clear both)
      await pool.query(
        'UPDATE confirmed_bookings SET paid_at = NULL, payment_method = NULL WHERE conf_id = $1',
        [confId]
      );
    }
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Permanently delete a booking from the DB (use for test bookings only)
app.post('/admin/hard-delete-booking', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId } = req.body;
  if (!confId) return res.status(400).json({ error: 'No confId' });
  if (!(await roleCanTouchBooking(adminRole(req), confId))) return res.status(403).json({ error: 'Forbidden' });
  try {
    // Best-effort: try to delete calendar event if one was created
    const r = await pool.query('SELECT data, operator FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    if (r.rows.length && r.rows[0].data && r.rows[0].data.calId) {
      const delCalId = (getOperator(r.rows[0].operator || r.rows[0].data.operator) === 'jeff') ? (process.env.CALENDAR_ID_JEFF || CALENDAR_ID) : CALENDAR_ID;
      try { await calendar.events.delete({ calendarId: delCalId, eventId: r.rows[0].data.calId, sendUpdates: 'none' }); }
      catch(e) { console.warn('Calendar delete (hard-delete):', e.message); }
    }
    await pool.query('DELETE FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    console.log('Hard-deleted booking: ' + confId);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Stream signed agreement PDF from R2 to admin browser
app.get('/admin/agreement-pdf/:confId', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) {
    return res.redirect('/admin/login');
  }
  const { confId } = req.params;
  if (!(await roleCanTouchBooking(adminRole(req), confId))) return res.status(403).send('Forbidden');
  try {
    const r = await pool.query('SELECT agreement_pdf_key FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    if (!r.rows.length || !r.rows[0].agreement_pdf_key) {
      return res.status(404).send('No signed agreement on file for ' + confId);
    }
    const pdf = await downloadFromR2(r.rows[0].agreement_pdf_key);
    res.set('Content-Type', 'application/pdf');
    res.set('Content-Disposition', 'inline; filename="' + confId + '-agreement.pdf"');
    res.send(pdf);
  } catch(e) {
    console.error('Agreement PDF fetch:', e.message);
    res.status(500).send('Could not retrieve PDF: ' + e.message);
  }
});

// Stream the fully-executed (counter-signed) agreement PDF to admin browser.
app.get('/admin/executed-pdf/:confId', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) {
    return res.redirect('/admin/login');
  }
  const { confId } = req.params;
  if (!(await roleCanTouchBooking(adminRole(req), confId))) return res.status(403).send('Forbidden');
  try {
    const r = await pool.query('SELECT counter_signed_pdf_key FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    if (!r.rows.length || !r.rows[0].counter_signed_pdf_key) {
      return res.status(404).send('No counter-signed agreement on file for ' + confId);
    }
    const pdf = await downloadFromR2(r.rows[0].counter_signed_pdf_key);
    res.set('Content-Type', 'application/pdf');
    res.set('Content-Disposition', 'inline; filename="' + confId + '-executed-agreement.pdf"');
    res.send(pdf);
  } catch(e) {
    console.error('Executed PDF fetch:', e.message);
    res.status(500).send('Could not retrieve PDF: ' + e.message);
  }
});

// Counter-sign a previously-signed agreement.
// Generates a new fully-executed PDF, uploads to R2, and emails it to client + owner.
app.post('/admin/counter-sign', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId } = req.body;
  if (!confId) return res.status(400).json({ error: 'No confId' });
  if (!(await roleCanTouchBooking(adminRole(req), confId))) return res.status(403).json({ error: 'Forbidden' });

  try {
    const r = await pool.query(
      'SELECT data, operator, agreement_signed_at, agreement_signature, agreement_ip, counter_signed_at FROM confirmed_bookings WHERE conf_id = $1',
      [confId]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'Booking not found' });
    const row = r.rows[0];

    // Block if not yet client-signed
    if (!row.agreement_signed_at) {
      return res.status(400).json({ error: 'Client has not yet signed the agreement. Counter-signature requires the client signature first.' });
    }
    // Block if already counter-signed (avoid accidental re-runs that re-email the client)
    if (row.counter_signed_at) {
      return res.status(409).json({ error: 'This agreement has already been counter-signed on ' + new Date(row.counter_signed_at).toLocaleString('en-US', { timeZone: 'America/Phoenix' }) });
    }

    const booking         = row.data || {};
    const opCfg           = operatorConfig(row.operator || booking.operator);
    // Counter-signer is the operator of record (their name on the executed PDF).
    const counterSignedBy = opCfg.inspectorName;
    const counterSignedAt = new Date().toISOString();

    // Generate the executed PDF (both signatures stamped)
    const pdf = await generateExecutedAgreementPdf(
      booking,
      row.agreement_signed_at,
      row.agreement_signature,
      row.agreement_ip || 'unknown',
      counterSignedAt,
      counterSignedBy
    );
    if (!pdf) {
      return res.status(500).json({ error: 'Failed to generate executed PDF' });
    }

    // Upload to R2 under a versioned key so we keep both the client-only and executed PDFs
    const executedKey = 'agreements/' + confId + '-executed-agreement.pdf';
    try {
      await uploadToR2(executedKey, pdf, 'application/pdf');
    } catch(e) {
      console.error('Executed PDF R2 upload failed:', e.message);
      return res.status(500).json({ error: 'Could not save PDF to storage: ' + e.message });
    }

    // Stamp the counter-sign in the DB
    await pool.query(
      `UPDATE confirmed_bookings
       SET counter_signed_at = $1, counter_signed_by = $2, counter_signed_pdf_key = $3
       WHERE conf_id = $4`,
      [counterSignedAt, counterSignedBy, executedKey, confId]
    );

    // Email the executed PDF to client + owner. Fire-and-forget per recipient so
    // one failure doesn't kill the others.
    const pdfBase64 = pdf.toString('base64');
    const counterSignedDate = new Date(counterSignedAt).toLocaleString('en-US', {
      timeZone: 'America/Phoenix', dateStyle: 'medium', timeStyle: 'short',
    });

    const buyerFirst = (booking.buyer && booking.buyer.firstName) || booking.fullName || 'there';

    // Client email
    if (booking.buyer && booking.buyer.email) {
      const clientHtml = emailWrap(
        '<h2 style="color:#0F1C35">Agreement Fully Executed</h2>'
        + '<p>Hi ' + buyerFirst + ',</p>'
        + '<p>Your inspection agreement is now fully executed. Both signatures have been recorded.</p>'
        + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
        + '<tr><td style="padding:6px 0;color:#888;width:160px">Property</td><td style="color:#2C2C2C;font-weight:600">' + (booking.address || '') + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Inspection Date</td><td style="color:#2C2C2C;font-weight:600">' + (booking.dateFmt || '') + ' @ ' + (booking.time || '') + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Confirmation #</td><td style="color:#C9A84C;font-weight:700">' + confId + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Counter-Signed</td><td style="color:#2C2C2C;font-weight:600">' + counterSignedDate + ' (AZ)</td></tr>'
        + '</table>'
        + '<p>The fully-executed agreement is attached for your records.</p>'
        + '<p>Questions? Call or text <strong>' + opCfg.phone + '</strong></p>'
      );
      sendEmail(
        booking.buyer.email,
        'Agreement Fully Executed — ' + confId,
        clientHtml,
        [{ filename: confId + '-executed-agreement.pdf', content: pdfBase64 }],
        opCfg.replyTo
      ).catch(function(e){ console.error('Client executed-PDF email failed:', e.message); });
    }

    // Owner email (so the operator has a copy in inbox). For Jeff, goes to his
    // notify list (him + Jaren); for Jaren, just OWNER_EMAIL.
    const csNotify = (opCfg.notifyEmails && opCfg.notifyEmails.length) ? opCfg.notifyEmails : [process.env.OWNER_EMAIL];
    for (const recip of csNotify) {
      if (!recip) continue;
      const ownerHtml = '<div style="font-family:Arial,sans-serif;max-width:520px">'
        + '<h2 style="color:#1B2D52">Counter-Signed: ' + (booking.fullName || '') + '</h2>'
        + '<p>The inspection agreement for <strong>' + (booking.address || '') + '</strong> has been counter-signed.</p>'
        + '<p>The fully-executed PDF is attached.</p>'
        + '<p><b>Conf #:</b> ' + confId + '<br>'
        + '<b>Counter-signed:</b> ' + counterSignedDate + ' (AZ)</p>'
        + '</div>';
      sendEmail(
        recip,
        'COUNTER-SIGNED: ' + (booking.fullName || '') + ' [' + confId + ']',
        ownerHtml,
        [{ filename: confId + '-executed-agreement.pdf', content: pdfBase64 }]
      ).catch(function(e){ console.error('Owner executed-PDF email failed:', e.message); });
    }

    res.json({ success: true, counterSignedAt, executedKey });
  } catch(e) {
    console.error('counter-sign:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/admin/csv', adminActionLimiter, async function(req, res) {
  const role = adminRole(req);
  if (!role) {
    return res.redirect('/admin/login');
  }
  try {
    const result = (role === 'jaren')
      ? await pool.query('SELECT * FROM confirmed_bookings ORDER BY confirmed_at DESC')
      : await pool.query("SELECT * FROM confirmed_bookings WHERE operator = 'jeff' ORDER BY confirmed_at DESC");
    const headers = ['Conf #','Date Confirmed','Inspection Date','Time','Buyer','Buyer Phone','Buyer Email','Address','Service','Add-Ons','Buyer Agent','Agent Phone','Listing Agent','Sq Ft','Year Built','Base Price','Final Price','Discount Code','Discount Amt','Trip Charge','Notes','Paid','Date Paid','Agreement Signed','Date Signed'];
    const lines = [headers.join(',')];
    for (const row of result.rows) {
      const d = row.data;
      const escape = (v) => '"' + String(v||'').replace(/"/g,'""') + '"';
      lines.push([
        escape(d.confId),
        escape(new Date(row.confirmed_at).toLocaleDateString('en-US')),
        escape(d.dateFmt||''),
        escape(d.time||''),
        escape(d.fullName||''),
        escape(d.buyer&&d.buyer.phone?d.buyer.phone:''),
        escape(d.buyer&&d.buyer.email?d.buyer.email:''),
        escape(d.address||''),
        escape(d.svcLabel||''),
        escape(d.addonsLine||''),
        escape(d.buyerAgent&&d.buyerAgent.name?d.buyerAgent.name:''),
        escape(d.buyerAgent&&d.buyerAgent.phone?d.buyerAgent.phone:''),
        escape(d.sellerAgent&&d.sellerAgent.name?d.sellerAgent.name:''),
        escape(d.sqft||''),
        escape(d.yearBuilt||''),
        escape(d.totalPrice||''),
        escape(d.finalPrice||''),
        escape(d.discountCode||''),
        escape(d.discountAmount||''),
        escape(d.tripCharge&&d.tripCharge.apply?'Yes':'No'),
        escape(d.notes||''),
        escape(row.paid_at ? 'Yes' : 'No'),
        escape(row.paid_at ? new Date(row.paid_at).toLocaleDateString('en-US') : ''),
        escape(row.agreement_signed_at ? 'Yes' : 'No'),
        escape(row.agreement_signed_at ? new Date(row.agreement_signed_at).toLocaleDateString('en-US') : ''),
      ].join(','));
    }
    res.set('Content-Type', 'text/csv');
    res.set('Content-Disposition', 'attachment; filename="santan_bookings_' + new Date().toISOString().slice(0,10) + '.csv"');
    res.send(lines.join('\n'));
  } catch(e) {
    res.status(500).send('Error generating CSV');
  }
});

// Mileage CSV — date-bounded round-trip miles for tax reporting.
// Usage: /admin/mileage-csv?from=2026-01-01&to=2026-12-31
// Header row + per-booking rows + a totals summary row at the bottom.
app.get('/admin/mileage-csv', adminActionLimiter, async function(req, res) {
  const role = adminRole(req);
  if (!role) {
    return res.redirect('/admin/login');
  }
  const from = String(req.query.from || '');
  const to   = String(req.query.to   || '');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
    return res.status(400).send('from and to query params required, format YYYY-MM-DD');
  }
  const scoped = role !== 'jaren';
  try {
    const result = await pool.query(
      `SELECT conf_id, data, miles, confirmed_at
       FROM confirmed_bookings
       WHERE cancelled_at IS NULL
         AND miles IS NOT NULL
         AND confirmed_at >= $1::date
         AND confirmed_at <  ($2::date + INTERVAL '1 day')` + (scoped ? " AND operator = 'jeff'" : '') + `
       ORDER BY confirmed_at ASC`,
      [from, to]
    );
    const escapeCell = function(v){ return '"' + String(v==null?'':v).replace(/"/g,'""') + '"'; };
    const lines = ['Date Booked,Inspection Date,Conf #,Buyer,Address,Round-Trip Miles'];
    let totalMiles = 0;
    for (const row of result.rows) {
      const d = row.data || {};
      const mi = Number(row.miles) || 0;
      totalMiles += mi;
      lines.push([
        escapeCell(new Date(row.confirmed_at).toLocaleDateString('en-US')),
        escapeCell(d.dateFmt || ''),
        escapeCell(row.conf_id || ''),
        escapeCell(d.fullName || ''),
        escapeCell(d.address || ''),
        escapeCell(mi.toFixed(2)),
      ].join(','));
    }
    lines.push(',,,,Total,' + escapeCell(totalMiles.toFixed(2)));
    res.set('Content-Type', 'text/csv');
    res.set('Content-Disposition', 'attachment; filename="santan_mileage_' + from + '_to_' + to + '.csv"');
    res.send(lines.join('\n'));
  } catch(e) {
    console.error('mileage-csv:', e.message);
    res.status(500).send('Error generating mileage CSV');
  }
});

app.post('/admin/codes/add', adminActionLimiter, async function(req, res) {
  if (adminRole(req) !== 'jaren') return res.status(403).json({ error: 'Forbidden' });
  const { code, pct } = req.body;
  if (!code || !pct || isNaN(pct) || pct < 1 || pct > 100) return res.status(400).json({ error: 'Invalid code or percentage' });
  try {
    await pool.query('INSERT INTO discount_codes (code, pct) VALUES ($1, $2) ON CONFLICT (code) DO UPDATE SET pct = $2', [code.toUpperCase().trim(), parseInt(pct)]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/admin/codes/delete', adminActionLimiter, async function(req, res) {
  if (adminRole(req) !== 'jaren') return res.status(403).json({ error: 'Forbidden' });
  const { code } = req.body;
  try {
    await pool.query('DELETE FROM discount_codes WHERE code = $1', [code]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/validate-code', async function(req, res) {
  const code = (req.query.code || '').toUpperCase().trim();
  if (!code) return res.status(400).json({ error: 'No code provided' });
  try {
    const r = await pool.query('SELECT pct FROM discount_codes WHERE code = $1', [code]);
    if (r.rows.length) return res.json({ valid: true, pct: r.rows[0].pct });
    return res.json({ valid: false });
  } catch(e) { return res.json({ valid: false }); }
});

app.get('/admin/data', adminActionLimiter, async function(req, res) {
  const role = adminRole(req);
  if (!role) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  // Jeff's admin is scoped to his own data; Jaren sees everything.
  const scoped = role !== 'jaren';
  const opFilterConfirmed = scoped ? " WHERE operator = 'jeff'" : '';
  // pending rows: keep the existing 48h + non-agree filter, add operator if scoped.
  const pendingWhere = "WHERE created_at > NOW() - INTERVAL '48 hours' AND token NOT LIKE 'agree_%'" + (scoped ? " AND operator = 'jeff'" : '');
  try {
    const [bookings, reschedules, codes, pending, mileageAgg, jeffTally] = await Promise.all([
      pool.query('SELECT *, agreement_signed_at, agreement_signature FROM confirmed_bookings' + opFilterConfirmed + ' ORDER BY confirmed_at DESC'),
      pool.query('SELECT * FROM reschedule_requests ORDER BY requested_at DESC'),
      pool.query('SELECT * FROM discount_codes ORDER BY created_at DESC'),
      pool.query("SELECT token, data, created_at FROM pending_bookings " + pendingWhere + " ORDER BY created_at DESC"),
      // Mileage roll-up — only counts miles on bookings that aren't cancelled.
      // Uses confirmed_at for the cutoff so the tile reflects when the inspection was booked.
      pool.query(`
        SELECT
          COALESCE(SUM(miles) FILTER (WHERE confirmed_at >= date_trunc('month', NOW())), 0) AS month_miles,
          COALESCE(SUM(miles) FILTER (WHERE confirmed_at >= date_trunc('year',  NOW())), 0) AS ytd_miles
        FROM confirmed_bookings
        WHERE cancelled_at IS NULL` + (scoped ? " AND operator = 'jeff'" : '') + `
      `),
      // Jeff sub-contractor tally: completed (delivered) Jeff inspections × $50.
      // Counts reports for Jeff's bookings that have been delivered. Only shown to Jaren.
      pool.query(`
        SELECT COUNT(*)::int AS cnt
        FROM confirmed_bookings cb
        WHERE cb.operator = 'jeff' AND cb.cancelled_at IS NULL
      `),
    ]);
    const m = mileageAgg.rows[0] || {};
    const jeffCount = (jeffTally.rows[0] && jeffTally.rows[0].cnt) || 0;
    res.json({
      role: role,
      bookings: bookings.rows,
      reschedules: reschedules.rows,
      codes: codes.rows,
      pending: pending.rows,
      mileage: {
        monthMiles: Number(m.month_miles) || 0,
        ytdMiles:   Number(m.ytd_miles)   || 0,
      },
      // Only meaningful for Jaren's view; harmless for Jeff (his own count).
      jeffOwes: { count: jeffCount, rate: 50, total: jeffCount * 50 },
    });
  } catch(e) {
    res.status(500).json({ error: 'DB error' });
  }
});

// Delete a single pending booking (frees its slot immediately)
app.post('/admin/delete-pending', adminActionLimiter, async function(req, res) {
  const role = adminRole(req);
  if (!role) return res.status(401).json({ error: 'Unauthorized' });
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'No token' });
  try {
    // Jeff may only delete his own pending rows; Jaren may delete any.
    if (role === 'jaren') {
      await pool.query('DELETE FROM pending_bookings WHERE token = $1', [token]);
    } else {
      const r = await pool.query("DELETE FROM pending_bookings WHERE token = $1 AND operator = 'jeff' RETURNING token", [token]);
      if (!r.rows.length) return res.status(403).json({ error: 'Forbidden' });
    }
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Clear ALL pending bookings — for cleaning up after testing.
// Scoped: Jeff clears only his own pending rows; Jaren clears all.
app.post('/admin/clear-all-pending', adminActionLimiter, async function(req, res) {
  const role = adminRole(req);
  if (!role) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const r = (role === 'jaren')
      ? await pool.query('DELETE FROM pending_bookings RETURNING token')
      : await pool.query("DELETE FROM pending_bookings WHERE operator = 'jeff' RETURNING token");
    res.json({ success: true, deleted: r.rows.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── ADMIN: PER-BOOKING EDIT PAGE ──────────────────────────────
// Renders a full edit form for one booking. Auth via Basic Auth (same as
// /admin). Editable fields cover everything Jaren typically needs to adjust
// after the customer books: date/time, address, sqft, year, service type,
// add-ons, manual price override, discount, paid status, contact info,
// internal notes.
//
// Submit hits POST /admin/booking/:confId — see that handler for the save logic.
app.get('/admin/booking/:confId', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) {
    return res.redirect('/admin/login');
  }
  const confId = req.params.confId;
  if (!(await roleCanTouchBooking(adminRole(req), confId))) return res.status(403).send('<h2 style="font-family:sans-serif;padding:60px 24px;text-align:center">Forbidden</h2><p style="text-align:center"><a href="/admin">← Back to admin</a></p>');
  let row;
  try {
    const r = await pool.query('SELECT * FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    if (!r.rows.length) return res.status(404).send('<h2 style="font-family:sans-serif;padding:60px 24px;text-align:center">Booking not found: ' + escapeHtml(confId) + '</h2><p style="text-align:center"><a href="/admin">← Back to admin</a></p>');
    row = r.rows[0];
  } catch(e) {
    console.error('Admin edit GET:', e.message);
    return res.status(500).send('<h2>Database error.</h2>');
  }

  const d  = row.data || {};
  const ba = d.buyerAgent  || {};
  const sa = d.sellerAgent || {};
  const bu = d.buyer       || {};
  const addons = Array.isArray(d.addons) ? d.addons : [];

  // Map of normalized addon ids → enabled state. Existing data may have legacy
  // string-shape addons (display labels) or {id, name} objects, so normalize both.
  const addonEnabled = { termite: false, pool: false, spa: false, shed: false };
  for (const a of addons) {
    let id = null;
    if (typeof a === 'string') {
      const lower = a.toLowerCase();
      for (const k of Object.keys(PRICE_ADDONS)) {
        if (lower === k || lower.indexOf(PRICE_ADDONS[k].name.toLowerCase()) !== -1) { id = k; break; }
      }
    } else if (a && typeof a === 'object' && a.id) {
      if (PRICE_ADDONS[a.id]) id = a.id;
    }
    if (id) addonEnabled[id] = true;
  }

  const SVC = {
    'pre-purchase':'Pre-Purchase Inspection','pre-listing':'Pre-Listing Inspection',
    'new-construction':'New Construction Inspection','warranty':'Pre-One Year Warranty Inspection','reinspection':'Re-Inspection',
  };
  const svcOptions = Object.keys(SVC).map(function(k){
    return '<option value="' + k + '"' + (d.inspType === k ? ' selected' : '') + '>' + SVC[k] + '</option>';
  }).join('');

  const tripApply  = !!(d.tripCharge && d.tripCharge.apply);
  const tripMiles  = d.tripCharge && d.tripCharge.miles ? d.tripCharge.miles : null;
  const milesRound = row.miles != null ? Number(row.miles).toFixed(2) : null;

  const paymentMethod = row.payment_method || '';
  const isPaid        = !!row.paid_at;
  const isCancelled   = !!row.cancelled_at;
  const isSigned      = !!row.agreement_signed_at;

  const hubUrl = d.agreementToken
    ? '/i/' + encodeURIComponent(d.agreementToken) + '?s=' + encodeURIComponent(signToken(d.agreementToken))
    : null;

  // Pull report info for this booking — used to render View Report / Edit in Inspector
  // links at the top of the edit page. Defensive: returns {state:'none', ...} on miss.
  const reportInfo = await getReportInfoForConfId(confId);
  const inspectorBase = (process.env.INSPECTOR_URL || '').replace(/\/$/, '');
  const inspectorTpl  = process.env.INSPECTOR_EDIT_PATH_TPL || '';
  const inspectorEditUrl = (inspectorBase && reportInfo.id)
    ? inspectorBase + inspectorTpl.replace('{id}', encodeURIComponent(reportInfo.id))
    : null;

  // Pre-build conditional status badges (avoid backtick-in-template quoting)
  const badgeCancelled = isCancelled ? '<span class="status-tag tag-bad">Cancelled</span>' : '';
  const badgeSigned    = isSigned    ? '<span class="status-tag tag-good">Signed</span>' : '<span class="status-tag tag-warn">Unsigned</span>';
  const badgePaid      = isPaid      ? '<span class="status-tag tag-good">Paid' + (paymentMethod ? ' · ' + escapeHtml(paymentMethod) : '') + '</span>' : '<span class="status-tag tag-warn">Unpaid</span>';
  const hubLink        = hubUrl ? '<a href="' + escapeHtml(hubUrl) + '" target="_blank" class="status-link">View customer hub →</a>' : '';
  const reportLink     = (reportInfo.state !== 'none' && reportInfo.pdfKey)
    ? '<a href="/admin/report-pdf/' + encodeURIComponent(confId) + '" target="_blank" class="status-link" style="color:' + (reportInfo.state === 'delivered' ? '#1ab464' : '#C9A84C') + '">' + (reportInfo.state === 'delivered' ? 'View Report' + (reportInfo.version > 1 ? ' v' + reportInfo.version : '') : 'Preview Report (Draft)') + ' →</a>'
    : '';
  const inspectorLink  = inspectorEditUrl
    ? '<a href="' + escapeHtml(inspectorEditUrl) + '" target="_blank" class="status-link">Edit in Inspector →</a>'
    : '';

  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Edit ${escapeHtml(confId)} — Admin</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0F1C35;color:#BEC8D8;min-height:100vh;padding:0;}
nav{background:#0a1428;border-bottom:2px solid #C9A84C;padding:14px 28px;display:flex;align-items:center;justify-content:space-between;}
nav h1{font-size:1rem;font-weight:700;color:#C9A84C;letter-spacing:1px;text-transform:uppercase;}
nav a{color:#8A9AB5;text-decoration:none;font-size:.85rem;}
nav a:hover{color:#C9A84C;}
.wrap{max-width:920px;margin:0 auto;padding:28px 20px;}
.title-bar{display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:24px;flex-wrap:wrap;gap:12px;}
.title-bar h2{font-size:1.4rem;color:#fff;font-weight:700;}
.title-bar .sub{font-size:.85rem;color:#8A9AB5;margin-top:4px;}
.status-row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;}
.status-tag{display:inline-block;font-size:.72rem;font-weight:700;padding:4px 10px;border-radius:12px;text-transform:uppercase;letter-spacing:.5px;}
.tag-good{background:rgba(26,180,100,.15);color:#1ab464;}
.tag-warn{background:rgba(232,168,124,.15);color:#e8a87c;}
.tag-bad{background:rgba(192,57,43,.18);color:#e8a87c;}
.status-link{font-size:.75rem;color:#C9A84C;text-decoration:none;margin-left:4px;}
.status-link:hover{text-decoration:underline;}
.card{background:#1B2D52;border-radius:10px;padding:24px 28px;margin-bottom:20px;}
.card h3{font-size:.82rem;font-weight:700;color:#C9A84C;text-transform:uppercase;letter-spacing:1.2px;margin-bottom:18px;padding-bottom:10px;border-bottom:1px solid #243660;}
.row{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px 18px;margin-bottom:14px;}
.field{display:flex;flex-direction:column;gap:5px;}
.field-wide{grid-column:1 / -1;}
label{font-size:.78rem;color:#8A9AB5;text-transform:uppercase;letter-spacing:.8px;}
input[type=text],input[type=email],input[type=tel],input[type=number],input[type=date],input[type=time],select,textarea{
  background:#243660;border:1px solid #344870;color:#fff;padding:9px 12px;border-radius:6px;font-size:.9rem;font-family:inherit;outline:none;width:100%;
}
input:focus,select:focus,textarea:focus{border-color:#C9A84C;background:#2a3e6f;}
textarea{resize:vertical;min-height:64px;line-height:1.5;}
.addons-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:8px;}
.addon-chk{display:flex;align-items:center;gap:8px;background:#243660;border:1px solid #344870;padding:9px 12px;border-radius:6px;cursor:pointer;font-size:.88rem;color:#E8DEC4;}
.addon-chk input{accent-color:#C9A84C;}
.addon-chk:hover{background:#2a3e6f;}
.hint{font-size:.74rem;color:#8A9AB5;font-style:italic;margin-top:3px;line-height:1.4;}
.checkbox-row{display:flex;align-items:center;gap:10px;background:#243660;border:1px solid #344870;padding:10px 14px;border-radius:6px;margin-bottom:10px;}
.checkbox-row input{accent-color:#C9A84C;width:16px;height:16px;}
.checkbox-row label{text-transform:none;letter-spacing:0;color:#E8DEC4;font-size:.88rem;margin:0;cursor:pointer;}
.actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:8px;}
button[type=submit],.btn-primary{background:#C9A84C;color:#0F1C35;border:none;border-radius:8px;padding:12px 28px;font-size:.92rem;font-weight:700;cursor:pointer;font-family:inherit;}
button[type=submit]:hover,.btn-primary:hover{background:#d4b25a;}
button[type=submit]:disabled{background:#888;cursor:not-allowed;}
.btn-secondary{background:transparent;color:#8A9AB5;border:1px solid #344870;border-radius:8px;padding:11px 22px;font-size:.88rem;font-weight:600;cursor:pointer;font-family:inherit;text-decoration:none;display:inline-block;}
.btn-secondary:hover{color:#fff;border-color:#8A9AB5;}
.notify-row{background:#243660;border:1px solid #344870;border-radius:8px;padding:14px 18px;margin-bottom:14px;}
.notify-row label{color:#E8DEC4;font-size:.9rem;text-transform:none;letter-spacing:0;display:flex;align-items:center;gap:10px;cursor:pointer;}
.notify-row .hint{margin-top:8px;margin-left:26px;}
.read-only{color:#8A9AB5;font-size:.85rem;background:#162240;padding:8px 12px;border-radius:6px;}
.flash{padding:12px 18px;border-radius:8px;margin-bottom:20px;font-size:.9rem;}
.flash-ok{background:rgba(26,180,100,.15);color:#1ab464;border:1px solid rgba(26,180,100,.4);}
.flash-bad{background:rgba(192,57,43,.18);color:#e8a87c;border:1px solid rgba(192,57,43,.4);}
@media(max-width:600px){.wrap{padding:18px 14px;}.card{padding:18px 18px;}.title-bar h2{font-size:1.15rem;}}
</style>
</head>
<body>
<nav>
  <h1>Edit Booking</h1>
  <a href="/admin">← Back to admin</a>
</nav>
<div class="wrap">
  <div class="title-bar">
    <div>
      <h2>${escapeHtml(d.fullName || '(no name)')}</h2>
      <div class="sub">${escapeHtml(confId)} · confirmed ${escapeHtml(new Date(row.confirmed_at).toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'}))}</div>
    </div>
    <div class="status-row">
      ${badgeCancelled}
      ${badgeSigned}
      ${badgePaid}
      ${hubLink}
      ${reportLink}
      ${inspectorLink}
    </div>
  </div>

  <div id="flash"></div>

  <form id="editForm" method="POST" action="/admin/booking/${encodeURIComponent(confId)}">

    <div class="card">
      <h3>Date &amp; Time</h3>
      <div class="row">
        <div class="field">
          <label>Date</label>
          <input type="date" name="date" value="${escapeHtml(d.date || '')}"/>
        </div>
        <div class="field">
          <label>Time</label>
          <select name="time">
            ${ALL_SLOTS.map(function(s){ return '<option value="' + s + '"' + (d.time === s ? ' selected' : '') + '>' + s + '</option>'; }).join('')}
          </select>
        </div>
      </div>
      <div class="hint">Changing the date or time will update the Google Calendar event if one exists. Tick the notify box below to also email the client.</div>
    </div>

    <div class="card">
      <h3>Property</h3>
      <div class="row">
        <div class="field field-wide">
          <label>Address</label>
          <input type="text" name="address" value="${escapeHtml(d.address || '')}" maxlength="${LEN.address}"/>
          ${milesRound ? '<div class="hint">Current round-trip: ' + milesRound + ' miles' + (tripApply ? ' (trip charge applies, $' + TRIP_CHARGE_AMT + ')' : '') + '. Re-saving with a new address recomputes both.</div>' : '<div class="hint">Saving with a new address triggers a fresh Distance Matrix lookup for mileage and trip charge.</div>'}
        </div>
        <div class="field">
          <label>Square Footage</label>
          <input type="number" name="sqft" value="${escapeHtml(String(d.sqft || ''))}" min="0" max="20000"/>
        </div>
        <div class="field">
          <label>Year Built</label>
          <input type="number" name="yearBuilt" value="${escapeHtml(String(d.yearBuilt || ''))}" min="1800" max="2100"/>
        </div>
      </div>
    </div>

    <div class="card">
      <h3>Service &amp; Add-Ons</h3>
      <div class="row">
        <div class="field">
          <label>Primary Service</label>
          <select name="inspType">${svcOptions}</select>
        </div>
      </div>
      <div class="field" style="margin-top:8px">
        <label>Add-Ons</label>
        <div class="addons-grid">
          ${Object.keys(PRICE_ADDONS).map(function(k){
            const a = PRICE_ADDONS[k];
            return '<label class="addon-chk"><input type="checkbox" name="addon_' + k + '" value="1"' + (addonEnabled[k] ? ' checked' : '') + '/> ' + a.name + ' (+$' + a.p + ')</label>';
          }).join('')}
        </div>
      </div>
    </div>

    <div class="card">
      <h3>Pricing</h3>
      <div class="row">
        <div class="field">
          <label>Price Override ($)</label>
          <input type="number" name="priceOverride" value="${escapeHtml(String(d.priceOverride || ''))}" min="0" max="10000" step="1" placeholder="Leave blank to auto-compute"/>
          <div class="hint">When set, bypasses square-footage pricing. Discount + trip charge still apply on top.</div>
        </div>
        <div class="field">
          <label>Discount Code</label>
          <input type="text" name="discountCode" value="${escapeHtml(d.discountCode || '')}" maxlength="${LEN.code}" placeholder="Optional"/>
        </div>
        <div class="field">
          <label>Discount % (manual)</label>
          <input type="number" name="discountPct" value="${escapeHtml(String(d.discountPct || ''))}" min="0" max="100" placeholder="Only if no code"/>
          <div class="hint">If both code &amp; manual % are set, the code wins (server looks it up).</div>
        </div>
      </div>
      <div class="row">
        <div class="field">
          <label>Current Final Price</label>
          <div class="read-only">$${Number(d.finalPrice)||0}${tripApply ? ' (incl. $' + TRIP_CHARGE_AMT + ' trip charge)' : ''}</div>
        </div>
        <div class="field">
          <label>Payment Status</label>
          <select name="paymentMethod">
            <option value=""${paymentMethod === '' && !isPaid ? ' selected' : ''}>— Unpaid —</option>
            <option value="cash"${paymentMethod === 'cash' ? ' selected' : ''}>Paid · Cash</option>
            <option value="card"${paymentMethod === 'card' ? ' selected' : ''}>Paid · Card</option>
            <option value="venmo"${paymentMethod === 'venmo' ? ' selected' : ''}>Paid · Venmo</option>
            <option value="zelle"${paymentMethod === 'zelle' ? ' selected' : ''}>Paid · Zelle</option>
            <option value="__paid_other"${isPaid && !paymentMethod ? ' selected' : ''}>Paid · Other</option>
          </select>
        </div>
      </div>
    </div>

    <div class="card">
      <h3>Client</h3>
      <div class="row">
        <div class="field">
          <label>First Name</label>
          <input type="text" name="buyer_firstName" value="${escapeHtml(bu.firstName || '')}" maxlength="${LEN.name}"/>
        </div>
        <div class="field">
          <label>Last Name</label>
          <input type="text" name="buyer_lastName" value="${escapeHtml(bu.lastName || '')}" maxlength="${LEN.name}"/>
        </div>
        <div class="field">
          <label>Email</label>
          <input type="email" name="buyer_email" value="${escapeHtml(bu.email || '')}" maxlength="${LEN.email}"/>
        </div>
        <div class="field">
          <label>Phone</label>
          <input type="tel" name="buyer_phone" value="${escapeHtml(bu.phone || '')}" maxlength="${LEN.phone}"/>
        </div>
      </div>
    </div>

    <div class="card">
      <h3>Buyer's Agent <span style="font-weight:400;color:#8A9AB5;text-transform:none;letter-spacing:0">(optional)</span></h3>
      <div class="row">
        <div class="field">
          <label>Name</label>
          <input type="text" name="ba_name" value="${escapeHtml(ba.name || '')}" maxlength="${LEN.name}"/>
        </div>
        <div class="field">
          <label>Brokerage</label>
          <input type="text" name="ba_brokerage" value="${escapeHtml(ba.brokerage || '')}" maxlength="${LEN.brokerage}"/>
        </div>
        <div class="field">
          <label>Email</label>
          <input type="email" name="ba_email" value="${escapeHtml(ba.email || '')}" maxlength="${LEN.email}"/>
        </div>
        <div class="field">
          <label>Phone</label>
          <input type="tel" name="ba_phone" value="${escapeHtml(ba.phone || '')}" maxlength="${LEN.phone}"/>
        </div>
      </div>
    </div>

    <div class="card">
      <h3>Listing Agent <span style="font-weight:400;color:#8A9AB5;text-transform:none;letter-spacing:0">(optional)</span></h3>
      <div class="row">
        <div class="field">
          <label>Name</label>
          <input type="text" name="sa_name" value="${escapeHtml(sa.name || '')}" maxlength="${LEN.name}"/>
        </div>
        <div class="field">
          <label>Brokerage</label>
          <input type="text" name="sa_brokerage" value="${escapeHtml(sa.brokerage || '')}" maxlength="${LEN.brokerage}"/>
        </div>
        <div class="field">
          <label>Email</label>
          <input type="email" name="sa_email" value="${escapeHtml(sa.email || '')}" maxlength="${LEN.email}"/>
        </div>
        <div class="field">
          <label>Phone</label>
          <input type="tel" name="sa_phone" value="${escapeHtml(sa.phone || '')}" maxlength="${LEN.phone}"/>
        </div>
      </div>
    </div>

    <div class="card">
      <h3>Notes</h3>
      <div class="row">
        <div class="field field-wide">
          <label>Customer Notes <span style="font-weight:400;color:#8A9AB5;text-transform:none;letter-spacing:0">(provided by client during booking)</span></label>
          <textarea name="notes" maxlength="${LEN.notes}" rows="3">${escapeHtml(d.notes || '')}</textarea>
        </div>
        <div class="field field-wide">
          <label>Internal Notes <span style="font-weight:400;color:#8A9AB5;text-transform:none;letter-spacing:0">(Jaren only, never shown to client)</span></label>
          <textarea name="internalNotes" maxlength="${LEN.notes}" rows="3" placeholder="CBS code, lockbox info, access notes, anything to remember about this job...">${escapeHtml(d.internalNotes || '')}</textarea>
        </div>
      </div>
    </div>

    <div class="card">
      <h3>Save</h3>
      <div class="notify-row">
        <label><input type="checkbox" name="notifyClient" value="1"/> <strong>Notify client of these changes by email</strong></label>
        <div class="hint">Tick this when the date, time, or address has changed and you want the client to know. Pricing-only or notes-only edits usually don't need an email.</div>
      </div>
      <div class="actions">
        <button type="submit" id="submitBtn">Save Changes</button>
        <a href="/admin" class="btn-secondary">Cancel</a>
      </div>
    </div>
  </form>

  <!-- Communication card sits OUTSIDE the edit form so its button doesn't trip
       the form's submit handler. Self-contained: clicking Resend hits a
       separate endpoint and shows its own success/error flash. -->
  <div class="card">
    <h3>Communication</h3>
    <p style="color:#8A9AB5;font-size:.86rem;line-height:1.5;margin-bottom:14px;">
      ${escapeHtml(((d.buyer && d.buyer.email) || 'No email on file'))} ·
      ${escapeHtml(((d.buyer && d.buyer.phone) || 'No phone on file'))}
    </p>
    <button type="button" id="resendHubBtn" class="btn-secondary" style="background:#243660;color:#C9A84C;border:1px solid #C9A84C;">
      ✉ Resend Hub Link to Client
    </button>
    <div id="resendFlash" style="margin-top:12px;"></div>
    <div class="hint" style="margin-top:10px;">Sends a fresh email to the buyer with the link to their inspection hub. Use this if a client lost their original confirmation email.</div>
  </div>
</div>

<script>
(function(){
  var form = document.getElementById('editForm');
  var btn  = document.getElementById('submitBtn');
  var flash = document.getElementById('flash');
  form.addEventListener('submit', function(e){
    e.preventDefault();
    btn.disabled = true;
    btn.textContent = 'Saving...';
    flash.innerHTML = '';
    var fd = new FormData(form);
    var body = {};
    fd.forEach(function(v, k){
      if (body[k] === undefined) body[k] = v;
      else if (Array.isArray(body[k])) body[k].push(v);
      else body[k] = [body[k], v];
    });
    fetch(form.action, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' /* basic auth auto-sent */ },
      body: JSON.stringify(body)
    })
    .then(function(r){ return r.json().then(function(d){ return { ok: r.ok, data: d }; }); })
    .then(function(res){
      if (res.ok && res.data.success) {
        flash.innerHTML = '<div class="flash flash-ok">Saved.' + (res.data.notified ? ' Client was emailed.' : '') + '</div>';
        btn.textContent = 'Saved ✓';
        // Scroll up so the flash is visible, then re-enable after a moment
        window.scrollTo({ top: 0, behavior: 'smooth' });
        setTimeout(function(){ btn.disabled = false; btn.textContent = 'Save Changes'; }, 2000);
      } else {
        flash.innerHTML = '<div class="flash flash-bad">' + (res.data && res.data.error ? res.data.error : 'Save failed.') + '</div>';
        window.scrollTo({ top: 0, behavior: 'smooth' });
        btn.disabled = false;
        btn.textContent = 'Save Changes';
      }
    })
    .catch(function(err){
      flash.innerHTML = '<div class="flash flash-bad">Network error: ' + err.message + '</div>';
      btn.disabled = false;
      btn.textContent = 'Save Changes';
    });
  });

  // Resend Hub Link handler — separate from the form submit so it doesn't
  // accidentally trigger a save. Self-contained: posts to its own endpoint,
  // shows its own flash inside the Communication card.
  var resendBtn = document.getElementById('resendHubBtn');
  if (resendBtn) {
    resendBtn.addEventListener('click', function() {
      var resendFlash = document.getElementById('resendFlash');
      resendBtn.disabled = true;
      var origText = resendBtn.textContent;
      resendBtn.textContent = 'Sending...';
      resendFlash.innerHTML = '';
      fetch(form.action + '/resend-hub-link', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      })
      .then(function(r){ return r.json().then(function(d){ return { ok: r.ok, data: d }; }); })
      .then(function(res){
        if (res.ok && res.data.success) {
          resendFlash.innerHTML = '<div class="flash flash-ok">Sent to ' + res.data.sentTo + '</div>';
          resendBtn.textContent = 'Sent ✓';
          setTimeout(function(){ resendBtn.disabled = false; resendBtn.textContent = origText; }, 3000);
        } else {
          resendFlash.innerHTML = '<div class="flash flash-bad">' + ((res.data && res.data.error) || 'Send failed.') + '</div>';
          resendBtn.disabled = false;
          resendBtn.textContent = origText;
        }
      })
      .catch(function(err){
        resendFlash.innerHTML = '<div class="flash flash-bad">Network error: ' + err.message + '</div>';
        resendBtn.disabled = false;
        resendBtn.textContent = origText;
      });
    });
  }
})();
</script>
</body>
</html>`);
});

// POST: apply edit
// - Validates input lengths and email/phone formats
// - Recomputes price via computePrice unless priceOverride is set
// - Reruns trip-charge/mileage if address changed
// - Updates Google Calendar event if date/time/address/service changed
// - Optionally sends client an "updated" email when notifyClient=1
// ── ADMIN: RESEND HUB LINK ────────────────────────────────────
// Re-emails the customer their hub URL. Used when a client says they lost
// their confirmation email or can't find the hub link. Idempotent — can be
// triggered as many times as needed; no DB state changes.
app.post('/admin/booking/:confId/resend-hub-link', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const confId = req.params.confId;
  if (!(await roleCanTouchBooking(adminRole(req), confId))) return res.status(403).json({ error: 'Forbidden' });

  let row;
  try {
    const r = await pool.query('SELECT * FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    if (!r.rows.length) return res.status(404).json({ error: 'Booking not found' });
    row = r.rows[0];
  } catch(e) {
    return res.status(500).json({ error: 'DB read failed: ' + e.message });
  }

  if (row.cancelled_at) return res.status(400).json({ error: 'Booking is cancelled.' });

  const d = row.data || {};
  const opCfg = operatorConfig(row.operator || d.operator);
  const buyer = d.buyer || {};
  if (!buyer.email) return res.status(400).json({ error: 'Buyer has no email on file.' });
  if (!d.agreementToken) return res.status(400).json({ error: 'No hub link available — this booking pre-dates the customer hub feature. Edit and re-save the booking to generate one.' });

  const BASE_URL = process.env.RAILWAY_URL || 'https://santanproperty-backend-production.up.railway.app';
  const hubUrl = withSig(BASE_URL + '/i/' + d.agreementToken, d.agreementToken);

  // Choose subject + body based on agreement state — gives the client useful
  // context about what they should do next when they open the email.
  const isSigned = !!row.agreement_signed_at;
  const ctaCopy = isSigned
    ? 'Open the hub below for your booking details, reschedule requests, and report status.'
    : 'Open the hub below to sign your inspection agreement, view your details, or reschedule.';

  try {
    const html = emailWrap(
      '<h2 style="color:#0F1C35">Your Inspection Link</h2>'
      + '<p>Hi ' + escapeHtml(buyer.firstName || 'there') + ', here is your inspection hub link as requested:</p>'
      + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
      + '<tr><td style="padding:6px 0;color:#888;width:130px">Property</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(d.address || '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(d.dateFmt || '') + ' @ ' + escapeHtml(d.time || '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Confirmation</td><td style="color:#C9A84C;font-weight:700">' + escapeHtml(confId) + '</td></tr>'
      + '</table>'
      + '<div style="background:#EAF3FB;border-left:4px solid #1B2D52;padding:16px 18px;margin:20px 0;border-radius:0 8px 8px 0">'
      + '<p style="margin:0 0 10px;font-size:.9rem;color:#1B2D52"><strong>Your Inspection Hub</strong></p>'
      + '<p style="margin:0 0 12px;font-size:.84rem;color:#555">' + escapeHtml(ctaCopy) + '</p>'
      + '<a href="' + hubUrl + '" style="display:inline-block;background:#1B2D52;color:white;padding:11px 22px;border-radius:6px;text-decoration:none;font-weight:700;font-size:.85rem">Open Inspection Hub</a>'
      + '</div>'
      + '<p>Questions? Call or text <strong>' + opCfg.phone + '</strong></p>'
    );
    await sendEmail(
      buyer.email,
      'Your Inspection Hub — ' + (d.dateFmt || '') + ' [' + confId + ']',
      html,
      null,
      opCfg.replyTo
    );
    res.json({ success: true, sentTo: buyer.email });
  } catch(e) {
    console.error('Resend hub link failed:', e.message);
    res.status(500).json({ error: 'Could not send email: ' + e.message });
  }
});

app.post('/admin/booking/:confId', adminActionLimiter, async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const confId = req.params.confId;
  if (!(await roleCanTouchBooking(adminRole(req), confId))) return res.status(403).json({ error: 'Forbidden' });

  let row;
  try {
    const r = await pool.query('SELECT * FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    if (!r.rows.length) return res.status(404).json({ error: 'Booking not found' });
    row = r.rows[0];
  } catch(e) {
    return res.status(500).json({ error: 'DB read failed: ' + e.message });
  }

  const old = row.data || {};
  const b   = req.body || {};
  const opCfg = operatorConfig(row.operator || old.operator);
  const opId  = getOperator(row.operator || old.operator);

  // ── Pull + clip inputs ─────────────────────────────────────
  const newDate    = clip(b.date,    20);
  const newTime    = clip(b.time,    20);
  const newAddress = clip(b.address, LEN.address);
  const newSqft    = parseInt(b.sqft) || 0;
  const newYear    = parseInt(b.yearBuilt) || 0;
  const newInspType= clip(b.inspType, 50);

  const newAddons = [];
  for (const k of Object.keys(PRICE_ADDONS)) {
    if (b['addon_' + k] === '1' || b['addon_' + k] === 'on' || b['addon_' + k] === true) {
      newAddons.push({ id: k, name: PRICE_ADDONS[k].name });
    }
  }

  const priceOverrideRaw = b.priceOverride;
  const priceOverride = (priceOverrideRaw !== '' && priceOverrideRaw != null && !isNaN(priceOverrideRaw))
    ? Math.max(0, Math.min(10000, parseInt(priceOverrideRaw))) : null;

  const newDiscountCode = clip(b.discountCode, LEN.code).toUpperCase() || null;
  const manualDiscountPct = (b.discountPct !== '' && b.discountPct != null && !isNaN(b.discountPct))
    ? Math.max(0, Math.min(100, parseInt(b.discountPct))) : null;

  const paymentMethodRaw = clip(b.paymentMethod, 20);
  // Allowed payment values
  const isPaidNow = paymentMethodRaw !== '';
  const paymentMethod = (paymentMethodRaw === '__paid_other') ? null : (paymentMethodRaw || null);

  const newNotes         = clip(b.notes,         LEN.notes);
  const newInternalNotes = clip(b.internalNotes, LEN.notes);

  const newBuyer = {
    firstName: clip(b.buyer_firstName, LEN.name),
    lastName:  clip(b.buyer_lastName,  LEN.name),
    email:     clip(b.buyer_email,     LEN.email),
    phone:     clip(b.buyer_phone,     LEN.phone),
  };
  const newBA = {
    name:      clip(b.ba_name,      LEN.name),
    brokerage: clip(b.ba_brokerage, LEN.brokerage),
    email:     clip(b.ba_email,     LEN.email),
    phone:     clip(b.ba_phone,     LEN.phone),
  };
  const newSA = {
    name:      clip(b.sa_name,      LEN.name),
    brokerage: clip(b.sa_brokerage, LEN.brokerage),
    email:     clip(b.sa_email,     LEN.email),
    phone:     clip(b.sa_phone,     LEN.phone),
  };
  const notifyClient = b.notifyClient === '1' || b.notifyClient === 'on' || b.notifyClient === true;

  // ── Validate ──────────────────────────────────────────────
  if (!newDate || !newTime) return res.status(400).json({ error: 'Date and time are required.' });
  if (!newAddress)          return res.status(400).json({ error: 'Address is required.' });
  if (!newBuyer.firstName)  return res.status(400).json({ error: 'Buyer first name is required.' });
  if (!newBuyer.email || !isValidEmail(newBuyer.email)) return res.status(400).json({ error: 'Buyer email is invalid.' });
  if (!newBuyer.phone || !isValidPhone(newBuyer.phone)) return res.status(400).json({ error: 'Buyer phone is invalid.' });
  if (newBA.email && !isValidEmail(newBA.email)) return res.status(400).json({ error: "Buyer's agent email is invalid." });
  if (newBA.phone && !isValidPhone(newBA.phone)) return res.status(400).json({ error: "Buyer's agent phone is invalid." });
  if (newSA.email && !isValidEmail(newSA.email)) return res.status(400).json({ error: "Listing agent email is invalid." });
  if (newSA.phone && !isValidPhone(newSA.phone)) return res.status(400).json({ error: "Listing agent phone is invalid." });
  if (ALL_SLOTS.indexOf(newTime) === -1)         return res.status(400).json({ error: 'Time slot is invalid.' });
  if (!/^\d{4}-\d{2}-\d{2}$/.test(newDate))      return res.status(400).json({ error: 'Date must be YYYY-MM-DD.' });
  if (newSqft <= 0)                              return res.status(400).json({ error: 'Square footage is required.' });

  // ── Recompute pricing ─────────────────────────────────────
  // computePrice gives us base + age + addons + heat from authoritative tables.
  const priced = computePrice({ sqft: newSqft, yearBuilt: newYear, addons: newAddons, date: newDate, time: newTime });
  if (!priced) return res.status(400).json({ error: 'Invalid square footage for pricing.' });

  // Base = override if set, else computed
  let totalPrice = priceOverride !== null ? priceOverride : priced.price;

  // Apply discount (code beats manual %)
  let discountPctApplied = null;
  let discountAmt = null;
  if (newDiscountCode) {
    try {
      const c = await pool.query('SELECT pct FROM discount_codes WHERE UPPER(code) = $1 LIMIT 1', [newDiscountCode]);
      if (c.rows.length) {
        discountPctApplied = Math.max(0, Math.min(100, Number(c.rows[0].pct)||0));
        discountAmt = Math.round(totalPrice * discountPctApplied / 100);
        totalPrice  = Math.max(0, totalPrice - discountAmt);
      }
    } catch(_) {}
  } else if (manualDiscountPct !== null && manualDiscountPct > 0) {
    discountPctApplied = manualDiscountPct;
    discountAmt = Math.round(totalPrice * manualDiscountPct / 100);
    totalPrice  = Math.max(0, totalPrice - discountAmt);
  }

  // Recompute trip charge / mileage if address changed
  let tripCharge, miles;
  const addressChanged = newAddress.trim() !== (old.address || '').trim();
  if (addressChanged) {
    const trip = await checkTripCharge(newAddress);
    tripCharge = trip;
    miles = (trip.miles !== null && trip.miles !== undefined)
      ? Math.round(trip.miles * 2 * 100) / 100
      : null;
  } else {
    tripCharge = old.tripCharge || { apply: false, miles: null, city: null };
    miles = row.miles != null ? Number(row.miles) : null;
  }
  const finalPrice = tripCharge.apply ? totalPrice + TRIP_CHARGE_AMT : totalPrice;

  // Recompute dateFmt + duration
  const sm = slotToMins(newTime);
  const slotH = Math.floor(sm/60), slotM = sm%60;
  const startDT = new Date(`${newDate}T${String(slotH).padStart(2,'0')}:${String(slotM).padStart(2,'0')}:00-07:00`);
  const totalMins = priced.mins;
  const endDT = new Date(startDT.getTime() + totalMins*60000);
  const dateFmt = startDT.toLocaleDateString('en-US',{ weekday:'long', year:'numeric', month:'long', day:'numeric', timeZone: TIMEZONE });
  // endTime label e.g. "10:30 AM"
  const endTime = endDT.toLocaleTimeString('en-US', { timeZone: TIMEZONE, hour: 'numeric', minute: '2-digit' });

  const SVC = {
    'pre-purchase':'Pre-Purchase Inspection','pre-listing':'Pre-Listing Inspection',
    'new-construction':'New Construction Inspection','warranty':'Pre-One Year Warranty Inspection','reinspection':'Re-Inspection',
  };
  const svcLabel = SVC[newInspType] || old.svcLabel || newInspType;
  const addonsLine = newAddons.length ? newAddons.map(function(a){ return a.name; }).join(', ') : 'None';

  // ── Merge data ────────────────────────────────────────────
  // Preserve immutable fields (confId, agreementToken, calId, createdAt, confirmedAt).
  const newData = {
    ...old,
    address: newAddress,
    sqft: newSqft,
    yearBuilt: newYear,
    inspType: newInspType,
    svcLabel,
    addons: newAddons,
    addonsLine,
    totalPrice,
    finalPrice,
    totalMins,
    date: newDate,
    time: newTime,
    endTime,
    dateFmt,
    fullName: (newBuyer.firstName + ' ' + newBuyer.lastName).trim(),
    buyer: newBuyer,
    buyerAgent: (newBA.name || newBA.email || newBA.phone) ? newBA : null,
    sellerAgent: (newSA.name || newSA.email || newSA.phone) ? newSA : null,
    notes: newNotes,
    internalNotes: newInternalNotes,
    discountCode: newDiscountCode,
    discountPct: discountPctApplied,
    discountAmount: discountAmt,
    priceOverride,
    tripCharge,
    miles,
  };

  // ── Save to DB ────────────────────────────────────────────
  try {
    if (isPaidNow) {
      // COALESCE keeps the existing paid_at if already paid; only stamps fresh if unpaid.
      await pool.query(
        `UPDATE confirmed_bookings
            SET data = $1::jsonb,
                miles = $2,
                paid_at = COALESCE(paid_at, NOW()),
                payment_method = $3
          WHERE conf_id = $4`,
        [JSON.stringify(newData), miles, paymentMethod, confId]
      );
    } else {
      await pool.query(
        `UPDATE confirmed_bookings
            SET data = $1::jsonb,
                miles = $2,
                paid_at = NULL,
                payment_method = NULL
          WHERE conf_id = $3`,
        [JSON.stringify(newData), miles, confId]
      );
    }
  } catch(e) {
    console.error('Admin edit save:', e.message);
    return res.status(500).json({ error: 'Save failed: ' + e.message });
  }

  // ── Calendar update ───────────────────────────────────────
  // Update the existing event if calId is stored. If anything fails, log + move on
  // — DB is the source of truth.
  const calVisibleChanged = (
    newDate !== old.date ||
    newTime !== old.time ||
    newAddress !== old.address ||
    svcLabel !== old.svcLabel ||
    newData.fullName !== old.fullName
  );
  const editCalId = (opId === 'jeff') ? (process.env.CALENDAR_ID_JEFF || null) : CALENDAR_ID;
  if (old.calId && calVisibleChanged && editCalId) {
    try {
      const descLines = [
        'BUYER',
        '  ' + newData.fullName + (newBuyer.phone ? '  |  ' + newBuyer.phone : '') + (newBuyer.email ? '  |  ' + newBuyer.email : ''),
        '',
        (newBA.name || newBA.email || newBA.phone)
          ? 'BUYER\u2019S AGENT\n  ' + newBA.name + (newBA.brokerage ? '  \u2014  ' + newBA.brokerage : '') + (newBA.phone ? '  |  ' + newBA.phone : '') + (newBA.email ? '  |  ' + newBA.email : '')
          : 'BUYER\u2019S AGENT\n  None provided',
        '',
        (newSA.name || newSA.email || newSA.phone)
          ? 'LISTING AGENT\n  ' + newSA.name + (newSA.brokerage ? '  \u2014  ' + newSA.brokerage : '') + (newSA.phone ? '  |  ' + newSA.phone : '') + '\n'
          : null,
        'SERVICES (Total: $' + finalPrice + (tripCharge.apply ? ', incl. trip charge' : '') + ')',
        '  ' + svcLabel + (newAddons.length ? '  +  ' + addonsLine : ''),
        '',
        'DETAILS',
        '  Conf #: ' + confId,
        '  Year Built: ' + (newYear || '\u2014'),
        '  Square Footage: ' + (newSqft || '\u2014'),
        (newNotes || newInternalNotes) ? '  Notes: ' + [newNotes, newInternalNotes].filter(Boolean).join(' | ') : null,
        '',
        '[Edited via admin ' + new Date().toLocaleString('en-US', { timeZone: TIMEZONE }) + ']',
      ].filter(function(x){ return x !== null && x !== undefined; }).join('\n');

      await calendar.events.update({
        calendarId: editCalId,
        eventId: old.calId,
        sendUpdates: 'none',  // Suppress Google's own "event changed" emails — we send our own when notifyClient is true.
        resource: {
          summary: svcLabel + ' \u2014 ' + newData.fullName,
          location: newAddress,
          description: descLines,
          start: { dateTime: startDT.toISOString(), timeZone: TIMEZONE },
          end:   { dateTime: endDT.toISOString(),   timeZone: TIMEZONE },
        },
      });
      console.log('Calendar event updated: ' + old.calId);
    } catch(e) {
      console.warn('Calendar update failed (DB still saved):', e.message);
    }
  }

  // ── Notify client if requested ───────────────────────────
  let notified = false;
  if (notifyClient && newBuyer.email) {
    try {
      const BASE_URL = process.env.RAILWAY_URL || 'https://santanproperty-backend-production.up.railway.app';
      const hubUrl = old.agreementToken
        ? withSig(BASE_URL + '/i/' + old.agreementToken, old.agreementToken)
        : null;

      // Build a "what changed" list. Keep it factual and short.
      const changes = [];
      if (newDate !== old.date)         changes.push('Date');
      if (newTime !== old.time)         changes.push('Time');
      if (newAddress !== old.address)   changes.push('Address');
      if (svcLabel !== old.svcLabel)    changes.push('Service');
      if (addonsLine !== old.addonsLine) changes.push('Add-Ons');
      if (Number(finalPrice) !== Number(old.finalPrice)) changes.push('Price');
      const changesLine = changes.length ? changes.join(', ') : 'Booking details';

      const notifyHtml = emailWrap(
        '<h2 style="color:#0F1C35">Inspection Details Updated</h2>'
        + '<p>Hi ' + escapeHtml(newBuyer.firstName) + ', we wanted to let you know we have updated your inspection. The current details are below:</p>'
        + '<p style="color:#888;font-size:.85rem"><strong style="color:#1B2D52">What changed:</strong> ' + escapeHtml(changesLine) + '</p>'
        + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
        + '<tr><td style="padding:6px 0;color:#888;width:130px">Service</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(svcLabel) + (newAddons.length ? ' + ' + escapeHtml(addonsLine) : '') + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(dateFmt) + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(newTime) + (endTime ? ' to ' + escapeHtml(endTime) : '') + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Property</td><td style="color:#2C2C2C;font-weight:600">' + escapeHtml(newAddress) + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Est. Total</td><td style="color:#C9A84C;font-weight:700">$' + (Number(finalPrice)||0) + (tripCharge.apply ? ' (incl. $' + TRIP_CHARGE_AMT + ' trip charge)' : '') + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Confirmation</td><td style="color:#C9A84C;font-weight:700">' + escapeHtml(confId) + '</td></tr>'
        + '</table>'
        + (hubUrl
            ? '<div style="background:#EAF3FB;border-left:4px solid #1B2D52;padding:14px 18px;margin:20px 0;border-radius:0 8px 8px 0">'
              + '<p style="margin:0 0 10px;font-size:.88rem;color:#1B2D52"><strong>Your Inspection Hub</strong> has the latest details:</p>'
              + '<a href="' + hubUrl + '" style="display:inline-block;background:#1B2D52;color:white;padding:11px 22px;border-radius:6px;text-decoration:none;font-weight:700;font-size:.85rem">Open Inspection Hub</a>'
              + '</div>'
            : '')
        + '<p>If anything looks wrong or you have questions, please call or text <strong>' + opCfg.phone + '</strong>.</p>'
      );

      await sendEmail(
        newBuyer.email,
        'Inspection Updated — ' + dateFmt + ' @ ' + newTime + ' [' + confId + ']',
        notifyHtml,
        null,
        opCfg.replyTo
      );
      notified = true;
    } catch(e) {
      console.error('Notify client email failed:', e.message);
    }
  }

  res.json({ success: true, notified });
});

// ── START ─────────────────────────────────────────────────────
initDb().then(function() {
  app.listen(PORT, function(){ console.log('San Tan Property Inspections backend on port ' + PORT); });
});

// Process-level safety net — catches anything that escaped route handlers.
// Without these, an uncaught exception silently crashes the Node process and
// Railway restarts cold (mid-flight bookings lost, no signal in logs).
process.on('unhandledRejection', function(reason) {
  console.error('[UNHANDLED REJECTION]', reason && reason.stack ? reason.stack : reason);
});
process.on('uncaughtException', function(err) {
  console.error('[UNCAUGHT EXCEPTION]', err && err.stack ? err.stack : err);
  // Don't exit — log and keep running. Railway will restart if process actually dies.
});
