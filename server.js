/**
 * San Tan Property Inspections — Backend Server v3
 * + Agreement signature flow (sign online, PDF to R2, DB tracking)
 */

require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const { google } = require('googleapis');
const { v4: uuidv4 } = require('uuid');
const { Pool }   = require('pg');

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

// ── GEO / TRIP CHARGE ─────────────────────────────────────────
const SERVICE_CITIES = ['chandler','gilbert','mesa','tempe','queen creek','san tan valley','florence','apache junction'];
const BASE_LAT = 33.1534;
const BASE_LNG = -111.5368;
const TRIP_CHARGE_MILES = 50;
const TRIP_CHARGE_AMT   = 50;

function toRad(d){ return d * Math.PI / 180; }
function milesBetween(lat1,lng1,lat2,lng2){
  const R=3958.8, dLat=toRad(lat2-lat1), dLng=toRad(lng2-lng1);
  const a=Math.sin(dLat/2)**2+Math.cos(toRad(lat1))*Math.cos(toRad(lat2))*Math.sin(dLng/2)**2;
  return R*2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a));
}

async function checkTripCharge(address) {
  const addrLower = address.toLowerCase();
  const inServiceArea = SERVICE_CITIES.some(function(c){ return addrLower.includes(c); });
  if (inServiceArea) return { apply: false, miles: 0 };
  try {
    const encoded = encodeURIComponent(address);
    const url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + encoded + '&key=' + process.env.GOOGLE_MAPS_API_KEY;
    const controller = new AbortController();
    const timeout = setTimeout(function(){ controller.abort(); }, 4000);
    const r = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);
    const data = await r.json();
    if (data.results && data.results[0]) {
      const loc = data.results[0].geometry.location;
      const miles = milesBetween(BASE_LAT, BASE_LNG, loc.lat, loc.lng);
      if (miles >= TRIP_CHARGE_MILES) return { apply: true, miles: Math.round(miles) };
    }
  } catch(e) { console.warn('Geocode failed:', e.message); }
  return { apply: false, miles: 0 };
}

// ── EMAIL ─────────────────────────────────────────────────────
async function sendEmail(to, subject, html, attachments) {
  try {
    const controller = new AbortController();
    // 15s — bumped from 8s because attachments make the request larger
    const timeout = setTimeout(function(){ controller.abort(); }, 15000);
    const body = {
      from: 'San Tan Property Inspections <noreply@santanpropertyinspections.com>',
      reply_to: 'santanpropertyinspections@gmail.com',
      to: to,
      subject: subject,
      html: html,
    };
    if (attachments && attachments.length) body.attachments = attachments;
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

// ── AGREEMENT TEXT ────────────────────────────────────────────
// Versioned agreement text — stored with each signed record
const AGREEMENT_VERSION = '2026-v2';
const AGREEMENT_TEXT = `SAN TAN PROPERTY INSPECTIONS
Certified Home Inspector — BTR #79346
Jaren Drummond
823 W Leadwood Ave, San Tan Valley, AZ 85140
(480) 618-0805 | santanpropertyinspections@gmail.com | santanpropertyinspections.com

HOME INSPECTION AGREEMENT

1. SCOPE OF INSPECTION
The Inspector will perform a non-invasive visual inspection of the accessible systems and components of the property in accordance with the Standards of Professional Practice for Arizona Home Inspectors as adopted by the Arizona State Board of Technical Registration (available at btr.az.gov). The inspection will produce a written report identifying material defects observed at the time of inspection. The report will be delivered the same day as the inspection.

The standard inspection covers: Roof; Exterior; Electrical System; Plumbing System; Basement, Foundation and Structure; Garage; Heating and Cooling; Doors, Windows and Interior; Insulation and Ventilation; Built-In Kitchen Appliances.

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

// ── AGREEMENT PAGE ────────────────────────────────────────────
function buildAgreementPage(booking, token, opts = {}) {
  const { confId, address, date, time, dateFmt, fullName, buyer, addonsLine, finalPrice } = booking;
  const { signed = false, error = null } = opts;

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
  <p>Thank you, ${buyer.firstName}. Your inspection agreement has been signed and recorded.</p>
  <div class="conf">
    <strong>Confirmation:</strong> ${confId}<br>
    <strong>Property:</strong> ${address}<br>
    <strong>Date:</strong> ${dateFmt} @ ${time}
  </div>
  <p>You are all set. We look forward to seeing you at the inspection.<br>Questions? Call or text <strong>(480) 618-0805</strong>.</p>
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
      <strong>Client:</strong> ${fullName}<br>
      <strong>Property:</strong> ${address}<br>
      <strong>Inspection Date:</strong> ${dateFmt} @ ${time}<br>
      ${addonsDisplay ? '<strong>Add-Ons:</strong> ' + addonsDisplay + '<br>' : ''}
      <strong>Estimated Total:</strong> $${finalPrice}<br>
      <strong>Confirmation #:</strong> ${confId}
    </div>

    <div class="section-title">Agreement</div>
    <div class="agreement-box">${AGREEMENT_TEXT}</div>

    ${error ? '<div class="error">' + error + '</div>' : ''}

    <form method="POST" action="/agreement/${token}/sign" id="agreementForm">
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

// ── AGREEMENT PDF GENERATION ──────────────────────────────────
async function generateAgreementPdf(booking, signedAt, signature, ip) {
  // Use puppeteer (already available in santan-inspector) to generate PDF
  // We build a simple HTML page and render it
  const { confId, address, dateFmt, time, fullName, buyer, finalPrice, addonsLine } = booking;
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
  <div class="sub">Certified Home Inspector — BTR #79346 &nbsp;|&nbsp; Jaren Drummond<br>
  823 W Leadwood Ave, San Tan Valley, AZ 85140 &nbsp;|&nbsp; (480) 618-0805 &nbsp;|&nbsp; santanpropertyinspections.com</div>
</div>

<h1>HOME INSPECTION AGREEMENT</h1>

<div class="info-grid">
  <span class="lbl">Client</span><span class="val">${fullName}</span>
  <span class="lbl">Property</span><span class="val">${address}</span>
  <span class="lbl">Inspection Date</span><span class="val">${dateFmt} @ ${time}</span>
  <span class="lbl">Add-On Services</span><span class="val">${addonsLine || 'None'}</span>
  <span class="lbl">Est. Total</span><span class="val">$${finalPrice}</span>
  <span class="lbl">Confirmation #</span><span class="val">${confId}</span>
</div>

<div class="agreement-text">${AGREEMENT_TEXT}</div>

<div class="sig-block">
  <h2>Electronic Signature Record</h2>
  <div class="sig-row">
    <div>
      <div class="lbl">Client Printed Name</div>
      <div class="val">${fullName}</div>
    </div>
    <div>
      <div class="lbl">Electronic Signature</div>
      <div class="val">${signature}</div>
    </div>
    <div>
      <div class="lbl">Date Signed</div>
      <div class="val">${signedDate} (AZ)</div>
    </div>
  </div>
  <div class="record">
    <strong>Signature Record:</strong> This agreement was electronically signed on ${signedDate} (Arizona Time).
    IP Address: ${ip} &nbsp;|&nbsp; Agreement Version: ${AGREEMENT_VERSION} &nbsp;|&nbsp; Confirmation: ${confId}<br>
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
  <div class="sub">Certified Home Inspector — BTR #79346 &nbsp;|&nbsp; Jaren Drummond<br>
  823 W Leadwood Ave, San Tan Valley, AZ 85140 &nbsp;|&nbsp; (480) 618-0805 &nbsp;|&nbsp; santanpropertyinspections.com</div>
</div>

<h1>HOME INSPECTION AGREEMENT</h1>

<div class="info-grid">
  <span class="lbl">Client</span><span class="val">${fullName}</span>
  <span class="lbl">Property</span><span class="val">${address}</span>
  <span class="lbl">Inspection Date</span><span class="val">${dateFmt} @ ${time}</span>
  <span class="lbl">Add-On Services</span><span class="val">${addonsLine || 'None'}</span>
  <span class="lbl">Est. Total</span><span class="val">$${finalPrice}</span>
  <span class="lbl">Confirmation #</span><span class="val">${confId}</span>
</div>

<div class="agreement-text">${AGREEMENT_TEXT}</div>

<div class="sig-block">
  <h2>Signatures of the Parties</h2>
  <div class="parties">
    <div class="party">
      <h3>Client</h3>
      <div class="row">
        <div class="lbl">Electronic Signature</div>
        <div class="val">${signature}</div>
      </div>
      <div class="row">
        <div class="lbl">Printed Name</div>
        <div class="meta">${fullName}</div>
      </div>
      <div class="row">
        <div class="lbl">Date Signed</div>
        <div class="meta">${signedDate} (AZ)</div>
      </div>
      <div class="row">
        <div class="lbl">IP Address</div>
        <div class="meta">${ip}</div>
      </div>
    </div>
    <div class="party">
      <h3>Inspector (Counter-Signature)</h3>
      <div class="row">
        <div class="lbl">Electronic Signature</div>
        <div class="val">${counterSignedBy}</div>
      </div>
      <div class="row">
        <div class="lbl">Printed Name</div>
        <div class="meta">${counterSignedBy}</div>
      </div>
      <div class="row">
        <div class="lbl">Date Counter-Signed</div>
        <div class="meta">${counterSignedDate} (AZ)</div>
      </div>
      <div class="row">
        <div class="lbl">License</div>
        <div class="meta">AZ BTR #79346</div>
      </div>
    </div>
  </div>
  <div class="record">
    <strong>Execution Record:</strong> This agreement is fully executed.
    Client electronically signed on ${signedDate}. Inspector counter-signed on ${counterSignedDate}.
    Both signatures are legally binding pursuant to the terms of Section 9 of this Agreement.<br>
    Agreement Version: ${AGREEMENT_VERSION} &nbsp;|&nbsp; Confirmation: ${confId} &nbsp;|&nbsp; Client IP: ${ip}
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

app.post('/api/book', async function(req, res) {
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
  const { address, sqft, yearBuilt, inspType, totalPrice, totalMins, date, time, endTime, buyer, buyerAgent, sellerAgent, notes } = b;
  const addons         = b.addons || [];
  const extraEmails    = (b.extraEmails || []).filter(function(e){ return e && e.trim(); });
  const discountCode   = b.discountCode   || null;
  const discountPct    = b.discountPct    || null;
  const discountAmount = b.discountAmount || null;

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
    const chk = await calendar.events.list({ calendarId: CALENDAR_ID, timeMin: startDT.toISOString(), timeMax: endDT.toISOString(), singleEvents: true });
    if ((chk.data.items||[]).length) return res.status(409).json({ error:'That slot was just booked — please choose another.' });
  } catch(e) { console.warn('Slot check failed:', e.message); }

  const token      = uuidv4();
  const trip       = await checkTripCharge(address);
  const finalPrice = trip.apply ? totalPrice + TRIP_CHARGE_AMT : totalPrice;

  const bookingData = { confId, address, sqft, yearBuilt, inspType, svcLabel, addons, addonsLine, totalPrice, finalPrice, totalMins, date, time, endTime, dateFmt, fullName, buyer, buyerAgent, sellerAgent, notes, extraEmails, discountCode, discountPct, discountAmount, tripCharge: trip, createdAt: Date.now() };

  try {
    await dbSet(token, bookingData);
  } catch(e) {
    console.error('DB write failed:', e.message);
    return res.status(500).json({ error: 'Could not save booking. Please try again.' });
  }

  const BASE_URL   = process.env.RAILWAY_URL || 'https://santanproperty-backend-production.up.railway.app';
  const confirmUrl = BASE_URL + '/confirm/' + token;
  const cancelUrl  = BASE_URL + '/cancel/'  + token;

  const sellerLineOwner    = sellerAgent && sellerAgent.name ? '<p><b>Seller Agent:</b> ' + sellerAgent.name + (sellerAgent.brokerage ? ' — ' + sellerAgent.brokerage : '') + '<br>Phone: ' + (sellerAgent.phone||'—') + '</p>' : '';
  const tripLineOwner      = trip.apply ? '<p style="background:#FFF3CD;padding:10px;border-radius:6px">Trip charge: $' + TRIP_CHARGE_AMT + ' (' + trip.miles + ' miles)</p>' : '';
  const notesLineOwner     = notes ? '<p><b>Notes:</b> ' + notes + '</p>' : '';
  const extraEmailsLineOwner = extraEmails.length ? '<p><b>Extra Report Recipients:</b> ' + extraEmails.join(', ') + '</p>' : '';
  const discountLineOwner  = discountCode ? '<p style="background:#e8f7ee;padding:10px;border-radius:6px"><b>Discount Code:</b> ' + discountCode + ' (' + discountPct + '% off — −$' + discountAmount + ')</p>' : '';

  const ownerHtml = '<div style="font-family:Arial,sans-serif;max-width:560px">'
    + '<h2>New Booking Request — ' + confId + '</h2>'
    + '<p><b>Service:</b> ' + svcLabel + '<br><b>Add-ons:</b> ' + addonsLine + '</p>'
    + '<p><b>Date/Time:</b> ' + dateFmt + ' @ ' + time + (endTime ? ' to ' + endTime : '') + '</p>'
    + '<p><b>Address:</b> ' + address + '<br><b>Sq Ft:</b> ' + sqft + ' / <b>Year:</b> ' + yearBuilt + '</p>'
    + '<p><b>Est. Total:</b> $' + finalPrice + (trip.apply ? ' (incl. $' + TRIP_CHARGE_AMT + ' trip charge)' : '') + '</p>'
    + '<hr/>'
    + '<p><b>Buyer:</b> ' + fullName + '<br>Phone: ' + buyer.phone + '<br>Email: ' + buyer.email + '</p>'
    + (hasBA ? '<p><b>Buyer Agent:</b> ' + baName + (baBrok ? ' — ' + baBrok : '') + (baPhone ? '<br>Phone: ' + baPhone : '') + (baEmail ? '<br>Email: ' + baEmail : '') + '</p>' : '<p><b>Buyer Agent:</b> <i>None provided</i></p>')
    + sellerLineOwner + notesLineOwner + extraEmailsLineOwner + discountLineOwner + tripLineOwner
    + '<div style="margin:28px 0">'
    + '<a href="' + confirmUrl + '" style="background:#1B2D52;color:white;padding:14px 28px;border-radius:6px;text-decoration:none;font-weight:700">CONFIRM AND SEND TEXTS</a>'
    + '&nbsp;&nbsp;'
    + '<a href="' + cancelUrl + '" style="background:#C0392B;color:white;padding:14px 28px;border-radius:6px;text-decoration:none;font-weight:700">CANCEL</a>'
    + '</div>'
    + '<p style="color:#888;font-size:.8rem">Texts will NOT go out until you tap Confirm.</p>'
    + '</div>';

  sendEmail(process.env.OWNER_EMAIL, 'PENDING BOOKING: ' + fullName + ' — ' + dateFmt + ' @ ' + time, ownerHtml)
    .then(function(){ console.log('Owner alert sent for ' + confId); })
    .catch(function(e){ console.error('Owner alert email:', e.message); });

  res.json({ success: true, confirmationId: confId, message: 'Request received! You will be confirmed shortly.' });

  const ownerSmsBody = 'NEW BOOKING — ' + confId + '\n' + fullName + '\n' + address + '\n' + dateFmt + ' @ ' + time + '\n' + svcLabel + '\n$' + finalPrice + (trip.apply ? ' (incl. trip charge)' : '') + '\n\nCONFIRM:\n' + confirmUrl + '\n\nCANCEL:\n' + cancelUrl;
  sms(process.env.OWNER_PHONE, ownerSmsBody).catch(function(e){ console.error('Owner SMS:', e.message); });
});

// ── CONFIRM BOOKING ───────────────────────────────────────────
app.get('/confirm/:token', async function(req, res) {
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
  try {
    const descLines = [
      'Conf: ' + confId, 'Service: ' + svcLabel,
      addons.length ? 'Add-ons: ' + addonsLine : null,
      'Address: ' + address, 'Sq Ft: ' + sqft + ' | Year: ' + yearBuilt,
      'Total: $' + finalPrice + (tripCharge.apply ? ' (incl. trip charge)' : ''), '',
      'BUYER: ' + fullName + ' | ' + buyer.phone + ' | ' + buyer.email,
      hasBA ? 'BUYERS AGENT: ' + baName + (baBrok ? ' — ' + baBrok : '') + (baPhone ? ' | ' + baPhone : '') : 'BUYERS AGENT: None provided',
      sellerAgent && sellerAgent.name ? 'SELLERS AGENT: ' + sellerAgent.name + (sellerAgent.brokerage ? ' — ' + sellerAgent.brokerage : '') + ' | ' + (sellerAgent.phone||'—') : null,
      notes ? 'Notes: ' + notes : null,
      extraEmails && extraEmails.length ? 'Extra report recipients: ' + extraEmails.join(', ') : null,
      discountCode ? 'Discount: ' + discountCode + ' (' + discountPct + '% off — −$' + discountAmount + ')' : null,
    ].filter(Boolean).join('\n');

    const ev = {
      summary: svcLabel + ' — ' + fullName, location: address, description: descLines,
      start: { dateTime: startDT2.toISOString(), timeZone: TIMEZONE },
      end:   { dateTime: endDT2.toISOString(),   timeZone: TIMEZONE },
      colorId: '5',
      attendees: [{ email: buyer.email, displayName: fullName }],
      reminders: { useDefault: false, overrides: [{ method:'email', minutes:24*60 },{ method:'popup', minutes:60 }] },
    };
    const r = await calendar.events.insert({ calendarId: CALENDAR_ID, resource: ev, sendUpdates:'all' });
    calId = r.data.id;
    console.log('Calendar event created: ' + calId);
  } catch(e) { console.error('Calendar:', e.message); }

  // Save to confirmed_bookings — runs regardless of Calendar success/failure
  try {
    await pool.query(
      'INSERT INTO confirmed_bookings (conf_id, data) VALUES ($1, $2) ON CONFLICT (conf_id) DO NOTHING',
      [confId, JSON.stringify({ ...booking, calId, confirmedAt: new Date().toISOString() })]
    );
    console.log('Confirmed booking saved to DB: ' + confId);
  } catch(e) { console.error('DB confirmed save:', e.message); }

  // Generate agreement token for this confirmed booking
  const agreeToken = uuidv4();
  const BASE_URL = process.env.RAILWAY_URL || 'https://santanproperty-backend-production.up.railway.app';
  const agreementUrl = BASE_URL + '/agreement/' + agreeToken;

  // Store agreement token temporarily so we can look up booking from it
  try {
    await pool.query(
      'UPDATE confirmed_bookings SET agreement_sent_at = NOW(), data = data || $1::jsonb WHERE conf_id = $2',
      [JSON.stringify({ agreementToken: agreeToken }), confId]
    );
    // Also store in pending_bookings temporarily for agreement lookup
    await dbSet('agree_' + agreeToken, { ...booking, calId, confirmedAt: new Date().toISOString(), agreeToken });
  } catch(e) { console.error('Agreement token store error:', e.message); }

  // SMS - buyer
  await sms(buyer.phone,
    'Hi ' + buyer.firstName + '! Your inspection is confirmed.\n\nAddress: ' + address + '\nDate: ' + dateFmt + '\nTime: ' + time + (endTime ? ' to ' + endTime : '') + '\nService: ' + svcLabel + (addons.length ? '\nAdd-ons: ' + addonsLine : '') + '\nEst. Total: $' + finalPrice + ' (pay day-of)' + (tripCharge.apply ? ' incl. $' + TRIP_CHARGE_AMT + ' trip charge' : '') + '\nConf #: ' + confId + '\n\nPlease sign your inspection agreement:\n' + agreementUrl + '\n\nQuestions? (480) 618-0805 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
  );

  // SMS - buyer agent (only if phone provided)
  if (baPhone) {
    await sms(baPhone,
      'Hi ' + (baName || 'there') + '! Inspection scheduled for your buyer.\n\nAddress: ' + address + '\nBuyer: ' + fullName + '\nDate: ' + dateFmt + ' @ ' + time + '\nService: ' + svcLabel + '\nConf #: ' + confId + '\n\nACTION NEEDED — Confirm with seller\'s agent:\n- Seller\'s agent aware of date & time\n- GAS on & accessible\n- WATER on & accessible\n- ELECTRICAL on & accessible\n- ATTIC ACCESS clear & accessible\n\nQuestions? (480) 618-0805 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
    );
  }

  if (sellerAgent && sellerAgent.phone) {
    await sms(sellerAgent.phone,
      'Hello' + (sellerAgent.name ? ' ' + sellerAgent.name : '') + '! Inspection scheduled at your listing.\n\nAddress: ' + address + '\nDate: ' + dateFmt + ' @ ' + time + '\nService: ' + svcLabel + '\n\nPlease ensure by inspection day:\n- GAS on & accessible\n- WATER on & accessible\n- ELECTRICAL on & accessible\n- ATTIC ACCESS clear & accessible\n\nIMPORTANT: Please send the CBS code so I can access the home, and reply to this message to confirm the inspection.\n\nWARNING: If utilities are NOT on, a $125 re-inspection fee will apply.\n\nQuestions? (480) 618-0805 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
    );
  }

  const tripLineBuyer = tripCharge.apply ? ' (incl. $' + TRIP_CHARGE_AMT + ' trip charge)' : '';

  // Buyer confirmation email — includes agreement link
  const buyerHtml = emailWrap(
    '<h2 style="color:#0F1C35">Inspection Confirmed</h2>'
    + '<p>Hi ' + buyer.firstName + ', here are your booking details:</p>'
    + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
    + '<tr><td style="padding:6px 0;color:#888;width:130px">Service</td><td style="color:#2C2C2C;font-weight:600">' + svcLabel + (addons.length ? ' + ' + addonsLine : '') + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + dateFmt + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + time + (endTime ? ' to ' + endTime : '') + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Property</td><td style="color:#2C2C2C;font-weight:600">' + address + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Est. Total</td><td style="color:#C9A84C;font-weight:700">$' + finalPrice + tripLineBuyer + '</td></tr>'
    + '<tr><td style="padding:6px 0;color:#888">Confirmation</td><td style="color:#C9A84C;font-weight:700">' + confId + '</td></tr>'
    + '</table>'
    + '<p>Payment can be made on inspection day. We accept cash, Venmo, Zelle, or credit/debit card.</p>'
    + '<div style="background:#EAF3FB;border-left:4px solid #1B2D52;padding:16px 18px;margin:20px 0;border-radius:0 8px 8px 0">'
    + '<p style="margin:0 0 8px;font-size:.92rem;font-weight:700;color:#1B2D52">ACTION REQUIRED: Sign Your Inspection Agreement</p>'
    + '<p style="margin:0 0 12px;font-size:.84rem;color:#555;line-height:1.6">Your report will not be released until your agreement is signed. Please take a moment to review and sign before inspection day.</p>'
    + '<a href="' + agreementUrl + '" style="display:inline-block;background:#1B2D52;color:white;padding:12px 24px;border-radius:6px;text-decoration:none;font-weight:700;font-size:.88rem">Review &amp; Sign Agreement</a>'
    + '</div>'
    + '<p>Your report will be delivered the <strong>same day</strong> as your inspection.</p>'
    + '<p>Questions? Call/text <strong>(480) 618-0805</strong></p>'
    + '<hr style="border:none;border-top:1px solid #E8DFC8;margin:20px 0"/>'
    + '<div style="background:#FAF7F0;border-radius:8px;padding:16px;margin-top:8px">'
    + '<p style="font-size:.82rem;color:#8C7B6B;margin-bottom:10px"><strong style="color:#1B2D52">Need to reschedule?</strong> Fill out the form below and Jaren will reach out to find a new time.</p>'
    + '<form action="https://santanproperty-backend-production.up.railway.app/api/reschedule" method="POST" style="display:flex;flex-direction:column;gap:8px">'
    + '<input type="hidden" name="confId" value="' + confId + '"/>'
    + '<input type="hidden" name="name" value="' + buyer.firstName + ' ' + buyer.lastName + '"/>'
    + '<input type="hidden" name="phone" value="' + buyer.phone + '"/>'
    + '<input type="hidden" name="email" value="' + buyer.email + '"/>'
    + '<textarea name="message" rows="2" placeholder="Preferred dates/times or reason for rescheduling..." style="padding:8px;border:1px solid #E2D9C8;border-radius:6px;font-family:Georgia,serif;font-size:.83rem;resize:vertical"></textarea>'
    + '<button type="submit" style="background:#1B2D52;color:white;padding:9px 20px;border:none;border-radius:6px;font-size:.83rem;font-weight:700;cursor:pointer;align-self:flex-start">Request Reschedule</button>'
    + '</form>'
    + '</div>'
  );

  try {
    await sendEmail(buyer.email, 'Inspection Confirmed — ' + dateFmt + ' @ ' + time + ' [' + confId + ']', buyerHtml);
  } catch(e) { console.error('Buyer email:', e.message); }

  // Extra recipients email
  if (extraEmails && extraEmails.length) {
    const extraHtml = emailWrap(
      '<h2 style="color:#0F1C35">Inspection Confirmed</h2>'
      + '<p>You have been added as a report recipient for the following inspection:</p>'
      + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
      + '<tr><td style="padding:6px 0;color:#888;width:130px">Buyer</td><td style="color:#2C2C2C;font-weight:600">' + fullName + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Service</td><td style="color:#2C2C2C;font-weight:600">' + svcLabel + (addons.length ? ' + ' + addonsLine : '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + dateFmt + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + time + (endTime ? ' to ' + endTime : '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Property</td><td style="color:#2C2C2C;font-weight:600">' + address + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Confirmation</td><td style="color:#C9A84C;font-weight:700">' + confId + '</td></tr>'
      + '</table>'
      + '<p>The inspection report will be delivered the <strong>same day</strong> as the inspection.</p>'
      + '<p>Questions? Call/text <strong>(480) 618-0805</strong></p>'
    );
    for (const email of extraEmails) {
      try {
        await sendEmail(email, 'Inspection Confirmed — ' + dateFmt + ' @ ' + time + ' [' + confId + ']', extraHtml);
      } catch(e) { console.error('Extra recipient email to ' + email + ':', e.message); }
    }
  }

  // Buyer agent email
  if (baEmail) {
    const baHtml = emailWrap(
      '<h2 style="color:#0F1C35">Inspection Confirmed for Your Buyer</h2>'
      + '<p>Hi ' + (baName || 'there') + ',</p>'
      + '<p>The inspection for your buyer has been confirmed. Details below:</p>'
      + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
      + '<tr><td style="padding:6px 0;color:#888;width:130px">Buyer</td><td style="color:#2C2C2C;font-weight:600">' + fullName + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Property</td><td style="color:#2C2C2C;font-weight:600">' + address + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + dateFmt + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + time + (endTime ? ' to ' + endTime : '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Service</td><td style="color:#2C2C2C;font-weight:600">' + svcLabel + (addons.length ? ' + ' + addonsLine : '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Confirmation</td><td style="color:#C9A84C;font-weight:700">' + confId + '</td></tr>'
      + '</table>'
      + '<p>Please confirm the seller\'s agent is aware of the inspection date and that <strong>gas, water, electrical, and attic access are on &amp; accessible</strong>.</p>'
      + '<p>Questions? Call/text <strong>(480) 618-0805</strong></p>'
    );
    try {
      await sendEmail(baEmail, 'Inspection Confirmed — ' + fullName + ' — ' + dateFmt + ' @ ' + time, baHtml);
    } catch(e) { console.error('Buyer agent email:', e.message); }
  }

  // Seller agent email
  if (sellerAgent && sellerAgent.email) {
    const sellerHtml = emailWrap(
      '<h2 style="color:#0F1C35">Inspection Scheduled at Your Listing</h2>'
      + '<p>Hi ' + (sellerAgent.name || 'there') + ',</p>'
      + '<p>A home inspection has been scheduled at your listing. Please ensure the following are ready by inspection day:</p>'
      + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
      + '<tr><td style="padding:6px 0;color:#888;width:130px">Property</td><td style="color:#2C2C2C;font-weight:600">' + address + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + dateFmt + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + time + (endTime ? ' to ' + endTime : '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Service</td><td style="color:#2C2C2C;font-weight:600">' + svcLabel + '</td></tr>'
      + '</table>'
      + '<ul style="margin:12px 0 12px 20px"><li>Gas on &amp; accessible</li><li>Water on &amp; accessible</li><li>Electrical on &amp; accessible</li><li>Attic access clear &amp; accessible</li></ul>'
      + '<div style="background:#EAF3FB;border-left:4px solid #1B2D52;padding:12px 16px;border-radius:0 8px 8px 0;margin:14px 0"><p style="margin:0;font-size:.92rem"><strong style="color:#1B2D52">Important:</strong> Please send the <strong>CBS code</strong> so I can access the home, and reply to confirm the inspection.</p></div>'
      + '<p style="background:#FFF3CD;padding:10px;border-radius:6px"><strong>Note:</strong> If utilities are not on at the time of inspection, a $125 re-inspection fee will apply.</p>'
      + '<p>Questions? Call/text <strong>(480) 618-0805</strong></p>'
    );
    try {
      await sendEmail(sellerAgent.email, 'Inspection Scheduled — ' + address + ' on ' + dateFmt, sellerHtml);
    } catch(e) { console.error('Seller agent email:', e.message); }
  }

  // Owner confirmation page
  const tripConfirmLine = tripCharge.apply ? '<tr><td style="padding:6px 0;color:#888;width:130px">Trip Charge</td><td style="color:#C9A84C;font-weight:600">+$' + TRIP_CHARGE_AMT + ' (' + tripCharge.miles + ' miles)</td></tr>' : '';
  const discountConfirmLine = discountCode ? '<tr><td style="padding:6px 0;color:#888">Discount</td><td style="color:#1ab464;font-weight:600">' + discountCode + ' (-$' + discountAmount + ')</td></tr>' : '';

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
  <p class="sub">Confirmation texts and emails have been sent to the buyer and agents.</p>
  <div class="conf-badge">Confirmation # ${confId}</div>
  <table>
    <tr><td>Buyer</td><td>${fullName}</td></tr>
    <tr><td>Property</td><td>${address}</td></tr>
    <tr><td>Service</td><td>${svcLabel}${addons.length ? ' + ' + addonsLine : ''}</td></tr>
    <tr><td>Date</td><td>${dateFmt}</td></tr>
    <tr><td>Time</td><td>${time}${endTime ? ' to ' + endTime : ''}</td></tr>
    ${tripConfirmLine}${discountConfirmLine}
    <tr class="total-row"><td>Total</td><td>$${finalPrice}</td></tr>
  </table>
  <div class="agree-notice">
    <strong>Agreement link sent to buyer.</strong> The report will be locked until the agreement is signed. You can check signature status in the admin dashboard.
  </div>
  <div class="notice">
    <strong>Texts sent to:</strong> ${fullName}${baPhone ? ' &middot; ' + baName : ''}${sellerAgent && sellerAgent.name ? ' &middot; ' + sellerAgent.name : ''}<br>
    <strong>Emails sent to:</strong> ${buyer.email}${baEmail ? ' &middot; ' + baEmail : ''}
  </div>
  <div class="footer">
    <a href="https://santanpropertyinspections.com">santanpropertyinspections.com</a> &nbsp;&middot;&nbsp; (480) 618-0805 &nbsp;&middot;&nbsp; BTR #79346
  </div>
</div>
</body>
</html>`);
});

// ── AGREEMENT ROUTES ──────────────────────────────────────────

// Show agreement page
app.get('/agreement/:token', async function(req, res) {
  const token = req.params.token;
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
app.post('/agreement/:token/sign', async function(req, res) {
  const token = req.params.token;
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
        + '<p><b>' + booking.fullName + '</b> has signed the inspection agreement.</p>'
        + '<p><b>Conf #:</b> ' + booking.confId + '<br>'
        + '<b>Property:</b> ' + booking.address + '<br>'
        + '<b>Inspection:</b> ' + booking.dateFmt + ' @ ' + booking.time + '<br>'
        + '<b>Signed:</b> ' + signedDate + ' (AZ)<br>'
        + '<b>Signature:</b> ' + signature + '<br>'
        + '<b>IP:</b> ' + ip + '</p>'
        + '</div>';
      await sendEmail(process.env.OWNER_EMAIL, 'AGREEMENT SIGNED: ' + booking.fullName + ' [' + booking.confId + ']', ownerHtml);
    } catch(e) { console.error('Owner agreement notification email:', e.message); }

    // Send client confirmation email
    try {
      const clientHtml = emailWrap(
        '<h2 style="color:#0F1C35">Agreement Signed</h2>'
        + '<p>Hi ' + booking.buyer.firstName + ', this confirms that you have signed the inspection agreement for:</p>'
        + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
        + '<tr><td style="padding:6px 0;color:#888;width:130px">Property</td><td style="color:#2C2C2C;font-weight:600">' + booking.address + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Inspection Date</td><td style="color:#2C2C2C;font-weight:600">' + booking.dateFmt + ' @ ' + booking.time + '</td></tr>'
        + '<tr><td style="padding:6px 0;color:#888">Confirmation #</td><td style="color:#C9A84C;font-weight:700">' + booking.confId + '</td></tr>'
        + '</table>'
        + '<p>Your agreement has been recorded. You are all set for your inspection.</p>'
        + '<p>Questions? Call or text <strong>(480) 618-0805</strong></p>'
      );
      await sendEmail(booking.buyer.email, 'Agreement Signed — ' + booking.confId, clientHtml);
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

// ── CANCEL BOOKING ────────────────────────────────────────────
app.get('/cancel/:token', async function(req, res) {
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
app.post('/api/contact', async function(req, res) {
  const { name, phone, email, role, message } = req.body;
  if (!name || !email || !message) return res.status(400).json({ error: 'Name, email, and message are required.' });

  const roleLabel = {
    'buyer': 'Buyer / Homeowner',
    'agent': "Buyer's Agent",
    'seller': 'Seller / Listing Agent',
    'other': 'Other'
  }[role] || role || 'Not specified';

  const html = '<div style="font-family:Arial,sans-serif;max-width:520px">'
    + '<h2 style="color:#1B2D52">New Contact Form Submission</h2>'
    + '<p><b>Name:</b> ' + name + '</p>'
    + '<p><b>Role:</b> ' + roleLabel + '</p>'
    + (phone ? '<p><b>Phone:</b> ' + phone + '</p>' : '')
    + '<p><b>Email:</b> ' + email + '</p>'
    + '<p><b>Message:</b></p><p style="background:#FAF7F0;padding:12px;border-radius:6px;border-left:4px solid #C9A84C">' + message + '</p>'
    + '<p style="color:#888;font-size:.85rem;margin-top:16px">Reply directly to this email to respond to ' + name + '.</p>'
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
app.post('/api/reschedule', async function(req, res) {
  const { confId, name, phone, email, message } = req.body;
  if (!name || !email) return res.status(400).json({ error: 'Name and email required.' });

  try {
    await pool.query(
      'INSERT INTO reschedule_requests (conf_id, name, phone, email, message) VALUES ($1,$2,$3,$4,$5)',
      [confId||null, name, phone||null, email, message||null]
    );
  } catch(e) { console.error('Reschedule DB:', e.message); }

  const rHtml = '<div style="font-family:Arial,sans-serif;max-width:520px">'
    + '<h2 style="color:#1B2D52">Reschedule Request</h2>'
    + '<p><b>From:</b> ' + name + '</p>'
    + (confId ? '<p><b>Conf #:</b> ' + confId + '</p>' : '')
    + (phone ? '<p><b>Phone:</b> ' + phone + '</p>' : '')
    + '<p><b>Email:</b> ' + email + '</p>'
    + (message ? '<p><b>Message:</b> ' + message + '</p>' : '')
    + '</div>';

  try {
    await sendEmail(process.env.OWNER_EMAIL, 'RESCHEDULE REQUEST: ' + name + (confId ? ' [' + confId + ']' : ''), rHtml);
  } catch(e) { console.error('Reschedule email:', e.message); }

  res.json({ success: true });
});

// ── ADMIN DASHBOARD ───────────────────────────────────────────
const ADMIN_PASSWORD = 'monroe';

function checkAdmin(req) {
  const auth = req.headers['authorization'];
  const pass = auth && auth.startsWith('Basic ') ? Buffer.from(auth.slice(6), 'base64').toString().split(':')[1] : null;
  return pass === ADMIN_PASSWORD;
}

app.get('/admin', function(req, res) {
  if (!checkAdmin(req)) {
    res.set('WWW-Authenticate', 'Basic realm="San Tan Admin"');
    return res.status(401).send('Unauthorized');
  }

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
  <span id="lastRefresh"></span>
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

  <div class="card" style="margin-bottom:20px">
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
async function load() {
  try {
    const r = await fetch('/admin/data');
    const d = await r.json();

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
      '<div class="stat"><div class="lbl">Reschedule Requests</div><div class="val" style="color:'+(d.reschedules.length>0?'#e8a87c':'#C9A84C')+'">'+d.reschedules.length+'</div><div class="sub">open requests</div></div>';

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
        return '<tr>' +
          '<td><div class="conf">'+bd.confId+'</div><div style="font-size:.72rem;color:#4A5A7A;margin-top:2px">'+dt+'</div></td>' +
          '<td><div class="name">'+(bd.fullName||'')+'</div><div class="addr">'+(bd.address||'')+'</div></td>' +
          '<td><div class="svc">'+(bd.svcLabel||'')+'</div><div class="svc" style="margin-top:2px">'+addons+'</div></td>' +
          '<td><div class="agent">'+(bd.buyerAgent&&bd.buyerAgent.name?bd.buyerAgent.name:'—')+'</div><div class="svc">'+(bd.buyerAgent&&bd.buyerAgent.brokerage?bd.buyerAgent.brokerage:'')+'</div></td>' +
          '<td><div class="price">'+discBadge+tripBadge+'$'+(bd.finalPrice||'—')+'</div><div style="font-size:.72rem;color:#4A5A7A">'+(bd.dateFmt||'')+' @ '+(bd.time||'')+'</div><div style="margin-top:4px">'+signedBadge+pdfLink+counterSignUi+'</div><div>'+payDropdown+cancelBtn+'</div></td>' +
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
          '<td><div class="name">'+r.name+'</div><div class="svc">'+dt+'</div></td>' +
          '<td>'+(r.conf_id?'<span class="conf">'+r.conf_id+'</span>':'—')+'</td>' +
          '<td><div class="svc">'+(r.email||'—')+'</div><div class="svc">'+(r.phone||'—')+'</div></td>' +
          '<td><div class="resc-msg">'+(r.message||'No message provided.')+'</div></td>' +
          '</tr>';
      }).join('');
      document.getElementById('rescheduleTable').innerHTML = '<table><thead><tr><th>Name</th><th>Conf #</th><th>Contact</th><th>Message</th></tr></thead><tbody>'+rrows+'</tbody></table>';
    }

    document.getElementById('lastRefresh').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch(e) { console.error(e); }
}

async function cancelBooking(confId) {
  if (!confirm('Cancel booking ' + confId + '? This will delete the calendar event and email the buyer and agent.')) return;
  const r = await fetch('/admin/cancel-booking', { method:'POST', headers:{'Content-Type':'application/json','Authorization':'Basic ' + btoa(':monroe')}, body: JSON.stringify({confId}) });
  const data = await r.json();
  if (data.success) { alert('Booking cancelled. Cancellation emails sent.'); load(); }
  else { alert('Error: ' + (data.error||'Unknown error')); }
}

async function markPaid(confId) {
  await fetch('/admin/mark-paid', { method:'POST', headers:{'Content-Type':'application/json','Authorization':'Basic ' + btoa(':monroe')}, body: JSON.stringify({confId}) });
  load();
}
async function markUnpaid(confId) {
  if (!confirm('Mark as unpaid?')) return;
  await fetch('/admin/mark-unpaid', { method:'POST', headers:{'Content-Type':'application/json','Authorization':'Basic ' + btoa(':monroe')}, body: JSON.stringify({confId}) });
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
  await fetch('/admin/codes/add', { method:'POST', headers:{'Content-Type':'application/json','Authorization':'Basic ' + btoa(':monroe')}, body: JSON.stringify({code,pct}) });
  document.getElementById('newCode').value='';
  document.getElementById('newPct').value='';
  load();
}

async function deleteCode(code) {
  if (!confirm('Remove code ' + code + '?')) return;
  await fetch('/admin/codes/delete', { method:'POST', headers:{'Content-Type':'application/json','Authorization':'Basic ' + btoa(':monroe')}, body: JSON.stringify({code}) });
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
      '<td><div class="conf">' + (d.confId || '—') + '</div><div style="font-size:.72rem;color:#4A5A7A;margin-top:2px">' + ageStr + '</div></td>' +
      '<td><div class="name">' + (d.fullName || '') + '</div><div class="addr">' + (d.address || '') + '</div></td>' +
      '<td><div class="svc">' + (d.svcLabel || '') + '</div></td>' +
      '<td><div style="font-size:.85rem;color:#E8DEC4">' + (d.dateFmt || '') + '</div><div style="font-size:.72rem;color:#4A5A7A">@ ' + (d.time || '') + '</div></td>' +
      '<td><div class="price">$' + (d.finalPrice || '—') + '</div><button data-action="delete-pending" data-token="' + r.token + '" style="background:none;color:#C0392B;border:1px solid #C0392B;border-radius:5px;padding:4px 10px;cursor:pointer;font-size:.72rem;margin-top:4px">Delete</button></td>' +
      '</tr>';
  }).join('');
  el.innerHTML = '<table><thead><tr><th>Conf #</th><th>Customer / Address</th><th>Service</th><th>Date / Time</th><th>Price</th></tr></thead><tbody>' + rowsHtml + '</tbody></table>';
}

async function deletePending(token) {
  if (!confirm('Delete this pending booking? This frees the slot for other customers.')) return;
  const r = await fetch('/admin/delete-pending', { method:'POST', headers:{'Content-Type':'application/json','Authorization':'Basic ' + btoa(':monroe')}, body: JSON.stringify({token}) });
  const data = await r.json();
  if (data.success) { load(); }
  else { alert('Error: ' + (data.error || 'Unknown')); }
}

async function clearAllPending() {
  if (!confirm('Clear ALL pending bookings? This frees every locked slot from bookings that you never tapped CONFIRM on. Use this for cleaning up after testing.')) return;
  const r = await fetch('/admin/clear-all-pending', { method:'POST', headers:{'Authorization':'Basic ' + btoa(':monroe')} });
  const data = await r.json();
  if (data.success) { alert('Cleared ' + data.deleted + ' pending booking(s).'); load(); }
  else { alert('Error: ' + (data.error || 'Unknown')); }
}

async function setPayment(confId, method) {
  // method: '' = unpaid, 'cash'|'card'|'venmo'|'zelle' = paid w/ method, '__unpaid' = unpaid
  const r = await fetch('/admin/set-payment', { method:'POST', headers:{'Content-Type':'application/json','Authorization':'Basic ' + btoa(':monroe')}, body: JSON.stringify({confId, method}) });
  load();
}

async function hardDelete(confId) {
  if (!confirm('PERMANENTLY DELETE booking ' + confId + '? This removes it from the database. Use this for test bookings only.')) return;
  const r = await fetch('/admin/hard-delete-booking', { method:'POST', headers:{'Content-Type':'application/json','Authorization':'Basic ' + btoa(':monroe')}, body: JSON.stringify({confId}) });
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
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Basic ' + btoa(':monroe') },
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
app.post('/admin/cancel-booking', async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId } = req.body;
  if (!confId) return res.status(400).json({ error: 'No confId' });

  let booking;
  try {
    const r = await pool.query('SELECT * FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    if (!r.rows.length) return res.status(404).json({ error: 'Booking not found' });
    booking = r.rows[0];
  } catch(e) { return res.status(500).json({ error: e.message }); }

  const d = booking.data;

  if (d.calId) {
    try {
      await calendar.events.delete({ calendarId: CALENDAR_ID, eventId: d.calId, sendUpdates: 'none' });
    } catch(e) { console.warn('Calendar delete failed:', e.message); }
  }

  try {
    await pool.query('UPDATE confirmed_bookings SET cancelled_at = NOW() WHERE conf_id = $1', [confId]);
  } catch(e) { console.error('DB cancel update:', e.message); }

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
    + '<p>&#128222; <strong>(480) 618-0805</strong><br>&#9993; <strong>santanpropertyinspections@gmail.com</strong></p>'
  );

  try {
    if (d.buyer && d.buyer.email) await sendEmail(d.buyer.email, 'Inspection Cancelled — ' + confId, buyerHtml);
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
      + '<p>Questions? Call/text <strong>(480) 618-0805</strong></p>'
    );
    try { await sendEmail(d.buyerAgent.email, 'Inspection Cancelled — ' + (d.fullName||'') + ' [' + confId + ']', agentHtml); } catch(e) {}
  }

  res.json({ success: true });
});

app.post('/admin/mark-paid', async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId } = req.body;
  if (!confId) return res.status(400).json({ error: 'No confId' });
  try {
    await pool.query('UPDATE confirmed_bookings SET paid_at = NOW() WHERE conf_id = $1', [confId]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/admin/mark-unpaid', async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId } = req.body;
  try {
    await pool.query('UPDATE confirmed_bookings SET paid_at = NULL WHERE conf_id = $1', [confId]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Set payment method + auto-mark paid (or unpaid if method blank)
app.post('/admin/set-payment', async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId, method } = req.body;
  if (!confId) return res.status(400).json({ error: 'No confId' });
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
app.post('/admin/hard-delete-booking', async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId } = req.body;
  if (!confId) return res.status(400).json({ error: 'No confId' });
  try {
    // Best-effort: try to delete calendar event if one was created
    const r = await pool.query('SELECT data FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    if (r.rows.length && r.rows[0].data && r.rows[0].data.calId) {
      try { await calendar.events.delete({ calendarId: CALENDAR_ID, eventId: r.rows[0].data.calId, sendUpdates: 'none' }); }
      catch(e) { console.warn('Calendar delete (hard-delete):', e.message); }
    }
    await pool.query('DELETE FROM confirmed_bookings WHERE conf_id = $1', [confId]);
    console.log('Hard-deleted booking: ' + confId);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Stream signed agreement PDF from R2 to admin browser
app.get('/admin/agreement-pdf/:confId', async function(req, res) {
  if (!checkAdmin(req)) {
    res.set('WWW-Authenticate', 'Basic realm="San Tan Admin"');
    return res.status(401).send('Unauthorized');
  }
  const { confId } = req.params;
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
app.get('/admin/executed-pdf/:confId', async function(req, res) {
  if (!checkAdmin(req)) {
    res.set('WWW-Authenticate', 'Basic realm="San Tan Admin"');
    return res.status(401).send('Unauthorized');
  }
  const { confId } = req.params;
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
app.post('/admin/counter-sign', async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { confId } = req.body;
  if (!confId) return res.status(400).json({ error: 'No confId' });

  // Owner name comes from env so it stays consistent and we don't trust the browser
  const counterSignedBy = process.env.OWNER_NAME || 'Jaren Drummond';

  try {
    const r = await pool.query(
      'SELECT data, agreement_signed_at, agreement_signature, agreement_ip, counter_signed_at FROM confirmed_bookings WHERE conf_id = $1',
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
        + '<p>Questions? Call or text <strong>(480) 618-0805</strong></p>'
      );
      sendEmail(
        booking.buyer.email,
        'Agreement Fully Executed — ' + confId,
        clientHtml,
        [{ filename: confId + '-executed-agreement.pdf', content: pdfBase64 }]
      ).catch(function(e){ console.error('Client executed-PDF email failed:', e.message); });
    }

    // Owner email (so Jaren has a copy in inbox)
    if (process.env.OWNER_EMAIL) {
      const ownerHtml = '<div style="font-family:Arial,sans-serif;max-width:520px">'
        + '<h2 style="color:#1B2D52">Counter-Signed: ' + (booking.fullName || '') + '</h2>'
        + '<p>You counter-signed the inspection agreement for <strong>' + (booking.address || '') + '</strong>.</p>'
        + '<p>The fully-executed PDF is attached.</p>'
        + '<p><b>Conf #:</b> ' + confId + '<br>'
        + '<b>Counter-signed:</b> ' + counterSignedDate + ' (AZ)</p>'
        + '</div>';
      sendEmail(
        process.env.OWNER_EMAIL,
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

app.get('/admin/csv', async function(req, res) {
  if (!checkAdmin(req)) {
    res.set('WWW-Authenticate', 'Basic realm="San Tan Admin"');
    return res.status(401).send('Unauthorized');
  }
  try {
    const result = await pool.query('SELECT * FROM confirmed_bookings ORDER BY confirmed_at DESC');
    const headers = ['Conf #','Date Confirmed','Inspection Date','Time','Buyer','Buyer Phone','Buyer Email','Address','Service','Add-Ons','Buyer Agent','Agent Phone','Seller Agent','Sq Ft','Year Built','Base Price','Final Price','Discount Code','Discount Amt','Trip Charge','Notes','Paid','Date Paid','Agreement Signed','Date Signed'];
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

app.post('/admin/codes/add', async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { code, pct } = req.body;
  if (!code || !pct || isNaN(pct) || pct < 1 || pct > 100) return res.status(400).json({ error: 'Invalid code or percentage' });
  try {
    await pool.query('INSERT INTO discount_codes (code, pct) VALUES ($1, $2) ON CONFLICT (code) DO UPDATE SET pct = $2', [code.toUpperCase().trim(), parseInt(pct)]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/admin/codes/delete', async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
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

app.get('/admin/data', async function(req, res) {
  if (!checkAdmin(req)) {
    res.set('WWW-Authenticate', 'Basic realm="San Tan Admin"');
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    const [bookings, reschedules, codes, pending] = await Promise.all([
      pool.query('SELECT *, agreement_signed_at, agreement_signature FROM confirmed_bookings ORDER BY confirmed_at DESC'),
      pool.query('SELECT * FROM reschedule_requests ORDER BY requested_at DESC'),
      pool.query('SELECT * FROM discount_codes ORDER BY created_at DESC'),
      pool.query("SELECT token, data, created_at FROM pending_bookings WHERE created_at > NOW() - INTERVAL '48 hours' ORDER BY created_at DESC"),
    ]);
    res.json({ bookings: bookings.rows, reschedules: reschedules.rows, codes: codes.rows, pending: pending.rows });
  } catch(e) {
    res.status(500).json({ error: 'DB error' });
  }
});

// Delete a single pending booking (frees its slot immediately)
app.post('/admin/delete-pending', async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'No token' });
  try {
    await pool.query('DELETE FROM pending_bookings WHERE token = $1', [token]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Clear ALL pending bookings — for cleaning up after testing
app.post('/admin/clear-all-pending', async function(req, res) {
  if (!checkAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const r = await pool.query('DELETE FROM pending_bookings RETURNING token');
    res.json({ success: true, deleted: r.rows.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── START ─────────────────────────────────────────────────────
initDb().then(function() {
  app.listen(PORT, function(){ console.log('San Tan Property Inspections backend on port ' + PORT); });
});
