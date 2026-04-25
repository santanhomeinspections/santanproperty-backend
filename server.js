/**
 * San Tan Property Inspections — Backend Server v3
 * + Agreement signature flow (sign online, PDF to R2, DB tracking)
 */

require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const { google } = require('googleapis');
const twilio     = require('twilio');
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

// ── TWILIO ────────────────────────────────────────────────────
const tw      = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
const TW_FROM = process.env.TWILIO_PHONE_NUMBER;

function fmtPhone(raw) {
  const d = raw.replace(/\D/g,'');
  if (d.length === 10)               return '+1' + d;
  if (d.length === 11 && d[0]==='1') return '+' + d;
  return null;
}

async function sms(to, body) {
  const num = fmtPhone(to);
  if (!num) { console.warn('Bad phone, skipping SMS:', to); return; }
  try {
    await tw.messages.create({ from: TW_FROM, to: num, body });
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
async function sendEmail(to, subject, html) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(function(){ controller.abort(); }, 8000);
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      signal: controller.signal,
      headers: {
        'Authorization': 'Bearer ' + process.env.RESEND_API_KEY,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        from: 'San Tan Property Inspections <noreply@santanpropertyinspections.com>',
        reply_to: 'santanpropertyinspections@gmail.com',
        to: to,
        subject: subject,
        html: html,
      }),
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

// ── AGREEMENT TEXT ────────────────────────────────────────────
// Versioned agreement text — stored with each signed record
const AGREEMENT_VERSION = '2025-v1';
const AGREEMENT_TEXT = `SAN TAN PROPERTY INSPECTIONS
Licensed Home Inspector — License #79346
Jaren Drummond
823 W Leadwood Ave, San Tan Valley, AZ 85140
(480) 418-7633 | santanpropertyinspections@gmail.com | santanpropertyinspections.com

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
An electronic signature shall be deemed valid and binding to the same extent as a handwritten signature.

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
  <p>You are all set. We look forward to seeing you at the inspection.<br>Questions? Call or text <strong>(480) 418-7633</strong>.</p>
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
  <div class="sub">Licensed Home Inspector — License #79346 &nbsp;|&nbsp; Jaren Drummond<br>
  823 W Leadwood Ave, San Tan Valley, AZ 85140 &nbsp;|&nbsp; (480) 418-7633 &nbsp;|&nbsp; santanpropertyinspections.com</div>
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

// ── ROUTES ────────────────────────────────────────────────────
app.get('/api/health', function(req, res){ res.json({ status:'ok', ts: new Date().toISOString() }); });

app.get('/api/availability', async function(req, res) {
  const date = req.query.date;
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date))
    return res.status(400).json({ error: 'Use YYYY-MM-DD' });

  const inspDuration   = parseInt(req.query.duration) || 360;
  const BUFFER_MINS    = 90;
  const totalBlockMins = inspDuration + BUFFER_MINS;

  try {
    const timeMin = `${date}T00:00:00-07:00`;
    const timeMax = `${date}T23:59:59-07:00`;
    const [mainResp, blockResp] = await Promise.all([
      calendar.events.list({ calendarId: CALENDAR_ID, timeMin, timeMax, singleEvents: true, orderBy: 'startTime' }),
      calendar.events.list({ calendarId: BLOCK_CALENDAR_ID, timeMin, timeMax, singleEvents: true, orderBy: 'startTime' }).catch(function(e){
        console.warn('Block calendar fetch failed:', e.message);
        return { data: { items: [] } };
      }),
    ]);
    const allItems = [...(mainResp.data.items||[]), ...(blockResp.data.items||[])];
    const dayBlocked = allItems.some(function(ev){ return ev.start && ev.start.date && !ev.start.dateTime; });
    if (dayBlocked) return res.json({ date, booked: ALL_SLOTS, available: [], dayBlocked: true });

    const booked = [];
    for (let s = 0; s < ALL_SLOTS.length; s++) {
      const slot    = ALL_SLOTS[s];
      const slotMins = slotToMins(slot);
      const slotH   = Math.floor(slotMins/60), slotM = slotMins%60;
      const slotStart     = new Date(`${date}T${String(slotH).padStart(2,'0')}:${String(slotM).padStart(2,'0')}:00-07:00`);
      const slotWindowEnd = new Date(slotStart.getTime() + totalBlockMins * 60000);

      for (let i = 0; i < allItems.length; i++) {
        const ev = allItems[i];
        if (!ev.start || !ev.start.dateTime) continue;
        const evStart = new Date(ev.start.dateTime);
        const evEnd   = ev.end && ev.end.dateTime ? new Date(ev.end.dateTime) : new Date(evStart.getTime() + 60*60000);
        const eventStartsInWindow = evStart >= slotStart && evStart < slotWindowEnd;
        const eventOverlapsSlot   = evStart <= slotStart && evEnd > slotStart;
        if ((eventStartsInWindow || eventOverlapsSlot) && !booked.includes(slot)) {
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
      res.status(504).json({ error: 'Request timed out. Please try again or call (480) 418-7633.' });
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
  if(!buyerAgent||!buyerAgent.name) miss.push('buyerAgent.name');
  if(!buyerAgent||!buyerAgent.phone) miss.push('buyerAgent.phone');
  if(miss.length) return res.status(400).json({ error:'Missing: '+miss.join(', ') });

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
    + '<p><b>Buyer Agent:</b> ' + buyerAgent.name + (buyerAgent.brokerage ? ' — ' + buyerAgent.brokerage : '') + '<br>Phone: ' + buyerAgent.phone + '</p>'
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
    booking = await dbGet(req.params.token);
  } catch(e) {
    console.error('DB read error:', e.message);
    return res.send('<h2>Database error. Please try again or call (480) 418-7633.</h2>');
  }
  if (!booking) return res.send('<h2>This confirmation link has expired or already been used.</h2>');

  const { confId, address, sqft, yearBuilt, svcLabel, addons, addonsLine, finalPrice, totalMins, date, time, endTime, dateFmt, fullName, buyer, buyerAgent, sellerAgent, notes, extraEmails, discountCode, discountPct, discountAmount, tripCharge } = booking;

  try { await dbDelete(req.params.token); } catch(e) { console.error('DB delete error:', e.message); }

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
      'BUYERS AGENT: ' + buyerAgent.name + (buyerAgent.brokerage ? ' — ' + buyerAgent.brokerage : '') + ' | ' + buyerAgent.phone,
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

    // Save to confirmed_bookings
    try {
      await pool.query(
        'INSERT INTO confirmed_bookings (conf_id, data) VALUES ($1, $2) ON CONFLICT (conf_id) DO NOTHING',
        [confId, JSON.stringify({ ...booking, calId, confirmedAt: new Date().toISOString() })]
      );
    } catch(e) { console.error('DB confirmed save:', e.message); }
  } catch(e) { console.error('Calendar:', e.message); }

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
    'Hi ' + buyer.firstName + '! Your inspection is confirmed.\n\nAddress: ' + address + '\nDate: ' + dateFmt + '\nTime: ' + time + (endTime ? ' to ' + endTime : '') + '\nService: ' + svcLabel + (addons.length ? '\nAdd-ons: ' + addonsLine : '') + '\nEst. Total: $' + finalPrice + ' (pay day-of)' + (tripCharge.apply ? ' incl. $' + TRIP_CHARGE_AMT + ' trip charge' : '') + '\nConf #: ' + confId + '\n\nPlease sign your inspection agreement:\n' + agreementUrl + '\n\nQuestions? (480) 418-7633 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
  );

  // SMS - buyer agent
  await sms(buyerAgent.phone,
    'Hi ' + buyerAgent.name + '! Inspection scheduled for your buyer.\n\nAddress: ' + address + '\nBuyer: ' + fullName + '\nDate: ' + dateFmt + ' @ ' + time + '\nService: ' + svcLabel + '\nConf #: ' + confId + '\n\nACTION NEEDED — Confirm with seller\'s agent:\n- Seller\'s agent aware of date & time\n- GAS on & accessible\n- WATER on & accessible\n- ELECTRICAL on & accessible\n- ATTIC ACCESS clear & accessible\n\nQuestions? (480) 418-7633 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
  );

  if (sellerAgent && sellerAgent.phone) {
    await sms(sellerAgent.phone,
      'Hello' + (sellerAgent.name ? ' ' + sellerAgent.name : '') + '! Inspection scheduled at your listing.\n\nAddress: ' + address + '\nDate: ' + dateFmt + ' @ ' + time + '\nService: ' + svcLabel + '\n\nPlease ensure by inspection day:\n- GAS on & accessible\n- WATER on & accessible\n- ELECTRICAL on & accessible\n- ATTIC ACCESS clear & accessible\n\nWARNING: If utilities are NOT on, a $125 re-inspection fee will apply.\n\nQuestions? (480) 418-7633 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
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
    + '<p>Questions? Call/text <strong>(480) 418-7633</strong></p>'
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
      + '<p>Questions? Call/text <strong>(480) 418-7633</strong></p>'
    );
    for (const email of extraEmails) {
      try {
        await sendEmail(email, 'Inspection Confirmed — ' + dateFmt + ' @ ' + time + ' [' + confId + ']', extraHtml);
      } catch(e) { console.error('Extra recipient email to ' + email + ':', e.message); }
    }
  }

  // Buyer agent email
  if (buyerAgent && buyerAgent.email) {
    const baHtml = emailWrap(
      '<h2 style="color:#0F1C35">Inspection Confirmed for Your Buyer</h2>'
      + '<p>Hi ' + buyerAgent.name + ',</p>'
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
      + '<p>Questions? Call/text <strong>(480) 418-7633</strong></p>'
    );
    try {
      await sendEmail(buyerAgent.email, 'Inspection Confirmed — ' + fullName + ' — ' + dateFmt + ' @ ' + time, baHtml);
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
      + '<p style="background:#FFF3CD;padding:10px;border-radius:6px"><strong>Note:</strong> If utilities are not on at the time of inspection, a $125 re-inspection fee will apply.</p>'
      + '<p>Questions? Call/text <strong>(480) 418-7633</strong></p>'
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
    <strong>Texts sent to:</strong> ${fullName} &middot; ${buyerAgent.name}${sellerAgent && sellerAgent.name ? ' &middot; ' + sellerAgent.name : ''}<br>
    <strong>Emails sent to:</strong> ${buyer.email}${buyerAgent.email ? ' &middot; ' + buyerAgent.email : ''}
  </div>
  <div class="footer">
    <a href="https://santanpropertyinspections.com">santanpropertyinspections.com</a> &nbsp;&middot;&nbsp; (480) 418-7633 &nbsp;&middot;&nbsp; License #79346
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
    return res.status(500).send('<h2>Database error. Please call (480) 418-7633.</h2>');
  }

  if (!booking) {
    return res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Agreement — San Tan Property Inspections</title></head>
<body style="font-family:sans-serif;text-align:center;padding:60px 24px;background:#0F1C35;color:#fff">
<h2>This agreement link has expired or already been signed.</h2>
<p style="margin-top:12px;color:#aaa">If you need to sign your agreement, please contact us at (480) 418-7633.</p>
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
    return res.status(500).send('<h2>Database error. Please call (480) 418-7633.</h2>');
  }

  if (!booking) {
    return res.send('<h2>This agreement link has expired. Please contact us at (480) 418-7633.</h2>');
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
    return res.status(500).send('<h2>Could not save signature. Please call (480) 418-7633.</h2>');
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
        + '<p>Questions? Call or text <strong>(480) 418-7633</strong></p>'
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
    return res.send('<h2>Database error. Please try again or call (480) 418-7633.</h2>');
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

// ── SMS REPLY ─────────────────────────────────────────────────
app.post('/sms/reply', express.urlencoded({ extended: false }), async function(req, res) {
  const from  = req.body.From || 'Unknown';
  const body  = req.body.Body || '';
  if (process.env.OWNER_PHONE) await sms(process.env.OWNER_PHONE, 'Reply from ' + from + ':\n' + body);
  res.set('Content-Type', 'text/xml');
  res.send('<Response></Response>');
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

  <div class="card" style="margin-bottom:20px">
    <div class="card-hd"><h2>Discount Codes</h2></div>
    <div style="padding:16px 20px;border-bottom:1px solid #243660;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <input id="newCode" placeholder="Code (e.g. AGENT15)" style="background:#243660;border:1px solid #344870;color:#fff;padding:8px 12px;border-radius:6px;font-size:.83rem;width:160px;outline:none"/>
      <input id="newPct" placeholder="% off" type="number" min="1" max="100" style="background:#243660;border:1px solid #344870;color:#fff;padding:8px 12px;border-radius:6px;font-size:.83rem;width:80px;outline:none"/>
      <button onclick="addCode()" style="background:#C9A84C;color:#0F1C35;border:none;border-radius:6px;padding:8px 18px;font-weight:700;font-size:.83rem;cursor:pointer">Add Code</button>
    </div>
    <div id="codeList"><div class="empty">Loading...</div></div>
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
        const signedBadge = isCancelled ? '' : (isSigned
          ? '<span class="badge badge-signed">Signed</span>'
          : '<span class="badge badge-unsigned">Unsigned</span>');
        const paidBtn = isCancelled
          ? '<span style="color:#C0392B;font-size:.72rem;font-weight:700">CANCELLED</span>'
          : (isPaid
            ? '<button data-action="unpaid" data-id="'+bd.confId+'" style="background:#243660;color:#8A9AB5;border:1px solid #344870;border-radius:5px;padding:4px 10px;cursor:pointer;font-size:.72rem;margin-top:4px">&#10003; Paid &mdash; Undo</button>'
            : '<button data-action="paid" data-id="'+bd.confId+'" style="background:#1ab464;color:white;border:none;border-radius:5px;padding:4px 10px;cursor:pointer;font-size:.72rem;margin-top:4px">Mark as Paid</button>');
        const cancelBtn = isCancelled ? '' : '<button data-action="cancel" data-id="'+bd.confId+'" style="background:none;color:#C0392B;border:1px solid #C0392B;border-radius:5px;padding:4px 10px;cursor:pointer;font-size:.72rem;margin-top:4px;margin-left:4px">Cancel</button>';
        return '<tr>' +
          '<td><div class="conf">'+bd.confId+'</div><div style="font-size:.72rem;color:#4A5A7A;margin-top:2px">'+dt+'</div></td>' +
          '<td><div class="name">'+(bd.fullName||'')+'</div><div class="addr">'+(bd.address||'')+'</div></td>' +
          '<td><div class="svc">'+(bd.svcLabel||'')+'</div><div class="svc" style="margin-top:2px">'+addons+'</div></td>' +
          '<td><div class="agent">'+(bd.buyerAgent?bd.buyerAgent.name:'—')+'</div><div class="svc">'+(bd.buyerAgent&&bd.buyerAgent.brokerage?bd.buyerAgent.brokerage:'')+'</div></td>' +
          '<td><div class="price">'+discBadge+tripBadge+'$'+(bd.finalPrice||'—')+'</div><div style="font-size:.72rem;color:#4A5A7A">'+(bd.dateFmt||'')+' @ '+(bd.time||'')+'</div><div style="margin-top:4px">'+signedBadge+'</div><div>'+paidBtn+cancelBtn+'</div></td>' +
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

// Event delegation for booking action buttons
document.addEventListener('click', function(e) {
  const btn = e.target.closest('button[data-action]');
  if (!btn) return;
  const action = btn.dataset.action;
  const id = btn.dataset.id;
  const code = btn.dataset.code;
  if (action === 'paid') markPaid(id);
  if (action === 'unpaid') markUnpaid(id);
  if (action === 'cancel') cancelBooking(id);
  if (action === 'deletecode') deleteCode(code);
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
    + '<p>&#128222; <strong>(480) 418-7633</strong><br>&#9993; <strong>santanpropertyinspections@gmail.com</strong></p>'
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
      + '<p>Questions? Call/text <strong>(480) 418-7633</strong></p>'
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
    const [bookings, reschedules, codes] = await Promise.all([
      pool.query('SELECT *, agreement_signed_at, agreement_signature FROM confirmed_bookings ORDER BY confirmed_at DESC'),
      pool.query('SELECT * FROM reschedule_requests ORDER BY requested_at DESC'),
      pool.query('SELECT * FROM discount_codes ORDER BY created_at DESC'),
    ]);
    res.json({ bookings: bookings.rows, reschedules: reschedules.rows, codes: codes.rows });
  } catch(e) {
    res.status(500).json({ error: 'DB error' });
  }
});

// ── START ─────────────────────────────────────────────────────
initDb().then(function() {
  app.listen(PORT, function(){ console.log('San Tan Property Inspections backend on port ' + PORT); });
});
