/**
 * San Tan Property Inspections — Backend Server v2
 * Postgres-backed pending bookings
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
        confirmed_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
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
    const r = await fetch(url);
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
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + process.env.RESEND_API_KEY,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        from: 'San Tan Property Inspections <noreply@santanpropertyinspections.com>',
        to: to,
        subject: subject,
        html: html,
      }),
    });
    const data = await r.json();
    if (!r.ok) throw new Error(JSON.stringify(data));
    return data;
  } catch(e) { throw e; }
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

    // Save to confirmed_bookings for dashboard
    try {
      await pool.query(
        'INSERT INTO confirmed_bookings (conf_id, data) VALUES ($1, $2) ON CONFLICT (conf_id) DO NOTHING',
        [confId, JSON.stringify({ ...booking, calId, confirmedAt: new Date().toISOString() })]
      );
    } catch(e) { console.error('DB confirmed save:', e.message); }
  } catch(e) { console.error('Calendar:', e.message); }

  await sms(buyer.phone,
    'Hi ' + buyer.firstName + '! Your inspection is confirmed.\n\nAddress: ' + address + '\nDate: ' + dateFmt + '\nTime: ' + time + (endTime ? ' to ' + endTime : '') + '\nService: ' + svcLabel + (addons.length ? '\nAdd-ons: ' + addonsLine : '') + '\nEst. Total: $' + finalPrice + ' (pay day-of)' + (tripCharge.apply ? ' incl. $' + TRIP_CHARGE_AMT + ' trip charge' : '') + '\nConf #: ' + confId + '\n\nQuestions? (480) 418-7633 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
  );

  await sms(buyerAgent.phone,
    'Hi ' + buyerAgent.name + '! Inspection scheduled for your buyer.\n\nAddress: ' + address + '\nBuyer: ' + fullName + '\nDate: ' + dateFmt + ' @ ' + time + '\nService: ' + svcLabel + '\nConf #: ' + confId + '\n\nACTION NEEDED — Confirm with seller\'s agent:\n- Seller\'s agent aware of date & time\n- GAS on & accessible\n- WATER on & accessible\n- ELECTRICAL on & accessible\n- ATTIC ACCESS clear & accessible\n\nQuestions? (480) 418-7633 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
  );

  if (sellerAgent && sellerAgent.phone) {
    await sms(sellerAgent.phone,
      'Hello' + (sellerAgent.name ? ' ' + sellerAgent.name : '') + '! Inspection scheduled at your listing.\n\nAddress: ' + address + '\nDate: ' + dateFmt + ' @ ' + time + '\nService: ' + svcLabel + '\n\nPlease ensure by inspection day:\n- GAS on & accessible\n- WATER on & accessible\n- ELECTRICAL on & accessible\n- ATTIC ACCESS clear & accessible\n\nWARNING: If utilities are NOT on, a $125 re-inspection fee will apply.\n\nQuestions? (480) 418-7633 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
    );
  }

  const tripLineBuyer = tripCharge.apply ? ' (incl. $' + TRIP_CHARGE_AMT + ' trip charge)' : '';

  const buyerHtml = '<div style="font-family:Georgia,serif;max-width:580px;margin:0 auto;border-top:4px solid #C9A84C;padding-top:20px">'
    + '<h2 style="color:#0F1C35">Inspection Confirmed</h2>'
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
    + '<hr style="border:none;border-top:1px solid #E8DFC8;margin:20px 0"/>'
    + '<p style="color:#888;font-size:.8rem">San Tan Property Inspections · East Valley, AZ · santanpropertyinspections.com</p>'
    + '</div>';

  try {
    await sendEmail(buyer.email, 'Inspection Confirmed — ' + dateFmt + ' @ ' + time + ' [' + confId + ']', buyerHtml);
  } catch(e) { console.error('Buyer email:', e.message); }

  if (extraEmails && extraEmails.length) {
    const extraHtml = '<div style="font-family:Georgia,serif;max-width:580px;margin:0 auto;border-top:4px solid #C9A84C;padding-top:20px">'
      + '<h2 style="color:#0F1C35">Inspection Confirmed</h2>'
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
      + '<hr style="border:none;border-top:1px solid #E8DFC8;margin:20px 0"/>'
      + '<p style="color:#888;font-size:.8rem">San Tan Property Inspections · East Valley, AZ · santanpropertyinspections.com</p>'
      + '</div>';
    for (const email of extraEmails) {
      try {
        await sendEmail(email, 'Inspection Confirmed — ' + dateFmt + ' @ ' + time + ' [' + confId + ']', extraHtml);
        console.log('Extra recipient email sent to ' + email);
      } catch(e) { console.error('Extra recipient email to ' + email + ':', e.message); }
    }
  }

  if (buyerAgent && buyerAgent.email) {
    const baHtml = '<div style="font-family:Georgia,serif;max-width:580px;margin:0 auto;border-top:4px solid #C9A84C;padding-top:20px">'
      + '<h2 style="color:#0F1C35">Inspection Confirmed for Your Buyer</h2>'
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
      + '<hr style="border:none;border-top:1px solid #E8DFC8;margin:20px 0"/>'
      + '<p style="color:#888;font-size:.8rem">San Tan Property Inspections · East Valley, AZ · santanpropertyinspections.com</p>'
      + '</div>';
    try {
      await sendEmail(buyerAgent.email, 'Inspection Confirmed — ' + fullName + ' — ' + dateFmt + ' @ ' + time, baHtml);
    } catch(e) { console.error('Buyer agent email:', e.message); }
  }

  if (sellerAgent && sellerAgent.email) {
    const sellerHtml = '<div style="font-family:Georgia,serif;max-width:580px;margin:0 auto;border-top:4px solid #C9A84C;padding-top:20px">'
      + '<h2 style="color:#0F1C35">Inspection Scheduled at Your Listing</h2>'
      + '<p>Hi ' + (sellerAgent.name || 'there') + ',</p>'
      + '<p>A home inspection has been scheduled at your listing. Please ensure the following are ready by inspection day:</p>'
      + '<table style="width:100%;border-collapse:collapse;margin:16px 0">'
      + '<tr><td style="padding:6px 0;color:#888;width:130px">Property</td><td style="color:#2C2C2C;font-weight:600">' + address + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">' + dateFmt + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">' + time + (endTime ? ' to ' + endTime : '') + '</td></tr>'
      + '<tr><td style="padding:6px 0;color:#888">Service</td><td style="color:#2C2C2C;font-weight:600">' + svcLabel + '</td></tr>'
      + '</table>'
      + '<p><strong>Please ensure by inspection day:</strong></p>'
      + '<ul><li>Gas on &amp; accessible</li><li>Water on &amp; accessible</li><li>Electrical on &amp; accessible</li><li>Attic access clear &amp; accessible</li></ul>'
      + '<p style="background:#FFF3CD;padding:10px;border-radius:6px"><strong>WARNING:</strong> If utilities are NOT on, a $125 re-inspection fee will apply.</p>'
      + '<p>Questions? Call/text <strong>(480) 418-7633</strong></p>'
      + '<hr style="border:none;border-top:1px solid #E8DFC8;margin:20px 0"/>'
      + '<p style="color:#888;font-size:.8rem">San Tan Property Inspections · East Valley, AZ · santanpropertyinspections.com</p>'
      + '</div>';
    try {
      await sendEmail(sellerAgent.email, 'Inspection Scheduled — ' + address + ' on ' + dateFmt, sellerHtml);
    } catch(e) { console.error('Seller agent email:', e.message); }
  }

  const tripConfirmLine = tripCharge.apply ? '<p>Trip charge of $' + TRIP_CHARGE_AMT + ' applied (' + tripCharge.miles + ' miles)</p>' : '';
  res.send('<div style="font-family:Arial,sans-serif;max-width:500px;margin:60px auto;text-align:center;padding:40px;border-top:4px solid #C9A84C">'
    + '<h2 style="color:#1B2D52">Booking Confirmed!</h2>'
    + '<p>Texts sent to buyer and agents.</p>'
    + '<p><b>' + fullName + '</b><br>' + dateFmt + ' @ ' + time + '<br>' + address + '</p>'
    + '<p style="color:#C9A84C;font-weight:700">Conf: ' + confId + '</p>'
    + tripConfirmLine + '</div>');
});

app.get('/cancel/:token', async function(req, res) {
  let booking;
  try {
    booking = await dbGet(req.params.token);
  } catch(e) {
    console.error('DB read error:', e.message);
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

app.get('/auth/google', function(req, res) {
  res.redirect(oAuth2Client.generateAuthUrl({ access_type:'offline', scope:['https://www.googleapis.com/auth/calendar'], prompt:'consent' }));
});
app.get('/auth/google/callback', async function(req, res) {
  const result = await oAuth2Client.getToken(req.query.code);
  console.log('REFRESH TOKEN:', result.tokens.refresh_token);
  res.send('<pre>Add to Railway as GOOGLE_REFRESH_TOKEN:\n\n' + result.tokens.refresh_token + '</pre>');
});

app.post('/sms/reply', express.urlencoded({ extended: false }), async function(req, res) {
  const from  = req.body.From || 'Unknown';
  const body  = req.body.Body || '';
  if (process.env.OWNER_PHONE) await sms(process.env.OWNER_PHONE, 'Reply from ' + from + ':\n' + body);
  res.set('Content-Type', 'text/xml');
  res.send('<Response></Response>');
});

// ── RESCHEDULE REQUEST ───────────────────────────────────────
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
    + '<p style="color:#888;font-size:.85rem">Reply to this email or call/text the client to reschedule.</p>'
    + '</div>';

  try {
    await sendEmail(process.env.OWNER_EMAIL, 'RESCHEDULE REQUEST: ' + name + (confId ? ' [' + confId + ']' : ''), rHtml);
  } catch(e) { console.error('Reschedule email:', e.message); }

  res.json({ success: true });
});

// ── ADMIN DASHBOARD ───────────────────────────────────────────
const ADMIN_PASSWORD = 'monroe';

app.get('/admin', function(req, res) {
  const auth = req.headers['authorization'];
  const pass = auth && auth.startsWith('Basic ') ? Buffer.from(auth.slice(6), 'base64').toString().split(':')[1] : null;
  if (pass !== ADMIN_PASSWORD) {
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

    // Stats
    const totalRev = d.bookings.reduce(function(s,b){ return s + (b.data.finalPrice||0); }, 0);
    const thisMonth = new Date(); thisMonth.setDate(1); thisMonth.setHours(0,0,0,0);
    const monthJobs = d.bookings.filter(function(b){ return new Date(b.confirmed_at) >= thisMonth; }).length;
    const monthRev  = d.bookings.filter(function(b){ return new Date(b.confirmed_at) >= thisMonth; }).reduce(function(s,b){ return s+(b.data.finalPrice||0);},0);

    // Top agent
    const agentCount = {};
    d.bookings.forEach(function(b){ const n=b.data.buyerAgent&&b.data.buyerAgent.name?b.data.buyerAgent.name:'Unknown'; agentCount[n]=(agentCount[n]||0)+1; });
    const topAgent = Object.entries(agentCount).sort(function(a,b){return b[1]-a[1];})[0];

    document.getElementById('stats').innerHTML =
      '<div class="stat"><div class="lbl">Total Jobs</div><div class="val">'+d.bookings.length+'</div><div class="sub">all time</div></div>' +
      '<div class="stat"><div class="lbl">Total Revenue</div><div class="val">$'+totalRev.toLocaleString()+'</div><div class="sub">all time</div></div>' +
      '<div class="stat"><div class="lbl">This Month</div><div class="val">'+monthJobs+'</div><div class="sub">$'+monthRev.toLocaleString()+' revenue</div></div>' +
      '<div class="stat"><div class="lbl">Top Agent</div><div class="val" style="font-size:1rem;padding-top:4px">'+(topAgent?topAgent[0]:'—')+'</div><div class="sub">'+(topAgent?topAgent[1]+' booking'+(topAgent[1]>1?'s':''):'')+'</div></div>' +
      '<div class="stat"><div class="lbl">Pending Reschedules</div><div class="val" style="color:'+(d.reschedules.length>0?'#e8a87c':'#C9A84C')+'">'+d.reschedules.length+'</div><div class="sub">open requests</div></div>';

    // Bookings table
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
        return '<tr>' +
          '<td><div class="conf">'+bd.confId+'</div><div style="font-size:.72rem;color:#4A5A7A;margin-top:2px">'+dt+'</div></td>' +
          '<td><div class="name">'+(bd.fullName||'')+'</div><div class="addr">'+(bd.address||'')+'</div></td>' +
          '<td><div class="svc">'+(bd.svcLabel||'')+'</div><div class="svc" style="margin-top:2px">'+addons+'</div></td>' +
          '<td><div class="agent">'+(bd.buyerAgent?bd.buyerAgent.name:'—')+'</div><div class="svc">'+(bd.buyerAgent&&bd.buyerAgent.brokerage?bd.buyerAgent.brokerage:'')+'</div></td>' +
          '<td><div class="price">'+discBadge+tripBadge+'$'+(bd.finalPrice||'—')+'</div><div style="font-size:.72rem;color:#4A5A7A">'+(bd.dateFmt||'')+' @ '+(bd.time||'')+'</div></td>' +
          '</tr>';
      }).join('');
      document.getElementById('bookingTable').innerHTML = '<table><thead><tr><th>Conf #</th><th>Buyer / Address</th><th>Service</th><th>Agent</th><th>Total / Date</th></tr></thead><tbody>'+rows+'</tbody></table>';
    }

    // Reschedule table
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
  } catch(e) {
    console.error(e);
  }
}
load();
setInterval(load, 60000);
</script>
</body>
</html>`);
});

app.get('/admin/data', async function(req, res) {
  const auth = req.headers['authorization'];
  const pass = auth && auth.startsWith('Basic ') ? Buffer.from(auth.slice(6), 'base64').toString().split(':')[1] : null;
  if (pass !== ADMIN_PASSWORD) {
    res.set('WWW-Authenticate', 'Basic realm="San Tan Admin"');
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    const [bookings, reschedules] = await Promise.all([
      pool.query('SELECT * FROM confirmed_bookings ORDER BY confirmed_at DESC'),
      pool.query('SELECT * FROM reschedule_requests ORDER BY requested_at DESC'),
    ]);
    res.json({ bookings: bookings.rows, reschedules: reschedules.rows });
  } catch(e) {
    console.error('Admin data:', e.message);
    res.status(500).json({ error: 'DB error' });
  }
});

// ── START ─────────────────────────────────────────────────────
initDb().then(function() {
  app.listen(PORT, function(){ console.log('San Tan Property Inspections backend on port ' + PORT); });
});
