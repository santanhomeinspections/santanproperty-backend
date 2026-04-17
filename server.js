/**
 * San Tan Property Inspections — Backend Server v2
 * Node.js + Express
 * Google Calendar · Twilio SMS · Square Invoices · Nodemailer
 */

require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const { google } = require('googleapis');
const { SquareClient, SquareEnvironment } = require('square');
const nodemailer = require('nodemailer');
const twilio     = require('twilio');
const { v4: uuidv4 } = require('uuid');

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

// ── GOOGLE CALENDAR ─────────────────────────────────────────────
const oAuth2Client = new google.auth.OAuth2(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  process.env.GOOGLE_REDIRECT_URI
);
oAuth2Client.setCredentials({ refresh_token: process.env.GOOGLE_REFRESH_TOKEN });
const calendar   = google.calendar({ version: 'v3', auth: oAuth2Client });
const CALENDAR_ID = process.env.GOOGLE_CALENDAR_ID || 'primary';
const TIMEZONE    = 'America/Phoenix';
const ALL_SLOTS   = ['8:00 AM','9:00 AM','10:00 AM','11:00 AM','1:00 PM','2:00 PM','3:00 PM'];

function slotToMins(slot) {
  const [time, period] = slot.split(' ');
  let [h, m] = time.split(':').map(Number);
  if (period === 'PM' && h !== 12) h += 12;
  if (period === 'AM' && h === 12) h = 0;
  return h * 60 + m;
}

// ── TWILIO SMS ───────────────────────────────────────────────────
const tw      = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
const TW_FROM = process.env.TWILIO_PHONE_NUMBER;

function fmtPhone(raw) {
  const d = raw.replace(/\D/g,'');
  if (d.length === 10)              return '+1' + d;
  if (d.length === 11 && d[0]==='1') return '+' + d;
  return null;
}

async function sms(to, body) {
  const num = fmtPhone(to);
  if (!num) { console.warn('Bad phone, skipping SMS:', to); return; }
  try {
    await tw.messages.create({ from: TW_FROM, to: num, body });
    console.log(`✅ SMS → ${num}`);
  } catch (e) {
    console.error(`SMS error → ${num}:`, e.message);
  }
}

// ── PENDING BOOKINGS (in-memory, survives restarts via confirm token) ──
const pendingBookings = new Map();

// ── SQUARE ───────────────────────────────────────────────────────
const sq = new SquareClient({
  token: process.env.SQUARE_ACCESS_TOKEN,
  environment: process.env.NODE_ENV === 'production'
    ? SquareEnvironment.Production
    : SquareEnvironment.Sandbox,
});

// ── SERVICE AREA & TRIP CHARGE ───────────────────────────────────
const SERVICE_CITIES = [
  'chandler','gilbert','mesa','tempe',
  'queen creek','san tan valley','florence','apache junction'
];
const BASE_LAT = 33.1534;  // 3850 E Gallatin Way, San Tan Valley 85143 (private)
const BASE_LNG = -111.5368;
const TRIP_CHARGE_MILES = 50;
const TRIP_CHARGE_AMT   = 50;

function toRad(d){ return d * Math.PI / 180; }
function milesBetween(lat1,lng1,lat2,lng2){
  const R=3958.8,dLat=toRad(lat2-lat1),dLng=toRad(lng2-lng1);
  const a=Math.sin(dLat/2)**2+Math.cos(toRad(lat1))*Math.cos(toRad(lat2))*Math.sin(dLng/2)**2;
  return R*2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a));
}

async function checkTripCharge(address) {
  // Check if any service city is in the address string
  const addrLower = address.toLowerCase();
  const inServiceArea = SERVICE_CITIES.some(c => addrLower.includes(c));
  if (inServiceArea) return { apply: false, miles: 0 };
  // Geocode the address using Google Maps Geocoding API
  try {
    const encoded = encodeURIComponent(address);
    const url = \`https://maps.googleapis.com/maps/api/geocode/json?address=\${encoded}&key=\${process.env.GOOGLE_MAPS_API_KEY}\`;
    const r = await fetch(url);
    const data = await r.json();
    if (data.results?.[0]) {
      const { lat, lng } = data.results[0].geometry.location;
      const miles = milesBetween(BASE_LAT, BASE_LNG, lat, lng);
      if (miles >= TRIP_CHARGE_MILES) return { apply: true, miles: Math.round(miles) };
    }
  } catch(e) { console.warn('Geocode failed:', e.message); }
  return { apply: false, miles: 0 };
}

// ── EMAIL ────────────────────────────────────────────────────────
const mailer = nodemailer.createTransport({
  service: 'gmail',
  auth: { user: process.env.EMAIL_USER, pass: process.env.EMAIL_APP_PASSWORD },
});

// ── HEALTH ───────────────────────────────────────────────────────
app.get('/api/health', (_, res) => res.json({ status:'ok', ts: new Date().toISOString() }));

// ── AVAILABILITY ─────────────────────────────────────────────────
app.get('/api/availability', async (req, res) => {
  const { date } = req.query;
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date))
    return res.status(400).json({ error: 'Use YYYY-MM-DD' });

  try {
    const [y,mo,d] = date.split('-').map(Number);
    const resp = await calendar.events.list({
      calendarId: CALENDAR_ID,
      timeMin: new Date(y, mo-1, d, 0,  0,  0).toISOString(),
      timeMax: new Date(y, mo-1, d, 23, 59, 59).toISOString(),
      singleEvents: true, orderBy: 'startTime',
    });
    const booked = [];
    for (const ev of (resp.data.items || [])) {
      if (!ev.start?.dateTime) continue;
      const dt = new Date(ev.start.dateTime);
      const h=dt.getHours(), m=dt.getMinutes(), p=h>=12?'PM':'AM';
      const dh=h>12?h-12:h===0?12:h;
      const slot=`${dh}:${String(m).padStart(2,'0')} ${p}`;
      if (ALL_SLOTS.includes(slot)) booked.push(slot);
    }
    res.json({ date, booked, available: ALL_SLOTS.filter(s=>!booked.includes(s)) });
  } catch (e) {
    console.error('Availability:', e.message);
    res.json({ date, booked: [], available: ALL_SLOTS, warning: 'Calendar unavailable' });
  }
});

// ── BOOK ─────────────────────────────────────────────────────────
app.post('/api/book', async (req, res) => {
  const {
    address, sqft, yearBuilt, inspType,
    addons=[], totalPrice, totalMins,
    date, time, endTime,
    buyer, buyerAgent, sellerAgent, notes,
  } = req.body;

  // Validate
  const miss=[];
  if(!address) miss.push('address');
  if(!sqft)    miss.push('sqft');
  if(!inspType) miss.push('inspType');
  if(!date)    miss.push('date');
  if(!time)    miss.push('time');
  if(!buyer?.firstName) miss.push('buyer.firstName');
  if(!buyer?.phone)     miss.push('buyer.phone');
  if(!buyer?.email)     miss.push('buyer.email');
  if(!buyerAgent?.name) miss.push('buyerAgent.name');
  if(!buyerAgent?.phone)miss.push('buyerAgent.phone');
  if(miss.length) return res.status(400).json({ error:'Missing: '+miss.join(', ') });

  const confId   = 'STH-' + uuidv4().slice(0,8).toUpperCase();
  const fullName = `${buyer.firstName} ${buyer.lastName}`;
  const [y,mo,d] = date.split('-').map(Number);
  const sm       = slotToMins(time);
  const startDT  = new Date(y, mo-1, d, Math.floor(sm/60), sm%60, 0);
  const endDT    = new Date(startDT.getTime() + (totalMins||120)*60000);

  const SVC = {
    'pre-purchase':'Pre-Purchase Inspection',
    'pre-listing':'Pre-Listing Inspection',
    'new-construction':'New Construction Inspection',
    'warranty':'Pre-One Year Warranty Inspection',
    'reinspection':'Re-Inspection',
  };
  const svcLabel   = SVC[inspType] || inspType;
  const addonsLine = addons.length ? addons.join(', ') : 'None';
  const dateFmt    = startDT.toLocaleDateString('en-US',{
    weekday:'long', year:'numeric', month:'long', day:'numeric', timeZone: TIMEZONE,
  });

  // 1. Double-check slot
  try {
    const chk = await calendar.events.list({
      calendarId: CALENDAR_ID,
      timeMin: startDT.toISOString(),
      timeMax: endDT.toISOString(),
      singleEvents: true,
    });
    if ((chk.data.items||[]).length)
      return res.status(409).json({ error:'That slot was just booked — please choose another.' });
  } catch(e) { console.warn('Slot check failed:', e.message); }

  // 2. Google Calendar event
  let calId = null;
  try {
    const ev = {
      summary:  `${svcLabel} — ${fullName}`,
      location: address,
      description: [
        `Conf: ${confId}`, `Service: ${svcLabel}`,
        addons.length ? `Add-ons: ${addonsLine}` : null,
        `Address: ${address}`, `Sq Ft: ${sqft}  |  Year: ${yearBuilt}`,
        `Est. Total: $${totalPrice}  |  Duration: ${totalMins} min`, ``,
        `BUYER: ${fullName}  |  ${buyer.phone}  |  ${buyer.email}`,
        `BUYER'S AGENT: ${buyerAgent.name}${buyerAgent.brokerage?' — '+buyerAgent.brokerage:''}  |  ${buyerAgent.phone}`,
        sellerAgent?.name ? `SELLER'S AGENT: ${sellerAgent.name}${sellerAgent.brokerage?' — '+sellerAgent.brokerage:''}  |  ${sellerAgent.phone||'—'}` : null,
        notes ? `Notes: ${notes}` : null,
      ].filter(Boolean).join('\n'),
      start: { dateTime: startDT.toISOString(), timeZone: TIMEZONE },
      end:   { dateTime: endDT.toISOString(),   timeZone: TIMEZONE },
      colorId: '5',
      attendees: [{ email: buyer.email, displayName: fullName }],
      reminders: {
        useDefault: false,
        overrides: [{ method:'email', minutes:24*60 }, { method:'popup', minutes:60 }],
      },
    };
    const r = await calendar.events.insert({ calendarId: CALENDAR_ID, resource: ev, sendUpdates:'all' });
    calId = r.data.id;
    console.log(`✅ Calendar: ${calId}`);
  } catch(e) { console.error('Calendar:', e.message); }

  // 3. SMS — Buyer
  await sms(buyer.phone,
    `Hi ${buyer.firstName}! Your inspection is confirmed.\n\n` +
    `📍 ${address}\n📅 ${dateFmt}\n⏰ ${time}–${endTime||'TBD'}\n` +
    `🔖 ${svcLabel}` + (addons.length ? `\n➕ ${addonsLine}` : '') + `\n` +
    `💰 Est. $${totalPrice} — pay day-of\n` +
    `🔖 Conf: ${confId}\n\n` +
    `Questions or need to reschedule?\n📞 (480) 418-7633\n📧 santanpropertyinspections@gmail.com\n– San Tan Property Inspections`
  );

  // 4. SMS — Buyer's Agent
  await sms(buyerAgent.phone,
    `Hi ${buyerAgent.name}! Inspection scheduled for your buyer.\n\n` +
    `📍 ${address}\n👤 Buyer: ${fullName}\n📅 ${dateFmt} @ ${time}\n🔖 ${svcLabel}\n🔖 Conf: ${confId}\n\n` +
    `✅ ACTION NEEDED — Please confirm with seller's agent:\n` +
    `• Seller's agent is aware of inspection date & time\n` +
    `• GAS is on & accessible\n` +
    `• WATER is on & accessible\n` +
    `• ELECTRICAL is on & accessible\n` +
    `• ATTIC ACCESS is clear & accessible\n\n` +
    `Questions? 📞 (480) 418-7633 | santanpropertyinspections@gmail.com\n– San Tan Property Inspections`
  );

  // 5. SMS — Seller's Agent (if provided)
  if (sellerAgent?.phone) {
    await sms(sellerAgent.phone,
      `Hello${sellerAgent.name?' '+sellerAgent.name:''}! An inspection is scheduled at your listing.\n\n` +
      `📍 ${address}\n📅 ${dateFmt} @ ${time}\n🔖 ${svcLabel}\n🔖 Conf: ${confId}\n\n` +
      `Please ensure by inspection day:\n` +
      `• GAS — on & accessible\n• WATER — on & accessible\n` +
      `• ELECTRICAL — on & accessible\n• ATTIC ACCESS — clear & accessible\n\n` +
      `⚠️ IMPORTANT: If utilities are NOT on and accessible at time of inspection, a $125 re-inspection fee will apply before the inspection can be completed.\n\n` +
      `Questions? 📞 (480) 418-7633 | santanpropertyinspections@gmail.com\n– San Tan Property Inspections`
    );
  }

  // 6. Square Invoice
  let invoiceUrl = null;
  try {
    const custSearch = await sq.customers.searchCustomers({
      query: { filter: { emailAddress: { exact: buyer.email } } },
    });
    let custId;
    if (custSearch.result.customers?.length) {
      custId = custSearch.result.customers[0].id;
    } else {
      const nc = await sq.customers.createCustomer({
        givenName: buyer.firstName, familyName: buyer.lastName,
        emailAddress: buyer.email, phoneNumber: buyer.phone,
        idempotencyKey: uuidv4(),
      });
      custId = nc.result.customer.id;
    }
    const order = await sq.orders.createOrder({
      order: {
        locationId: process.env.SQUARE_LOCATION_ID, customerId: custId,
        lineItems: [{
          name: svcLabel+(addons.length?' + Add-ons':''),
          quantity: '1',
          note: `${address} — ${date} @ ${time} | ${confId}`,
          basePriceMoney: { amount: BigInt(totalPrice*100), currency:'USD' },
        }],
        referenceId: confId,
      },
      idempotencyKey: uuidv4(),
    });
    const inv = await sq.invoices.createInvoice({
      invoice: {
        locationId: process.env.SQUARE_LOCATION_ID,
        orderId: order.result.order.id,
        primaryRecipient: { customerId: custId },
        paymentRequests: [{
          requestType: 'BALANCE', dueDate: date, automaticPaymentSource: 'NONE',
        }],
        deliveryMethod: 'EMAIL',
        title: `Inspection – ${svcLabel}`,
        description: `${address}\n${date} @ ${time}\n${confId}`,
        acceptedPaymentMethods: { card:true, bankAccount:false, squareGiftCard:false },
      },
      idempotencyKey: uuidv4(),
    });
    await sq.invoices.publishInvoice(inv.result.invoice.id, {
      version: inv.result.invoice.version, idempotencyKey: uuidv4(),
    });
    invoiceUrl = inv.result.invoice.publicUrl;
    console.log(`✅ Square invoice created`);
  } catch(e) { console.error('Square:', e.message); }

  // 7. Confirmation email to buyer
  const invLine = invoiceUrl
    ? `<p>💳 <a href="${invoiceUrl}" style="color:#C9A84C">View your Square invoice</a> — or pay in person on inspection day.</p>`
    : '<p>💳 Payment via Square, Venmo, Zelle, or cash on inspection day.</p>';

  try {
    await mailer.sendMail({
      from: `"San Tan Property Inspections" <${process.env.EMAIL_USER}>`,
      to: buyer.email,
      subject: `Inspection Confirmed — ${dateFmt} @ ${time} [${confId}]`,
      html: `<div style="font-family:Georgia,serif;max-width:580px;margin:0 auto;border-top:4px solid #C9A84C;padding-top:20px">
        <h2 style="color:#0F1C35">Inspection Confirmed ✅</h2>
        <p style="color:#555">Hi ${buyer.firstName}, here are your booking details:</p>
        <table style="width:100%;border-collapse:collapse;margin:16px 0">
          <tr><td style="padding:6px 0;color:#888;width:130px">Service</td><td style="color:#2C2C2C;font-weight:600">${svcLabel}${addons.length?'<br><small>Add-ons: '+addonsLine+'</small>':''}</td></tr>
          <tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">${dateFmt}</td></tr>
          <tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">${time}${endTime?' — approx. ends '+endTime:''}</td></tr>
          <tr><td style="padding:6px 0;color:#888">Property</td><td style="color:#2C2C2C;font-weight:600">${address}</td></tr>
          <tr><td style="padding:6px 0;color:#888">Est. Total</td><td style="color:#C9A84C;font-weight:700">$${totalPrice}</td></tr>
          <tr><td style="padding:6px 0;color:#888">Confirmation</td><td style="color:#C9A84C;font-weight:700">${confId}</td></tr>
        </table>
        ${invLine}
        <p style="color:#555">Your report will be delivered the <strong>same day</strong> as your inspection.</p>
        <p style="color:#555">Questions? Call/text <strong>(480) 418-7633</strong></p>
        <hr style="border:none;border-top:1px solid #E8DFC8;margin:20px 0"/>
        <p style="color:#888;font-size:.8rem">San Tan Property Inspections · East Valley, AZ · santanpropertyinspections.com</p>
      </div>`,
    });

    await mailer.sendMail({
      from: `"San Tan Booking" <${process.env.EMAIL_USER}>`,
      to: process.env.OWNER_EMAIL,
      subject: `NEW BOOKING: ${fullName} — ${dateFmt} @ ${time}`,
      html: `<div style="font-family:Arial,sans-serif;max-width:560px">
        <h2>📅 New Booking — ${confId}</h2>
        <p><b>Service:</b> ${svcLabel}<br><b>Add-ons:</b> ${addonsLine}</p>
        <p><b>Date/Time:</b> ${dateFmt} @ ${time}${endTime?' → '+endTime:''}</p>
        <p><b>Address:</b> ${address}<br><b>Sq Ft:</b> ${sqft} / <b>Year:</b> ${yearBuilt}<br><b>Est. Total:</b> $${totalPrice}</p>
        <hr/>
        <p><b>Buyer:</b> ${fullName}<br>📞 ${buyer.phone}<br>✉️ ${buyer.email}</p>
        <p><b>Buyer's Agent:</b> ${buyerAgent.name}${buyerAgent.brokerage?' — '+buyerAgent.brokerage:''}<br>📞 ${buyerAgent.phone}</p>
        ${sellerAgent?.name ? `<p><b>Seller's Agent:</b> ${sellerAgent.name}${sellerAgent.brokerage?' — '+sellerAgent.brokerage:''}<br>📞 ${sellerAgent.phone||'—'}</p>` : ''}
        ${notes ? `<p><b>Notes:</b> ${notes}</p>` : ''}
        <p><b>Calendar ID:</b> ${calId||'—'}<br><b>Invoice:</b> ${invoiceUrl||'—'}</p>
      </div>`,
    });
    console.log(`✅ Emails sent for ${confId}`);
  } catch(e) { console.error('Email:', e.message); }

  // ── Store as pending, send owner confirmation request ──────────
  const token = uuidv4();
  pendingBookings.set(token, {
    confId, address, sqft, yearBuilt, inspType, svcLabel,
    addons, addonsLine, totalPrice, totalMins,
    date, time, endTime, dateFmt, fullName,
    buyer, buyerAgent, sellerAgent, notes,
    tripCharge: { apply: false, miles: 0 },
    createdAt: Date.now(),
  });

  // Check trip charge
  const trip = await checkTripCharge(address);
  const booking = pendingBookings.get(token);
  booking.tripCharge = trip;
  const finalPrice = trip.apply ? totalPrice + TRIP_CHARGE_AMT : totalPrice;
  booking.finalPrice = finalPrice;

  // Build confirm URL
  const BASE_URL = process.env.RAILWAY_URL || \`https://santanproperty-backend-production.up.railway.app\`;
  const confirmUrl = \`\${BASE_URL}/confirm/\${token}\`;
  const cancelUrl  = \`\${BASE_URL}/cancel/\${token}\`;

  // Send owner alert email with confirm/cancel links
  try {
    await mailer.sendMail({
      from: \`"San Tan Booking" <\${process.env.EMAIL_USER}>\`,
      to: process.env.OWNER_EMAIL,
      subject: \`⏳ PENDING BOOKING: \${fullName} — \${dateFmt} @ \${time}\`,
      html: \`<div style="font-family:Arial,sans-serif;max-width:560px">
        <h2 style="color:#0F1C35">📋 New Booking Request — Awaiting Your Confirmation</h2>
        <p><b>Service:</b> \${svcLabel}<br><b>Add-ons:</b> \${addonsLine}</p>
        <p><b>Date/Time:</b> \${dateFmt} @ \${time}\${endTime?' → '+endTime:''}</p>
        <p><b>Address:</b> \${address}<br><b>Sq Ft:</b> \${sqft} / <b>Year:</b> \${yearBuilt}</p>
        <p><b>Est. Total:</b> $\${totalPrice}\${trip.apply?' + $'+TRIP_CHARGE_AMT+' trip charge ('+trip.miles+' miles) = <b style=\"color:#C9A84C\">$'+finalPrice+'</b>':''}</p>
        <hr/>
        <p><b>Buyer:</b> \${fullName}<br>📞 \${buyer.phone}<br>✉️ \${buyer.email}</p>
        <p><b>Buyer's Agent:</b> \${buyerAgent.name}\${buyerAgent.brokerage?' — '+buyerAgent.brokerage:''}<br>📞 \${buyerAgent.phone}</p>
        \${sellerAgent?.name ? \`<p><b>Seller's Agent:</b> \${sellerAgent.name}\${sellerAgent.brokerage?' — '+sellerAgent.brokerage:''}<br>📞 \${sellerAgent.phone||'—'}</p>\` : ''}
        \${notes ? \`<p><b>Notes:</b> \${notes}</p>\` : ''}
        \${trip.apply ? \`<p style="background:#FFF3CD;padding:10px;border-radius:6px">⚠️ <b>Trip charge applies</b> — address is \${trip.miles} miles from base. $\${TRIP_CHARGE_AMT} will be added to total.</p>\` : ''}
        <div style="margin:28px 0;display:flex;gap:16px">
          <a href="\${confirmUrl}" style="background:#1B2D52;color:white;padding:14px 28px;border-radius:6px;text-decoration:none;font-weight:700;font-size:1rem">✅ CONFIRM &amp; SEND TEXTS</a>
          &nbsp;&nbsp;
          <a href="\${cancelUrl}" style="background:#C0392B;color:white;padding:14px 28px;border-radius:6px;text-decoration:none;font-weight:700;font-size:1rem">❌ CANCEL</a>
        </div>
        <p style="color:#888;font-size:.8rem">This request expires in 24 hours. Texts will NOT go out until you tap Confirm.</p>
      </div>\`,
    });
    console.log(\`📧 Owner alert sent for pending \${confId}\`);
  } catch(e) { console.error('Owner alert email:', e.message); }

  // Also text owner
  await sms(process.env.OWNER_PHONE,
    \`⏳ NEW BOOKING REQUEST\n\${fullName}\n📍 \${address}\n📅 \${dateFmt} @ \${time}\n💰 $\${finalPrice}\${trip.apply?' (incl. $'+TRIP_CHARGE_AMT+' trip charge)':''}\n\nCheck your email to confirm.\`
  );

  res.json({
    success: true, confirmationId: confId,
    message: 'Request received! You will be confirmed shortly.',
  });
});

// ── CONFIRM BOOKING ───────────────────────────────────────────────
app.get('/confirm/:token', async (req, res) => {
  const booking = pendingBookings.get(req.params.token);
  if (!booking) return res.send('<h2>❌ This confirmation link has expired or already been used.</h2>');

  const {
    confId, address, sqft, yearBuilt, svcLabel, addons, addonsLine,
    finalPrice, totalMins, date, time, endTime, dateFmt, fullName,
    buyer, buyerAgent, sellerAgent, notes, tripCharge,
  } = booking;

  pendingBookings.delete(req.params.token); // one-time use

  const [y,mo,d2] = date.split('-').map(Number);
  const sm = slotToMins(time);
  const startDT = new Date(y, mo-1, d2, Math.floor(sm/60), sm%60, 0);
  const endDT   = new Date(startDT.getTime() + (totalMins||120)*60000);

  // 1. Google Calendar
  let calId = null;
  try {
    const ev = {
      summary:  \`\${svcLabel} — \${fullName}\`,
      location: address,
      description: [
        \`Conf: \${confId}\`, \`Service: \${svcLabel}\`,
        addons.length ? \`Add-ons: \${addonsLine}\` : null,
        \`Address: \${address}\`, \`Sq Ft: \${sqft}  |  Year: \${yearBuilt}\`,
        \`Est. Total: $\${finalPrice}\${tripCharge.apply?' (incl. trip charge)':''}  |  Duration: \${totalMins} min\`, \`\`,
        \`BUYER: \${fullName}  |  \${buyer.phone}  |  \${buyer.email}\`,
        \`BUYER'S AGENT: \${buyerAgent.name}\${buyerAgent.brokerage?' — '+buyerAgent.brokerage:''}  |  \${buyerAgent.phone}\`,
        sellerAgent?.name ? \`SELLER'S AGENT: \${sellerAgent.name}\${sellerAgent.brokerage?' — '+sellerAgent.brokerage:''}  |  \${sellerAgent.phone||'—'}\` : null,
        notes ? \`Notes: \${notes}\` : null,
      ].filter(Boolean).join('\n'),
      start: { dateTime: startDT.toISOString(), timeZone: TIMEZONE },
      end:   { dateTime: endDT.toISOString(),   timeZone: TIMEZONE },
      colorId: '5',
      attendees: [{ email: buyer.email, displayName: fullName }],
      reminders: { useDefault: false, overrides: [{ method:'email', minutes:24*60 }, { method:'popup', minutes:60 }] },
    };
    const r = await calendar.events.insert({ calendarId: CALENDAR_ID, resource: ev, sendUpdates:'all' });
    calId = r.data.id;
    console.log(\`✅ Calendar: \${calId}\`);
  } catch(e) { console.error('Calendar:', e.message); }

  // 2. SMS — Buyer
  await sms(buyer.phone,
    \`Hi \${buyer.firstName}! Your inspection is confirmed.\n\n\` +
    \`📍 \${address}\n📅 \${dateFmt}\n⏰ \${time}–\${endTime||'TBD'}\n\` +
    \`🔖 \${svcLabel}\` + (addons.length ? \`\n➕ \${addonsLine}\` : '') + \`\n\` +
    \`💰 Est. $\${finalPrice} — pay day-of\${tripCharge.apply?' (incl. $'+TRIP_CHARGE_AMT+' trip charge)':''}\n\` +
    \`🔖 Conf: \${confId}\n\n\` +
    \`Questions or need to reschedule?\n📞 (480) 418-7633\n📧 santanpropertyinspections@gmail.com\n– San Tan Property Inspections\`
  );

  // 3. SMS — Buyer's Agent
  await sms(buyerAgent.phone,
    \`Hi \${buyerAgent.name}! Inspection scheduled for your buyer.\n\n\` +
    \`📍 \${address}\n👤 Buyer: \${fullName}\n📅 \${dateFmt} @ \${time}\n🔖 \${svcLabel}\n🔖 Conf: \${confId}\n\n\` +
    \`✅ ACTION NEEDED — Please confirm with seller's agent:\n\` +
    \`• Seller's agent is aware of inspection date & time\n\` +
    \`• GAS is on & accessible\n\` +
    \`• WATER is on & accessible\n\` +
    \`• ELECTRICAL is on & accessible\n\` +
    \`• ATTIC ACCESS is clear & accessible\n\n\` +
    \`Questions? 📞 (480) 418-7633 | santanpropertyinspections@gmail.com\n– San Tan Property Inspections\`
  );

  // 4. SMS — Seller's Agent (if provided)
  if (sellerAgent?.phone) {
    await sms(sellerAgent.phone,
      \`Hello\${sellerAgent.name?' '+sellerAgent.name:''}! An inspection is scheduled at your listing.\n\n\` +
      \`📍 \${address}\n📅 \${dateFmt} @ \${time}\n🔖 \${svcLabel}\n\n\` +
      \`Please ensure by inspection day:\n\` +
      \`• GAS — on & accessible\n• WATER — on & accessible\n\` +
      \`• ELECTRICAL — on & accessible\n• ATTIC ACCESS — clear & accessible\n\n\` +
      \`Questions? 📞 (480) 418-7633 | santanpropertyinspections@gmail.com\n– San Tan Property Inspections\`
    );
  }

  // 5. Square Invoice
  let invoiceUrl = null;
  try {
    const custSearch = await sq.customers.searchCustomers({
      query: { filter: { emailAddress: { exact: buyer.email } } },
    });
    let custId;
    if (custSearch.result.customers?.length) {
      custId = custSearch.result.customers[0].id;
    } else {
      const nc = await sq.customers.createCustomer({
        givenName: buyer.firstName, familyName: buyer.lastName,
        emailAddress: buyer.email, phoneNumber: buyer.phone,
        idempotencyKey: uuidv4(),
      });
      custId = nc.result.customer.id;
    }
    const order = await sq.orders.createOrder({
      order: {
        locationId: process.env.SQUARE_LOCATION_ID, customerId: custId,
        lineItems: [{
          name: svcLabel+(addons.length?' + Add-ons':'')+(tripCharge.apply?' + Trip Charge':''),
          quantity: '1',
          note: \`\${address} — \${date} @ \${time} | \${confId}\`,
          basePriceMoney: { amount: BigInt(finalPrice*100), currency:'USD' },
        }],
        referenceId: confId,
      },
      idempotencyKey: uuidv4(),
    });
    const inv = await sq.invoices.createInvoice({
      invoice: {
        locationId: process.env.SQUARE_LOCATION_ID,
        orderId: order.result.order.id,
        primaryRecipient: { customerId: custId },
        paymentRequests: [{ requestType: 'BALANCE', dueDate: date, automaticPaymentSource: 'NONE' }],
        deliveryMethod: 'EMAIL',
        title: \`Inspection – \${svcLabel}\`,
        description: \`\${address}\n\${date} @ \${time}\n\${confId}\`,
        acceptedPaymentMethods: { card:true, bankAccount:false, squareGiftCard:false },
      },
      idempotencyKey: uuidv4(),
    });
    await sq.invoices.publishInvoice(inv.result.invoice.id, {
      version: inv.result.invoice.version, idempotencyKey: uuidv4(),
    });
    invoiceUrl = inv.result.invoice.publicUrl;
    console.log(\`✅ Square invoice created\`);
  } catch(e) { console.error('Square:', e.message); }

  // 6. Confirmation email to buyer
  const invLine = invoiceUrl
    ? \`<p>💳 <a href="\${invoiceUrl}" style="color:#C9A84C">View your Square invoice</a> — or pay in person on inspection day.</p>\`
    : '<p>💳 Payment via Square, Venmo, Zelle, or cash on inspection day.</p>';

  try {
    await mailer.sendMail({
      from: \`"San Tan Property Inspections" <\${process.env.EMAIL_USER}>\`,
      to: buyer.email,
      subject: \`Inspection Confirmed — \${dateFmt} @ \${time} [\${confId}]\`,
      html: \`<div style="font-family:Georgia,serif;max-width:580px;margin:0 auto;border-top:4px solid #C9A84C;padding-top:20px">
        <h2 style="color:#0F1C35">Inspection Confirmed ✅</h2>
        <p style="color:#555">Hi \${buyer.firstName}, here are your booking details:</p>
        <table style="width:100%;border-collapse:collapse;margin:16px 0">
          <tr><td style="padding:6px 0;color:#888;width:130px">Service</td><td style="color:#2C2C2C;font-weight:600">\${svcLabel}\${addons.length?'<br><small>Add-ons: '+addonsLine+'</small>':''}</td></tr>
          <tr><td style="padding:6px 0;color:#888">Date</td><td style="color:#2C2C2C;font-weight:600">\${dateFmt}</td></tr>
          <tr><td style="padding:6px 0;color:#888">Time</td><td style="color:#2C2C2C;font-weight:600">\${time}\${endTime?' — approx. ends '+endTime:''}</td></tr>
          <tr><td style="padding:6px 0;color:#888">Property</td><td style="color:#2C2C2C;font-weight:600">\${address}</td></tr>
          <tr><td style="padding:6px 0;color:#888">Est. Total</td><td style="color:#C9A84C;font-weight:700">$\${finalPrice}\${tripCharge.apply?' (incl. $'+TRIP_CHARGE_AMT+' trip charge)':''}</td></tr>
          <tr><td style="padding:6px 0;color:#888">Confirmation</td><td style="color:#C9A84C;font-weight:700">\${confId}</td></tr>
        </table>
        \${invLine}
        <p style="color:#555">Your report will be delivered the <strong>same day</strong> as your inspection.</p>
        <p style="color:#555">Questions? Call/text <strong>(480) 418-7633</strong></p>
        <hr style="border:none;border-top:1px solid #E8DFC8;margin:20px 0"/>
        <p style="color:#888;font-size:.8rem">San Tan Property Inspections · East Valley, AZ · santanpropertyinspections.com</p>
      </div>\`,
    });
    console.log(\`✅ Confirmation email sent to buyer for \${confId}\`);
  } catch(e) { console.error('Buyer email:', e.message); }

  res.send(\`<div style="font-family:Arial,sans-serif;max-width:500px;margin:60px auto;text-align:center;padding:40px;border-top:4px solid #C9A84C">
    <h2 style="color:#1B2D52">✅ Booking Confirmed!</h2>
    <p>Texts sent to buyer and agents.</p>
    <p><b>\${fullName}</b><br>\${dateFmt} @ \${time}<br>\${address}</p>
    <p style="color:#C9A84C;font-weight:700">Conf: \${confId}</p>
    \${tripCharge.apply ? \`<p style="background:#FFF3CD;padding:10px;border-radius:6px">⚠️ Trip charge of $\${TRIP_CHARGE_AMT} applied (\${tripCharge.miles} miles)</p>\` : ''}
  </div>\`);
});

// ── CANCEL BOOKING ────────────────────────────────────────────────
app.get('/cancel/:token', async (req, res) => {
  const booking = pendingBookings.get(req.params.token);
  if (!booking) return res.send('<h2>❌ This link has expired or already been used.</h2>');
  const { confId, fullName, dateFmt, time } = booking;
  pendingBookings.delete(req.params.token);
  console.log(\`❌ Booking cancelled: \${confId}\`);
  res.send(\`<div style="font-family:Arial,sans-serif;max-width:500px;margin:60px auto;text-align:center;padding:40px;border-top:4px solid #C0392B">
    <h2 style="color:#C0392B">❌ Booking Cancelled</h2>
    <p>The booking for <b>\${fullName}</b> on \${dateFmt} @ \${time} has been cancelled.</p>
    <p>No texts were sent.</p>
  </div>\`);
});

// ── GOOGLE OAUTH (one-time setup) ────────────────────────────────
app.get('/auth/google', (_, res) => {
  res.redirect(oAuth2Client.generateAuthUrl({
    access_type:'offline',
    scope:['https://www.googleapis.com/auth/calendar'],
    prompt:'consent',
  }));
});
app.get('/auth/google/callback', async (req, res) => {
  const { tokens } = await oAuth2Client.getToken(req.query.code);
  console.log('🔑 REFRESH TOKEN:', tokens.refresh_token);
  res.send(`<pre>Add to Railway as GOOGLE_REFRESH_TOKEN:\n\n${tokens.refresh_token}</pre>`);
});

// ── TWILIO REPLY FORWARDING ──────────────────────────────────────
// When someone replies to the Twilio number, forward it to your cell
app.post('/sms/reply', express.urlencoded({ extended: false }), async (req, res) => {
  const from    = req.body.From  || 'Unknown';
  const body    = req.body.Body  || '';
  const OWNER   = process.env.OWNER_PHONE; // your personal cell
  if (OWNER) {
    await sms(OWNER, `Reply from ${from}:\n${body}`);
  }
  // Respond with empty TwiML so Twilio doesn't error
  res.set('Content-Type', 'text/xml');
  res.send('<Response></Response>');
});

app.listen(PORT, () => console.log(`🏠 San Tan Property Inspections backend on port ${PORT}`));
