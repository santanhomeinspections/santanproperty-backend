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
// Email via Resend API (no package needed)
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

// GOOGLE CALENDAR
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

// TWILIO SMS
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

// PENDING BOOKINGS
const pendingBookings = new Map();

// SQUARE
const sq = new SquareClient({
  token: process.env.SQUARE_ACCESS_TOKEN,
  environment: process.env.NODE_ENV === 'production'
    ? SquareEnvironment.Production
    : SquareEnvironment.Sandbox,
});

// SERVICE AREA & TRIP CHARGE
const SERVICE_CITIES = [
  'chandler','gilbert','mesa','tempe',
  'queen creek','san tan valley','florence','apache junction'
];
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

// EMAIL
// Send email via Resend REST API
async function sendEmail(to, subject, html) {
  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer re_QP15DwwE_BMkWn2nPJ6HT9BVRC6EBCtuG',
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
  } catch(e) {
    throw e;
  }
}

// HEALTH
app.get('/api/health', function(req, res){ res.json({ status:'ok', ts: new Date().toISOString() }); });

// AVAILABILITY
app.get('/api/availability', async function(req, res) {
  const date = req.query.date;
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date))
    return res.status(400).json({ error: 'Use YYYY-MM-DD' });
  try {
    const parts = date.split('-').map(Number);
    const y = parts[0], mo = parts[1], d = parts[2];
    // Use Arizona time (MST = UTC-7, no DST)
    const timeMin = `${date}T00:00:00-07:00`;
    const timeMax = `${date}T23:59:59-07:00`;

    // Fetch both calendars in parallel — blocking calendar failure won't crash availability
    const [mainResp, blockResp] = await Promise.all([
      calendar.events.list({ calendarId: CALENDAR_ID, timeMin, timeMax, singleEvents: true, orderBy: 'startTime' }),
      calendar.events.list({ calendarId: BLOCK_CALENDAR_ID, timeMin, timeMax, singleEvents: true, orderBy: 'startTime' }).catch(function(e){
        console.warn('Block calendar fetch failed (check sharing permissions):', e.message);
        return { data: { items: [] } };
      }),
    ]);

    const allItems = [
      ...(mainResp.data.items  || []),
      ...(blockResp.data.items || []),
    ];

    // Check if any event is all-day (blocks entire day)
    const dayBlocked = allItems.some(function(ev) {
      return ev.start && ev.start.date && !ev.start.dateTime;
    });

    if (dayBlocked) {
      return res.json({ date: date, booked: ALL_SLOTS, available: [], dayBlocked: true });
    }

    // Collect booked slots from timed events
    const booked = [];
    for (let i = 0; i < allItems.length; i++) {
      const ev = allItems[i];
      if (!ev.start || !ev.start.dateTime) continue;
      const evStart = new Date(ev.start.dateTime);
      const evEnd   = ev.end && ev.end.dateTime ? new Date(ev.end.dateTime) : null;

      for (let s = 0; s < ALL_SLOTS.length; s++) {
        const slot = ALL_SLOTS[s];
        const slotMins = slotToMins(slot);
        const slotH = Math.floor(slotMins / 60);
        const slotM = slotMins % 60;
        const slotStart = new Date(`${date}T${String(slotH).padStart(2,'0')}:${String(slotM).padStart(2,'0')}:00-07:00`);

        // Block slot if: event starts exactly at slot, OR event spans across slot start
        const startsAtSlot = evStart.getHours() === slotH && evStart.getMinutes() === slotM;
        const spansSlot    = evEnd && evStart <= slotStart && evEnd > slotStart;
        if ((startsAtSlot || spansSlot) && !booked.includes(slot)) {
          booked.push(slot);
        }
      }
    }

    res.json({ date: date, booked: booked, available: ALL_SLOTS.filter(function(s){ return !booked.includes(s); }) });
  } catch (e) {
    console.error('Availability:', e.message);
    res.json({ date: date, booked: [], available: ALL_SLOTS, warning: 'Calendar unavailable' });
  }
});

// BOOK
app.post('/api/book', async function(req, res) {
  const b = req.body;
  const address    = b.address;
  const sqft       = b.sqft;
  const yearBuilt  = b.yearBuilt;
  const inspType   = b.inspType;
  const addons     = b.addons || [];
  const totalPrice = b.totalPrice;
  const totalMins  = b.totalMins;
  const date       = b.date;
  const time       = b.time;
  const endTime    = b.endTime;
  const buyer      = b.buyer;
  const buyerAgent = b.buyerAgent;
  const sellerAgent= b.sellerAgent;
  const notes      = b.notes;

  const miss=[];
  if(!address) miss.push('address');
  if(!sqft)    miss.push('sqft');
  if(!inspType) miss.push('inspType');
  if(!date)    miss.push('date');
  if(!time)    miss.push('time');
  if(!buyer || !buyer.firstName) miss.push('buyer.firstName');
  if(!buyer || !buyer.phone)     miss.push('buyer.phone');
  if(!buyer || !buyer.email)     miss.push('buyer.email');
  if(!buyerAgent || !buyerAgent.name)  miss.push('buyerAgent.name');
  if(!buyerAgent || !buyerAgent.phone) miss.push('buyerAgent.phone');
  if(miss.length) return res.status(400).json({ error:'Missing: '+miss.join(', ') });

  const confId   = 'STH-' + uuidv4().slice(0,8).toUpperCase();
  const fullName = buyer.firstName + ' ' + buyer.lastName;
  const parts2   = date.split('-').map(Number);
  const y2=parts2[0], mo2=parts2[1], d2=parts2[2];
  const sm       = slotToMins(time);
  const startDT  = new Date(y2, mo2-1, d2, Math.floor(sm/60), sm%60, 0);
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

  // Double-check slot
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

  // Store as pending
  const token = uuidv4();
  const trip = await checkTripCharge(address);
  const finalPrice = trip.apply ? totalPrice + TRIP_CHARGE_AMT : totalPrice;

  pendingBookings.set(token, {
    confId, address, sqft, yearBuilt, inspType, svcLabel,
    addons, addonsLine, totalPrice, finalPrice, totalMins,
    date, time, endTime, dateFmt, fullName,
    buyer, buyerAgent, sellerAgent, notes,
    tripCharge: trip,
    createdAt: Date.now(),
  });

  const BASE_URL = process.env.RAILWAY_URL || 'https://santanproperty-backend-production.up.railway.app';
  const confirmUrl = BASE_URL + '/confirm/' + token;
  const cancelUrl  = BASE_URL + '/cancel/'  + token;

  // Build owner email HTML without nested template literals
  const sellerLineOwner = sellerAgent && sellerAgent.name
    ? '<p><b>Seller Agent:</b> ' + sellerAgent.name + (sellerAgent.brokerage ? ' — ' + sellerAgent.brokerage : '') + '<br>Phone: ' + (sellerAgent.phone||'—') + '</p>'
    : '';
  const tripLineOwner = trip.apply
    ? '<p style="background:#FFF3CD;padding:10px;border-radius:6px">Trip charge: $' + TRIP_CHARGE_AMT + ' (' + trip.miles + ' miles)</p>'
    : '';
  const notesLineOwner = notes ? '<p><b>Notes:</b> ' + notes + '</p>' : '';

  const ownerHtml = '<div style="font-family:Arial,sans-serif;max-width:560px">'
    + '<h2>New Booking Request — ' + confId + '</h2>'
    + '<p><b>Service:</b> ' + svcLabel + '<br><b>Add-ons:</b> ' + addonsLine + '</p>'
    + '<p><b>Date/Time:</b> ' + dateFmt + ' @ ' + time + (endTime ? ' to ' + endTime : '') + '</p>'
    + '<p><b>Address:</b> ' + address + '<br><b>Sq Ft:</b> ' + sqft + ' / <b>Year:</b> ' + yearBuilt + '</p>'
    + '<p><b>Est. Total:</b> $' + finalPrice + (trip.apply ? ' (incl. $' + TRIP_CHARGE_AMT + ' trip charge)' : '') + '</p>'
    + '<hr/>'
    + '<p><b>Buyer:</b> ' + fullName + '<br>Phone: ' + buyer.phone + '<br>Email: ' + buyer.email + '</p>'
    + '<p><b>Buyer Agent:</b> ' + buyerAgent.name + (buyerAgent.brokerage ? ' — ' + buyerAgent.brokerage : '') + '<br>Phone: ' + buyerAgent.phone + '</p>'
    + sellerLineOwner
    + notesLineOwner
    + tripLineOwner
    + '<div style="margin:28px 0">'
    + '<a href="' + confirmUrl + '" style="background:#1B2D52;color:white;padding:14px 28px;border-radius:6px;text-decoration:none;font-weight:700">CONFIRM AND SEND TEXTS</a>'
    + '&nbsp;&nbsp;'
    + '<a href="' + cancelUrl + '" style="background:#C0392B;color:white;padding:14px 28px;border-radius:6px;text-decoration:none;font-weight:700">CANCEL</a>'
    + '</div>'
    + '<p style="color:#888;font-size:.8rem">Texts will NOT go out until you tap Confirm.</p>'
    + '</div>';

  // Fire and forget — owner alert email
  sendEmail(process.env.OWNER_EMAIL, 'PENDING BOOKING: ' + fullName + ' — ' + dateFmt + ' @ ' + time, ownerHtml)
    .then(function(){ console.log('Owner alert sent for ' + confId); })
    .catch(function(e){ console.error('Owner alert email:', e.message); });

  // Send response immediately
  res.json({ success: true, confirmationId: confId, message: 'Request received! You will be confirmed shortly.' });

  // Text owner with booking summary + confirm/cancel links (non-blocking)
  const ownerSmsBody =
    'NEW BOOKING — ' + confId + '\n' +
    fullName + '\n' +
    address + '\n' +
    dateFmt + ' @ ' + time + '\n' +
    svcLabel + '\n' +
    '$' + finalPrice + (trip.apply ? ' (incl. trip charge)' : '') + '\n\n' +
    'CONFIRM:\n' + confirmUrl + '\n\n' +
    'CANCEL:\n' + cancelUrl;

  sms(process.env.OWNER_PHONE, ownerSmsBody)
    .catch(function(e){ console.error('Owner SMS:', e.message); });
});

// CONFIRM BOOKING
app.get('/confirm/:token', async function(req, res) {
  const booking = pendingBookings.get(req.params.token);
  if (!booking) return res.send('<h2>This confirmation link has expired or already been used.</h2>');

  const confId      = booking.confId;
  const address     = booking.address;
  const sqft        = booking.sqft;
  const yearBuilt   = booking.yearBuilt;
  const svcLabel    = booking.svcLabel;
  const addons      = booking.addons;
  const addonsLine  = booking.addonsLine;
  const finalPrice  = booking.finalPrice;
  const totalMins   = booking.totalMins;
  const date        = booking.date;
  const time        = booking.time;
  const endTime     = booking.endTime;
  const dateFmt     = booking.dateFmt;
  const fullName    = booking.fullName;
  const buyer       = booking.buyer;
  const buyerAgent  = booking.buyerAgent;
  const sellerAgent = booking.sellerAgent;
  const notes       = booking.notes;
  const tripCharge  = booking.tripCharge;

  pendingBookings.delete(req.params.token);

  const parts3 = date.split('-').map(Number);
  const y3=parts3[0], mo3=parts3[1], d3=parts3[2];
  const sm2 = slotToMins(time);
  const startDT2 = new Date(y3, mo3-1, d3, Math.floor(sm2/60), sm2%60, 0);
  const endDT2   = new Date(startDT2.getTime() + (totalMins||120)*60000);

  // Google Calendar
  let calId = null;
  try {
    const descLines = [
      'Conf: ' + confId,
      'Service: ' + svcLabel,
      addons.length ? 'Add-ons: ' + addonsLine : null,
      'Address: ' + address,
      'Sq Ft: ' + sqft + ' | Year: ' + yearBuilt,
      'Total: $' + finalPrice + (tripCharge.apply ? ' (incl. trip charge)' : ''),
      '',
      'BUYER: ' + fullName + ' | ' + buyer.phone + ' | ' + buyer.email,
      'BUYERS AGENT: ' + buyerAgent.name + (buyerAgent.brokerage ? ' — ' + buyerAgent.brokerage : '') + ' | ' + buyerAgent.phone,
      sellerAgent && sellerAgent.name ? 'SELLERS AGENT: ' + sellerAgent.name + (sellerAgent.brokerage ? ' — ' + sellerAgent.brokerage : '') + ' | ' + (sellerAgent.phone||'—') : null,
      notes ? 'Notes: ' + notes : null,
    ].filter(Boolean).join('\n');

    const ev = {
      summary:  svcLabel + ' — ' + fullName,
      location: address,
      description: descLines,
      start: { dateTime: startDT2.toISOString(), timeZone: TIMEZONE },
      end:   { dateTime: endDT2.toISOString(),   timeZone: TIMEZONE },
      colorId: '5',
      attendees: [{ email: buyer.email, displayName: fullName }],
      reminders: { useDefault: false, overrides: [{ method:'email', minutes:24*60 }, { method:'popup', minutes:60 }] },
    };
    const r = await calendar.events.insert({ calendarId: CALENDAR_ID, resource: ev, sendUpdates:'all' });
    calId = r.data.id;
    console.log('Calendar event created: ' + calId);
  } catch(e) { console.error('Calendar:', e.message); }

  // SMS Buyer
  await sms(buyer.phone,
    'Hi ' + buyer.firstName + '! Your inspection is confirmed.\n\n'
    + 'Address: ' + address + '\n'
    + 'Date: ' + dateFmt + '\n'
    + 'Time: ' + time + (endTime ? ' to ' + endTime : '') + '\n'
    + 'Service: ' + svcLabel + (addons.length ? '\nAdd-ons: ' + addonsLine : '') + '\n'
    + 'Est. Total: $' + finalPrice + ' (pay day-of)' + (tripCharge.apply ? ' incl. $' + TRIP_CHARGE_AMT + ' trip charge' : '') + '\n'
    + 'Conf #: ' + confId + '\n\n'
    + 'Questions? (480) 418-7633 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
  );

  // SMS Buyers Agent
  await sms(buyerAgent.phone,
    'Hi ' + buyerAgent.name + '! Inspection scheduled for your buyer.\n\n'
    + 'Address: ' + address + '\n'
    + 'Buyer: ' + fullName + '\n'
    + 'Date: ' + dateFmt + ' @ ' + time + '\n'
    + 'Service: ' + svcLabel + '\n'
    + 'Conf #: ' + confId + '\n\n'
    + 'ACTION NEEDED — Confirm with sellers agent:\n'
    + '- Sellers agent aware of date & time\n'
    + '- GAS on & accessible\n'
    + '- WATER on & accessible\n'
    + '- ELECTRICAL on & accessible\n'
    + '- ATTIC ACCESS clear & accessible\n\n'
    + 'Questions? (480) 418-7633 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
  );

  // SMS Sellers Agent
  if (sellerAgent && sellerAgent.phone) {
    await sms(sellerAgent.phone,
      'Hello' + (sellerAgent.name ? ' ' + sellerAgent.name : '') + '! Inspection scheduled at your listing.\n\n'
      + 'Address: ' + address + '\n'
      + 'Date: ' + dateFmt + ' @ ' + time + '\n'
      + 'Service: ' + svcLabel + '\n\n'
      + 'Please ensure by inspection day:\n'
      + '- GAS on & accessible\n'
      + '- WATER on & accessible\n'
      + '- ELECTRICAL on & accessible\n'
      + '- ATTIC ACCESS clear & accessible\n\n'
      + 'WARNING: If utilities are NOT on, a $125 re-inspection fee will apply.\n\n'
      + 'Questions? (480) 418-7633 | santanpropertyinspections@gmail.com\n— San Tan Property Inspections'
    );
  }

  // Square Invoice
  let invoiceUrl = null;
  try {
    const custSearch = await sq.customers.searchCustomers({
      query: { filter: { emailAddress: { exact: buyer.email } } },
    });
    let custId;
    if (custSearch.result.customers && custSearch.result.customers.length) {
      custId = custSearch.result.customers[0].id;
    } else {
      const nc = await sq.customers.createCustomer({
        givenName: buyer.firstName, familyName: buyer.lastName,
        emailAddress: buyer.email, phoneNumber: buyer.phone,
        idempotencyKey: uuidv4(),
      });
      custId = nc.result.customer.id;
    }
    const itemName = svcLabel + (addons.length ? ' + Add-ons' : '') + (tripCharge.apply ? ' + Trip Charge' : '');
    const order = await sq.orders.createOrder({
      order: {
        locationId: process.env.SQUARE_LOCATION_ID, customerId: custId,
        lineItems: [{
          name: itemName,
          quantity: '1',
          note: address + ' — ' + date + ' @ ' + time + ' | ' + confId,
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
        title: 'Inspection – ' + svcLabel,
        description: address + '\n' + date + ' @ ' + time + '\n' + confId,
        acceptedPaymentMethods: { card:true, bankAccount:false, squareGiftCard:false },
      },
      idempotencyKey: uuidv4(),
    });
    await sq.invoices.publishInvoice(inv.result.invoice.id, {
      version: inv.result.invoice.version, idempotencyKey: uuidv4(),
    });
    invoiceUrl = inv.result.invoice.publicUrl;
    console.log('Square invoice created');
  } catch(e) { console.error('Square:', e.message); }

  // Buyer confirmation email
  const invLine = invoiceUrl
    ? '<p>View your Square invoice: <a href="' + invoiceUrl + '">' + invoiceUrl + '</a> — or pay in person.</p>'
    : '<p>Payment via Square, Venmo, Zelle, or cash on inspection day.</p>';
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
    + invLine
    + '<p>Your report will be delivered the <strong>same day</strong> as your inspection.</p>'
    + '<p>Questions? Call/text <strong>(480) 418-7633</strong></p>'
    + '<hr style="border:none;border-top:1px solid #E8DFC8;margin:20px 0"/>'
    + '<p style="color:#888;font-size:.8rem">San Tan Property Inspections · East Valley, AZ · santanpropertyinspections.com</p>'
    + '</div>';

  try {
    await sendEmail(buyer.email, 'Inspection Confirmed — ' + dateFmt + ' @ ' + time + ' [' + confId + ']', buyerHtml);
    console.log('Confirmation email sent to buyer for ' + confId);
  } catch(e) { console.error('Buyer email:', e.message); }

  const tripConfirmLine = tripCharge.apply ? '<p>Trip charge of $' + TRIP_CHARGE_AMT + ' applied (' + tripCharge.miles + ' miles)</p>' : '';

  res.send('<div style="font-family:Arial,sans-serif;max-width:500px;margin:60px auto;text-align:center;padding:40px;border-top:4px solid #C9A84C">'
    + '<h2 style="color:#1B2D52">Booking Confirmed!</h2>'
    + '<p>Texts sent to buyer and agents.</p>'
    + '<p><b>' + fullName + '</b><br>' + dateFmt + ' @ ' + time + '<br>' + address + '</p>'
    + '<p style="color:#C9A84C;font-weight:700">Conf: ' + confId + '</p>'
    + tripConfirmLine
    + '</div>');
});

// CANCEL BOOKING
app.get('/cancel/:token', async function(req, res) {
  const booking = pendingBookings.get(req.params.token);
  if (!booking) return res.send('<h2>This link has expired or already been used.</h2>');
  const confId = booking.confId;
  const fullName = booking.fullName;
  const dateFmt = booking.dateFmt;
  const time = booking.time;
  pendingBookings.delete(req.params.token);
  console.log('Booking cancelled: ' + confId);
  res.send('<div style="font-family:Arial,sans-serif;max-width:500px;margin:60px auto;text-align:center;padding:40px;border-top:4px solid #C0392B">'
    + '<h2 style="color:#C0392B">Booking Cancelled</h2>'
    + '<p>The booking for <b>' + fullName + '</b> on ' + dateFmt + ' @ ' + time + ' has been cancelled.</p>'
    + '<p>No texts were sent.</p>'
    + '</div>');
});

// GOOGLE OAUTH
app.get('/auth/google', function(req, res) {
  res.redirect(oAuth2Client.generateAuthUrl({
    access_type:'offline',
    scope:['https://www.googleapis.com/auth/calendar'],
    prompt:'consent',
  }));
});
app.get('/auth/google/callback', async function(req, res) {
  const result = await oAuth2Client.getToken(req.query.code);
  console.log('REFRESH TOKEN:', result.tokens.refresh_token);
  res.send('<pre>Add to Railway as GOOGLE_REFRESH_TOKEN:\n\n' + result.tokens.refresh_token + '</pre>');
});

// TWILIO REPLY FORWARDING
app.post('/sms/reply', express.urlencoded({ extended: false }), async function(req, res) {
  const from  = req.body.From || 'Unknown';
  const body  = req.body.Body || '';
  const OWNER = process.env.OWNER_PHONE;
  if (OWNER) {
    await sms(OWNER, 'Reply from ' + from + ':\n' + body);
  }
  res.set('Content-Type', 'text/xml');
  res.send('<Response></Response>');
});

app.listen(PORT, function(){ console.log('San Tan Property Inspections backend on port ' + PORT); });
