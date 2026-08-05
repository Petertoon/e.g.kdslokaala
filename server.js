// KDS Webhook Receiver
// -----------------------------------------------------------------------------
// Receives new-order events pushed FROM Wix Restaurants and broadcasts a
// normalized order object to every connected Kitchen Display System (KDS)
// screen over a WebSocket, in real time.
//
// Wix has TWO different push mechanisms depending on how your restaurant
// site is integrated. This server exposes ONE endpoint for each, so it
// works no matter which one your Wix setup uses. You only need to point
// Wix at whichever one applies to you.
//
//   1. /webhooks/pos-order   -> Legacy "Wix Restaurants POS SPI" webhook.
//                                Wix POSTs the raw order as plain JSON.
//                                docs: github.com/wix-incubator/wix-restaurants-api
//
//   2. /webhooks/ecom-order  -> Newer Wix Apps / eCommerce "Order Created"
//                                webhook. Wix POSTs the event as a signed
//                                JWT in the request body (text/plain).
//                                docs: dev.wix.com/docs .../orders/order-created
//
// Both paths funnel into the same broadcastOrder() function so the KDS
// screen doesn't care which one fired.
// -----------------------------------------------------------------------------

const express = require("express");
const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");
const jwt = require("jsonwebtoken");
const { Pool } = require("pg");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/kds-stream" });

const PORT = process.env.PORT || 3000;
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || null;

// -----------------------------------------------------------------------------
// Menu database (Postgres) — required for the kiosk + admin menu editor.
// Menu data has to survive server restarts (which happen on every deploy),
// so unlike the in-memory "known locations" list, this cannot just live in a
// JS variable. Set DATABASE_URL in Render's environment variables to enable.
// -----------------------------------------------------------------------------
const pool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    })
  : null;

async function initDb() {
  if (!pool) {
    console.warn("[db] DATABASE_URL not set — kiosk/admin menu features are disabled until it is.");
    return;
  }
  await pool.query(`
    CREATE TABLE IF NOT EXISTS menu_items (
      id SERIAL PRIMARY KEY,
      location_id TEXT NOT NULL,
      category TEXT DEFAULT 'Menu',
      name TEXT NOT NULL,
      price NUMERIC,
      description TEXT,
      modifiers JSONB DEFAULT '[]',
      active BOOLEAN DEFAULT true,
      sort_order INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT now()
    );
  `);
  console.log("[db] connected and ready");
}
initDb().catch((err) => console.error("[db] failed to initialize:", err.message));

// Wix's public key for verifying eCommerce webhook JWTs (Order Created, etc).
// Get this from your app's dashboard on dev.wix.com and set it as an env var
// before going to production. Until then we decode WITHOUT verifying, which
// is fine for local testing but must not be used in production.
const WIX_WEBHOOK_PUBLIC_KEY = process.env.WIX_WEBHOOK_PUBLIC_KEY || null;

// -----------------------------------------------------------------------------
// Connected KDS screens, grouped by location.
// A screen connects as:  ws://host/kds-stream?location=<locationId>
// A screen that wants to see EVERY location (e.g. an owner's overview
// screen) connects as:   ws://host/kds-stream?location=all
// -----------------------------------------------------------------------------
const screensByLocation = new Map(); // locationId -> Set<ws>

function addScreen(locationId, ws) {
  if (!screensByLocation.has(locationId)) screensByLocation.set(locationId, new Set());
  screensByLocation.get(locationId).add(ws);
}

function removeScreen(locationId, ws) {
  screensByLocation.get(locationId)?.delete(ws);
}

// Remembers every location we've ever received an order from, so you can
// look up the right ID for each kitchen's screen without digging through logs.
const knownLocations = new Map(); // locationId -> { name, lastSeen }

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, "http://localhost");
  const locationId = url.searchParams.get("location") || "unassigned";

  ws.locationId = locationId;
  addScreen(locationId, ws);
  console.log(`[kds] screen connected for location "${locationId}"`);

  ws.on("close", () => {
    removeScreen(locationId, ws);
    console.log(`[kds] screen disconnected for location "${locationId}"`);
  });
});

function broadcastOrder(order) {
  if (order.locationId) {
    knownLocations.set(order.locationId, {
      name: order.locationName || null,
      lastSeen: new Date().toISOString(),
    });
  }

  const payload = JSON.stringify({ type: "new_order", order });
  let delivered = 0;

  // Screens watching this specific location
  for (const client of screensByLocation.get(order.locationId) || []) {
    if (client.readyState === client.OPEN) {
      client.send(payload);
      delivered++;
    }
  }
  // Screens watching "all" locations (e.g. an owner's dashboard)
  for (const client of screensByLocation.get("all") || []) {
    if (client.readyState === client.OPEN) {
      client.send(payload);
      delivered++;
    }
  }

  console.log(
    `[order] #${order.id} (location: ${order.locationId || "none"}) delivered to ${delivered} screen(s)`
  );
}


// -----------------------------------------------------------------------------
// Route 1: legacy POS SPI webhook — Wix POSTs raw JSON order.
// Registered under both spellings so it doesn't matter which one ends up
// pasted into Wix's Webhook URL field.
// -----------------------------------------------------------------------------
// Set DEBUG_FULL_PAYLOAD=true in Render's environment variables temporarily
// if you ever need to see the complete raw order again while debugging a
// new field-mapping issue. Leave it unset/false for normal operation so
// customer PII (email, phone, home address) never sits in the logs.
const DEBUG_FULL_PAYLOAD = process.env.DEBUG_FULL_PAYLOAD === "true";

function summarizeForLog(order) {
  return {
    id: order.id,
    ticketNumber: order.ticketNumber,
    locationId: order.locationId,
    itemCount: order.items.length,
    items: order.items.map((i) => `${i.quantity}x ${i.name}`),
    fulfillmentMethod: order.fulfillmentMethod,
  };
}

function handlePosOrder(req, res) {
  try {
    const raw = req.body;
    if (DEBUG_FULL_PAYLOAD) {
      console.log("[pos-order] FULL raw payload (debug mode):", JSON.stringify(raw, null, 2));
    }
    const order = normalizePosOrder(raw);
    console.log("[pos-order] order received:", JSON.stringify(summarizeForLog(order)));
    broadcastOrder(order);
    // Wix expects a 200 quickly to consider the order accepted-for-delivery.
    res.status(200).json({ status: "received" });
  } catch (err) {
    console.error("[pos-order] failed to process webhook:", err.message);
    res.status(400).json({ error: "invalid order payload" });
  }
}

app.post("/webhooks/pos-order", express.json({ limit: "2mb" }), handlePosOrder);
app.post("/webhooks/pos-orders", express.json({ limit: "2mb" }), handlePosOrder);

// -----------------------------------------------------------------------------
// Route 2: newer eCommerce "Order Created" webhook — body is a JWT string
// -----------------------------------------------------------------------------
app.post(
  "/webhooks/ecom-order",
  express.text({ type: "*/*", limit: "2mb" }),
  (req, res) => {
    try {
      const token = req.body.trim();

      const decoded = WIX_WEBHOOK_PUBLIC_KEY
        ? jwt.verify(token, WIX_WEBHOOK_PUBLIC_KEY, { algorithms: ["RS256"] })
        : jwt.decode(token); // dev-only fallback, no signature check

      if (!decoded) throw new Error("could not decode JWT");

      const eventData =
        typeof decoded.data === "string" ? JSON.parse(decoded.data) : decoded.data;

      const order = normalizeEcomOrder(eventData);
      broadcastOrder(order);
      res.status(200).send("ok");
    } catch (err) {
      console.error("[ecom-order] failed to process webhook:", err.message);
      res.status(400).json({ error: "invalid webhook token" });
    }
  }
);

// -----------------------------------------------------------------------------
// Normalizers — map each Wix payload shape into one simple ticket format
// the KDS screen understands: { id, locationId, placedAt, items[], notes, customer }
//
// NOTE: the exact field name Wix uses for location on an order (locationId vs
// businessLocationId vs something nested) can vary by app version. Log one
// real incoming payload from YOUR site (console.log(raw) in the route below)
// and adjust the lookups here to match exactly — the guesses below cover the
// common cases but should be confirmed against a live order.
// -----------------------------------------------------------------------------
// Looks up the first key (in order) that exists with a real, non-empty value.
function firstDefined(obj, keys) {
  for (const k of keys) {
    if (obj && obj[k] !== undefined && obj[k] !== null && obj[k] !== "") return obj[k];
  }
  return null;
}

function slugify(s = "") {
  return s
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");
}

function normalizePosOrder(raw) {
  // Wix Studio Automations wraps the real order under a top-level "data" key.
  const data = raw.data || raw;

  // Case 0 (preferred): "Entire payload" — the real, un-mapped order object.
  // This is the clean path confirmed against a real order; everything below
  // is legacy fallback from earlier experiments with custom body params.
  if (data.businessLocation || (data.orderId && Array.isArray(data.lineItems))) {
    const locationId =
      data.businessLocation?.id || slugify(data.businessLocation?.name || "unknown-location");

    const items = (data.lineItems || []).map((li) => ({
      name: li.name || li.title?.en_US || li.itemName || "Item",
      quantity: li.quantity || 1,
      modifiers: normalizeModifiers(
        li.modifierList ?? li.modifiers ?? li.options ?? li.choices ?? li.selectedOptions
      ),
      notes: li.specialRequests || li.comment || li.notes || null,
    }));

    const customerName =
      [data.contact?.name?.first, data.contact?.name?.last].filter(Boolean).join(" ") ||
      [data.customerDetails?.firstName, data.customerDetails?.lastName].filter(Boolean).join(" ") ||
      null;

    const customerPhone =
      data.customerDetails?.phone ||
      data.customerDetails?.recipientInfoPhoneNumber ||
      data.contact?.phones?.[0]?.formattedPhone ||
      null;

    return buildOrder({
      orderId: data.orderId,
      ticketNumber: data.orderNumber,
      locationId,
      locationName: data.businessLocation?.name || null,
      createdAt: data.createdDate,
      readyTime: data.orderReadyTime || null,
      fulfillmentMethod: data.fulfillmentMethod || null,
      items,
      customerName,
      customerPhone,
      notes: data.buyerNote || data.pickupInstructions || null,
    });
  }

  const orderId = firstDefined(data, ["orderId", "order id", "ticketNumber", "orderNumber", "id"]);
  const locationId = firstDefined(data, ["locationId", "Location", "location", "businessLocationId"]);
  const createdAt = firstDefined(data, ["createdAt", "createdDate", "dueDate"]);

  // Case 1: a real array of line items, under either name Wix has used.
  const rawItems = data.items || data.lineItems || raw.items || raw.lineItems;
  if (Array.isArray(rawItems) && rawItems.length) {
    const items = rawItems.map((li) => ({
      name: li.title?.en_US || li.name || li.itemName || "Item",
      quantity: li.quantity || 1,
      modifiers: (li.modifiers || [])
        .map((m) => m.title?.en_US || m.name)
        .filter(Boolean),
      notes: li.comment || li.notes || null,
    }));
    return buildOrder({ orderId, locationId, createdAt, items, data, raw });
  }

  // Case 2: a flat single-item shape (Item / Quantity / Modifiers & add-ons).
  if (data["Item"] !== undefined || data["Quantity"] !== undefined) {
    return buildOrder({
      orderId,
      locationId,
      createdAt,
      items: [
        {
          name: data["Item"] || "Item",
          quantity: Number(data["Quantity"]) || 1,
          modifiers: data["Modifiers & add-ons"] ? [data["Modifiers & add-ons"]] : [],
          notes: null,
        },
      ],
      data,
      raw,
    });
  }

  // Case 3: items came through as an unresolved placeholder string (e.g. the
  // Test button showing "lineItems" instead of real data) — nothing to show yet.
  return buildOrder({ orderId, locationId, createdAt, items: [], data, raw });
}

// Modifiers can arrive as an array of strings, an array of objects with a
// name/title, or something unexpected — normalize whatever shows up into a
// flat array of display strings without throwing.
function normalizeModifiers(mods) {
  if (!Array.isArray(mods)) return [];
  return mods
    .map((m) => (typeof m === "string" ? m : m?.title?.en_US || m?.name || m?.value || null))
    .filter(Boolean);
}

function buildOrder({
  orderId,
  ticketNumber,
  locationId,
  locationName,
  createdAt,
  readyTime,
  fulfillmentMethod,
  items,
  customerName,
  customerPhone,
  notes,
  data,
  raw,
}) {
  const customer =
    customerName ||
    (typeof data?.customerDetails === "string"
      ? data.customerDetails
      : data?.customerDetails?.firstName || raw?.contact?.name || raw?.customer?.name || null);

  return {
    id: orderId || String(Date.now()),
    ticketNumber: ticketNumber || null,
    locationId: locationId || null,
    locationName: locationName || null,
    placedAt: createdAt || new Date().toISOString(),
    readyTime: readyTime || null,
    fulfillmentMethod: fulfillmentMethod || null,
    items: items || [],
    notes: notes || data?.comment || data?.checkoutAdditionalFields || null,
    customer,
    customerPhone: customerPhone || null,
    deliveryType: null,
  };
}

function normalizeEcomOrder(entity) {
  const order = entity?.order || entity;
  const items = (order.lineItems || []).map((li) => ({
    name: li.productName?.original || li.name || "Item",
    quantity: li.quantity || 1,
    modifiers: (li.descriptionLines || [])
      .map((d) => d.plainText?.original || d.name?.original)
      .filter(Boolean),
    notes: null,
  }));

  return {
    id: order.number || order.id || String(Date.now()),
    locationId: order.locationId || order.businessLocationId || null,
    locationName: order.location?.name || null,
    placedAt: order._createdDate || new Date().toISOString(),
    items,
    notes: order.buyerNote || null,
    customer: order.buyerInfo?.contactDetails?.firstName || null,
    deliveryType: null,
  };
}

// -----------------------------------------------------------------------------
// Health check + static test KDS page
// -----------------------------------------------------------------------------
app.get("/health", (req, res) => {
  const screens = {};
  for (const [locationId, clients] of screensByLocation) screens[locationId] = clients.size;
  res.json({ status: "ok", screens });
});

// Simple lookup page: every location we've ever received an order from,
// with a ready-to-click link to that location's board. No auth on this —
// it only shows location IDs and names, no customer data.
app.get("/locations", (req, res) => {
  const rows = [...knownLocations.entries()].sort((a, b) => b[1].lastSeen.localeCompare(a[1].lastSeen));
  const rowsHtml = rows.length
    ? rows
        .map(
          ([id, info]) => `
        <tr>
          <td>${info.name || "(name unknown)"}</td>
          <td><code>${id}</code></td>
          <td>${new Date(info.lastSeen).toLocaleString()}</td>
          <td><a href="/board.html?location=${encodeURIComponent(id)}">Open board →</a></td>
        </tr>`
        )
        .join("")
    : `<tr><td colspan="4">No orders received yet — place a test order first.</td></tr>`;

  res.send(`
    <!DOCTYPE html>
    <html><head><title>KDS Locations</title>
    <style>
      body { font-family: -apple-system, Arial, sans-serif; padding: 24px; background:#111; color:#eee; }
      table { border-collapse: collapse; width: 100%; }
      td, th { text-align: left; padding: 8px 12px; border-bottom: 1px solid #333; }
      code { color: #ffb74d; }
      a { color: #4caf50; }
    </style></head>
    <body>
      <h2>Known Locations</h2>
      <p><a href="/board.html?location=all">Open the "all locations" overview board →</a></p>
      <table>
        <tr><th>Location name</th><th>Location ID</th><th>Last order seen</th><th>Screen link</th></tr>
        ${rowsHtml}
      </table>
    </body></html>
  `);
});

app.use(express.static("public"));

// -----------------------------------------------------------------------------
// Menu + Kiosk API
// -----------------------------------------------------------------------------
// Everything below reuses the SAME broadcastOrder() function the Wix webhooks
// use, so a kiosk order shows up on the KDS board / prints exactly like a
// Wix order does — just without ever touching Wix.
// -----------------------------------------------------------------------------

function requireDb(req, res, next) {
  if (!pool) return res.status(503).json({ error: "Menu database not configured yet (DATABASE_URL missing)." });
  next();
}

// Simple shared-password protection for menu editing. Not full user
// accounts — fine for a single owner managing menus remotely, not meant
// for multiple staff logins with different permissions.
function requireAdmin(req, res, next) {
  if (!ADMIN_PASSWORD) {
    return res.status(503).send("Set ADMIN_PASSWORD in your environment variables to enable the menu editor.");
  }
  const auth = req.headers.authorization;
  if (auth && auth.startsWith("Basic ")) {
    const [, password] = Buffer.from(auth.slice(6), "base64").toString().split(":");
    if (password === ADMIN_PASSWORD) return next();
  }
  res.set("WWW-Authenticate", 'Basic realm="KDS Menu Admin"');
  return res.status(401).send("Authentication required.");
}

let kioskOrderSeq = 0; // resets on restart — cosmetic ticket label only, not a unique ID

// ---- Public: fetch a location's active menu, for the kiosk to render ------
app.get("/api/menu/:locationId", requireDb, async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT id, category, name, price, description, modifiers
       FROM menu_items WHERE location_id = $1 AND active = true
       ORDER BY category, sort_order, name`,
      [req.params.locationId]
    );
    res.json(rows);
  } catch (err) {
    console.error("[menu] fetch failed:", err.message);
    res.status(500).json({ error: "Could not load menu" });
  }
});

// ---- Admin: full CRUD on a location's menu (protected) --------------------
app.get("/api/admin/menu/:locationId", requireDb, requireAdmin, async (req, res) => {
  const { rows } = await pool.query(
    `SELECT * FROM menu_items WHERE location_id = $1 ORDER BY category, sort_order, name`,
    [req.params.locationId]
  );
  res.json(rows);
});

app.post("/api/admin/menu/:locationId", requireDb, requireAdmin, express.json(), async (req, res) => {
  const { category, name, price, description, modifiers, active, sortOrder } = req.body;
  if (!name) return res.status(400).json({ error: "name is required" });
  const { rows } = await pool.query(
    `INSERT INTO menu_items (location_id, category, name, price, description, modifiers, active, sort_order)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
    [
      req.params.locationId,
      category || "Menu",
      name,
      price ?? null,
      description || null,
      JSON.stringify(modifiers || []),
      active !== false,
      sortOrder || 0,
    ]
  );
  res.json(rows[0]);
});

app.put("/api/admin/menu/item/:id", requireDb, requireAdmin, express.json(), async (req, res) => {
  const { category, name, price, description, modifiers, active, sortOrder } = req.body;
  const { rows } = await pool.query(
    `UPDATE menu_items SET category=$1, name=$2, price=$3, description=$4, modifiers=$5, active=$6, sort_order=$7
     WHERE id=$8 RETURNING *`,
    [category || "Menu", name, price ?? null, description || null, JSON.stringify(modifiers || []), active !== false, sortOrder || 0, req.params.id]
  );
  if (!rows.length) return res.status(404).json({ error: "not found" });
  res.json(rows[0]);
});

app.delete("/api/admin/menu/item/:id", requireDb, requireAdmin, async (req, res) => {
  await pool.query(`DELETE FROM menu_items WHERE id = $1`, [req.params.id]);
  res.json({ ok: true });
});

// ---- Admin page itself (protected) -----------------------------------------
// Served from /private, NOT /public — the public folder is served statically
// with no auth, so the admin HTML must never live there or it could be
// reached directly at its filename, bypassing requireAdmin entirely.
app.get("/admin", requireAdmin, (req, res) => {
  res.sendFile(__dirname + "/private/admin.html");
});

// ---- Kiosk order submission -------------------------------------------------
// Looks up real prices/names server-side (never trusts the browser's copy),
// builds the same order shape the Wix webhooks produce, and feeds it into
// the exact same broadcastOrder() the rest of the system already uses.
app.post("/api/kiosk-order", requireDb, express.json(), async (req, res) => {
  try {
    const { locationId, customerName, items } = req.body;
    if (!locationId || !Array.isArray(items) || !items.length) {
      return res.status(400).json({ error: "locationId and at least one item are required" });
    }

    const ids = items.map((i) => i.itemId);
    const { rows: dbItems } = await pool.query(
      `SELECT id, name, price, modifiers FROM menu_items WHERE id = ANY($1::int[]) AND location_id = $2`,
      [ids, locationId]
    );
    const byId = Object.fromEntries(dbItems.map((r) => [r.id, r]));

    const orderItems = items
      .map((i) => {
        const dbItem = byId[i.itemId];
        if (!dbItem) return null;
        return {
          name: dbItem.name,
          quantity: i.quantity || 1,
          modifiers: Array.isArray(i.selectedModifiers) ? i.selectedModifiers : [],
          notes: i.notes || null,
        };
      })
      .filter(Boolean);

    if (!orderItems.length) return res.status(400).json({ error: "No valid items in order" });

    kioskOrderSeq++;
    const order = {
      id: crypto.randomUUID(),
      ticketNumber: `K${kioskOrderSeq}`,
      locationId,
      locationName: knownLocations.get(locationId)?.name || null,
      placedAt: new Date().toISOString(),
      readyTime: null,
      fulfillmentMethod: "Kiosk — pay at counter",
      items: orderItems,
      notes: null,
      customer: customerName || null,
      customerPhone: null,
      deliveryType: null,
    };

    broadcastOrder(order);
    res.json({ ok: true, ticketNumber: order.ticketNumber });
  } catch (err) {
    console.error("[kiosk-order] failed:", err.message);
    res.status(500).json({ error: "Could not place order" });
  }
});

server.listen(PORT, () => {
  console.log(`KDS webhook receiver listening on port ${PORT}`);
  console.log(`  POS SPI webhook URL:  http://localhost:${PORT}/webhooks/pos-order  (also accepts /webhooks/pos-orders)`);
  console.log(`  eCommerce webhook URL: http://localhost:${PORT}/webhooks/ecom-order`);
  console.log(`  KDS WebSocket stream:  ws://localhost:${PORT}/kds-stream?location=<id>`);
  console.log(`  Test display page:     http://localhost:${PORT}/kds.html?location=<id>`);
  console.log(`  ("all" as the location shows every location's orders on one screen)`);
});
