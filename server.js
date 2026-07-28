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
const { WebSocketServer } = require("ws");
const jwt = require("jsonwebtoken");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/kds-stream" });

const PORT = process.env.PORT || 3000;

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
// Route 1: legacy POS SPI webhook — Wix POSTs raw JSON order
// -----------------------------------------------------------------------------
app.post(
  "/webhooks/pos-order",
  express.json({ limit: "2mb" }),
  (req, res) => {
    try {
      const raw = req.body;
      console.log("[pos-order] raw payload received:", JSON.stringify(raw, null, 2));
      const order = normalizePosOrder(raw);
      broadcastOrder(order);
      // Wix expects a 200 quickly to consider the order accepted-for-delivery.
      res.status(200).json({ status: "received" });
    } catch (err) {
      console.error("[pos-order] failed to process webhook:", err.message);
      res.status(400).json({ error: "invalid order payload" });
    }
  }
);

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
function normalizePosOrder(raw) {
  const rawItems = raw.lineItems || raw.items || [];
  const items = (Array.isArray(rawItems) ? rawItems : []).map((li) => ({
    name: li.title?.en_US || li.name || li.itemName || "Item",
    quantity: li.quantity || 1,
    modifiers: (li.modifiers || [])
      .map((m) => m.title?.en_US || m.name)
      .filter(Boolean),
    notes: li.comment || li.notes || null,
  }));

  // customerDetails may arrive as an object ({ firstName, lastName, ... })
  // or, from Wix Automations' custom body params, as a plain string.
  const customer =
    typeof raw.customerDetails === "string"
      ? raw.customerDetails
      : raw.customerDetails?.firstName || raw.contact?.name || raw.customer?.name || null;

  return {
    id: raw.orderId || raw.id || raw.orderNumber || String(Date.now()),
    locationId: raw.locationId || raw.businessLocationId || null,
    locationName: raw.locationName || raw.location?.name || null,
    placedAt: raw.createdDate || raw.dueDate || new Date().toISOString(),
    items,
    notes: raw.comment || raw.checkoutAdditionalFields || null,
    customer,
    deliveryType: raw.deliveryType || raw.type || null,
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
app.use(express.static("public"));

server.listen(PORT, () => {
  console.log(`KDS webhook receiver listening on port ${PORT}`);
  console.log(`  POS SPI webhook URL:  http://localhost:${PORT}/webhooks/pos-order`);
  console.log(`  eCommerce webhook URL: http://localhost:${PORT}/webhooks/ecom-order`);
  console.log(`  KDS WebSocket stream:  ws://localhost:${PORT}/kds-stream?location=<id>`);
  console.log(`  Test display page:     http://localhost:${PORT}/kds.html?location=<id>`);
  console.log(`  ("all" as the location shows every location's orders on one screen)`);
});
