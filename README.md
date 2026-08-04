# KDS Webhook Receiver — Setup Guide

## 1. Figure out which Wix integration you have

Wix pushes orders one of two ways. Check your Wix account to see which applies:

- **Wix dashboard → Settings → Restaurants app** — if you see "Wix Restaurants Orders"
  installed and you were approved as a **POS partner** (this requires applying to
  Wix as a POS integration), use the `/webhooks/pos-order` endpoint.
- **dev.wix.com → your registered app** — if you've built/registered a Wix app
  with the "Read Orders" permission scope, use the `/webhooks/ecom-order` endpoint
  and subscribe it to the `Order Created` event.

If neither exists yet, the eCommerce app path is the one to set up today — it's
self-serve at dev.wix.com, whereas POS partner approval is a manual Wix review.
Either way, this server handles both, so nothing here needs to change later.

## 2. Run it locally

```bash
npm install
npm start
```

This starts the server on port 3000 and prints the webhook URLs and a test
display page at `http://localhost:3000/kds.html`. Open that page in a browser
— that's your kitchen monitor for now.

## 3. Expose it to the internet (for testing with real Wix webhooks)

Wix needs an HTTPS URL it can reach, so during development tunnel your local
server with a tool like ngrok:

```bash
ngrok http 3000
```

Use the `https://...ngrok.io` URL it gives you + `/webhooks/pos-order` or
`/webhooks/ecom-order` as the webhook URL you register with Wix.

## 4. Simulate an order without waiting on Wix

```bash
curl -X POST http://localhost:3000/webhooks/pos-order \
  -H "Content-Type: application/json" \
  -d '{
    "id": "1001",
    "createdDate": "2026-07-25T18:30:00Z",
    "comment": "No onions please",
    "contact": { "name": "Alex" },
    "lineItems": [
      { "title": { "en_US": "Margherita Pizza" }, "quantity": 2, "modifiers": [{"title": {"en_US": "Extra cheese"}}] },
      { "title": { "en_US": "Caesar Salad" }, "quantity": 1 }
    ]
  }'
```

Watch it appear instantly on `http://localhost:3000/kds.html`.

## 5. Multi-location kitchens

One webhook receives orders for your **whole site** — Wix tags each order with
its `locationId`. This server routes each incoming order only to the screens
watching that location, so you can run a KDS per branch off the same backend.

Point each kitchen's monitor at its own location:
```
https://your-domain/kds.html?location=<locationId>
```
Find each branch's location ID from your Wix dashboard (Business Info →
location settings) or from the `locationId`/`businessLocationId` field on a
real incoming order — log one with `console.log(raw)` in the webhook route to
confirm the exact field name and ID values before wiring up all screens.

An overview screen that shows every location's orders in one place can
connect with `?location=all` instead.

## 6. Deploying for real use

For production, move this off localhost onto a real host (Render, Fly.io,
a small VPS, etc.) with a stable HTTPS domain, and:
- Set `WIX_WEBHOOK_PUBLIC_KEY` so the eCommerce route verifies JWT signatures
  instead of just decoding them.
- Point the kitchen monitor's browser permanently at `https://your-domain/kds.html`
  (or I can build you a fuller KDS UI — lanes for new/in-progress/done, sound
  alert on new order, auto-clear on complete, etc.).

## Files

- `server.js` — Express + WebSocket server, receives both webhook shapes
- `public/kds.html` — minimal live test display
- `package.json` — dependencies (`express`, `ws`, `jsonwebtoken`)
