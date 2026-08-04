// KDS Print Agent
// -----------------------------------------------------------------------------
// Runs on a computer that's on the SAME local network as your printers.
// Connects out to the KDS server's live order feed (the exact same WebSocket
// the on-screen board uses) and, for every new order at your configured
// location, prints a ticket to each printer listed in config.json.
//
// This does NOT need any changes to server.js — the server already broadcasts
// orders to anyone watching a location; this script is just a "screen" that
// prints instead of displaying pixels.
//
// Setup:
//   1. npm install
//   2. Edit config.json: set locationId (from your /locations page) and each
//      printer's IP address (port 9100 is the standard raw print port both
//      Epson and Star network printers listen on).
//   3. node agent.js
//   4. Leave this running (see README.md for keeping it running in the
//      background / starting automatically with the computer).
// -----------------------------------------------------------------------------

const fs = require("fs");
const WebSocket = require("ws");
const { printer: ThermalPrinter, types: PrinterTypes } = require("node-thermal-printer");

const config = JSON.parse(fs.readFileSync("./config.json", "utf8"));

if (!config.locationId || config.locationId.startsWith("PASTE_THIS")) {
  console.error("[config] Please set a real locationId in config.json before starting.");
  process.exit(1);
}

// -----------------------------------------------------------------------------
// Build one printer connection per configured printer, up front
// -----------------------------------------------------------------------------
const printers = config.printers.map((p) => {
  const type = p.type.toLowerCase() === "star" ? PrinterTypes.STAR : PrinterTypes.EPSON;
  return {
    name: p.name,
    instance: new ThermalPrinter({
      type,
      interface: `tcp://${p.ip}:${p.port || 9100}`,
      removeSpecialCharacters: false,
      lineCharacter: "-",
    }),
  };
});

// -----------------------------------------------------------------------------
// Ticket formatting — one order becomes one printed ticket per printer
// -----------------------------------------------------------------------------
function buildTicket(printer, order) {
  printer.alignCenter();
  printer.setTextDoubleHeight();
  printer.bold(true);
  printer.println(`#${order.ticketNumber || order.id}`);
  printer.bold(false);
  printer.setTextNormal();

  if (order.customer || order.customerPhone) {
    printer.println([order.customer, order.customerPhone].filter(Boolean).join("  "));
  }

  const meta = [order.fulfillmentMethod, order.readyTime ? `ready ${order.readyTime}` : null]
    .filter(Boolean)
    .join(" · ");
  if (meta) printer.println(meta);

  printer.println(new Date(order.placedAt || Date.now()).toLocaleString());
  printer.drawLine();

  printer.alignLeft();
  for (const item of order.items || []) {
    printer.bold(true);
    printer.println(`${item.quantity}x ${item.name}`);
    printer.bold(false);
    if (item.modifiers && item.modifiers.length) {
      printer.println(`   ${item.modifiers.join(", ")}`);
    }
  }

  if (order.notes) {
    printer.drawLine();
    printer.println(`Note: ${order.notes}`);
  }

  printer.drawLine();
  printer.cut();
}

async function printOrder(order) {
  for (const p of printers) {
    try {
      p.instance.clear();
      buildTicket(p.instance, order);
      await p.instance.execute();
      console.log(`[print] ticket #${order.ticketNumber || order.id} sent to "${p.name}"`);
    } catch (err) {
      // One printer being offline/out of paper should never stop the others.
      console.error(`[print] FAILED on "${p.name}" (${err.message}) — check it's powered on, on the network, and has paper.`);
    }
  }
}

// -----------------------------------------------------------------------------
// Live order feed — same WebSocket the KDS board connects to
// -----------------------------------------------------------------------------
function connect() {
  const url = `wss://${config.serverHost}/kds-stream?location=${encodeURIComponent(config.locationId)}`;
  console.log(`[agent] connecting to ${url}`);
  const ws = new WebSocket(url);

  ws.on("open", () => console.log("[agent] connected — waiting for orders"));

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "new_order") {
        console.log(`[agent] new order #${msg.order.ticketNumber || msg.order.id}`);
        printOrder(msg.order);
      }
    } catch (err) {
      console.error("[agent] failed to process incoming message:", err.message);
    }
  });

  ws.on("close", () => {
    console.log("[agent] disconnected — retrying in 3s");
    setTimeout(connect, 3000);
  });

  ws.on("error", (err) => {
    console.error("[agent] connection error:", err.message);
  });
}

console.log(`[agent] KDS print agent starting for location "${config.locationId}"`);
console.log(`[agent] printers configured: ${printers.map((p) => p.name).join(", ") || "(none)"}`);
connect();
