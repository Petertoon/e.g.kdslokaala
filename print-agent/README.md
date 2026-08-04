# KDS Print Agent — Setup Guide

Prints every order to your network receipt printers instead of (or alongside)
showing it on a screen. It works by connecting to the exact same live order
feed the on-screen board uses, so it plugs into a location's setup with no
changes needed on the server.

## What you need

- A computer that stays on and stays connected to the **same local network**
  as your printers (a cheap mini-PC or an old laptop tucked in the kitchen
  works fine — it doesn't need to be powerful)
- [Node.js](https://nodejs.org) installed on that computer
- Your printers' IP addresses (see below if you don't know them)
- Your location's ID from `https://<your-app>.onrender.com/locations`

## 1. Find your printers' IP addresses

Both the Epson and Star TSP100IV can print a "network status" or "self-test"
page directly from the printer (usually by holding a button while powering
it on, or from a small on-printer menu) — that page shows the current IP
address. Alternatively, check your router's connected-devices list for
"EPSON" or "STAR" entries.

Write both IPs down — you'll need them in step 3.

## 2. Install

Copy this whole `print-agent` folder onto the kitchen computer, then in a
terminal:

```bash
cd print-agent
npm install
```

## 3. Configure

Open `config.json` and fill in:

```json
{
  "serverHost": "e-g-kdslokaala-1.onrender.com",
  "locationId": "<paste the real ID from /locations here>",
  "printers": [
    { "name": "Epson kitchen printer", "type": "epson", "ip": "192.168.1.50", "port": 9100 },
    { "name": "Star TSP100IV", "type": "star", "ip": "192.168.1.51", "port": 9100 }
  ]
}
```

- `type` must be `"epson"` or `"star"` — this tells the agent which command
  language to use.
- `port` is almost always `9100` for both brands (the standard raw-print
  port) — you shouldn't need to change it unless your printer was
  specifically configured otherwise.
- If a location only has one printer, just delete the other entry from the
  `printers` array.

## 4. Run it

```bash
node agent.js
```

You should see:
```
[agent] KDS print agent starting for location "..."
[agent] printers configured: Epson kitchen printer, Star TSP100IV
[agent] connecting to wss://.../kds-stream?location=...
[agent] connected — waiting for orders
```

Trigger a test order (the **+ TEST TICKET** button on the board works fine
too, if that location also has a screen open) and confirm both printers
produce a ticket.

## 5. Keep it running permanently

Closing the terminal window stops the agent. For real use, keep it running
in the background and have it auto-start with the computer:

- **Easiest cross-platform option:** install [PM2](https://pm2.keymetrics.io/)
  ```bash
  npm install -g pm2
  pm2 start agent.js --name kds-print-agent
  pm2 save
  pm2 startup   # follow the printed instructions to enable auto-start on boot
  ```
- **Windows:** PM2 works here too, or use Task Scheduler to run
  `node agent.js` at login.
- **Mac:** PM2, or set it up as a `launchd` agent.

## Troubleshooting

- **"FAILED on [printer name]"** in the log — the printer is off, out of
  paper, or its IP address changed. Network printers can sometimes get a new
  IP after a router restart; if this starts happening repeatedly, set a
  **static IP** or a **DHCP reservation** for each printer in your router's
  settings so the IP never changes.
- **Star TSP100IV prints garbled text** — Star printers have a couple of
  different command modes. If `"star"` doesn't print cleanly, it's worth
  checking the printer's own utility/config page for a "emulation mode" or
  "command mode" setting and confirming it's set to Star Line Mode (the
  standard for this model), or trying `"epson"` as the type instead — many
  Star printers also support ESC/POS emulation.
- **Nothing prints and no error appears** — double check `locationId`
  matches exactly what's shown on `/locations`, and that the agent's log
  shows "connected — waiting for orders" (if it keeps saying
  "disconnected — retrying," the `serverHost` value is likely wrong).
