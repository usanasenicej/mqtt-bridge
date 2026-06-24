"""
MQTT Bridge – Web Dashboard
────────────────────────────────────────────────────────────────────────────────
Run alongside mqtt_serial_bridge.py:
    python dashboard.py

Then open http://localhost:5000 in your browser.

Subscribes to the same MQTT topics and exposes:
  • GET  /           → live HTML dashboard
  • GET  /api/state  → current state as JSON
  • POST /api/led    → send LED command  { "command": "ON" }
────────────────────────────────────────────────────────────────────────────────
Install deps:
    pip install flask paho-mqtt
"""

import json
import os
import time
from collections import deque
from datetime import datetime, timezone
from threading import Lock

import paho.mqtt.client as mqtt
from flask import Flask, jsonify, render_template_string, request

# ── Load shared config ────────────────────────────────────────────────────────

CONFIG_FILE = "config.json"
DEFAULT_CONFIG = {
    "mqtt_broker": "157.178.101.159",
    "mqtt_port": 1883,
    "topic_sensor": "iot/sensor",
    "topic_led": "iot/led",
    "topic_status": "iot/status",
    "topic_heartbeat": "iot/heartbeat",
}

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            return {**DEFAULT_CONFIG, **json.load(f)}
    return DEFAULT_CONFIG.copy()

cfg = load_config()

# ── Shared state ──────────────────────────────────────────────────────────────

MAX_HISTORY = 50   # sensor readings to keep in memory

state = {
    "bridge_status": "unknown",
    "led_state": "UNKNOWN",
    "last_heartbeat": None,
    "serial_connected": False,
    "sensor_history": deque(maxlen=MAX_HISTORY),
}
state_lock = Lock()

VALID_COMMANDS = ["ON", "OFF", "BLINK", "BLINK_FAST", "BLINK_SLOW"]

# ── MQTT subscriber ───────────────────────────────────────────────────────────

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe([
            (cfg["topic_sensor"],    0),
            (cfg["topic_status"],    0),
            (cfg["topic_heartbeat"], 0),
        ])

def on_message(client, userdata, msg):
    topic   = msg.topic
    payload = msg.payload.decode(errors="replace").strip()

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        data = {"raw": payload}

    with state_lock:
        if topic == cfg["topic_sensor"]:
            data["_received"] = datetime.now(timezone.utc).isoformat()
            state["sensor_history"].appendleft(data)

        elif topic == cfg["topic_status"]:
            state["bridge_status"] = data.get("status", "unknown")

        elif topic == cfg["topic_heartbeat"]:
            state["led_state"]        = data.get("led_state", "UNKNOWN")
            state["serial_connected"] = data.get("serial_connected", False)
            state["last_heartbeat"]   = data.get("ts", datetime.now(timezone.utc).isoformat())


sub_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
sub_client.on_connect = on_connect
sub_client.on_message = on_message
sub_client.connect_async(cfg["mqtt_broker"], cfg["mqtt_port"], keepalive=60)
sub_client.loop_start()

# Publisher client (separate to avoid threading conflicts)
pub_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
pub_client.connect(cfg["mqtt_broker"], cfg["mqtt_port"], keepalive=60)
pub_client.loop_start()

# ── Flask app ─────────────────────────────────────────────────────────────────

app = Flask(__name__)

HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>IoT Bridge Dashboard</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:       #0f1117;
    --surface:  #1a1d27;
    --border:   #2a2d3a;
    --accent:   #4f8ef7;
    --green:    #3ecf8e;
    --red:      #f75a5a;
    --yellow:   #f7c948;
    --text:     #e2e8f0;
    --muted:    #64748b;
    --radius:   12px;
    --mono:     'JetBrains Mono', 'Fira Code', monospace;
  }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    min-height: 100vh;
    padding: 24px;
  }

  header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 28px;
  }
  header h1 { font-size: 1.4rem; font-weight: 700; letter-spacing: -0.02em; }
  header .dot {
    width: 10px; height: 10px;
    border-radius: 50%;
    background: var(--muted);
    transition: background .4s;
  }
  header .dot.online  { background: var(--green); box-shadow: 0 0 8px var(--green); }
  header .dot.offline { background: var(--red);   box-shadow: 0 0 8px var(--red);   }

  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }

  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
  }
  .card .label {
    font-size: .75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .08em;
    color: var(--muted);
    margin-bottom: 8px;
  }
  .card .value {
    font-size: 1.6rem;
    font-weight: 700;
    font-family: var(--mono);
  }
  .card .value.green  { color: var(--green); }
  .card .value.red    { color: var(--red);   }
  .card .value.yellow { color: var(--yellow);}
  .card .value.muted  { color: var(--muted); }

  /* LED control */
  .led-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
    margin-bottom: 24px;
  }
  .led-panel .label {
    font-size: .75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .08em;
    color: var(--muted);
    margin-bottom: 14px;
  }
  .btn-row { display: flex; flex-wrap: wrap; gap: 10px; }
  .btn {
    padding: 9px 20px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: transparent;
    color: var(--text);
    font-size: .875rem;
    font-weight: 600;
    cursor: pointer;
    transition: background .15s, border-color .15s, color .15s;
  }
  .btn:hover                    { background: var(--border); }
  .btn.active                   { background: var(--accent); border-color: var(--accent); color: #fff; }
  .btn[data-cmd="ON"]:hover     { background: var(--green);  border-color: var(--green);  color: #000; }
  .btn[data-cmd="OFF"]:hover    { background: var(--red);    border-color: var(--red);    color: #fff; }
  .btn[data-cmd="BLINK"]:hover,
  .btn[data-cmd="BLINK_FAST"]:hover,
  .btn[data-cmd="BLINK_SLOW"]:hover { background: var(--yellow); border-color: var(--yellow); color: #000; }

  /* Sensor log */
  .log-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
  }
  .log-panel .label {
    font-size: .75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .08em;
    color: var(--muted);
    margin-bottom: 14px;
  }
  #log {
    font-family: var(--mono);
    font-size: .8rem;
    line-height: 1.7;
    color: var(--text);
    max-height: 320px;
    overflow-y: auto;
  }
  #log .entry {
    padding: 4px 0;
    border-bottom: 1px solid var(--border);
    display: flex;
    gap: 12px;
  }
  #log .ts  { color: var(--muted); white-space: nowrap; }
  #log .val { color: var(--green); word-break: break-all; }

  .toast {
    position: fixed;
    bottom: 24px; right: 24px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 12px 18px;
    font-size: .875rem;
    opacity: 0;
    transform: translateY(8px);
    transition: opacity .2s, transform .2s;
    pointer-events: none;
  }
  .toast.show { opacity: 1; transform: translateY(0); }

  footer {
    margin-top: 28px;
    font-size: .75rem;
    color: var(--muted);
    text-align: center;
  }
</style>
</head>
<body>

<header>
  <div class="dot" id="dot"></div>
  <h1>IoT Bridge Dashboard</h1>
</header>

<div class="grid">
  <div class="card">
    <div class="label">Bridge Status</div>
    <div class="value" id="bridge-status">—</div>
  </div>
  <div class="card">
    <div class="label">LED State</div>
    <div class="value" id="led-state">—</div>
  </div>
  <div class="card">
    <div class="label">Serial Connected</div>
    <div class="value" id="serial-conn">—</div>
  </div>
  <div class="card">
    <div class="label">Last Heartbeat</div>
    <div class="value" id="heartbeat" style="font-size:1rem">—</div>
  </div>
</div>

<div class="led-panel">
  <div class="label">LED Control</div>
  <div class="btn-row" id="btn-row">
    <button class="btn" data-cmd="ON">ON</button>
    <button class="btn" data-cmd="OFF">OFF</button>
    <button class="btn" data-cmd="BLINK">Blink</button>
    <button class="btn" data-cmd="BLINK_FAST">Blink Fast</button>
    <button class="btn" data-cmd="BLINK_SLOW">Blink Slow</button>
  </div>
</div>

<div class="log-panel">
  <div class="label">Sensor Feed</div>
  <div id="log"><div style="color:var(--muted)">Waiting for data…</div></div>
</div>

<div class="toast" id="toast"></div>

<footer>Auto-refreshes every 2 s &nbsp;·&nbsp; MQTT broker: {{ broker }}</footer>

<script>
  const POLL_MS = 2000;
  let lastLed = null;

  function fmtTs(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    return d.toLocaleTimeString();
  }

  function showToast(msg, ok = true) {
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.style.borderColor = ok ? 'var(--green)' : 'var(--red)';
    t.classList.add('show');
    setTimeout(() => t.classList.remove('show'), 2200);
  }

  async function poll() {
    try {
      const res  = await fetch('/api/state');
      const data = await res.json();

      // Status dot + bridge status
      const dot  = document.getElementById('dot');
      const bSt  = document.getElementById('bridge-status');
      const s    = (data.bridge_status || 'unknown').toLowerCase();
      dot.className = 'dot ' + (s === 'online' ? 'online' : s === 'offline' ? 'offline' : '');
      bSt.textContent  = data.bridge_status || '—';
      bSt.className    = 'value ' + (s === 'online' ? 'green' : s === 'offline' ? 'red' : 'muted');

      // LED state
      const led = document.getElementById('led-state');
      led.textContent = data.led_state || '—';
      led.className   = 'value ' + (data.led_state === 'ON' ? 'green' : data.led_state === 'OFF' ? 'red' : 'yellow');

      // Highlight active button
      if (data.led_state !== lastLed) {
        document.querySelectorAll('#btn-row .btn').forEach(b => {
          b.classList.toggle('active', b.dataset.cmd === data.led_state);
        });
        lastLed = data.led_state;
      }

      // Serial
      const sc = document.getElementById('serial-conn');
      sc.textContent = data.serial_connected ? 'Yes' : 'No';
      sc.className   = 'value ' + (data.serial_connected ? 'green' : 'red');

      // Heartbeat
      document.getElementById('heartbeat').textContent = fmtTs(data.last_heartbeat);

      // Sensor log
      const log = document.getElementById('log');
      if (data.sensor_history && data.sensor_history.length) {
        log.innerHTML = data.sensor_history.map(e => {
          const ts  = fmtTs(e._received || e.ts);
          const val = JSON.stringify(e, (k, v) => k.startsWith('_') ? undefined : v);
          return `<div class="entry"><span class="ts">${ts}</span><span class="val">${val}</span></div>`;
        }).join('');
      }
    } catch (err) {
      console.warn('Poll error', err);
    }
  }

  // LED command buttons
  document.getElementById('btn-row').addEventListener('click', async e => {
    const btn = e.target.closest('.btn');
    if (!btn) return;
    const cmd = btn.dataset.cmd;
    try {
      const res = await fetch('/api/led', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ command: cmd }),
      });
      const data = await res.json();
      showToast(data.ok ? `Sent: ${cmd}` : data.error, data.ok);
    } catch (err) {
      showToast('Request failed', false);
    }
  });

  poll();
  setInterval(poll, POLL_MS);
</script>
</body>
</html>
"""

@app.get("/")
def index():
    return render_template_string(HTML, broker=cfg["mqtt_broker"])


@app.get("/api/state")
def api_state():
    with state_lock:
        return jsonify({
            "bridge_status":  state["bridge_status"],
            "led_state":      state["led_state"],
            "last_heartbeat": state["last_heartbeat"],
            "serial_connected": state["serial_connected"],
            "sensor_history": list(state["sensor_history"]),
        })


@app.post("/api/led")
def api_led():
    body = request.get_json(silent=True) or {}
    cmd  = str(body.get("command", "")).upper().strip()
    if cmd not in VALID_COMMANDS:
        return jsonify({"ok": False, "error": f"Invalid command. Valid: {VALID_COMMANDS}"}), 400
    pub_client.publish(cfg["topic_led"], cmd)
    return jsonify({"ok": True, "sent": cmd})


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Dashboard running → http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)