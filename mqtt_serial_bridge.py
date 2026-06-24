"""
Enhanced MQTT-Serial Bridge
────────────────────────────────────────────────────────────────────────────────
Features added over the original:
  • JSON-structured payloads with timestamps
  • Automatic MQTT reconnection with exponential back-off
  • Graceful serial reconnection if Arduino resets
  • Configurable via a config.json file (no hardcoded values)
  • Rotating log file (mqtt_bridge.log) + console output
  • Command validation before forwarding to Arduino
  • LED state tracking and heartbeat topic
  • SIGTERM / KeyboardInterrupt clean shutdown
────────────────────────────────────────────────────────────────────────────────
config.json example
{
    "mqtt_broker":  "157.178.101.159",
    "mqtt_port":    1883,
    "serial_port":  "COM3",
    "baud_rate":    9600,
    "topic_sensor": "iot/sensor",
    "topic_led":    "iot/led",
    "topic_status": "iot/status",
    "topic_heartbeat": "iot/heartbeat",
    "heartbeat_interval": 30
}
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

import paho.mqtt.client as mqtt
import serial
import serial.tools.list_ports

# ── Configuration ─────────────────────────────────────────────────────────────

CONFIG_FILE = "config.json"

DEFAULT_CONFIG = {
    "mqtt_broker": "157.178.101.159",
    "mqtt_port": 1883,
    "serial_port": "COM3",
    "baud_rate": 9600,
    "topic_sensor": "iot/sensor",
    "topic_led": "iot/led",
    "topic_status": "iot/status",
    "topic_heartbeat": "iot/heartbeat",
    "heartbeat_interval": 30,   # seconds between heartbeat publishes
}

VALID_LED_COMMANDS = {"ON", "OFF", "BLINK", "BLINK_FAST", "BLINK_SLOW"}


def load_config() -> dict:
    """Load config from file if it exists, otherwise use defaults."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            user_cfg = json.load(f)
        cfg = {**DEFAULT_CONFIG, **user_cfg}
        log.info("Config loaded from %s", CONFIG_FILE)
    else:
        cfg = DEFAULT_CONFIG.copy()
        # Write defaults so the user can edit them
        with open(CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=4)
        log.info("No config.json found – defaults written to %s", CONFIG_FILE)
    return cfg


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("mqtt_bridge")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    # Rotating file handler (5 MB × 3 files)
    fh = RotatingFileHandler("mqtt_bridge.log", maxBytes=5 * 1024 * 1024, backupCount=3)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


log = setup_logging()

# ── Serial helpers ────────────────────────────────────────────────────────────

def open_serial(port: str, baud: int, retries: int = 5) -> serial.Serial | None:
    """Try to open the serial port, with retries."""
    for attempt in range(1, retries + 1):
        try:
            ser = serial.Serial(port, baud, timeout=1)
            time.sleep(2)   # Wait for Arduino to reset
            log.info("Serial port %s opened (baud=%d)", port, baud)
            return ser
        except serial.SerialException as exc:
            log.warning("Serial attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(3)
    log.error("Could not open serial port %s after %d attempts", port, retries)
    return None


def list_serial_ports() -> list[str]:
    """Return a list of available serial port names (useful for debugging)."""
    return [p.device for p in serial.tools.list_ports.comports()]


# ── MQTT helpers ──────────────────────────────────────────────────────────────

def publish_json(client: mqtt.Client, topic: str, data: dict) -> None:
    """Publish a dict as a JSON string with an added UTC timestamp."""
    data["ts"] = datetime.now(timezone.utc).isoformat()
    payload = json.dumps(data)
    result = client.publish(topic, payload)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        log.warning("Publish to %s failed (rc=%d)", topic, result.rc)


# ── Bridge class ──────────────────────────────────────────────────────────────

class MQTTSerialBridge:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.ser: serial.Serial | None = None
        self.running = False
        self.led_state = "UNKNOWN"
        self._last_heartbeat = 0.0
        self._reconnect_delay = 1   # seconds (doubles on each failure)

        # MQTT client setup
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        self.client.on_connect    = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message    = self._on_message

        # Last-will so subscribers know if we drop unexpectedly
        self.client.will_set(
            cfg["topic_status"],
            json.dumps({"status": "offline", "reason": "unexpected_disconnect"}),
            retain=True,
        )

    # ── MQTT callbacks ──────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("MQTT connected to %s:%d", self.cfg["mqtt_broker"], self.cfg["mqtt_port"])
            self._reconnect_delay = 1
            client.subscribe(self.cfg["topic_led"])
            publish_json(client, self.cfg["topic_status"], {"status": "online"})
        else:
            log.error("MQTT connection refused (rc=%d)", rc)

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            log.warning("Unexpected MQTT disconnect (rc=%d) – will reconnect", rc)

    def _on_message(self, client, userdata, msg):
        raw = msg.payload.decode().strip().upper()
        log.debug("MQTT ← %s : %s", msg.topic, raw)

        # Validate command before forwarding
        if raw not in VALID_LED_COMMANDS:
            log.warning("Ignored unknown LED command: '%s'", raw)
            publish_json(client, self.cfg["topic_status"], {
                "warning": "unknown_command",
                "received": raw,
                "valid_commands": list(VALID_LED_COMMANDS),
            })
            return

        self.led_state = raw
        self._send_to_arduino(raw)

    # ── Serial I/O ──────────────────────────────────────────────────────────

    def _send_to_arduino(self, command: str) -> None:
        if self.ser and self.ser.is_open:
            try:
                self.ser.write((command + "\n").encode())
                log.info("Serial → Arduino : %s", command)
            except serial.SerialException as exc:
                log.error("Serial write failed: %s", exc)
                self._reconnect_serial()
        else:
            log.warning("Serial port not open – cannot send '%s'", command)

    def _reconnect_serial(self) -> None:
        log.info("Attempting serial reconnect …")
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                pass
        self.ser = open_serial(self.cfg["serial_port"], self.cfg["baud_rate"])

    def _read_serial_line(self) -> str | None:
        """Non-blocking line read; returns stripped string or None."""
        try:
            if self.ser and self.ser.in_waiting:
                return self.ser.readline().decode(errors="replace").strip()
        except serial.SerialException as exc:
            log.error("Serial read error: %s", exc)
            self._reconnect_serial()
        return None

    # ── Heartbeat ───────────────────────────────────────────────────────────

    def _maybe_publish_heartbeat(self) -> None:
        now = time.time()
        if now - self._last_heartbeat >= self.cfg["heartbeat_interval"]:
            publish_json(self.client, self.cfg["topic_heartbeat"], {
                "status": "alive",
                "led_state": self.led_state,
                "serial_connected": bool(self.ser and self.ser.is_open),
            })
            self._last_heartbeat = now

    # ── Main loop ───────────────────────────────────────────────────────────

    def start(self) -> None:
        log.info("Available serial ports: %s", list_serial_ports())
        self.ser = open_serial(self.cfg["serial_port"], self.cfg["baud_rate"])

        # Connect MQTT with automatic reconnect
        self.client.connect_async(self.cfg["mqtt_broker"], self.cfg["mqtt_port"], keepalive=60)
        self.client.reconnect_delay_set(min_delay=1, max_delay=60)
        self.client.loop_start()

        self.running = True
        log.info("Bridge running – press Ctrl+C to stop")

        try:
            while self.running:
                line = self._read_serial_line()
                if line:
                    log.info("Arduino → : %s", line)
                    # Try to parse structured JSON from Arduino; fall back to raw string
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        data = {"raw": line}
                    publish_json(self.client, self.cfg["topic_sensor"], data)

                self._maybe_publish_heartbeat()
                time.sleep(0.05)    # 50 ms polling – keeps CPU quiet

        except KeyboardInterrupt:
            log.info("KeyboardInterrupt received – shutting down …")
        finally:
            self.stop()

    def stop(self) -> None:
        self.running = False
        publish_json(self.client, self.cfg["topic_status"], {"status": "offline", "reason": "clean_shutdown"})
        time.sleep(0.3)     # Let the last publish flush
        self.client.loop_stop()
        self.client.disconnect()
        if self.ser and self.ser.is_open:
            self.ser.close()
        log.info("Bridge stopped cleanly.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cfg = load_config()
    bridge = MQTTSerialBridge(cfg)

    # Handle SIGTERM (e.g. systemd stop)
    signal.signal(signal.SIGTERM, lambda *_: bridge.stop() or sys.exit(0))

    bridge.start()