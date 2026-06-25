# ═══════════════════════════════════════════════════════
#  AmbulanceOS Pro — Backend API
#  Deploy on Render (Python / gunicorn)
#
#  Start command (Render):  gunicorn app:app
#  Build command (Render):  pip install -r requirements.txt
# ═══════════════════════════════════════════════════════

import os
import json
import time
import threading

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import paho.mqtt.client as mqtt

# ── Flask app ─────────────────────────────────────────
app = Flask(__name__, static_folder="static")
CORS(app)

# ── MQTT config ───────────────────────────────────────
MQTT_BROKER   = os.environ.get("MQTT_BROKER",   "broker.hivemq.com")
MQTT_PORT     = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC    = os.environ.get("MQTT_TOPIC",    "emergency/vehicle/amb_01/location")
MQTT_CLIENT   = f"ambulanceos_backend_{int(time.time())}"

# ── Shared state (written by MQTT thread, read by API) ──
_lock = threading.Lock()
_state = {
    "vehicle_id":        "AMBULANCE_01",
    "latitude":          12.6514,
    "longitude":         77.2089,
    "heading":           0,
    "emergency_active":  False,
    "gps_fix":           False,
    "last_update":       None,
    "mqtt_connected":    False,
}
_event_log = []          # newest first, capped at 200


def _log_event(tag: str, message: str, data: dict | None = None):
    entry = {
        "time":    time.strftime("%H:%M:%S"),
        "tag":     tag,
        "message": message,
    }
    if data:
        entry["data"] = data
    with _lock:
        _event_log.insert(0, entry)
        if len(_event_log) > 200:
            _event_log.pop()


# ═══════════════════════════════════════════════════════
#  MQTT callbacks
# ═══════════════════════════════════════════════════════
def _on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Connected to {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC)
        with _lock:
            _state["mqtt_connected"] = True
        _log_event("MQTT", f"Connected — subscribed to {MQTT_TOPIC}")
    else:
        codes = {
            1: "incorrect protocol",
            2: "invalid client ID",
            3: "server unavailable",
            4: "bad credentials",
            5: "not authorised",
        }
        reason = codes.get(rc, f"code {rc}")
        print(f"[MQTT] Connect failed: {reason}")
        _log_event("MQTT", f"Connection failed: {reason}")


def _on_disconnect(client, userdata, rc):
    with _lock:
        _state["mqtt_connected"] = False
    if rc != 0:
        print(f"[MQTT] Unexpected disconnect (rc={rc}) — will auto-reconnect")
        _log_event("MQTT", "Disconnected — reconnecting…")


def _on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"[MQTT] Bad payload: {e}")
        return

    with _lock:
        _state.update({
            "vehicle_id":        payload.get("vehicle_id",        _state["vehicle_id"]),
            "latitude":          float(payload.get("latitude",    _state["latitude"])),
            "longitude":         float(payload.get("longitude",   _state["longitude"])),
            "heading":           float(payload.get("heading",     _state["heading"])),
            "emergency_active":  bool(payload.get("emergency_active", False)),
            "gps_fix":           bool(payload.get("gps_fix",      False)),
            "last_update":       time.time(),
        })
        snap = dict(_state)

    _log_event(
        "LOC",
        "lat={latitude:.5f} lng={longitude:.5f} hdg={heading:.0f}° emg={emergency_active}".format(**snap),
        {"latitude": snap["latitude"], "longitude": snap["longitude"]},
    )
    print(f"[MQTT] Location → {snap['latitude']}, {snap['longitude']}")


# ═══════════════════════════════════════════════════════
#  MQTT background thread
# ═══════════════════════════════════════════════════════
def _mqtt_thread():
    client = mqtt.Client(client_id=MQTT_CLIENT)
    client.on_connect    = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message    = _on_message

    while True:
        try:
            print(f"[MQTT] Connecting to {MQTT_BROKER}:{MQTT_PORT} …")
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            client.loop_forever()          # blocks; re-connects automatically
        except Exception as e:
            print(f"[MQTT] Error: {e} — retry in 10 s")
            with _lock:
                _state["mqtt_connected"] = False
            time.sleep(10)


# ═══════════════════════════════════════════════════════
#  REST API
# ═══════════════════════════════════════════════════════

@app.get("/api/status")
def api_status():
    """Health check — MQTT connection state and last-update timestamp."""
    with _lock:
        return jsonify({
            "status":           "running",
            "mqtt_broker":      MQTT_BROKER,
            "mqtt_topic":       MQTT_TOPIC,
            "mqtt_connected":   _state["mqtt_connected"],
            "last_update":      _state["last_update"],
            "emergency_active": _state["emergency_active"],
        })


@app.get("/api/location")
def api_location():
    """Latest known ambulance location."""
    with _lock:
        return jsonify(dict(_state))


@app.post("/api/emergency")
def api_emergency():
    """
    Set / clear the emergency flag from an external controller.
    Body: { "active": true|false }
    """
    body = request.get_json(silent=True) or {}
    active = bool(body.get("active", False))
    with _lock:
        _state["emergency_active"] = active
    _log_event("EMG", f"Emergency {'activated' if active else 'cleared'} via API")
    return jsonify({"status": "ok", "emergency_active": active})


@app.get("/api/log")
def api_log():
    """Return the last 50 events (newest first)."""
    limit = min(int(request.args.get("limit", 50)), 200)
    with _lock:
        return jsonify(_event_log[:limit])


@app.delete("/api/log")
def api_clear_log():
    """Clear the event log."""
    with _lock:
        _event_log.clear()
    return jsonify({"status": "ok"})


# ── Serve the web app (static files in ./static/) ─────
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_frontend(path):
    static_dir = app.static_folder
    if path and os.path.exists(os.path.join(static_dir, path)):
        return send_from_directory(static_dir, path)
    return send_from_directory(static_dir, "index.html")


# ═══════════════════════════════════════════════════════
#  Startup
# ═══════════════════════════════════════════════════════
def _start_background_services():
    t = threading.Thread(target=_mqtt_thread, daemon=True, name="mqtt-thread")
    t.start()
    print("[APP] MQTT background thread started")


_start_background_services()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"[APP] Starting dev server on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
