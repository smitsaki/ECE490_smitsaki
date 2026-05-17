"""
ECE490 – Lab 5
Integrated IoT Monitoring, Decision Logic & MQTT Control Channel

This skeleton extends the Lab4 ingestion script.

Starting point from previous labs:
- subscribe to instructor MQTT topics
- filter assigned measurement
- store raw values in InfluxDB

Lab5 extensions:
- periodically compute analytics from recent values
- write analytics results to InfluxDB for Grafana
- publish MQTT control commands to an actuator/end device
- receive manual override commands
- receive actuator status messages
- log system events to InfluxDB

Important:
This is ONE integrated system, not independent scripts.
"""

import json
import random
import socket
import time
from typing import Optional, List

import paho.mqtt.client as mqtt
import requests
from requests.auth import HTTPBasicAuth


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

BROKER = "194.177.207.38"
PORT = 1883

TEAM = socket.gethostname()

MQTT_USERNAME = "team1"
MQTT_PASSWORD = "team1!@#$"

INFLUXDB_URL = "http://194.177.207.38:8086"
DB_NAME = f"{TEAM}_db"
DB_USERNAME = TEAM
DB_PASSWORD = "team1@#$"

ASSIGNED_MEASUREMENT = "airHumidity"    # "airTemperature"

# Telemetry from instructor / sensors
SUBSCRIPTION = "iot/instructor/#"

ACTUATOR = "led"

CONTROL_TOPIC = f"iot/{TEAM}/control/{ACTUATOR}"
STATUS_TOPIC = f"iot/{TEAM}/status/{ACTUATOR}"
MANUAL_TOPIC = f"iot/{TEAM}/manual/{ACTUATOR}"

# InfluxDB measurements
RAW_MEASUREMENT = ASSIGNED_MEASUREMENT
ANALYTICS_MEASUREMENT = f"{ASSIGNED_MEASUREMENT}_analytics"
EVENT_MEASUREMENT = "system_events"
STATUS_MEASUREMENT = "actuator_status"

# Analytics parameters
WINDOW_SIZE = 5
QUERY_INTERVAL_SECONDS = 5

WARNING_THRESHOLD = 24.0
ALERT_THRESHOLD = 25.0
RECOVERY_THRESHOLD = 23.0

# Temporal condition:
# moving average must stay above ALERT_THRESHOLD for this duration
ALERT_HOLD_SECONDS = 10

client_id = f"client_{random.randint(0, 1000)}"


# ---------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------

current_state = "NORMAL"
control_mode = "AUTO"
manual_command = None
last_published_command = None

high_condition_started_at = None
last_analytics_time = 0.0


# ---------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------

def parse_payload_as_float(payload: str) -> Optional[float]:
    """Return a float value or None if parsing fails."""
    try:
        return float(payload)
    except ValueError:
        try:
            data = json.loads(payload)
            return float(data.get("value"))
        except (json.JSONDecodeError, TypeError, ValueError):
            return None


def extract_measurement_from_topic(topic: str) -> str:
    """Extract measurement name from the MQTT topic."""
    return topic.split("/")[-1]


def interpret_message(topic: str, payload: str) -> str:
    """Return a human-readable message for the assigned measurement only."""
    measurement = extract_measurement_from_topic(topic)
    if measurement != ASSIGNED_MEASUREMENT:
        return ""
    return f"Received for {measurement}: {payload}"


def escape_string_field(value: str) -> str:
    """Escape a string before writing it as an InfluxDB string field."""
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


# ---------------------------------------------------------------------
# InfluxDB write helpers using requests.post()
# ---------------------------------------------------------------------

def write_line_protocol(line: str) -> bool:
    """Write one line-protocol point to InfluxDB."""
    url = f"{INFLUXDB_URL}/write"
    params = {"db": DB_NAME}
    
    try:
        response = requests.post(
            url,
            params=params,
            auth=HTTPBasicAuth(DB_USERNAME, DB_PASSWORD),
            data=line
        )
        return response.status_code >= 200 and response.status_code < 300
    except requests.RequestException as e:
        print(f"Failed to write to InfluxDB: {e}")
        return False


def insert_data(measurement: str, value: float) -> bool:
    """Insert one raw data point into InfluxDB using line protocol."""
    line = f"{measurement},team={TEAM} value={value}"
    return write_line_protocol(line)


def insert_analytics(moving_avg: float, trend: str, state: str, mode: str) -> bool:
    """Insert analytics results into InfluxDB."""
    trend_esc = escape_string_field(trend)
    state_esc = escape_string_field(state)
    mode_esc = escape_string_field(mode)
    
    line = f"{ANALYTICS_MEASUREMENT},team={TEAM} moving_avg={moving_avg},trend=\"{trend_esc}\",state=\"{state_esc}\",mode=\"{mode_esc}\""
    return write_line_protocol(line)


def log_event(
    event_type: str,
    details: str = "",
    state: str = "",
    mode: str = "",
    topic: str = "",
    command: str = "",
) -> bool:
    """Log a system/action event to InfluxDB."""
    details_esc = escape_string_field(details)
    state_esc = escape_string_field(state)
    mode_esc = escape_string_field(mode)
    topic_esc = escape_string_field(topic)
    command_esc = escape_string_field(command)
    
    line = (f'{EVENT_MEASUREMENT},event_type={event_type},team={TEAM} '
            f'details="{details_esc}",state="{state_esc}",mode="{mode_esc}",'
            f'topic="{topic_esc}",command="{command_esc}"')
    return write_line_protocol(line)


def insert_actuator_status(status: str, led: str = "", source: str = "") -> bool:
    """Store actuator status messages in InfluxDB."""
    status_esc = escape_string_field(status)
    led_esc = escape_string_field(led)
    source_esc = escape_string_field(source)
    
    line = f'{STATUS_MEASUREMENT},team={TEAM} status="{status_esc}",led="{led_esc}",source="{source_esc}"'
    return write_line_protocol(line)


# ---------------------------------------------------------------------
# InfluxDB query helper
# ---------------------------------------------------------------------

def query_recent_values(limit: int = WINDOW_SIZE) -> List[float]:
    """Query recent raw values from InfluxDB."""
    url = f"{INFLUXDB_URL}/query"
    query = (
        f'SELECT "value" FROM "{RAW_MEASUREMENT}" '
        f"ORDER BY time DESC LIMIT {limit}"
    )
    params = {
        "db": DB_NAME,
        "q": query,
    }

    try:
        response = requests.get(
            url,
            params=params,
            auth=HTTPBasicAuth(DB_USERNAME, DB_PASSWORD)
        )
        if response.status_code == 200:
            data = response.json()
            if "series" in data.get("results", [{}])[0]:
                values = [row[1] for row in data["results"][0]["series"][0]["values"] if row[1] is not None]
                return values[::-1]
    except requests.RequestException:
        pass

    return []


# ---------------------------------------------------------------------
# Analytics logic
# ---------------------------------------------------------------------

def moving_average(values: List[float]) -> Optional[float]:
    """Compute moving average over recent values."""
    if not values:
        return None
    return sum(values) / len(values)


def detect_trend(values: List[float], epsilon: float = 0.1) -> str:
    """Detect whether recent values are INCREASING, DECREASING, or STABLE."""
    if len(values) < 2:
        return "UNKNOWN"
    
    diff = values[-1] - values[0]
    if diff > epsilon:
        return "INCREASING"
    elif diff < -epsilon:
        return "DECREASING"
    else:
        return "STABLE"


def update_state(avg: float) -> str:
    """Update NORMAL / WARNING / ALERT state."""
    global current_state, high_condition_started_at
    
    new_state = current_state

    if avg >= ALERT_THRESHOLD:
        if high_condition_started_at is None:
            high_condition_started_at = time.time()
    else:
        high_condition_started_at = None

    if current_state == "NORMAL":
        if avg >= ALERT_THRESHOLD:
            if high_condition_started_at and (time.time() - high_condition_started_at >= ALERT_HOLD_SECONDS):
                new_state = "ALERT"
            else:
                new_state = "WARNING"
        elif avg >= WARNING_THRESHOLD:
            new_state = "WARNING"

    elif current_state == "WARNING":
        if avg >= ALERT_THRESHOLD:
            if high_condition_started_at and (time.time() - high_condition_started_at >= ALERT_HOLD_SECONDS):
                new_state = "ALERT"
        elif avg < RECOVERY_THRESHOLD:
            new_state = "NORMAL"

    elif current_state == "ALERT":
        if avg < RECOVERY_THRESHOLD:
            new_state = "NORMAL"
        elif avg < ALERT_THRESHOLD:
            new_state = "WARNING"

    # Log transition if state changed
    if new_state != current_state:
        log_event("state_transition", f"{current_state} -> {new_state}", state=new_state, mode=control_mode)
        current_state = new_state
        
    return current_state


# ---------------------------------------------------------------------
# MQTT control command publishing
# ---------------------------------------------------------------------

def state_to_command(state: str) -> str:
    """Map system state to actuator command."""
    if state in ["WARNING", "ALERT"]:
        return "ON"
    return "OFF"


def build_control_payload(command: str, source: str, reason: str = "") -> str:
    """Build JSON payload for actuator command."""
    payload = {
        "command": command,
        "state": current_state,
        "source": source,
        "reason": reason
    }
    return json.dumps(payload)


def publish_control_command(client: mqtt.Client, command: str, source: str, reason: str = "") -> None:
    """Publish command to the end-device actuator through MQTT."""
    global last_published_command

    payload = build_control_payload(command, source, reason)
    client.publish(CONTROL_TOPIC, payload)
    
    print(f"[MQTT PUB] Topic: {CONTROL_TOPIC} | Payload: {payload}")
    log_event("control_command_published", reason, state=current_state, mode=control_mode, topic=CONTROL_TOPIC, command=command)
    
    last_published_command = command


def apply_control_logic(client: mqtt.Client) -> None:
    """Decide which command should be sent to the actuator."""
    global last_published_command
    
    if control_mode == "AUTO":
        command = state_to_command(current_state)
        source = "AUTO"
        reason = f"State automatically resolved to {current_state}"
    elif control_mode == "MANUAL":
        command = manual_command
        source = "MANUAL"
        reason = "User manually overrode controls"
    else:
        return

    if command != last_published_command:
        publish_control_command(client, command, source, reason)


# ---------------------------------------------------------------------
# MQTT callbacks
# ---------------------------------------------------------------------

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to broker.")

        client.subscribe(SUBSCRIPTION)
        print(f"Subscribed to telemetry: {SUBSCRIPTION}")

        client.subscribe(MANUAL_TOPIC)
        print(f"Subscribed to manual override: {MANUAL_TOPIC}")

        client.subscribe(STATUS_TOPIC)
        print(f"Subscribed to actuator status: {STATUS_TOPIC}")

    else:
        print(f"Connection failed with code {rc}")


def handle_telemetry_message(topic: str, payload: str) -> None:
    """Handle instructor/sensor telemetry messages."""
    measurement = extract_measurement_from_topic(topic)
    
    if measurement != ASSIGNED_MEASUREMENT:
        return

    readable_msg = interpret_message(topic, payload)
    if readable_msg:
        print(f"[TELEMETRY] {readable_msg}")

    value = parse_payload_as_float(payload)
    if value is not None:
        insert_data(measurement, value)


def parse_command_payload(payload: str) -> str:
    """Parse plain-text or JSON command payload."""
    try:
        data = json.loads(payload)
        return str(data.get("command", "")).strip().upper()
    except json.JSONDecodeError:
        return payload.strip().upper()


def handle_manual_message(client: mqtt.Client, payload: str) -> None:
    """Handle manual override commands from MQTT."""
    global control_mode, manual_command, last_published_command

    cmd = parse_command_payload(payload)

    if cmd == "AUTO":
        control_mode = "AUTO"
        manual_command = None
        last_published_command = None
        log_event("returned_auto", "System returned to AUTO control", state=current_state, mode="AUTO", topic=MANUAL_TOPIC)
        print("\n[MANUAL] Switched to AUTO mode")
        
    elif cmd in ["ON", "OFF"]:
        control_mode = "MANUAL"
        manual_command = cmd
        log_event("manual_override", f"Manual override triggered: {cmd}", state=current_state, mode="MANUAL", topic=MANUAL_TOPIC, command=cmd)
        print(f"\n[MANUAL] Manual override active. Forcing {cmd}")
        
        publish_control_command(client, cmd, "MANUAL", "Manual command received via MQTT")


def handle_status_message(topic: str, payload: str) -> None:
    """Handle actuator status messages."""
    try:
        data = json.loads(payload)
        status = data.get("status", "UNKNOWN")
        source = data.get("source", "UNKNOWN")
        led = data.get("led", "")
        
        insert_actuator_status(status, led, source)
        log_event("actuator_status", payload, state=current_state, mode=control_mode, topic=topic)
        print(f"[STATUS] Actuator replied: {payload}")
        
    except json.JSONDecodeError:
        insert_actuator_status(payload, "", "UNKNOWN")
        log_event("actuator_status", f"Plain text: {payload}", topic=topic)
        print(f"[STATUS] Actuator replied (raw): {payload}")


def on_message(client, userdata, msg):
    payload = msg.payload.decode(errors="replace").strip()
    topic = msg.topic

    if topic == MANUAL_TOPIC:
        handle_manual_message(client, payload)
        return

    if topic == STATUS_TOPIC:
        handle_status_message(topic, payload)
        return

    handle_telemetry_message(topic, payload)


# ---------------------------------------------------------------------
# MQTT connection
# ---------------------------------------------------------------------

def connect_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT)
    return client


# ---------------------------------------------------------------------
# Periodic analytics loop
# ---------------------------------------------------------------------

def maybe_run_analytics(client: mqtt.Client) -> None:
    """Run analytics periodically without stopping MQTT ingestion."""
    global last_analytics_time

    now = time.time()

    if now - last_analytics_time < QUERY_INTERVAL_SECONDS:
        return

    last_analytics_time = now

    values = query_recent_values()
    if len(values) < 2:
        print("[ANALYTICS] Not enough data points to compute analytics yet.")
        return

    avg = moving_average(values)
    if avg is None:
        return
        
    trend = detect_trend(values)
    state = update_state(avg)

    insert_analytics(avg, trend, state, control_mode)
    print(f"\n[ANALYTICS] Window={values} | Avg={avg:.2f} | Trend={trend} | State={state}")

    apply_control_logic(client)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    client = connect_mqtt_client()

    try:
        client.loop_start()

        print("System running...")
        while True:
            maybe_run_analytics(client)
            time.sleep(0.2)

    except KeyboardInterrupt:
        print("\nStopping Lab5...")

    finally:
        client.loop_stop()
        client.disconnect()
        print("Disconnected")


if __name__ == "__main__":
    main()
