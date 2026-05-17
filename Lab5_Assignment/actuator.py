"""
ECE490 – Lab 5 (Exercise 5)
End Device Actuator Subscriber (LED Controller)
"""

import json
import socket
import paho.mqtt.client as mqtt
import RPi.GPIO as GPIO

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
BROKER = "194.177.207.38"
PORT = 1883

TEAM = socket.gethostname() 

MQTT_USERNAME = "team1"
MQTT_PASSWORD = "team1!@#$"

ACTUATOR = "led"
CONTROL_TOPIC = f"iot/{TEAM}/control/{ACTUATOR}"
STATUS_TOPIC = f"iot/{TEAM}/status/{ACTUATOR}"

# ---------------------------------------------------------------------
# Hardware Configuration (Raspberry Pi GPIO)
# ---------------------------------------------------------------------
LED_PIN = 18

def setup_gpio():
    """Αρχικοποίηση των GPIO pins του Raspberry Pi."""
    GPIO.setmode(GPIO.BCM)
    GPIO.setwarnings(False)
    GPIO.setup(LED_PIN, GPIO.OUT)
    GPIO.output(LED_PIN, GPIO.LOW)
    print(f"GPIO setup complete. LED is on BCM pin {LED_PIN}")

def turn_led_on():
    """Ανάβει το LED."""
    GPIO.output(LED_PIN, GPIO.HIGH)

def turn_led_off():
    """Σβήνει το LED."""
    GPIO.output(LED_PIN, GPIO.LOW)

# ---------------------------------------------------------------------
# MQTT Callbacks
# ---------------------------------------------------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Actuator connected to MQTT Broker!")
        # Κάνουμε subscribe στο control topic για να λαμβάνουμε εντολές
        client.subscribe(CONTROL_TOPIC)
        print(f"Listening for commands on: {CONTROL_TOPIC}")
    else:
        print(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode(errors="replace").strip()
    print(f"\n[RECEIVED COMMAND] Topic: {msg.topic} | Payload: {payload}")
    
    command = None
    source = "UNKNOWN"
    
    try:
        data = json.loads(payload)
        command = data.get("command", "").upper()
        source = data.get("source", "UNKNOWN")
    except json.JSONDecodeError:
        command = payload.upper()

    status_to_report = "UNKNOWN"
    
    if command == "ON":
        turn_led_on()
        print("-> Action: LED turned ON")
        status_to_report = "ON"
    elif command == "OFF":
        turn_led_off()
        print("-> Action: LED turned OFF")
        status_to_report = "OFF"
    else:
        print(f"-> Action: Ignored unknown command '{command}'")
        return

    # Δημιουργία και αποστολή μηνύματος επιβεβαίωσης (Status)
    status_payload = json.dumps({
        "status": status_to_report,
        "source": "ACTUATOR_PI",
        "led": f"GPIO_{LED_PIN}"
    })
    
    client.publish(STATUS_TOPIC, status_payload)
    print(f"[PUBLISHED STATUS] Topic: {STATUS_TOPIC} | Payload: {status_payload}")

# ---------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------
def main():
    # 1. Ετοιμάζουμε το hardware
    setup_gpio()
    
    # 2. Ετοιμάζουμε το MQTT Client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "actuator_client_01")
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect(BROKER, PORT)
        client.loop_forever()
        
    except KeyboardInterrupt:
        print("\nStopping Actuator script...")
    finally:
        client.disconnect()
        GPIO.cleanup()
        print("Hardware cleaned up and disconnected cleanly.")

if __name__ == "__main__":
    main()
