#!/usr/bin/env python3

import time
import sqlite3
import RPi.GPIO as GPIO
import paho.mqtt.client as mqtt
import os
import sys

# ============================================================
# Debug, Test, and MQTT flags
# ============================================================

DEBUG = True        # Enable detailed diagnostic prints
TEST_MODE = True   # Keyboard input instead of GPIO
MQTT_WRITE = False  # If False, MQTT messages are printed instead of sent

def dbg(msg):
    if DEBUG:
        print(f"[DEBUG] {msg}")

# ============================================================
# Configuration
# ============================================================

DB_PATH = "furnace_state.db"
STATE_CFG_PATH = "state.cfg"

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "furnace/state"

SAMPLE_INTERVAL = 0.05        # seconds
DEBOUNCE_COUNT = 5            # samples

# GPIO mapping: bit -> BCM GPIO
GPIO_MAP = {
    0: 5,
    1: 6,
    2: 12,
    3: 13,
    4: 16,
    5: 19,
    6: 20,
    7: 21,
    8: 26,
    9: 4,
}

# ============================================================
# Epoch generator
# ============================================================

class EpochGenerator:
    def __init__(self, initial=0):
        self.last_epoch = initial

    def next(self):
        now = int(time.time())
        if now <= self.last_epoch:
            now = self.last_epoch + 1
        self.last_epoch = now
        return now

# ============================================================
# state.cfg parsing
# ============================================================

def load_state_cfg(path):
    defs = {}
    dbg(f"Loading state configuration from '{path}'")
    with open(path, "r") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = {}
            for field in line.split(","):
                k, v = field.split("=", 1)
                parts[k.strip()] = v.strip()
            bit = int(parts["bit"])
            defs[bit] = {
                "name": parts["name"],
                "desc": parts.get("desc", ""),
                "used": int(parts["used"]),
            }
    dbg("Loaded bit definitions:")
    for bit, info in defs.items():
        dbg(f"  bit {bit}: name={info['name']} used={info['used']} desc='{info['desc']}'")
    return defs

# ============================================================
# Database
# ============================================================

def init_db(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS furnace_state (
            epoch INTEGER PRIMARY KEY,
            state INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS furnace_bits (
            bit INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            used INTEGER NOT NULL,
            gpio INTEGER
        )
    """)
    cur.execute("""
        CREATE VIEW IF NOT EXISTS furnace_state_bits AS
        SELECT
            s.epoch AS epoch,
            b.bit AS bit,
            b.name AS name,
            ((s.state >> b.bit) & 1) AS value
        FROM furnace_state s
        JOIN furnace_bits b
        WHERE b.used = 1
    """)
    conn.commit()
    dbg("Database schema verified/created")

def reconcile_bit_defs(conn, bit_defs):
    cur = conn.cursor()
    for bit, info in bit_defs.items():
        gpio = GPIO_MAP.get(bit)
        cur.execute("""
            INSERT INTO furnace_bits (bit, name, description, used, gpio)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(bit) DO UPDATE SET
                name=excluded.name,
                description=excluded.description,
                used=excluded.used,
                gpio=excluded.gpio
        """, (bit, info["name"], info["desc"], info["used"], gpio))
        dbg(f"DB bit def synced: bit={bit}, name={info['name']}, used={info['used']}, gpio={gpio}")
    conn.commit()

def load_last_epoch(conn):
    cur = conn.cursor()
    cur.execute("SELECT MAX(epoch) FROM furnace_state")
    row = cur.fetchone()
    last = row[0] if row and row[0] is not None else 0
    dbg(f"Last epoch in database: {last}")
    return last

# ============================================================
# GPIO / TEST input
# ============================================================

def gpio_setup():
    GPIO.setmode(GPIO.BCM)
    for pin in GPIO_MAP.values():
        GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_DOWN)
    dbg("GPIO initialized (BCM mode)")
    for bit, pin in GPIO_MAP.items():
        dbg(f"  bit {bit} -> GPIO{pin}")

def read_raw_state():
    if TEST_MODE:
        while True:
            try:
                user_input = input("Enter state (2-digit hex, e.g., 0A): ").strip()
                value = int(user_input, 16)
                dbg(f"TEST_MODE input raw value: 0x{value:02X}")
                return value
            except ValueError:
                print("Invalid input. Enter 2-digit hex (00-FF).")
    else:
        state = 0
        for bit, pin in GPIO_MAP.items():
            if GPIO.input(pin):
                state |= (1 << bit)
        return state

# ============================================================
# MQTT
# ============================================================

def mqtt_connect():
    if MQTT_WRITE:
        client = mqtt.Client()
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        client.loop_start()
        dbg(f"MQTT connected to {MQTT_BROKER}:{MQTT_PORT}, topic='{MQTT_TOPIC}'")
        return client
    else:
        dbg("MQTT_WRITE is OFF; broker connection skipped")
        return None

def mqtt_publish(client, epoch, state):
    payload = f'{{"epoch":{epoch},"state":{state}}}'
    if MQTT_WRITE and client:
        client.publish(MQTT_TOPIC, payload)
        dbg("State published via MQTT")
    else:
        print(f"[MQTT_WRITE=OFF] Would publish to '{MQTT_TOPIC}': {payload}")

# ============================================================
# Main collector
# ============================================================

def main():
    if not os.path.exists(STATE_CFG_PATH):
        raise RuntimeError("Missing state.cfg")

    bit_defs = load_state_cfg(STATE_CFG_PATH)

    used_mask = 0
    for bit, info in bit_defs.items():
        if info["used"]:
            used_mask |= (1 << bit)
    dbg(f"Used-bit mask: 0b{used_mask:010b} (0x{used_mask:03X})")

    if not TEST_MODE:
        gpio_setup()
    else:
        dbg("Running in TEST_MODE: keyboard input will provide state words")

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    init_db(conn)
    reconcile_bit_defs(conn, bit_defs)

    last_epoch = load_last_epoch(conn)
    epoch_gen = EpochGenerator(last_epoch)

    mqtt_client = mqtt_connect()

    # --- initial state ---
    raw_state = read_raw_state()
    stable_state = raw_state & used_mask
    epoch = epoch_gen.next()
    dbg(f"Initial raw state: 0b{raw_state:010b}")
    dbg(f"Initial masked state: 0b{stable_state:010b}")
    dbg(f"Logging initial state at epoch {epoch}")

    conn.execute("INSERT INTO furnace_state (epoch, state) VALUES (?, ?)", (epoch, stable_state))
    conn.commit()
    dbg(f"Initial state written to database: epoch={epoch}, state=0b{stable_state:010b} ({stable_state})")
    mqtt_publish(mqtt_client, epoch, stable_state)

    candidate_state = stable_state
    candidate_count = 0

    try:
        while True:
            raw_state = read_raw_state()
            masked_state = raw_state & used_mask

            if masked_state != stable_state:
                if masked_state != candidate_state:
                    candidate_state = masked_state
                    candidate_count = 1
                    dbg(f"New candidate state detected: 0b{candidate_state:010b}")
                else:
                    candidate_count += 1
                    dbg(f"Candidate state reaffirmed ({candidate_count}/{DEBOUNCE_COUNT})")

                if candidate_count >= DEBOUNCE_COUNT:
                    dbg(f"State change accepted: 0b{stable_state:010b} -> 0b{candidate_state:010b}")
                    stable_state = candidate_state
                    epoch = epoch_gen.next()
                    dbg(f"Assigned epoch {epoch}")

                    conn.execute("INSERT INTO furnace_state (epoch, state) VALUES (?, ?)", (epoch, stable_state))
                    conn.commit()
                    dbg(f"State written to database: epoch={epoch}, state=0b{stable_state:010b} ({stable_state})")
                    mqtt_publish(mqtt_client, epoch, stable_state)

                    candidate_count = 0
            else:
                candidate_count = 0

            if not TEST_MODE:
                time.sleep(SAMPLE_INTERVAL)

    except KeyboardInterrupt:
        dbg("KeyboardInterrupt received")
    finally:
        dbg("Shutting down collector")
        if not TEST_MODE:
            GPIO.cleanup()
        if MQTT_WRITE and mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        conn.close()

# ============================================================
# Entry
# ============================================================

if __name__ == "__main__":
    main()
