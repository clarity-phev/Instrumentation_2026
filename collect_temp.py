#!/usr/bin/env python3

import os
import time
import sqlite3
import signal
import json
from datetime import datetime

# =========================
# Configuration
# =========================

READ_RATE = 30
WRITE_RATE = 300
CONVERSION_DELAY = 0.9

DEBUG = True
DB_WRITE = True
MQTT_WRITE = False   # <-- Disabled by default

if MQTT_WRITE:
    import paho.mqtt.client as mqtt

DB_PATH = "/opt/logger/datastores/temperature.db"
TABLE_NAME = "Temperature"
DEVICE_ID = "rpi-zero-2"
SENSORS_CFG = "./temp.cfg"

# MQTT Settings
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC = "hvac/temperature"
MQTT_USER = None
MQTT_PASS = None
MQTT_QOS = 0
MQTT_KEEPALIVE = 60

# 1-Wire bus paths
W1_BUSES = {
    "bus1": "/sys/bus/w1/devices/w1_bus_master1",
    "bus2": "/sys/bus/w1/devices/w1_bus_master2",
}

# =========================
# Globals
# =========================

sensors = {}
sensor_order = []
buffer = []
shutdown_requested = False
mqtt_client = None

# =========================
# Signal Handling
# =========================

def handle_shutdown(signum, frame):
    global shutdown_requested
    shutdown_requested = True

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# =========================
# MQTT Functions
# =========================

def on_disconnect(client, userdata, rc):
    if rc != 0 and DEBUG:
        print("DEBUG: Unexpected MQTT disconnect, attempting reconnect")
        try:
            client.reconnect()
        except Exception as e:
            if DEBUG:
                print("DEBUG: MQTT reconnect failed:", e)

def init_mqtt():
    global mqtt_client
    mqtt_client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )
    if MQTT_USER and MQTT_PASS:
        mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    mqtt_client.on_disconnect = on_disconnect
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
        mqtt_client.loop_start()
        if DEBUG:
            print("DEBUG: MQTT client connected")
    except Exception as e:
        if DEBUG:
            print("DEBUG: MQTT connection failed:", e)

def publish_mqtt(records):
    if mqtt_client is None:
        return

    mqtt_records = []
    for rec in records:
        temps_by_name = {}
        for col_serial, value in rec["temps"].items():
            name = sensors[col_serial]["name"]
            temps_by_name[name] = value

        mqtt_records.append({
            "epoch": rec["epoch"],
            "temps": temps_by_name
        })

    payload = {
        "device": DEVICE_ID,
        "table": TABLE_NAME,
        "records": mqtt_records,
    }

    try:
        mqtt_client.publish(
            MQTT_TOPIC,
            json.dumps(payload),
            qos=MQTT_QOS
        )
        if DEBUG:
            print("DEBUG: MQTT payload published")
    except Exception as e:
        if DEBUG:
            print("DEBUG: MQTT publish failed:", e)

# =========================
# Utility Functions
# =========================

def next_aligned_epoch(rate):
    now = int(time.time())
    return now + (rate - (now % rate))

def load_sensor_cfg():
    cfg = {}
    if not os.path.isfile(SENSORS_CFG):
        return cfg
    with open(SENSORS_CFG, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            serial, name = line.split(maxsplit=1)
            cfg[serial] = name
    return cfg

def save_sensor_cfg(cfg):
    with open(SENSORS_CFG, "w") as f:
        for serial, name in cfg.items():
            f.write(f"{serial} {name}\n")

# =========================
# Sensor Discovery
# =========================

def discover_sensors():
    discovered = {}
    cfg = load_sensor_cfg()
    unknown_count = 1
    global sensor_order

    if DEBUG:
        print("DEBUG: Starting sensor discovery")

    for bus_name, bus_path in W1_BUSES.items():
        if not os.path.isdir(bus_path):
            if DEBUG:
                print(f"DEBUG: Bus {bus_name} missing")
            continue

        found = []

        for entry in sorted(os.listdir(bus_path)):
            if not entry.startswith("28-"):
                continue

            column_serial = entry.replace("-", "_")

            if entry not in cfg:
                while f"unknown{unknown_count}" in cfg.values():
                    unknown_count += 1
                cfg[entry] = f"unknown{unknown_count}"
                unknown_count += 1

            discovered[column_serial] = {
                "name": cfg.get(entry),
                "bus": bus_path,
                "raw_serial": entry,
            }

            found.append((column_serial, cfg.get(entry)))
            sensor_order.append(column_serial)

        if DEBUG:
            print(f"DEBUG: {bus_name} ({bus_path})")
            for s, n in found:
                print(f"  Found sensor: {s} -> {n}")

    save_sensor_cfg(cfg)

    if DEBUG:
        print(f"DEBUG: Discovery complete ({len(discovered)} total sensors)")
        print("DEBUG: Summary of discovered sensors:")
        for s, info in discovered.items():
            print(f"  {s} -> {info['name']}")

    return discovered

# =========================
# 1-Wire Reading
# =========================

def trigger_conversion():
    for sdata in sensors.values():
        try:
            with open(os.path.join(sdata["bus"], "w1_master_slaves"), "r"):
                pass
        except Exception:
            pass

def read_sensor_temp(column_serial):
    raw_serial = sensors[column_serial]["raw_serial"]
    try:
        with open(f"/sys/bus/w1/devices/{raw_serial}/w1_slave") as f:
            lines = f.readlines()

        if "YES" not in lines[0]:
            return None

        raw = lines[1].split("t=")[-1]
        temp_c = float(raw) / 1000.0

        if abs(temp_c - 85.0) < 0.01:
            return None

        temp_f = temp_c * 9 / 5 + 32
        return int(round(temp_f * 10))

    except Exception:
        return None

def debug_print(epoch, temps):
    ts = datetime.fromtimestamp(epoch).strftime("%m%d%Y %H:%M:%S")
    parts = [ts]
    for s in sensor_order:
        name = sensors[s]["name"]
        parts.append(f"{name}: {temps.get(s)}")
    print(" ".join(parts))

# =========================
# SQLite Functions
# =========================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" (
            epoch INTEGER PRIMARY KEY
        )
    ''')

    for serial in sensor_order:
        try:
            cur.execute(
                f'ALTER TABLE "{TABLE_NAME}" ADD COLUMN "s{serial}" INTEGER'
            )
        except sqlite3.OperationalError:
            pass

    conn.commit()
    conn.close()

def write_db(records):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for rec in records:
        cols = ['"epoch"'] + [f'"s{s}"' for s in sensor_order]
        vals = [rec["epoch"]] + [rec["temps"].get(s) for s in sensor_order]
        placeholders = ",".join("?" * len(vals))

        sql = f'''
            INSERT INTO "{TABLE_NAME}"
            ({",".join(cols)})
            VALUES ({placeholders})
        '''
        cur.execute(sql, vals)

    conn.commit()
    conn.close()

# =========================
# Main Loop
# =========================

def main():
    global sensors, buffer

    sensors = discover_sensors()

    if DB_WRITE:
        init_db()
    if MQTT_WRITE:
        init_mqtt()

    next_read = next_aligned_epoch(READ_RATE)
    records_per_block = WRITE_RATE // READ_RATE

    while not shutdown_requested:
        now = int(time.time())

        if now >= next_read:
            trigger_epoch = now
            trigger_conversion()
            time.sleep(CONVERSION_DELAY)

            temps = {}
            for s in sensor_order:
                temps[s] = read_sensor_temp(s)

            record = {"epoch": trigger_epoch, "temps": temps}

            if DEBUG:
                debug_print(trigger_epoch, temps)

            buffer.append(record)
            next_read += READ_RATE

        while len(buffer) >= records_per_block:
            block = buffer[:records_per_block]
            if DEBUG:
                print(f"DEBUG: Writing buffer ({len(block)} records)")
            if DB_WRITE:
                write_db(block)
            if MQTT_WRITE:
                publish_mqtt(block)
            buffer = buffer[records_per_block:]

        time.sleep(0.2)

    if buffer:
        if DEBUG:
            print(f"DEBUG: Shutdown flush ({len(buffer)} records)")
        if DB_WRITE:
            write_db(buffer)
        if MQTT_WRITE:
            publish_mqtt(buffer)

    if MQTT_WRITE and mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


if __name__ == "__main__":
    main()
