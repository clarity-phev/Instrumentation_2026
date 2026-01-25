#!/usr/bin/env python3

import pigpio
import time
import sys
import subprocess
import sqlite3
import signal

# ----------------------------
# Configuration
# ----------------------------

GPIOPORT = 18

DB_PATH = "./instrumentation.db"
TABLE_NAME = "energy"

# Debug levels
# False  -> no debug output
# 1      -> aggregated load + DB flush messages
# 2      -> level 1 + input/output detailed debug
DEBUG = 2

# Flush policy
DbaseBlkTime = 60        # seconds
MAX_RECORDS = 5000        # safety cap only

# Aggregation parameters
MIN_TIME = 5              # seconds
GLITCH_THRESHOLD_US = 50_000

# ----------------------------
# pigpio setup helper
# ----------------------------

def ensure_pigpiod():
    try:
        pi_test = pigpio.pi()
        if pi_test.connected:
            pi_test.stop()
            return
    except Exception:
        pass

    if DEBUG:
        print("pigpiod not running. Starting...")

    subprocess.run(["sudo", "pigpiod"], check=True)
    time.sleep(0.5)

# ----------------------------
# Global state
# ----------------------------

previous_tick = None
pulse_list = []

running = True

# ----------------------------
# GPIO callback
# ----------------------------

def edge_callback(gpio, level, tick):
    global previous_tick, pulse_list

    if level != 0:
        return

    if previous_tick is None:
        previous_tick = tick
        return

    delta = (tick - previous_tick) & 0xFFFFFFFF
    previous_tick = tick

    epoch = int(time.time())

    pulse_list.append((epoch, delta))

    if DEBUG == 2:
        print(f"Input: {epoch} {delta}")

# ----------------------------
# Aggregation + DB writer
# ----------------------------

def aggregator_loop():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            epoch       INTEGER PRIMARY KEY,
            dt_ms       INTEGER,
            pulse_count INTEGER
        )
    """)
    conn.commit()

    out_buffer = []
    last_flush_time = time.time()

    # Aggregation state
    old_epoch = None
    AggStart = 0
    dt_agg = 0
    pulse_count = 0

    block_input_pulses = 0
    block_output_records = 0

    def flush_buffer():
        nonlocal out_buffer, last_flush_time
        nonlocal block_input_pulses, block_output_records

        if not out_buffer:
            return

        cursor.executemany(
            f"INSERT OR REPLACE INTO {TABLE_NAME} (epoch, dt_ms, pulse_count) "
            f"VALUES (?, ?, ?)",
            out_buffer
        )
        conn.commit()

        if DEBUG:
            ts = time.strftime("%m-%d-%y %H:%M:%S", time.localtime())
            print(
                f"[{ts}] DB WRITE OK  "
                f"inputs={block_input_pulses}  "
                f"outputs={block_output_records}"
            )

        out_buffer.clear()
        last_flush_time = time.time()
        block_input_pulses = 0
        block_output_records = 0

    def emit_output(epoch_s, dt_ms, count):
        nonlocal out_buffer, block_output_records

        out_buffer.append((epoch_s, dt_ms, count))
        block_output_records += 1

        if DEBUG >= 1:
            power_w = (count * 3600.0) / (dt_ms / 1000.0) if dt_ms > 0 else 0.0
            ts = time.strftime("%m-%d-%y %H:%M:%S", time.localtime(epoch_s))
            print(f"{ts}  load={power_w:8.1f} W")

        if DEBUG == 2:
            print(f"Output: {epoch_s} {dt_ms} {count}")

    while running:
        if not pulse_list:
            # Check for DB flush interval
            if time.time() - last_flush_time >= DbaseBlkTime:
                flush_buffer()
            time.sleep(0.2)
            continue

        # Process pulses in arrival order
        epoch, delta_us = pulse_list.pop(0)
        block_input_pulses += 1

        if delta_us < GLITCH_THRESHOLD_US:
            continue

        # First pulse ever
        if old_epoch is None:
            old_epoch = epoch
            AggStart = epoch
            dt_agg = delta_us
            pulse_count = 1
            continue

        # Check if within aggregation window
        if epoch <= AggStart + MIN_TIME:
            dt_agg += delta_us
            pulse_count += 1
        else:
            # Emit the previous window
            emit_output(AggStart, round(dt_agg / 1000), pulse_count)
            AggStart = epoch
            dt_agg = delta_us
            pulse_count = 1

        old_epoch = epoch

        # Flush DB if time or max records reached
        if (time.time() - last_flush_time >= DbaseBlkTime) or (len(out_buffer) >= MAX_RECORDS):
            flush_buffer()

    # Emit last aggregation window on shutdown
    if pulse_count > 0:
        emit_output(AggStart, round(dt_agg / 1000), pulse_count)

    flush_buffer()
    conn.close()

# ----------------------------
# Signal handling
# ----------------------------

def shutdown_handler(signum, frame):
    global running
    running = False

# ----------------------------
# Main
# ----------------------------

def main():
    global running

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    ensure_pigpiod()

    pi = pigpio.pi()
    if not pi.connected:
        print("Failed to connect to pigpio daemon")
        sys.exit(1)

    pi.set_mode(GPIOPORT, pigpio.INPUT)
    pi.callback(GPIOPORT, pigpio.EITHER_EDGE, edge_callback)

    print(f"Monitoring GPIO {GPIOPORT}")
    print("Live aggregation → SQLite")
    if DEBUG:
        print(f"DEBUG diagnostics level: {DEBUG}")
    print("Press Ctrl+C to exit")

    try:
        aggregator_loop()
    finally:
        pi.stop()
        print("Stopped cleanly")

# ----------------------------
if __name__ == "__main__":
    main()
