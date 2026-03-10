import threading
import queue
import time
import math
import smbus
import sqlite3
import json
import lgpio
#import paho.mqtt.client as mqtt

from pathlib import Path
from datetime import datetime
from enum import Enum
from typing import Dict, Any

# ============================================================
# Event Object
# ============================================================

class Event:
    def __init__(self, timestamp: float, event_type: str, data: Dict[str, Any]):
        self.timestamp = timestamp
        self.type = event_type
        self.data = data


# ============================================================
# Furnace Bits Producer (lgpio polling)
# ============================================================

class FurnaceBitsProducer(threading.Thread):

    def __init__(self, name: str, event_queue: queue.Queue,
                 db_file: str, config_file: str):

        super().__init__(name=name, daemon=True)

        self.event_queue = event_queue
        self.stop_flag = threading.Event()
        self.db_file = db_file

        self.config = self.load_config(config_file)
        self.bits_config = self.config.get("bits", [])
        self.poll_interval = self.config.get("poll_interval_ms", 10) / 1000.0
        self.stability_ms = self.config.get("stability_ms", 20)

        self.previous_state = 0
        self.stable_candidate = 0
        self.last_change_time = time.monotonic()

        # ---- Initialize GPIO with pull-up ----
        self.gpiochip = lgpio.gpiochip_open(0)

        for b in self.bits_config:
            lgpio.gpio_claim_input(
                self.gpiochip,
                b["gpio"],
                lgpio.SET_PULL_UP   # <--- Added pull-up
            )

        # ---- Metadata in DB ----
        self.init_bit_metadata()

        # ---- Mask ----
        self.used_mask = sum(
            (1 << b["bit"]) for b in self.bits_config
            if b.get("used_mask", True)
        )

    def load_config(self, config_file: str) -> Dict[str, Any]:
        if Path(config_file).exists():
            with open(config_file, "r") as f:
                return json.load(f)
        return {}

    def init_bit_metadata(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS furnace_bits (
            bit INTEGER PRIMARY KEY,
            gpio INTEGER NOT NULL,
            short_name TEXT NOT NULL,
            description TEXT,
            used_mask INTEGER NOT NULL
        );
        """)

        for b in self.bits_config:
            cursor.execute("""
            INSERT OR REPLACE INTO furnace_bits
            (bit, gpio, short_name, description, used_mask)
            VALUES (?, ?, ?, ?, ?)
            """, (
                b["bit"],
                b["gpio"],
                b["short_name"],
                b.get("description", ""),
                int(b.get("used_mask", True))
            ))

        conn.commit()
        conn.close()

    def stop(self):
        self.stop_flag.set()
        lgpio.gpiochip_close(self.gpiochip)

    def read_state_word(self) -> int:
        state = 0

        for b in self.bits_config:
            gpio = b["gpio"]
            bit_idx = b["bit"]

            level = lgpio.gpio_read(self.gpiochip, gpio)

            # ACTIVE LOW: LOW = signal ON
            if level == 0:
                state |= (1 << bit_idx)

        return state

    def run(self):

        print("[FurnaceBits] Started")

        while not self.stop_flag.is_set():

            now = time.monotonic()
            current_state = self.read_state_word()

            if current_state != self.stable_candidate:
                self.stable_candidate = current_state
                self.last_change_time = now

            if (now - self.last_change_time) >= (self.stability_ms / 1000.0):

                changed_bits = (
                    (self.stable_candidate ^ self.previous_state)
                    & self.used_mask
                )

                if changed_bits:
                    self.previous_state = self.stable_candidate

                    self.event_queue.put(Event(
                        timestamp=time.time(),
                        event_type="bits",
                        data={"bits": self.previous_state}
                    ))

            time.sleep(self.poll_interval)


# ============================================================
# Fan Mode Producer (ADS1115 RMS)
# ============================================================

class MotorState(Enum):
    OFF = 0
    LO = 1
    MED = 2
    HI = 3


def classify_state(current, OFF_MAX, LO_MAX, MED_MAX):
    if current < OFF_MAX:
        return MotorState.OFF
    elif current < LO_MAX:
        return MotorState.LO
    elif current < MED_MAX:
        return MotorState.MED
    else:
        return MotorState.HI


class FanModeProducer(threading.Thread):

    def __init__(self, name: str,
                 event_queue: queue.Queue,
                 config: Dict[str, Any]):

        super().__init__(name=name, daemon=True)

        self.event_queue = event_queue
        self.stop_flag = threading.Event()

        # Configuration
        self.I2C_BUS = config.get("I2C_BUS", 0)
        self.ADS1115_ADDR = config.get("ADS1115_ADDR", 0x48)
        self.CHANNEL = config.get("CHANNEL", 0)
        self.GAIN = config.get("GAIN", 2)
        self.ADC_SPS = config.get("ADC_SPS", 860)
        self.RMS_RATE_HZ = config.get("RMS_RATE_HZ", 1)
        self.SINC_ATTEN = config.get("SINC_ATTEN", 0.991)
        self.CURRENT_GAIN = config.get("CURRENT_GAIN", 30.0)

        self.OFF_MAX = config.get("OFF_MAX", 0.1)
        self.LO_MAX = config.get("LO_MAX", 5.35)
        self.MED_MAX = config.get("MED_MAX", 5.65)

        self.DELTA_TRIGGER = config.get("DELTA_TRIGGER", 0.25)
        self.BIG_CHANGE = config.get("BIG_CHANGE", 1.0)
        self.STABILITY_SMALL = config.get("STABILITY_SMALL", 5)
        self.STABILITY_BIG = config.get("STABILITY_BIG", 15)

        self.WINDOW_TIME = 1.0 / self.RMS_RATE_HZ
        self.SAMPLES_PER_WINDOW = int(self.ADC_SPS * self.WINDOW_TIME)

        self.bus = smbus.SMBus(self.I2C_BUS)

        self.LSB = {
            2/3:6.144,1:4.096,2:2.048,4:1.024,8:0.512,16:0.256
        }[self.GAIN] / 32768.0

        self.prev_current = 0.0
        self.prev_prev_current = 0.0
        self.prev_state = MotorState.OFF
        self.in_stability = False
        self.delta_epoch = None
        self.stability_counter = 0
        self.stability_target = self.STABILITY_SMALL

    def stop(self):
        self.stop_flag.set()

    def read_adc(self):
        data = self.bus.read_i2c_block_data(self.ADS1115_ADDR, 0x00, 2)
        raw = (data[0] << 8) | data[1]
        if raw & 0x8000:
            raw -= 65536
        return raw * self.LSB

    def run(self):

        print("[FanMode] Started")

        while not self.stop_flag.is_set():

            start = time.time()
            sum_v = 0.0
            sum_v2 = 0.0

            # -----------------------------
            # RMS WINDOW COLLECTION
            # -----------------------------
            for _ in range(self.SAMPLES_PER_WINDOW):
                v = self.read_adc()
                sum_v += v
                sum_v2 += v * v

            mean = sum_v / self.SAMPLES_PER_WINDOW
            vrms = math.sqrt((sum_v2 / self.SAMPLES_PER_WINDOW) - (mean * mean))
            irms = vrms * self.CURRENT_GAIN / self.SINC_ATTEN

            epoch = int(time.time())

            # =========================================================
            # INSTANT OFF DETECTION (BYPASS STABILITY LOGIC)
            # =========================================================
            if irms < self.OFF_MAX and self.prev_state != MotorState.OFF:

                self.event_queue.put(Event(
                    timestamp=epoch,
                    event_type="fan",
                    data={
                        "fan_mode": MotorState.OFF.value,
                        "fan_rms": irms
                    }
                ))

                self.prev_state = MotorState.OFF
                self.in_stability = False
                self.prev_current = irms

                # Maintain timing
                elapsed = time.time() - start
                sleep_time = self.WINDOW_TIME - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

                continue

            # =========================================================
            # NORMAL STABILITY PROCESSING
            # =========================================================
            if not self.in_stability:

                delta = irms - self.prev_prev_current

                if abs(delta) > self.DELTA_TRIGGER:

                    self.delta_epoch = epoch
                    self.stability_counter = 0

                    # Direction-aware logic
                    if delta > self.BIG_CHANGE:
                        # Large upward jump (motor start)
                        self.stability_target = self.STABILITY_BIG
                    else:
                        # Speed change or moderate change
                        self.stability_target = self.STABILITY_SMALL

                    self.in_stability = True

            else:
                self.stability_counter += 1

                if self.stability_counter >= self.stability_target:

                    new_state = classify_state(
                        irms,
                        self.OFF_MAX,
                        self.LO_MAX,
                        self.MED_MAX
                    )

                    if new_state != self.prev_state:

                        self.event_queue.put(Event(
                            timestamp=self.delta_epoch,
                            event_type="fan",
                            data={
                                "fan_mode": new_state.value,
                                "fan_rms": irms
                            }
                        ))

                        self.prev_state = new_state

                    self.in_stability = False

            self.prev_prev_current = self.prev_current
            self.prev_current = irms

            # Maintain fixed RMS rate
            elapsed = time.time() - start
            sleep_time = self.WINDOW_TIME - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)


# ============================================================
# Aggregator (DB + optional MQTT)
# ============================================================

class Aggregator(threading.Thread):

    def __init__(self,
                 event_queue: queue.Queue,
                 db_file="furnace.db",
                 mqtt_config=None):

        super().__init__(daemon=True)

        self.event_queue = event_queue
        self.stop_flag = threading.Event()
        self.db_file = db_file

        self.current_bits = 0
        self.current_fan_mode = None
        self.current_fan_rms = None

        self.mqtt_client = None

        if mqtt_config:
            self.mqtt_client = mqtt.Client()
            self.mqtt_client.connect(
                mqtt_config["broker"],
                mqtt_config["port"],
                60
            )
            self.mqtt_client.loop_start()

        self.setup_db()

    def setup_db(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS furnace_log (
            time INTEGER PRIMARY KEY,
            bits INTEGER,
            fan_mode INTEGER,
            fan_rms REAL
        );
        """)

        conn.commit()
        conn.close()

    def stop(self):
        self.stop_flag.set()

    def run(self):

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        print("[Aggregator] Started")

        while not self.stop_flag.is_set():

            try:
                evt = self.event_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            ts_ms = int(evt.timestamp * 1000)

            if evt.type == "bits":

                self.current_bits = evt.data["bits"]

                cursor.execute("""
                INSERT INTO furnace_log
                (time, bits)
                VALUES (?, ?)
                """, (
                    ts_ms,
                    self.current_bits
                ))

            elif evt.type == "fan":

                self.current_fan_mode = evt.data["fan_mode"]
                self.current_fan_rms = evt.data["fan_rms"]

                cursor.execute("""
                INSERT INTO furnace_log
                (time, fan_mode, fan_rms)
                VALUES (?, ?, ?)
                """, (
                    ts_ms,
                    self.current_fan_mode,
                    self.current_fan_rms
                ))

            conn.commit()

            ts = datetime.fromtimestamp(evt.timestamp)

            if evt.type == "bits":
                print(
                    f"[DB WRITE] {ts} | bits={self.current_bits:010b}"
                )
            else:
                print(f"[DB WRITE] {ts} | fan_mode={self.current_fan_mode} | fan_rms={self.current_fan_rms:.3f}")


            if self.mqtt_client:
                payload = {
                    "timestamp": int(evt.timestamp * 1000),
                    "bits": self.current_bits,
                    "fan_mode": self.current_fan_mode,
                    "fan_rms": self.current_fan_rms
                }
                self.mqtt_client.publish("furnace/log",
                                         json.dumps(payload))


# ============================================================
# Main
# ============================================================

def main():

    event_queue = queue.Queue()

    producers = []

    producers.append(
        FurnaceBitsProducer(
            "FurnaceBits",
            event_queue,
            db_file="furnace.db",
            config_file="furnace_bits.json"
        )
    )

    fan_config = {}
    producers.append(
        FanModeProducer("FanMode", event_queue, fan_config)
    )

    MQTT_ENABLED = False
    mqtt_cfg = {
        "broker": "192.168.1.100",
        "port": 1883
    } if MQTT_ENABLED else None

    aggregator = Aggregator(event_queue,
                            db_file="furnace.db",
                            mqtt_config=mqtt_cfg)

    aggregator.start()

    for p in producers:
        p.start()

    print("System running. Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
        for p in producers:
            p.stop()
        aggregator.stop()

    for p in producers:
        p.join()
    aggregator.join()


if __name__ == "__main__":
    main()

