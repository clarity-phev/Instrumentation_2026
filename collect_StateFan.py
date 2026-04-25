import threading
import queue
import time
import math
import smbus
import sqlite3
import json
import lgpio
# import paho.mqtt.client as mqtt

from pathlib import Path
from datetime import datetime
from enum import Enum
from typing import Dict, Any

# ============================================================
# ===================== CONFIGURATION =======================
# ============================================================

CONFIG = {
    "DB_FILE": "/opt/logger/datastores/furnace.db",
    "BITS_CONFIG_FILE": "furnace_bits.json",
    "FAN_CONFIG": {
        "I2C_BUS": 0,
        "ADS1115_ADDR": 0x48,
        "CHANNEL": 0,
        "GAIN": 2,
        "ADC_SPS": 860,
        "RMS_RATE_HZ": 1,
        "SINC_ATTEN": 0.991,
        "CURRENT_GAIN": 30.0,
        "OFF_MAX": 0.1,
        "LO_MAX": 5.35,
        "MED_MAX": 5.65,
        "DELTA_TRIGGER": 0.25,
        "BIG_CHANGE": 1.0,
        "STABILITY_SMALL": 5,
        "STABILITY_BIG": 15
    },
    "BITS_POLL_INTERVAL_MS": 10,
    "BITS_STABILITY_MS": 20,
    "MQTT_ENABLED": False,
    "MQTT_BROKER": "192.168.1.100",
    "MQTT_PORT": 1883
}

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
                 db_file: str, config_file: str,
                 poll_interval_ms: int, stability_ms: int):
        super().__init__(name=name, daemon=True)
        self.event_queue = event_queue
        self.stop_flag = threading.Event()
        self.db_file = db_file

        self.config = self.load_config(config_file)
        self.bits_config = self.config.get("bits", [])
        self.poll_interval = poll_interval_ms / 1000.0
        self.stability_ms = stability_ms

        self.previous_state = 0
        self.stable_candidate = 0
        self.last_change_time = time.monotonic()

        # ---- Initialize GPIO with pull-up ----
        self.gpiochip = lgpio.gpiochip_open(0)
        for b in self.bits_config:
            lgpio.gpio_claim_input(
                self.gpiochip,
                b["gpio"],
                lgpio.SET_PULL_UP
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
            level = lgpio.gpio_read(self.gpiochip, b["gpio"])
            if level == 0:  # ACTIVE LOW
                state |= (1 << b["bit"])
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
                changed_bits = (self.stable_candidate ^ self.previous_state) & self.used_mask
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
    def __init__(self, name: str, event_queue: queue.Queue, config: Dict[str, Any]):
        super().__init__(name=name, daemon=True)
        self.event_queue = event_queue
        self.stop_flag = threading.Event()
        self.cfg = config

        self.I2C_BUS = config["I2C_BUS"]
        self.ADS1115_ADDR = config["ADS1115_ADDR"]
        self.CHANNEL = config["CHANNEL"]
        self.GAIN = config["GAIN"]
        self.ADC_SPS = config["ADC_SPS"]
        self.RMS_RATE_HZ = config["RMS_RATE_HZ"]
        self.SINC_ATTEN = config["SINC_ATTEN"]
        self.CURRENT_GAIN = config["CURRENT_GAIN"]

        self.OFF_MAX = config["OFF_MAX"]
        self.LO_MAX = config["LO_MAX"]
        self.MED_MAX = config["MED_MAX"]

        self.DELTA_TRIGGER = config["DELTA_TRIGGER"]
        self.BIG_CHANGE = config["BIG_CHANGE"]
        self.STABILITY_SMALL = config["STABILITY_SMALL"]
        self.STABILITY_BIG = config["STABILITY_BIG"]

        self.WINDOW_TIME = 1.0 / self.RMS_RATE_HZ
        self.SAMPLES_PER_WINDOW = int(self.ADC_SPS * self.WINDOW_TIME)

        self.bus = smbus.SMBus(self.I2C_BUS)

        # CORE FIX: Always configure ADC on startup
        self.configure_adc()

        self.LSB = {2/3:6.144,1:4.096,2:2.048,4:1.024,8:0.512,16:0.256}[self.GAIN] / 32768.0

        self.prev_current = 0.0
        self.prev_prev_current = 0.0
        self.prev_state = MotorState.OFF
        self.in_stability = False
        self.delta_epoch = None
        self.stability_counter = 0
        self.stability_target = self.STABILITY_SMALL

    # --------------------------------------------------------
    # CORE FIX: ADC configuration (continuous mode)
    # --------------------------------------------------------
    def configure_adc(self):
        CONFIG_REG = 0x01

        MUX_BITS = {
            0: 0x4000,
            1: 0x5000,
            2: 0x6000,
            3: 0x7000
        }

        GAIN_BITS = {
            2/3: 0x0000,
            1:   0x0200,
            2:   0x0400,
            4:   0x0600,
            8:   0x0800,
            16:  0x0A00
        }

        SPS_BITS = {
            8:    0x0000,
            16:   0x0020,
            32:   0x0040,
            64:   0x0060,
            128:  0x0080,
            250:  0x00A0,
            475:  0x00C0,
            860:  0x00E0
        }

        config = (
            MUX_BITS[self.CHANNEL] |
            GAIN_BITS[self.GAIN] |
            0x0000 |                  # Continuous mode
            SPS_BITS[self.ADC_SPS] |
            0x0003                   # Disable comparator
        )

        self.bus.write_i2c_block_data(
            self.ADS1115_ADDR,
            CONFIG_REG,
            [(config >> 8) & 0xFF, config & 0xFF]
        )

        time.sleep(0.02)

    def stop(self):
        self.stop_flag.set()

    # --------------------------------------------------------
    # Robust ADC read with recovery
    # --------------------------------------------------------
    def read_adc(self):
        try:
            data = self.bus.read_i2c_block_data(self.ADS1115_ADDR, 0x00, 2)
        except OSError as e:
            print(f"[FanMode] I2C error: {e} — recovering bus")

            try:
                self.bus.close()
            except Exception:
                pass

            time.sleep(0.05)

            try:
                self.bus = smbus.SMBus(self.I2C_BUS)

                # CORE FIX: Reconfigure ADC after recovery
                self.configure_adc()

            except Exception as e2:
                print(f"[FanMode] Bus reopen failed: {e2}")
                time.sleep(0.5)

            return None

        raw = (data[0] << 8) | data[1]
        if raw & 0x8000:
            raw -= 65536
        return raw * self.LSB

    # --------------------------------------------------------
    # One processing cycle
    # --------------------------------------------------------
    def _run_cycle(self):
        start = time.time()

        sum_v = 0.0
        sum_v2 = 0.0
        valid_samples = 0

        for _ in range(self.SAMPLES_PER_WINDOW):
            v = self.read_adc()
            if v is None:
                continue

            sum_v += v
            sum_v2 += v * v
            valid_samples += 1

        if valid_samples == 0:
            print("[FanMode] No valid ADC samples — skipping cycle")
            return

        mean = sum_v / valid_samples
        vrms = math.sqrt((sum_v2 / valid_samples) - (mean * mean))
        irms = vrms * self.CURRENT_GAIN / self.SINC_ATTEN
        epoch = int(time.time())

        # OFF detection
        if irms < self.OFF_MAX and self.prev_state != MotorState.OFF:
            self.event_queue.put(Event(
                timestamp=epoch,
                event_type="fan",
                data={"fan_mode": MotorState.OFF.value, "fan_rms": irms}
            ))
            self.prev_state = MotorState.OFF
            self.in_stability = False
            self.prev_current = irms

            sleep_time = self.WINDOW_TIME - (time.time() - start)
            if sleep_time > 0:
                time.sleep(sleep_time)
            return

        # Stability processing
        if not self.in_stability:
            delta = irms - self.prev_prev_current
            if abs(delta) > self.DELTA_TRIGGER:
                self.delta_epoch = epoch
                self.stability_counter = 0
                self.stability_target = (
                    self.STABILITY_BIG if abs(delta) > self.BIG_CHANGE else self.STABILITY_SMALL
                )
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
                        data={"fan_mode": new_state.value, "fan_rms": irms}
                    ))
                    self.prev_state = new_state

                self.in_stability = False

        self.prev_prev_current = self.prev_current
        self.prev_current = irms

        sleep_time = self.WINDOW_TIME - (time.time() - start)
        if sleep_time > 0:
            time.sleep(sleep_time)

    # --------------------------------------------------------
    # Thread entry
    # --------------------------------------------------------
    def run(self):
        print("[FanMode] Started")
        while not self.stop_flag.is_set():
            try:
                self._run_cycle()
            except Exception as e:
                print(f"[FanMode] Unexpected error: {e}")
                time.sleep(1)


# ============================================================
# Aggregator (DB + optional MQTT)
# ============================================================

class Aggregator(threading.Thread):
    def __init__(self, event_queue: queue.Queue, db_file: str, mqtt_enabled: bool, mqtt_cfg=None):
        super().__init__(daemon=True)
        self.event_queue = event_queue
        self.stop_flag = threading.Event()
        self.db_file = db_file
        self.current_bits = 0
        self.current_fan_mode = None
        self.current_fan_rms = None

        self.mqtt_client = None
        if mqtt_enabled:
            import paho.mqtt.client as mqtt
            self.mqtt_client = mqtt.Client()
            self.mqtt_client.connect(mqtt_cfg["broker"], mqtt_cfg["port"], 60)
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
                cursor.execute("INSERT INTO furnace_log (time, bits) VALUES (?, ?)",
                               (ts_ms, self.current_bits))
                print(f"[DB WRITE] {datetime.fromtimestamp(evt.timestamp)} | bits={self.current_bits:010b}")
            else:
                self.current_fan_mode = evt.data["fan_mode"]
                self.current_fan_rms = evt.data["fan_rms"]
                cursor.execute("INSERT INTO furnace_log (time, fan_mode, fan_rms) VALUES (?, ?, ?)",
                               (ts_ms, self.current_fan_mode, self.current_fan_rms))
                print(f"[DB WRITE] {datetime.fromtimestamp(evt.timestamp)} | fan_mode={self.current_fan_mode} | fan_rms={self.current_fan_rms:.3f}")

            conn.commit()

            if self.mqtt_client:
                payload = {
                    "timestamp": ts_ms,
                    "bits": self.current_bits,
                    "fan_mode": self.current_fan_mode,
                    "fan_rms": self.current_fan_rms
                }
                self.mqtt_client.publish("furnace/log", json.dumps(payload))


# ============================================================
# Main
# ============================================================

def main():
    event_queue = queue.Queue()

    producers = [
        FurnaceBitsProducer(
            "FurnaceBits",
            event_queue,
            db_file=CONFIG["DB_FILE"],
            config_file=CONFIG["BITS_CONFIG_FILE"],
            poll_interval_ms=CONFIG["BITS_POLL_INTERVAL_MS"],
            stability_ms=CONFIG["BITS_STABILITY_MS"]
        ),
        FanModeProducer(
            "FanMode",
            event_queue,
            CONFIG["FAN_CONFIG"]
        )
    ]

    mqtt_cfg = {"broker": CONFIG["MQTT_BROKER"], "port": CONFIG["MQTT_PORT"]} if CONFIG["MQTT_ENABLED"] else None
    aggregator = Aggregator(event_queue, CONFIG["DB_FILE"], CONFIG["MQTT_ENABLED"], mqtt_cfg)

    aggregator.start()
    for p in producers: p.start()

    print("System running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
        for p in producers: p.stop()
        aggregator.stop()
    for p in producers: p.join()
    aggregator.join()


if __name__ == "__main__":
    main()
