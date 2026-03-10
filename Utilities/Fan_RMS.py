import time
import math
import smbus
from datetime import datetime


# ================= USER CONFIGURATION =================

I2C_BUS = 0
ADS1115_ADDR = 0x48

CHANNEL = 0          # AIN2
GAIN = 2             # ±2.048 V
ADC_SPS = 860        # Continuous sample rate
WINDOW_TIME = 1.0    # Size of sample window (Sec)

LOOP_TIME_SEC  = 1   # Loop Time seconds

CURRENT_GAIN = 30.0  # Irms = Vrms * 30
SINC_ATTEN =  0.991

DISCARD_WINDOWS = 3  # Ignore startup settling windows

# ======================================================

bus = smbus.SMBus(I2C_BUS)

CONFIG_REG = 0x01
CONVERSION_REG = 0x00

GAIN_FS = {
    2/3: 6.144,
    1:   4.096,
    2:   2.048,
    4:   1.024,
    8:   0.512,
    16:  0.256
}

GAIN_BITS = {
    2/3: 0x0000,
    1:   0x0200,
    2:   0x0400,
    4:   0x0600,
    8:   0x0800,
    16:  0x0A00
}

MUX_BITS = {
    0: 0x4000,
    1: 0x5000,
    2: 0x6000,
    3: 0x7000
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

if GAIN not in GAIN_FS:
    raise ValueError("Invalid GAIN")

if ADC_SPS not in SPS_BITS:
    raise ValueError("Invalid ADC_SPS")

SAMPLES_PER_WINDOW = int(ADC_SPS * WINDOW_TIME)

LSB = GAIN_FS[GAIN] / 32768.0

# ---------- Configure ADS1115 ONCE (continuous mode) ----------

config = (
    MUX_BITS[CHANNEL] |
    GAIN_BITS[GAIN] |
    0x0000 |                  # Continuous-conversion mode
    SPS_BITS[ADC_SPS] |
    0x0003                    # Disable comparator
)

bus.write_i2c_block_data(
    ADS1115_ADDR,
    CONFIG_REG,
    [(config >> 8) & 0xFF, config & 0xFF]
)

# Small delay to allow filter pipeline to fill
time.sleep(0.02)

def read_adc():
    data = bus.read_i2c_block_data(ADS1115_ADDR, CONVERSION_REG, 2)
    raw = (data[0] << 8) | data[1]
    if raw & 0x8000:
        raw -= 65536
    return raw * LSB

print("ADS1115 AC RMS Current Measurement (Continuous Mode)")
print(f"Channel: AIN{CHANNEL}, Gain: {GAIN}x")
print(f"ADC SPS: {ADC_SPS}, LOOP Time: {LOOP_TIME_SEC} Sec")
print("Press Ctrl+C to stop\n")

window_count = 0

try:
    while True:
        sum_v = 0.0
        sum_v2 = 0.0

        start = time.time()

        for _ in range(SAMPLES_PER_WINDOW):
            v = read_adc()
            sum_v += v
            sum_v2 += v * v

        mean = sum_v / SAMPLES_PER_WINDOW
        vrms = math.sqrt((sum_v2 / SAMPLES_PER_WINDOW) - (mean * mean))
        irms = vrms * CURRENT_GAIN / SINC_ATTEN

        if window_count >= DISCARD_WINDOWS:
            if irms > -4:                # Only print while running
                ts = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
                print(f"{ts} Irms: {irms:.3f} A")

        window_count += 1

        elapsed = time.time() - start
        sleep_time = LOOP_TIME_SEC - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

except KeyboardInterrupt:
    print("\nMeasurement stopped.")
