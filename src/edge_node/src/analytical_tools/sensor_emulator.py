File: src/analytical_tools/sensor_emulator.py
Author: Sulaimanov Baibol (GitHub: CilaNim / cilan1m0)
Course: Electronic Components and Circuits / EEAIR
Description:
    Emulates an ESP32 analog sensor stream on GPIO36 generating a 0.5 Hz
    sinusoidal carrier wave (centered at 1.65V, amplitude 0.45V), additive
    Gaussian white noise (sigma_noise = 0.04V), and stochastic 5% probability
    voltage spike anomalies. Publishes both raw benchmark metadata and
    edge-filtered telemetry packets over MQTT to Eclipse Mosquitto.
"""

import argparse
import collections
import json
import math
import random
import time
from typing import Deque, Dict, Any

import numpy as np
import paho.mqtt.client as mqtt


class EdgeNodeEmulator:
    """Replicates the ESP32 N=50 Ring Buffer and Z-Score filter in Python 3.10."""

    def __init__(
        self,
        broker_host: str = "127.0.0.1",
        broker_port: int = 1883,
        topic: str = "eeair/cilan1m0/edge/telemetry",
        raw_topic: str = "eeair/cilan1m0/edge/raw_reference",
        window_size: int = 50,
        z_threshold: float = 2.5,
        sample_rate_hz: float = 10.0,
        anomaly_probability: float = 0.05,
        seed: int = 2026,
    ) -> None:
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.raw_topic = raw_topic
        self.window_size = window_size
        self.z_threshold = z_threshold
        self.sample_period = 1.0 / sample_rate_hz
        self.anomaly_probability = anomaly_probability

        # Seed PRNGs for reproducible empirical grading
        random.seed(seed)
        np.random.seed(seed)

        # Fixed-capacity ring buffer (N=50)
        self.ring_buffer: Deque[float] = collections.deque(maxlen=self.window_size)

        # Telemetry counters
        self.sample_id = 0
        self.published_count = 0
        self.dropped_count = 0

        # Configure Paho-MQTT client
        self.mqtt_client = mqtt.Client(
            client_id=f"emulator_cilan1m0_sigma_{self.z_threshold:.1f}"
        )

    def connect(self) -> None:
        """Connects to the Eclipse Mosquitto broker and starts background network loop."""
        self.mqtt_client.connect(self.broker_host, self.broker_port, keepalive=60)
        self.mqtt_client.loop_start()
        print(
            f"[EMULATOR] Connected to MQTT Broker at {self.broker_host}:{self.broker_port} "
            f"| Threshold sigma={self.z_threshold:.2f}"
        )

    def disconnect(self) -> None:
        """Stops network loop and cleanly disconnects from broker."""
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        print("[EMULATOR] Disconnected from MQTT Broker.")

    def generate_analog_sample(self, elapsed_sec: float) -> tuple[float, bool]:
        """
        Synthesizes an analog voltage reading within the ESP32 0.0V - 3.3V ADC range:
          - Baseline signal: 1.65V DC offset + 0.45V * sin(2 * pi * 0.5Hz * t)
          - Thermal/ADC noise: N(0, 0.04^2)
          - Anomaly injection: 5% probability spike of +/- [1.10V to 1.45V]
        """
        carrier_freq_hz = 0.5
        baseline_v = 1.65 + 0.45 * math.sin(2.0 * math.pi * carrier_freq_hz * elapsed_sec)
        gaussian_noise_v = float(np.random.normal(0.0, 0.04))
        voltage = baseline_v + gaussian_noise_v

        is_true_anomaly = False
        if len(self.ring_buffer) == self.window_size and random.random() < self.anomaly_probability:
            is_true_anomaly = True
            spike_direction = random.choice([-1.0, 1.0])
            spike_magnitude = random.uniform(1.10, 1.45)
            voltage += spike_direction * spike_magnitude

        # Clamp to physical 12-bit SAR ADC voltage rails [0.0V, 3.3V]
        voltage = max(0.0, min(3.3, voltage))
        return voltage, is_true_anomaly

    def compute_statistics(self) -> tuple[float, float]:
        """Computes rolling mean (mu) and population standard deviation (sigma)."""
        if not self.ring_buffer:
            return 0.0, 0.0
        arr = np.array(self.ring_buffer, dtype=np.float64)
        return float(np.mean(arr)), float(np.std(arr, ddof=0))

    def step(self, elapsed_sec: float) -> Dict[str, Any]:
        """Executes one sampling and edge-filtering cycle."""
        self.sample_id += 1
        voltage, is_true_anomaly = self.generate_analog_sample(elapsed_sec)

        # Warm-up Phase: Transmit raw telemetry until Ring Buffer reaches N=50
        if len(self.ring_buffer) < self.window_size:
            self.ring_buffer.append(voltage)
            mu, sigma = self.compute_statistics()
            z_score = 0.0
            status = "WARMUP"
            should_publish = True
        else:
            # Evaluate Z-score against historical window before appending new sample
            mu, sigma = self.compute_statistics()
            safe_sigma = max(sigma, 1e-6)
            z_score = (voltage - mu) / safe_sigma

            # Push sample to ring buffer only if it is not an extreme anomaly, OR push all
            # samples just like the C++ firmware while holding sufficient window inertia (N=50)
            self.ring_buffer.append(voltage)

            if abs(z_score) >= self.z_threshold:
                status = "ANOMALY"
                should_publish = True
            else:
                status = "FILTERED"
                should_publish = False

        if should_publish:
            self.published_count += 1
        else:
            self.dropped_count += 1

        packet = {
            "node_id": "esp32_edge_node_cilan1m0",
            "sample_id": self.sample_id,
            "timestamp": int(elapsed_sec * 1000),
            "voltage_v": round(voltage, 4),
            "rolling_mu": round(mu, 4),
            "rolling_std": round(sigma, 4),
            "z_score": round(z_score, 3),
            "threshold": self.z_threshold,
            "status": status,
            "is_ground_truth_spike": is_true_anomaly,
            "published": self.published_count,
            "dropped": self.dropped_count,
        }

        raw_json = json.dumps(packet, separators=(",", ":"))

        # Always publish to raw_reference topic so analyzer can track ground-truth bytes
        self.mqtt_client.publish(self.raw_topic, raw_json, qos=0)

        # Only publish to edge telemetry topic if Warm-up or |Z| >= threshold
        if should_publish:
            self.mqtt_client.publish(self.topic, raw_json, qos=0)

        return packet

    def run(self, duration_sec: float = 120.0, realtime: bool = True) -> None:
        """Runs the sensor emulation loop for the specified duration."""
        self.connect()
        total_steps = int(duration_sec / self.sample_period)
        start_wall = time.time()

        try:
            for step_idx in range(total_steps):
                sim_time = step_idx * self.sample_period
                packet = self.step(sim_time)

                savings_pct = (self.dropped_count / self.sample_id) * 100.0
                if packet["status"] in ("WARMUP", "ANOMALY"):
                    print(
                        f"[{packet['status']:7s}] #{packet['sample_id']:04d} | "
                        f"V={packet['voltage_v']:.4f}V | Z={packet['z_score']:+6.2f} | "
                        f"TrueSpike={str(packet['is_ground_truth_spike']):5s} | "
                        f"Saved={savings_pct:5.1f}%"
                    )

                if realtime:
                    target_wall = start_wall + ((step_idx + 1) * self.sample_period)
                    sleep_dt = target_wall - time.time()
                    if sleep_dt > 0:
                        time.sleep(sleep_dt)
        except KeyboardInterrupt:
            print("\n[EMULATOR] Interrupted by user.")
        finally:
            self.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ESP32 Analog Sensor & Z-Score Edge Filter MQTT Emulator"
    )
    parser.add_argument("--host", type=str, default="127.0.0.1", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--threshold", type=float, default=2.5, help="Z-score threshold")
    parser.add_argument("--duration", type=float, default=120.0, help="Run duration (s)")
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Run without wall-clock sleep delays for rapid batch evaluation",
    )
    args = parser.parse_args()

    emulator = EdgeNodeEmulator(
        broker_host=args.host,
        broker_port=args.port,
        z_threshold=args.threshold,
    )
    emulator.run(duration_sec=args.duration, realtime=not args.fast)


if __name__ == "__main__":
    main()
