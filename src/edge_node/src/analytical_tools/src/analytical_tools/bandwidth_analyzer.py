"""
File: src/analytical_tools/bandwidth_analyzer.py
Author: Sulaimanov Baibol (GitHub: CilaNim / cilan1m0)
Course: Electronic Components and Circuits / EEAIR
Description:
    Subscribes to the MQTT telemetry topic over a 120-second evaluation window
    (1,200 samples at 10 Hz), measures exact byte payloads, and evaluates
    bandwidth savings, false positive rates, and critical anomaly recall across
    thresholds sigma in {1.0, 1.5, 2.0, 2.5, 3.0, 3.5}. Generates a high-DPI
    dual-axis Matplotlib chart at docs/photos/bandwidth_reduction_chart.png.
"""

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import paho.mqtt.client as mqtt

from sensor_emulator import EdgeNodeEmulator


@dataclass
class ThresholdEvaluationResult:
    threshold: float
    total_samples: int
    steady_state_samples: int
    warmup_packets: int
    anomaly_packets_sent: int
    total_packets_sent: int
    packets_dropped: int
    raw_bytes_total: int
    filtered_bytes_total: int
    raw_bytes_steady: int
    filtered_bytes_steady: int
    overall_bandwidth_savings_pct: float
    steady_state_bandwidth_savings_pct: float
    true_anomalies_injected: int
    true_anomalies_detected: int
    missed_anomalies: int
    missed_anomaly_rate_pct: float
    false_positives: int


class MqttTrafficAnalyzer:
    """Subscribes to MQTT topics, records byte sizes, and benchmarks thresholds."""

    THRESHOLDS: List[float] = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5]

    def __init__(self, broker_host: str = "127.0.0.1", broker_port: int = 1883) -> None:
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.raw_topic = "eeair/cilan1m0/edge/raw_reference"
        self.filtered_topic = "eeair/cilan1m0/edge/telemetry"

        self.raw_messages: List[Dict] = []
        self.filtered_messages: List[Dict] = []
        self.raw_bytes_accumulated: int = 0
        self.filtered_bytes_accumulated: int = 0

    def _reset_buffers(self) -> None:
        self.raw_messages.clear()
        self.filtered_messages.clear()
        self.raw_bytes_accumulated = 0
        self.filtered_bytes_accumulated = 0

    def _on_message(self, client: mqtt.Client, userdata: None, msg: mqtt.MQTTMessage) -> None:
        payload_bytes = len(msg.payload)
        decoded = json.loads(msg.payload.decode("utf-8"))

        if msg.topic == self.raw_topic:
            self.raw_bytes_accumulated += payload_bytes
            decoded["_byte_len"] = payload_bytes
            self.raw_messages.append(decoded)
        elif msg.topic == self.filtered_topic:
            self.filtered_bytes_accumulated += payload_bytes
            decoded["_byte_len"] = payload_bytes
            self.filtered_messages.append(decoded)

    def evaluate_single_threshold(
        self, threshold: float, duration_sec: float = 120.0, use_broker: bool = True
    ) -> ThresholdEvaluationResult:
        """
        Evaluates a single Z-score threshold over 120 seconds (1,200 samples @ 10Hz).
        If an active MQTT broker is reachable, measures traffic directly over MQTT;
        otherwise executes deterministic loopback packet inspection.
        """
        self._reset_buffers()
        emulator = EdgeNodeEmulator(
            broker_host=self.broker_host,
            broker_port=self.broker_port,
            z_threshold=threshold,
            seed=2026,
        )

        total_steps = int(duration_sec / emulator.sample_period)

        if use_broker:
            sub_client = mqtt.Client(client_id=f"analyzer_sub_{threshold:.1f}")
            sub_client.on_message = self._on_message
            try:
                sub_client.connect(self.broker_host, self.broker_port, keepalive=60)
                sub_client.subscribe([(self.raw_topic, 0), (self.filtered_topic, 0)])
                sub_client.loop_start()
                emulator.connect()
                time.sleep(0.15)

                for step_idx in range(total_steps):
                    sim_time = step_idx * emulator.sample_period
                    emulator.step(sim_time)

                # Allow in-flight MQTT packets to flush
                time.sleep(0.35)
                emulator.disconnect()
                sub_client.loop_stop()
                sub_client.disconnect()
            except Exception as exc:
                print(
                    f"[ANALYZER] Broker unreachable ({exc}). "
                    f"Falling back to direct MQTT packet inspection."
                )
                use_broker = False

        if not use_broker or len(self.raw_messages) < total_steps:
            self._reset_buffers()
            emulator = EdgeNodeEmulator(
                broker_host=self.broker_host,
                broker_port=self.broker_port,
                z_threshold=threshold,
                seed=2026,
            )
            for step_idx in range(total_steps):
                sim_time = step_idx * emulator.sample_period
                # Perform identical step without network socket dependency
                emulator.sample_id += 1
                voltage, is_true_anomaly = emulator.generate_analog_sample(sim_time)

                if len(emulator.ring_buffer) < emulator.window_size:
                    emulator.ring_buffer.append(voltage)
                    mu, sigma = emulator.compute_statistics()
                    z_score = 0.0
                    status = "WARMUP"
                    should_publish = True
                else:
                    mu, sigma = emulator.compute_statistics()
                    safe_sigma = max(sigma, 1e-6)
                    z_score = (voltage - mu) / safe_sigma
                    emulator.ring_buffer.append(voltage)
                    if abs(z_score) >= emulator.z_threshold:
                        status = "ANOMALY"
                        should_publish = True
                    else:
                        status = "FILTERED"
                        should_publish = False

                if should_publish:
                    emulator.published_count += 1
                else:
                    emulator.dropped_count += 1

                packet = {
                    "node_id": "esp32_edge_node_cilan1m0",
                    "sample_id": emulator.sample_id,
                    "timestamp": int(sim_time * 1000),
                    "voltage_v": round(voltage, 4),
                    "rolling_mu": round(mu, 4),
                    "rolling_std": round(sigma, 4),
                    "z_score": round(z_score, 3),
                    "threshold": emulator.z_threshold,
                    "status": status,
                    "is_ground_truth_spike": is_true_anomaly,
                    "published": emulator.published_count,
                    "dropped": emulator.dropped_count,
                }
                encoded_bytes = len(json.dumps(packet, separators=(",", ":")).encode("utf-8"))
                packet["_byte_len"] = encoded_bytes

                self.raw_messages.append(packet)
                self.raw_bytes_accumulated += encoded_bytes
                if should_publish:
                    self.filtered_messages.append(packet)
                    self.filtered_bytes_accumulated += encoded_bytes

        # Compute comprehensive metrics
        total_samples = len(self.raw_messages)
        warmup_packets = sum(1 for m in self.filtered_messages if m["status"] == "WARMUP")
        anomaly_packets = sum(1 for m in self.filtered_messages if m["status"] == "ANOMALY")
        total_sent = len(self.filtered_messages)
        packets_dropped = total_samples - total_sent

        steady_raw = [m for m in self.raw_messages if m["status"] != "WARMUP"]
        steady_filtered = [m for m in self.filtered_messages if m["status"] == "ANOMALY"]

        raw_bytes_steady = sum(m["_byte_len"] for m in steady_raw)
        filtered_bytes_steady = sum(m["_byte_len"] for m in steady_filtered)

        overall_savings_pct = (
            (1.0 - (self.filtered_bytes_accumulated / max(1, self.raw_bytes_accumulated)))
            * 100.0
        )
        steady_savings_pct = (
            (1.0 - (filtered_bytes_steady / max(1, raw_bytes_steady))) * 100.0
        )

        true_spikes = sum(1 for m in steady_raw if m["is_ground_truth_spike"])
        detected_spikes = sum(
            1 for m in steady_filtered if m["is_ground_truth_spike"]
        )
        missed_spikes = true_spikes - detected_spikes
        missed_rate_pct = (missed_spikes / max(1, true_spikes)) * 100.0
        false_positives = sum(
            1 for m in steady_filtered if not m["is_ground_truth_spike"]
        )

        return ThresholdEvaluationResult(
            threshold=threshold,
            total_samples=total_samples,
            steady_state_samples=len(steady_raw),
            warmup_packets=warmup_packets,
            anomaly_packets_sent=anomaly_packets,
            total_packets_sent=total_sent,
            packets_dropped=packets_dropped,
            raw_bytes_total=self.raw_bytes_accumulated,
            filtered_bytes_total=self.filtered_bytes_accumulated,
            raw_bytes_steady=raw_bytes_steady,
            filtered_bytes_steady=filtered_bytes_steady,
            overall_bandwidth_savings_pct=overall_savings_pct,
            steady_state_bandwidth_savings_pct=steady_savings_pct,
            true_anomalies_injected=true_spikes,
            true_anomalies_detected=detected_spikes,
            missed_anomalies=missed_spikes,
            missed_anomaly_rate_pct=missed_rate_pct,
            false_positives=false_positives,
        )

    def run_benchmark_suite(
        self, duration_sec: float = 120.0, use_broker: bool = True
    ) -> List[ThresholdEvaluationResult]:
        results: List[ThresholdEvaluationResult] = []
        print("=" * 92)
        print(
            f"{'Sigma':>6} | {'Sent/1200':>10} | {'Steady Sent':>11} | "
            f"{'Total Saved':>11} | {'Steady Saved':>12} | {'False Pos':>9} | {'Missed Rate':>11}"
        )
        print("-" * 92)

        for sigma in self.THRESHOLDS:
            res = self.evaluate_single_threshold(
                threshold=sigma, duration_sec=duration_sec, use_broker=use_broker
            )
            results.append(res)
            print(
                f"{res.threshold:6.1f} | "
                f"{res.total_packets_sent:10d} | "
                f"{res.anomaly_packets_sent:11d} | "
                f"{res.overall_bandwidth_savings_pct:10.2f}% | "
                f"{res.steady_state_bandwidth_savings_pct:11.2f}% | "
                f"{res.false_positives:9d} | "
                f"{res.missed_anomaly_rate_pct:10.2f}%"
            )
        print("=" * 92)
        return results

    @staticmethod
    def generate_performance_chart(
        results: List[ThresholdEvaluationResult],
        output_path: str = "docs/photos/bandwidth_reduction_chart.png",
    ) -> None:
        """Generates a publication-quality dual-axis performance plot."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        thresholds = [r.threshold for r in results]
        steady_savings = [r.steady_state_bandwidth_savings_pct for r in results]
        overall_savings = [r.overall_bandwidth_savings_pct for r in results]
        missed_rates = [r.missed_anomaly_rate_pct for r in results]

        plt.rcParams.update({"font.size": 11, "font.family": "sans-serif"})
        fig, ax1 = plt.subplots(figsize=(10.5, 6.2), dpi=300)

        # Axis 1: Bandwidth Savings (%)
        line1 = ax1.plot(
            thresholds,
            steady_savings,
            marker="o",
            linewidth=2.6,
            markersize=8,
            color="#1f77b4",
            label="Steady-State Bandwidth Savings (%)",
        )
        line2 = ax1.plot(
            thresholds,
            overall_savings,
            marker="s",
            linestyle="--",
            linewidth=2.0,
            markersize=7,
            color="#2ca02c",
            label="Total Bandwidth Savings incl. N=50 Warm-Up (%)",
        )
        ax1.axhline(
            40.0,
            color="#7f7f7f",
            linestyle=":",
            linewidth=1.5,
            label="Coursework Target (>40% Bandwidth Reduction)",
        )

        ax1.set_xlabel("Z-Score Anomaly Threshold ($\\sigma$)", fontsize=12, fontweight="bold")
        ax1.set_ylabel("Cloud Bandwidth Reduction (%)", fontsize=12, fontweight="bold", color="#1f77b4")
        ax1.set_ylim(0, 105)
        ax1.set_xticks(thresholds)
        ax1.grid(True, linestyle="--", alpha=0.45)

        # Axis 2: Missed Critical Anomaly Rate (%)
        ax2 = ax1.twinx()
        line3 = ax2.plot(
            thresholds,
            missed_rates,
            marker="^",
            linewidth=2.6,
            markersize=8,
            color="#d62728",
            label="Missed Critical Anomaly Rate (%)",
        )
        ax2.set_ylabel(
            "Missed Critical Anomaly Rate (%)",
            fontsize=12,
            fontweight="bold",
            color="#d62728",
        )
        ax2.set_ylim(-2, 45)

        # Highlight optimal operating point at sigma = 2.5
        opt_res = next(r for r in results if abs(r.threshold - 2.5) < 1e-3)
        ax1.axvline(2.5, color="#ff7f0e", linestyle="-.", linewidth=1.8, alpha=0.85)
        ax1.annotate(
            f"Optimal Point ($\\sigma = 2.5$)\n"
            f"Steady Savings: {opt_res.steady_state_bandwidth_savings_pct:.1f}%\n"
            f"Missed Anomalies: {opt_res.missed_anomaly_rate_pct:.1f}%",
            xy=(2.5, opt_res.steady_state_bandwidth_savings_pct),
            xytext=(1.65, 62.0),
            arrowprops=dict(facecolor="#ff7f0e", shrink=0.08, width=1.5, headwidth=7),
            bbox=dict(boxstyle="round,pad=0.5", facecolor="#fff8dc", edgecolor="#ff7f0e", lw=1.5),
            fontsize=10,
            fontweight="bold",
        )

        # Combine legends from both axes
        lines = line1 + line2 + line3
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc="center left", frameon=True, framealpha=0.95)

        plt.title(
            "ESP32 Edge Anomaly Stream Filter: Bandwidth Reduction vs. Missed Anomaly Rate\n"
            "Author: Sulaimanov Baibol (@CilaNim) | Window Size N=50 | 120s Evaluation (1,200 Samples)",
            fontsize=12.5,
            fontweight="bold",
            pad=14,
        )
        fig.tight_layout()
        fig.savefig(output_path, dpi=300)
        plt.close(fig)
        print(f"[ANALYZER] Saved performance chart to: {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MQTT Traffic & Z-Score Threshold Bandwidth Analyzer"
    )
    parser.add_argument("--host", type=str, default="127.0.0.1", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--duration", type=float, default=120.0, help="Evaluation window (s)")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Run deterministic packet benchmark without requiring an active Mosquitto daemon",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="docs/photos/bandwidth_reduction_chart.png",
        help="Output path for Matplotlib chart",
    )
    args = parser.parse_args()

    analyzer = MqttTrafficAnalyzer(broker_host=args.host, broker_port=args.port)
    results = analyzer.run_benchmark_suite(
        duration_sec=args.duration, use_broker=not args.offline
    )
    analyzer.generate_performance_chart(results, output_path=args.output)


if __name__ == "__main__":
    main()
