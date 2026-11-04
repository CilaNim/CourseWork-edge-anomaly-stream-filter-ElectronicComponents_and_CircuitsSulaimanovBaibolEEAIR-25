"""
File: src/analytical_tools/bandwidth_analyzer.py
Author: Sulaimanov Baibol (GitHub: cilan1m0 / CilaNim)
Course: Electronic Components and Circuits (EEAIR)
Description:
    Evaluates bandwidth savings and critical anomaly recall across Z-score
    thresholds sigma in {1.0, 1.5, 2.0, 2.5, 3.0, 3.5} over a 120-second window
    (1,200 samples at 10 Hz) and saves a dual-axis Matplotlib performance plot
    to docs/photos/bandwidth_reduction_chart.png.
"""

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Dict, List

import matplotlib.pyplot as plt
import paho.mqtt.client as mqtt

from sensor_emulator import EdgeSensorEmulator


@dataclass
class BenchmarkMetrics:
    sigma_threshold: float
    total_samples: int
    steady_state_samples: int
    warmup_packets: int
    anomaly_packets: int
    total_packets_sent: int
    packets_dropped: int
    unfiltered_bytes_total: int
    filtered_bytes_total: int
    unfiltered_bytes_steady: int
    filtered_bytes_steady: int
    total_bandwidth_savings_pct: float
    steady_bandwidth_savings_pct: float
    ground_truth_spikes: int
    detected_spikes: int
    missed_spikes: int
    missed_spike_rate_pct: float
    false_positives: int


class BandwidthAnalyzer:
    """Measures MQTT traffic volume and anomaly detection recall across thresholds."""

    SIGMA_SWEEP: List[float] = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5]

    def __init__(self, broker_host: str = "127.0.0.1", broker_port: int = 1883) -> None:
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.raw_topic = "eeair/cilan1m0/edge/raw_reference"
        self.filtered_topic = "eeair/cilan1m0/edge/telemetry"

        self.raw_records: List[Dict] = []
        self.filtered_records: List[Dict] = []

    def _clear_records(self) -> None:
        self.raw_records.clear()
        self.filtered_records.clear()

    def _on_mqtt_message(
        self, client: mqtt.Client, userdata: None, msg: mqtt.MQTTMessage
    ) -> None:
        record = json.loads(msg.payload.decode("utf-8"))
        record["_byte_size"] = len(msg.payload)
        if msg.topic == self.raw_topic:
            self.raw_records.append(record)
        elif msg.topic == self.filtered_topic:
            self.filtered_records.append(record)

    def evaluate_threshold(
        self, sigma: float, duration_s: float = 120.0, use_live_broker: bool = True
    ) -> BenchmarkMetrics:
        """Runs a 120-second evaluation for a specific Z-score threshold."""
        self._clear_records()
        emulator = EdgeSensorEmulator(
            broker_host=self.broker_host,
            broker_port=self.broker_port,
            z_threshold=sigma,
            random_seed=2026,
        )
        total_steps = int(duration_s / emulator.sample_period_s)

        if use_live_broker:
            subscriber = mqtt.Client(client_id=f"analyzer_cilan1m0_{sigma:.1f}")
            subscriber.on_message = self._on_mqtt_message
            try:
                subscriber.connect(self.broker_host, self.broker_port, keepalive=60)
                subscriber.subscribe([(self.raw_topic, 0), (self.filtered_topic, 0)])
                subscriber.loop_start()
                emulator.connect_broker()
                time.sleep(0.15)

                for idx in range(total_steps):
                    emulator.step_simulation(idx * emulator.sample_period_s, publish_mqtt=True)

                time.sleep(0.35)
                emulator.disconnect_broker()
                subscriber.loop_stop()
                subscriber.disconnect()
            except Exception as err:
                print(
                    f"[ANALYZER] Local broker not detected ({err}). "
                    f"Using direct deterministic packet inspection."
                )
                use_live_broker = False

        if not use_live_broker or len(self.raw_records) < total_steps:
            self._clear_records()
            emulator = EdgeSensorEmulator(
                broker_host=self.broker_host,
                broker_port=self.broker_port,
                z_threshold=sigma,
                random_seed=2026,
            )
            for idx in range(total_steps):
                pkt = emulator.step_simulation(
                    idx * emulator.sample_period_s, publish_mqtt=False
                )
                self.raw_records.append(pkt)
                if pkt["status"] in ("WARMUP", "ANOMALY"):
                    self.filtered_records.append(pkt)

        total_samples = len(self.raw_records)
        warmup_pkts = sum(1 for r in self.filtered_records if r["status"] == "WARMUP")
        anomaly_pkts = sum(1 for r in self.filtered_records if r["status"] == "ANOMALY")
        total_sent = len(self.filtered_records)
        dropped_pkts = total_samples - total_sent

        steady_raw = [r for r in self.raw_records if r["status"] != "WARMUP"]
        steady_filtered = [r for r in self.filtered_records if r["status"] == "ANOMALY"]

        raw_bytes_total = sum(r["_byte_size"] for r in self.raw_records)
        filtered_bytes_total = sum(r["_byte_size"] for r in self.filtered_records)
        raw_bytes_steady = sum(r["_byte_size"] for r in steady_raw)
        filtered_bytes_steady = sum(r["_byte_size"] for r in steady_filtered)

        total_savings_pct = (
            1.0 - (filtered_bytes_total / max(1, raw_bytes_total))
        ) * 100.0
        steady_savings_pct = (
            1.0 - (filtered_bytes_steady / max(1, raw_bytes_steady))
        ) * 100.0

        true_spikes = sum(1 for r in steady_raw if r["is_ground_truth_spike"])
        caught_spikes = sum(1 for r in steady_filtered if r["is_ground_truth_spike"])
        missed_spikes = true_spikes - caught_spikes
        missed_rate_pct = (missed_spikes / max(1, true_spikes)) * 100.0
        false_positives = sum(1 for r in steady_filtered if not r["is_ground_truth_spike"])

        return BenchmarkMetrics(
            sigma_threshold=sigma,
            total_samples=total_samples,
            steady_state_samples=len(steady_raw),
            warmup_packets=warmup_pkts,
            anomaly_packets=anomaly_pkts,
            total_packets_sent=total_sent,
            packets_dropped=dropped_pkts,
            unfiltered_bytes_total=raw_bytes_total,
            filtered_bytes_total=filtered_bytes_total,
            unfiltered_bytes_steady=raw_bytes_steady,
            filtered_bytes_steady=filtered_bytes_steady,
            total_bandwidth_savings_pct=total_savings_pct,
            steady_bandwidth_savings_pct=steady_savings_pct,
            ground_truth_spikes=true_spikes,
            detected_spikes=caught_spikes,
            missed_spikes=missed_spikes,
            missed_spike_rate_pct=missed_rate_pct,
            false_positives=false_positives,
        )

    def run_full_sweep(
        self, duration_s: float = 120.0, use_live_broker: bool = True
    ) -> List[BenchmarkMetrics]:
        summary: List[BenchmarkMetrics] = []
        print("=" * 94)
        print(
            f"{'Sigma':>6} | {'Sent/1200':>10} | {'Steady Sent':>11} | "
            f"{'Total Saved':>11} | {'Steady Saved':>12} | {'False Pos':>9} | {'Missed Events':>13}"
        )
        print("-" * 94)

        for sigma in self.SIGMA_SWEEP:
            metrics = self.evaluate_threshold(
                sigma=sigma, duration_s=duration_s, use_live_broker=use_live_broker
            )
            summary.append(metrics)
            print(
                f"{metrics.sigma_threshold:6.1f} | "
                f"{metrics.total_packets_sent:10d} | "
                f"{metrics.anomaly_packets:11d} | "
                f"{metrics.total_bandwidth_savings_pct:10.2f}% | "
                f"{metrics.steady_bandwidth_savings_pct:11.2f}% | "
                f"{metrics.false_positives:9d} | "
                f"{metrics.missed_spike_rate_pct:12.2f}%"
            )
        print("=" * 94)
        return summary

    @staticmethod
    def plot_results(
        metrics_list: List[BenchmarkMetrics],
        output_file: str = "docs/photos/bandwidth_reduction_chart.png",
    ) -> None:
        """Generates and saves the Matplotlib bandwidth vs. missed anomaly chart."""
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        sigmas = [m.sigma_threshold for m in metrics_list]
        steady_saved = [m.steady_bandwidth_savings_pct for m in metrics_list]
        total_saved = [m.total_bandwidth_savings_pct for m in metrics_list]
        missed_pct = [m.missed_spike_rate_pct for m in metrics_list]

        plt.rcParams.update({"font.size": 11, "font.family": "sans-serif"})
        fig, ax_left = plt.subplots(figsize=(10.5, 6.2), dpi=300)

        l1 = ax_left.plot(
            sigmas,
            steady_saved,
            marker="o",
            linewidth=2.6,
            markersize=8,
            color="#1f77b4",
            label="Steady-State Bandwidth Savings (%)",
        )
        l2 = ax_left.plot(
            sigmas,
            total_saved,
            marker="s",
            linestyle="--",
            linewidth=2.0,
            markersize=7,
            color="#2ca02c",
            label="Total Bandwidth Savings incl. N=50 Warm-Up (%)",
        )
        ax_left.axhline(
            40.0,
            color="#7f7f7f",
            linestyle=":",
            linewidth=1.5,
            label="Target Research Threshold (>40% Savings)",
        )

        ax_left.set_xlabel("Z-Score Anomaly Threshold ($\\sigma$)", fontsize=12, fontweight="bold")
        ax_left.set_ylabel("Cloud Bandwidth Reduction (%)", fontsize=12, fontweight="bold", color="#1f77b4")
        ax_left.set_ylim(0, 105)
        ax_left.set_xticks(sigmas)
        ax_left.grid(True, linestyle="--", alpha=0.45)

        ax_right = ax_left.twinx()
        l3 = ax_right.plot(
            sigmas,
            missed_pct,
            marker="^",
            linewidth=2.6,
            markersize=8,
            color="#d62728",
            label="Missed Critical Anomaly Rate (%)",
        )
        ax_right.set_ylabel(
            "Missed Critical Anomaly Rate (%)",
            fontsize=12,
            fontweight="bold",
            color="#d62728",
        )
        ax_right.set_ylim(-2, 45)

        opt = next(m for m in metrics_list if abs(m.sigma_threshold - 2.5) < 1e-3)
        ax_left.axvline(2.5, color="#ff7f0e", linestyle="-.", linewidth=1.8, alpha=0.85)
        ax_left.annotate(
            f"Optimal Operating Point ($\\sigma = 2.5$)\n"
            f"Steady Savings: {opt.steady_bandwidth_savings_pct:.2f}%\n"
            f"Total Savings: {opt.total_bandwidth_savings_pct:.2f}%\n"
            f"Missed Anomalies: {opt.missed_spike_rate_pct:.2f}%",
            xy=(2.5, opt.steady_bandwidth_savings_pct),
            xytext=(1.62, 58.0),
            arrowprops=dict(facecolor="#ff7f0e", shrink=0.08, width=1.5, headwidth=7),
            bbox=dict(boxstyle="round,pad=0.5", facecolor="#fff8dc", edgecolor="#ff7f0e", lw=1.5),
            fontsize=10,
            fontweight="bold",
        )

        all_lines = l1 + l2 + l3
        all_labels = [line.get_label() for line in all_lines]
        ax_left.legend(all_lines, all_labels, loc="center left", frameon=True, framealpha=0.95)

        plt.title(
            "Edge Anomaly Stream Filter: Cloud Bandwidth Reduction vs. Missed Critical Events\n"
            "Author: Sulaimanov Baibol (@cilan1m0) | Ring Buffer N=50 | 120s Window (1,200 Samples)",
            fontsize=12,
            fontweight="bold",
            pad=14,
        )
        fig.tight_layout()
        fig.savefig(output_file, dpi=300)
        plt.close(fig)
        print(f"[ANALYZER] Chart successfully saved to: {output_file}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bandwidth Savings & Anomaly Recall Analyzer across Z-Thresholds"
    )
    parser.add_argument("--host", type=str, default="127.0.0.1", help="MQTT Broker Host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT Broker Port")
    parser.add_argument("--duration", type=float, default=120.0, help="Analysis duration (s)")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Run deterministic evaluation without requiring a live Mosquitto daemon",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="docs/photos/bandwidth_reduction_chart.png",
        help="Output chart path",
    )
    args = parser.parse_args()

    analyzer = BandwidthAnalyzer(broker_host=args.host, broker_port=args.port)
    results = analyzer.run_full_sweep(
        duration_s=args.duration, use_live_broker=not args.offline
    )
    analyzer.plot_results(results, output_file=args.output)


if __name__ == "__main__":
    main()
