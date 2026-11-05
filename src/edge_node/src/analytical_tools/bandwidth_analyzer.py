#!/usr/bin/env python3
"""
bandwidth_analyzer.py
=====================
Quantifies the bandwidth reduction obtained by the ESP32 edge Z-score filter.

METHOD
------
1. Subscribes to  eeair/telemetry/raw  (the unfiltered ground-truth stream
   produced either by `sensor_emulator.py` or by the ESP32 during warm-up).
2. For every incoming raw frame it *simultaneously* replays six independent
   digital twins of the firmware filter, one per threshold
   sigma in {1.0, 1.5, 2.0, 2.5, 3.0, 3.5}. Each twin is a bit-exact
   re-implementation of `main.cpp`: identical 50-slot ring buffer, identical
   Bessel-corrected sigma, identical zero-division guard, identical JSON
   payload layout.
3. Each frame that a twin would publish is charged the exact number of bytes
   that would appear on the wire (MQTT fixed header + topic length + payload).
4. At the end of the 120-second window the script reports, per threshold:
       - total transmitted bytes vs. the unfiltered baseline
       - bandwidth reduction (%)
       - number of injected spikes that were NOT flagged (false negatives)
5. Results are written to  data/threshold_sweep_results.csv  and plotted to
   docs/photos/bandwidth_reduction_chart.png

A second measurement channel counts the *actual* bytes arriving on
eeair/telemetry/anomaly, i.e. what the real ESP32 sent. This validates that
the Python twin matches the firmware.

Course : Electronic Components and Circuits (EEAIR)
Author : Sulaimanov Baibol (CilaNim / cilan1m0)
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import deque
from pathlib import Path

import matplotlib
matplotlib.use("Agg")                    
import matplotlib.pyplot as plt
import numpy as np
import paho.mqtt.client as mqtt

TOPIC_RAW     = "eeair/telemetry/raw"
TOPIC_ANOMALY = "eeair/telemetry/anomaly"
TOPIC_STATUS  = "eeair/telemetry/status"

RING_N     = 50
SIGMA_EPS  = 1e-4
THRESHOLDS = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5]

CSV_PATH   = Path("data/threshold_sweep_results.csv")
CHART_PATH = Path("docs/photos/bandwidth_reduction_chart.png")

def mqtt_packet_bytes(topic: str, payload: str, qos: int = 0) -> int:
    """
    Exact byte count of a single MQTT 3.1.1 PUBLISH control packet.

        fixed header   = 1 (type/flags) + N (remaining-length varint)
        variable header= 2 (topic length) + len(topic) [+ 2 (packet id) if qos>0]
        payload        = len(payload)
    """
    topic_len = len(topic.encode("utf-8"))
    body = 2 + topic_len + len(payload.encode("utf-8")) + (2 if qos > 0 else 0)

    varint = 1
    remaining = body
    while remaining >= 128:
        remaining //= 128
        varint += 1

    return 1 + varint + body

class ZFilterTwin:
    """Bit-exact Python replica of the firmware's edge filter."""

    def __init__(self, z_threshold: float, n: int = RING_N) -> None:
        self.z = z_threshold
        self.n = n
        self.buf: deque[float] = deque(maxlen=n)
        self.warm = True

    def process(self, t_ms: int, value: float):
        """
        Returns (published: bool, payload: str | None, status: str)
        status in {"warmup", "flat", "nominal", "anomaly"}
        """
        self.buf.append(value)

        if len(self.buf) < self.n:
            self.warm = True
            payload = json.dumps(
                {"t": t_ms, "v": round(value, 4), "src": "esp32-warmup"},
                separators=(",", ":"),
            )
            return True, payload, "warmup"

        self.warm = False
        arr = np.fromiter(self.buf, dtype=np.float64, count=self.n)
        mu = float(arr.mean())
        sd = float(arr.std(ddof=1))

        if sd < SIGMA_EPS:                     
            return False, None, "flat"

        z = (value - mu) / sd
        if abs(z) >= self.z:
            payload = json.dumps(
                {"t": t_ms,
                 "v": round(value, 4),
                 "z": round(z, 2),
                 "m": round(mu, 4),
                 "s": round(sd, 4)},
                separators=(",", ":"),
            )
            return True, payload, "anomaly"

        return False, None, "nominal"

class BandwidthAnalyzer:
    def __init__(self, host: str, port: int, client_id: str, duration: float):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.duration = duration

        self.twins = {thr: ZFilterTwin(thr) for thr in THRESHOLDS}
        self.bytes_out = {thr: 0 for thr in THRESHOLDS}
        self.packets_out = {thr: 0 for thr in THRESHOLDS}

        self.baseline_bytes = 0
        self.baseline_packets = 0

        self.spikes_total = 0
        self.spikes_missed = {thr: 0 for thr in THRESHOLDS}

        self.device_anomaly_bytes = 0
        self.device_anomaly_packets = 0

        self.t0 = 0.0
        self.finished = False
        self.client = self._make_client()

    def _make_client(self) -> mqtt.Client:
        try:                                 
            c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,
                            client_id=self.client_id)
        except AttributeError:              
            c = mqtt.Client(client_id=self.client_id)

        c.on_connect = self._on_connect
        c.on_message = self._on_message
        return c

    def _on_connect(self, client, userdata, flags, rc): 
        print(f"[AN] connected (rc={rc}); subscribing ...")
        client.subscribe(TOPIC_RAW, qos=0)
        client.subscribe(TOPIC_ANOMALY, qos=0)

    def _on_message(self, client, userdata, msg):       
        if self.finished:
            return

        topic = msg.topic
        payload = msg.payload.decode("utf-8", errors="replace")

        if topic == TOPIC_RAW:
            self._handle_raw(payload)

        elif topic == TOPIC_ANOMALY:
            self.device_anomaly_bytes += mqtt_packet_bytes(topic, payload)
            self.device_anomaly_packets += 1

    def _handle_raw(self, payload: str) -> None:
        try:
            frame = json.loads(payload)
            t_ms = int(frame.get("t", 0))
            value = float(frame["v"])
            gt = int(frame.get("gt", 0))
        except (ValueError, KeyError, TypeError):
            return                             
        self.baseline_bytes += mqtt_packet_bytes(TOPIC_RAW, payload)
        self.baseline_packets += 1

        if gt:
            self.spikes_total += 1

        for thr, twin in self.twins.items():
            published, out_payload, status = twin.process(t_ms, value)

            if published and out_payload is not None:
                topic = TOPIC_RAW if status == "warmup" else TOPIC_ANOMALY
                self.bytes_out[thr] += mqtt_packet_bytes(topic, out_payload)
                self.packets_out[thr] += 1

            if gt and status != "anomaly":
                self.spikes_missed[thr] += 1

    def run(self) -> None:
        print(f"[AN] connecting to {self.host}:{self.port} ...")
        self.client.connect(self.host, self.port, keepalive=60)
        self.client.loop_start()

        self.t0 = time.monotonic()
        next_report = 10.0

        try:
            while True:
                elapsed = time.monotonic() - self.t0
                if elapsed >= self.duration:
                    break
                if elapsed >= next_report:
                    print(f"[AN] {elapsed:5.1f}s  raw={self.baseline_packets:6d} "
                          f"dev_anom={self.device_anomaly_packets:5d}")
                    next_report += 10.0
                time.sleep(0.05)
        finally:
            self.finished = True
            self.client.loop_stop()
            self.client.disconnect()

        print(f"[AN] window closed after {self.duration:.1f} s.")

    def results(self) -> list[dict]:
        rows = []
        for thr in THRESHOLDS:
            sent = self.bytes_out[thr]
            reduction = 0.0
            if self.baseline_bytes > 0:
                reduction = 100.0 * (1.0 - sent / self.baseline_bytes)
            missed = self.spikes_missed[thr]
            fnr = (100.0 * missed / self.spikes_total) if self.spikes_total else 0.0
            rows.append({
                "threshold_sigma": thr,
                "packets_sent": self.packets_out[thr],
                "bytes_sent": sent,
                "baseline_packets": self.baseline_packets,
                "baseline_bytes": self.baseline_bytes,
                "bandwidth_reduction_pct": round(reduction, 2),
                "spikes_injected": self.spikes_total,
                "spikes_missed": missed,
                "false_negative_rate_pct": round(fnr, 2),
            })
        return rows

    def print_table(self, rows: list[dict]) -> None:
        print()
        print("=" * 96)
        print(" THRESHOLD SWEEP RESULTS  (window = %.0f s)" % self.duration)
        print("=" * 96)
        print(f"{'sigma':>6} | {'packets':>8} | {'bytes':>10} | "
              f"{'reduction %':>12} | {'missed':>7} | {'FNR %':>7}")
        print("-" * 96)
        for r in rows:
            print(f"{r['threshold_sigma']:>6.1f} | {r['packets_sent']:>8d} | "
                  f"{r['bytes_sent']:>10d} | "
                  f"{r['bandwidth_reduction_pct']:>12.2f} | "
                  f"{r['spikes_missed']:>7d} | "
                  f"{r['false_negative_rate_pct']:>7.2f}")
        print("-" * 96)
        print(f" baseline: {self.baseline_packets} packets / "
              f"{self.baseline_bytes} bytes")
        print(f" injected critical events (ground truth): {self.spikes_total}")
        print(f" device-reported anomaly traffic: "
              f"{self.device_anomaly_packets} packets / "
              f"{self.device_anomaly_bytes} bytes")
        print("=" * 96)
        print()

    def save_csv(self, rows: list[dict]) -> None:
        CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        with CSV_PATH.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"[AN] CSV written -> {CSV_PATH}")

    def save_chart(self, rows: list[dict]) -> None:
        CHART_PATH.parent.mkdir(parents=True, exist_ok=True)

        sigmas     = [r["threshold_sigma"] for r in rows]
        reductions = [r["bandwidth_reduction_pct"] for r in rows]
        fnrs       = [r["false_negative_rate_pct"] for r in rows]

        fig, ax1 = plt.subplots(figsize=(10, 6))

        ax1.plot(sigmas, reductions, marker="o", linewidth=2.2,
                 color="#1f77b4", label="Bandwidth reduction (%)")
        ax1.axhline(40.0, linestyle="--", linewidth=1.2, color="#2ca02c",
                    label="Research target: 40 % reduction")
        ax1.axhline(90.0, linestyle=":", linewidth=1.2, color="#9467bd",
                    label="Stretch goal: 90 % reduction")
        ax1.set_xlabel(r"Anomaly threshold  $Z_{\mathrm{TH}}$  ($\sigma$ units)",
                       fontsize=12)
        ax1.set_ylabel("Bandwidth reduction (%)", color="#1f77b4", fontsize=12)
        ax1.tick_params(axis="y", labelcolor="#1f77b4")
        ax1.set_xticks(sigmas)
        ax1.grid(True, alpha=0.30)
        ax1.set_ylim(0, 105)

        ax2 = ax1.twinx()
        ax2.plot(sigmas, fnrs, marker="s", linewidth=2.0, linestyle="--",
                 color="#d62728", label="Missed critical events (FNR %)")
        ax2.set_ylabel("False-negative rate (%)", color="#d62728", fontsize=12)
        ax2.tick_params(axis="y", labelcolor="#d62728")
        ax2.set_ylim(0, max(10.0, max(fnrs) * 1.4 + 1.0))


        if 2.5 in sigmas:
            idx = sigmas.index(2.5)
            ax1.axvline(2.5, color="#ff7f0e", linestyle="-.", linewidth=1.4)
            ax1.annotate(
                f"Selected operating point\n"
                f"σ = 2.5  →  {reductions[idx]:.1f} % saved\n"
                f"FNR = {fnrs[idx]:.1f} %",
                xy=(2.5, reductions[idx]),
                xytext=(2.9, reductions[idx] - 28),
                fontsize=10,
                arrowprops=dict(arrowstyle="->", color="#ff7f0e"),
                bbox=dict(boxstyle="round,pad=0.35", fc="#fff7e6", ec="#ff7f0e"),
            )

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2,
                   loc="lower right", fontsize=9, framealpha=0.95)

        plt.title("Edge Z-Score Filter — Bandwidth Reduction vs. Missed Events\n"
                  "ESP32 / GPIO36 · 50-sample window · 120 s capture",
                  fontsize=12.5, fontweight="bold")
        fig.tight_layout()
        fig.savefig(CHART_PATH, dpi=200, bbox_inches="tight")
        plt.close(fig)
        print(f"[AN] chart written -> {CHART_PATH}")
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Edge-filter bandwidth analyzer")
    p.add_argument("--host", default="test.mosquitto.org")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--client-id", default="eeair-analyzer-cilan1m0")
    p.add_argument("--duration", type=float, default=120.0,
                   help="capture window in seconds (default 120)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    analyzer = BandwidthAnalyzer(args.host, args.port, args.client_id,
                                 args.duration)
    print("[AN] start the emulator (or power the ESP32) in another terminal.")
    analyzer.run()
    rows = analyzer.results()
    analyzer.print_table(rows)
    analyzer.save_csv(rows)
    analyzer.save_chart(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
