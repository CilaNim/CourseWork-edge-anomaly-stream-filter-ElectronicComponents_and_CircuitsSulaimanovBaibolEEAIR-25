"""
sensor_emulator.py
==================
Synthetic analog telemetry source for the "Edge Anomaly Stream Filter" project.

Signal model (physically motivated):
    V(t) = V_offset + A * sin(2*pi*f*t)  +  n(t)  +  s(t)

    V_offset = 1.65 V     mid-rail bias of the voltage divider
    A        = 0.35 V     slow physical baseline swing (e.g. thermal drift)
    f        = 0.5 Hz     baseline oscillation frequency
    n(t)     ~ N(0, 0.015 V)   Gaussian thermal (Johnson-Nyquist) noise
    s(t)     = 0 normally; with p = 5 % a random-polarity spike of magnitude
               U(0.35, 1.60) V is injected  ->  a "critical event"

Each frame is published to  eeair/telemetry/raw  as:
    {"t": <ms>, "v": <float>, "gt": <0|1>}
where `gt` is the ground-truth anomaly flag used by the bandwidth analyzer to
compute the false-negative rate. It is NOT part of the edge device's payload.

Course : Electronic Components and Circuits (EEAIR)
Author : Sulaimanov Baibol (CilaNim / cilan1m0)
"""

from __future__ import annotations

import argparse
import json
import math
import random
import signal
import sys
import time

import paho.mqtt.client as mqtt

TOPIC_RAW = "eeair/telemetry/raw"

def make_client(client_id: str) -> mqtt.Client:
    try:                                   # paho-mqtt >= 2.0
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=client_id)
    except AttributeError:                 # paho-mqtt 1.x
        return mqtt.Client(client_id=client_id)

class SensorModel:
    """Generates the analog telemetry stream described in the module docstring."""

    def __init__(
        self,
        offset: float = 1.65,
        amplitude: float = 0.35,
        frequency: float = 0.5,
        noise_sigma: float = 0.015,
        spike_probability: float = 0.05,
        spike_min: float = 0.35,
        spike_max: float = 1.60,
        v_min: float = 0.0,
        v_max: float = 3.3,
        seed: int | None = 2026,
    ) -> None:
        self.offset = offset
        self.amplitude = amplitude
        self.frequency = frequency
        self.noise_sigma = noise_sigma
        self.spike_probability = spike_probability
        self.spike_min = spike_min
        self.spike_max = spike_max
        self.v_min = v_min
        self.v_max = v_max
        self.rng = random.Random(seed)

    def sample(self, t_seconds: float) -> tuple[float, bool]:
        """Return (voltage, is_ground_truth_anomaly) for time t_seconds."""
        baseline = self.offset + self.amplitude * math.sin(
            2.0 * math.pi * self.frequency * t_seconds
        )
        value = baseline + self.rng.gauss(0.0, self.noise_sigma)

        is_anomaly = self.rng.random() < self.spike_probability
        if is_anomaly:
            magnitude = self.rng.uniform(self.spike_min, self.spike_max)
            polarity = 1.0 if self.rng.random() < 0.5 else -1.0
            value += polarity * magnitude

        value = min(max(value, self.v_min), self.v_max)   
        return value, is_anomaly

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MQTT analog sensor emulator")
    p.add_argument("--host", default="test.mosquitto.org")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--client-id", default="eeair-sensor-emulator-cilan1m0")
    p.add_argument("--topic", default=TOPIC_RAW)
    p.add_argument("--rate", type=float, default=50.0, help="samples per second")
    p.add_argument("--duration", type=float, default=0.0,
                   help="seconds to run; 0 = until Ctrl+C")
    p.add_argument("--seed", type=int, default=2026)
    return p.parse_args()

def main() -> int:
    args = parse_args()
    model = SensorModel(seed=args.seed)

    client = make_client(args.client_id)
    print(f"[EMU] connecting to {args.host}:{args.port} ...")
    client.connect(args.host, args.port, keepalive=30)
    client.loop_start()

    stop = {"flag": False}

    def _handler(signum, frame):    
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handler)

    period = 1.0 / args.rate
    t0 = time.perf_counter()
    next_tick = t0
    published = 0
    spikes = 0

    print(f"[EMU] streaming at {args.rate:.1f} Hz -> topic '{args.topic}'")

    try:
        while not stop["flag"]:
            now = time.perf_counter()
            elapsed = now - t0

            if args.duration > 0.0 and elapsed >= args.duration:
                break

            value, is_anomaly = model.sample(elapsed)
            spikes += int(is_anomaly)

            payload = json.dumps(
                {"t": int(elapsed * 1000.0),
                 "v": round(value, 4),
                 "gt": int(is_anomaly)},
                separators=(",", ":"),
            )
            client.publish(args.topic, payload, qos=0, retain=False)
            published += 1

            if published % int(args.rate * 5) == 0:
                print(f"[EMU] t={elapsed:6.1f}s  frames={published:6d}  "
                      f"spikes={spikes:4d}  last={value:.4f} V")

            next_tick += period
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_tick = time.perf_counter()   
    finally:
        client.loop_stop()
        client.disconnect()

    print(f"[EMU] finished. frames={published}  injected spikes={spikes}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
