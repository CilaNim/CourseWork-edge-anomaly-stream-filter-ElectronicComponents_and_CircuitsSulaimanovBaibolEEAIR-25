<h1 align="center">🛰️ Edge Anomaly Stream Filter</h1>

<p align="center">
  <em>Filtering bad sensor telemetry at the edge node before it reaches the cloud.</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/course-EEAIR%20·%20Electronic%20Components%20%26%20Circuits-1f6feb?style=flat-square" alt="course">
  <img src="https://img.shields.io/badge/student-Sulaimanov%20Baibol-8957e5?style=flat-square" alt="student">
  <img src="https://img.shields.io/badge/handle-CilaNim%20·%20cilan1m0-2ea043?style=flat-square" alt="handle">
  <img src="https://img.shields.io/badge/ESP32-WROOM--32-E7352C?style=flat-square&logo=espressif&logoColor=white" alt="esp32">
  <img src="https://img.shields.io/badge/C%2B%2B-Arduino%20Framework-00599C?style=flat-square&logo=cplusplus&logoColor=white" alt="cpp">
  <img src="https://img.shields.io/badge/Python-3.10-3776AB?style=flat-square&logo=python&logoColor=white" alt="python">
  <img src="https://img.shields.io/badge/MQTT-Mosquitto-660066?style=flat-square&logo=eclipsemosquitto&logoColor=white" alt="mqtt">
  <img src="https://img.shields.io/badge/simulator-Wokwi-FF6B35?style=flat-square" alt="wokwi">
  <img src="https://img.shields.io/badge/bandwidth%20saved-90.3%25-brightgreen?style=flat-square" alt="bandwidth">
  <img src="https://img.shields.io/badge/missed%20events-0.0%25-success?style=flat-square" alt="fnr">
</p>

---

## 🔬 Research Question

> **"What streaming anomaly threshold reduces cloud bandwidth by >40 %
> without losing critical events?"**

**Answer established by this project:** the streaming Z-score threshold
**σ = 2.5** reduces MQTT telemetry bandwidth by **90.3 %** while detecting
**100 %** of the injected critical events (false-negative rate **0.0 %**)
on a 120-second capture at 50 Hz.

---

## 🧭 Operational Overview

A cheap analog sensor is sampled by an ESP32 at **50 Hz**. A **50-sample static
ring buffer** maintains a rolling statistical window. For every new sample the
node computes the window **mean (μ)** and **sample standard deviation (σ)** and
the **Z-score**

$$
Z = \frac{x - \mu}{\sigma}
$$

The frame is **published over MQTT only when |Z| ≥ 2.5**. Nominal samples are
discarded *on the microcontroller* — they never consume a TCP segment, never
consume broker storage, and never consume cloud egress. During the 50-sample
warm-up the node streams raw frames so the cloud keeps full observability while
the statistics converge.

---

## 🗺️ System Dataflow

```mermaid
flowchart LR
    A["Analog Sensor<br/>Voltage Divider<br/>0.0 – 3.3 V"] --> B["ESP32 ADC<br/>GPIO36 · 12-bit<br/>0 … 4095"]
    B --> C["Static Ring Buffer<br/>N = 50<br/>μ · σ · Z"]
    C -->|"nominal sample<br/>Z below threshold"| D["DROPPED AT EDGE<br/>(never leaves the node)"]
    C -->|"anomaly sample<br/>Z at or above threshold"| E["MQTT PUBLISH<br/>eeair/telemetry/anomaly"]
    C --> F["GPIO2 LED<br/>220 Ω"]
    E --> G["Eclipse Mosquitto<br/>Broker"]
    G --> H["Cloud Storage<br/>Dashboard / DB"]

    style D fill:#ffe6e6,stroke:#d62728,stroke-width:2px
    style E fill:#e6ffe6,stroke:#2ea043,stroke-width:2px
    style C fill:#e6f0ff,stroke:#1f6feb,stroke-width:2px
