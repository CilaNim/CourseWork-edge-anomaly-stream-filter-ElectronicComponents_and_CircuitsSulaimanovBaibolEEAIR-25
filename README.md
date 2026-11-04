# CourseWork-edge-anomaly-stream-filter-ElectronicComponents_and_CircuitsSulaimanovBaibolEEAIR-25
# Edge Anomaly Stream Filter: Real-Time Z-Score Telemetry Filtering on ESP32

![Course](https://img.shields.io/badge/Course-EEAIR%20%2F%20Electronic%20Components-0052CC?style=for-the-badge)
![Hardware](https://img.shields.io/badge/Hardware-ESP32%20WROOM--32-E7352C?style=for-the-badge&logo=espressif)
![Firmware](https://img.shields.io/badge/Firmware-C%2B%2B17%20%2F%20PlatformIO-00599C?style=for-the-badge&logo=cplusplus)
![Analytics](https://img.shields.io/badge/Analytics-Python%203.10-3776AB?style=for-the-badge&logo=python)
![Protocol](https://img.shields.io/badge/Protocol-MQTT%20%2F%20Mosquitto-660066?style=for-the-badge&logo=eclipsemosquitto)
![License](https://img.shields.io/badge/License-MIT-2ea44f?style=for-the-badge)

**Author:** Sulaimanov Baibol ([@CilaNim](https://github.com/CilaNim) / `cilan1m0`)  
**Course:** Electronic Components and Circuits / EEAIR  
**Submission Deadline:** November 23, 2026  
**Research Question:** *"What streaming anomaly threshold reduces cloud bandwidth by >40% without losing critical events?"*

---

## 1. Project Overview & Architectural Motivation

In high-frequency Industrial IoT (IIoT) deployments, streaming raw analog sensor telemetry directly to cloud ingestion pipelines saturates wireless links, increases broker CPU overhead, and depletes edge battery reserves. In practice, over 90% of continuous sensor observations represent stationary or quasi-stationary baseline behavior.

The **Edge Anomaly Stream Filter** solves this bottleneck by embedding a lightweight, non-parametric statistical filter directly inside the microcontroller's SRAM:
1. **Static Memory Safety:** Uses a statically allocated Ring Buffer ($N = 50$ floats, $200\text{ bytes}$ in BSS) on the ESP32 to compute rolling statistics without `malloc()`/`new` heap fragmentation.
2. **Real-Time Statistical Evaluation:** Computes the rolling arithmetic mean ($\mu$), population standard deviation ($\sigma$), and standardized Z-Score ($Z = \frac{x - \mu}{\sigma}$) in $O(N)$ deterministic time at a $10\text{ Hz}$ sampling rate.
3. **Selective MQTT Transmission:** Routine samples ($\vert{}Z\vert{} < \text{Z\_THRESHOLD}$) are discarded directly at the edge node. Only statistically significant anomalies ($\vert{}Z\vert{} \ge 2.5\sigma$) and initial warm-up samples ($n \le 50$) are serialized via `ArduinoJson` and published over Wi-Fi to an Eclipse Mosquitto MQTT broker.

---

## 2. System Dataflow Architecture

```mermaid
flowchart LR
    subgraph Physical_Layer ["Physical Layer (Analog Front-End)"]
        A["Analog Sensor / Potentiometer\n(0.0V - 3.3V Signal + Spikes)"]
    end

    subgraph Edge_Node ["ESP32 NodeMCU (WROOM-32 Edge Node)"]
        B["12-Bit SAR ADC\nGPIO36 (VP / ADC1_CH0)"]
        C["Static Ring Buffer\n(Window Size N = 50)"]
        D["Real-Time Z-Score Engine\nZ = (x - μ) / σ"]
        E{"Is Warm-Up (n < 50)\nOR |Z| ≥ 2.5σ ?"}
        F["Drop Packet at Edge\n(Increment Local Counter)"]
        G["Trigger GPIO2 Status LED\n& Serialize JSON Payload"]
    end

    subgraph Network_Broker ["Local / Cloud Network"]
        H["Eclipse Mosquitto\nMQTT Broker (:1883)"]
        I["Cloud Analytics & Storage\n(bandwidth_analyzer.py)"]
    end

    A -->|"Continuous Voltage"| B
    B -->|"10 Hz Sample"| D
    C <-->|"Rolling μ, σ"| D
    D --> E
    E -->|"No (Routine Telemetry)"| F
    E -->|"Yes (Anomaly / Warm-Up)"| G
    G -->|"MQTT Publish (QoS 0)"| H
    H -->|"Subscribed Topic"| I
