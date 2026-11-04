/**
 * @file main.cpp
 * @author Sulaimanov Baibol (GitHub: CilaNim / cilan1m0)
 * @brief Edge Anomaly Stream Filter for ESP32 NodeMCU (WROOM-32)
 * @course Electronic Components and Circuits / EEAIR
 * @date November 2026
 *
 * @details
 * Implements a statically allocated Ring Buffer (N=50) and real-time Z-Score
 * anomaly detector on an ESP32 edge node. Routine sensor telemetry is filtered
 * locally at the edge; only statistically significant anomalies (|Z| >= 2.5)
 * or initial buffer warm-up samples are serialized into JSON and transmitted
 * over Wi-Fi to an Eclipse Mosquitto MQTT broker.
 */

#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <math.h>

// 1. NETWORK & MQTT CONFIGURATION
constexpr char WIFI_SSID[]     = "EEAIR_Lab_Network";
constexpr char WIFI_PASSWORD[] = "Baibol_IoT_2026!";
constexpr char MQTT_BROKER[]   = "192.168.1.100";
constexpr uint16_t MQTT_PORT   = 1883;
constexpr char MQTT_CLIENT_ID[]= "esp32_edge_node_cilan1m0";
constexpr char MQTT_TOPIC[]    = "eeair/cilan1m0/edge/telemetry";

// 2. HARDWARE PIN DEFINITIONS & ADC CONSTANTS
constexpr uint8_t PIN_ANALOG_SENSOR = 36; // GPIO36 (VP / ADC1_CH0 - Input Only)
constexpr uint8_t PIN_STATUS_LED    = 2;  // GPIO2 (Onboard Blue LED)
constexpr float ADC_MAX_VALUE       = 4095.0f; // 12-bit SAR ADC (0 - 4095)
constexpr float ADC_REF_VOLTAGE     = 3.3f;    // Reference voltage (3.3V)

// 3. STATISTICAL FILTER HYPERPARAMETERS
constexpr size_t WINDOW_SIZE        = 50;      // Statically allocated Ring Buffer size (N=50)
constexpr float Z_THRESHOLD         = 2.5f;    // Anomaly threshold (|Z| >= 2.5 sigma)
constexpr float EPSILON             = 1e-6f;   // Guard against zero-variance division
constexpr uint32_t SAMPLE_PERIOD_MS = 100;     // 10 Hz sampling frequency (100 ms interval)
constexpr uint32_t LED_HOLD_MS      = 250;     // LED pulse duration upon anomaly detection

// 4. STATICALLY ALLOCATED RING BUFFER STRUCTURE
struct StaticRingBuffer {
    float data[WINDOW_SIZE];
    size_t head;
    size_t count;

    void init() {
        head = 0;
        count = 0;
        for (size_t i = 0; i < WINDOW_SIZE; ++i) {
            data[i] = 0.0f;
        }
    }

    void push(float sample) {
        data[head] = sample;
        head = (head + 1) % WINDOW_SIZE;
        if (count < WINDOW_SIZE) {
            count++;
        }
    }

    bool isFull() const {
        return count == WINDOW_SIZE;
    }

    /**
    Computes rolling arithmetic mean (mu) across valid samples in buffer.
    */
    float computeMean() const {
        if (count == 0) return 0.0f;
        float sum = 0.0f;
        for (size_t i = 0; i < count; ++i) {
            sum += data[i];
        }
        return sum / static_cast<float>(count);
    }

    /**
    Computes rolling population standard deviation (sigma).
    */
    float computeStdDev(float mean) const {
        if (count < 2) return 0.0f;
        float varianceSum = 0.0f;
        for (size_t i = 0; i < count; ++i) {
            float diff = data[i] - mean;
            varianceSum += (diff * diff);
        }
        return sqrtf(varianceSum / static_cast<float>(count));
    }
};

// 5. GLOBAL INSTANCES & TELEMETRY COUNTERS
WiFiClient espWiFiClient;
PubSubClient mqttClient(espWiFiClient);
StaticRingBuffer sensorBuffer;

uint32_t totalSamplesProcessed = 0;
uint32_t totalPacketsPublished = 0;
uint32_t totalPacketsDropped   = 0;
uint32_t lastSampleTimestamp   = 0;
uint32_t ledTurnOffTimestamp   = 0;
bool isLedActive               = false;

// 6. NETWORK CONNECTIVITY & RECOVERY ROUTINES
/**
Establishes Wi-Fi connection with exponential/fixed retry polling.
*/
void connectToWiFi() {
    if (WiFi.status() == WL_CONNECTED) {
        return;
    }

    Serial.println();
    Serial.print(F("[WIFI] Connecting to SSID: "));
    Serial.println(WIFI_SSID);

    WiFi.mode(WIFI_STA);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

    uint8_t attempts = 0;
    while (WiFi.status() != WL_CONNECTED && attempts < 40) {
        delay(250);
        Serial.print(F("."));
        attempts++;
    }

    if (WiFi.status() == WL_CONNECTED) {
        Serial.println();
        Serial.print(F("[WIFI] Connected! IPv4 Address: "));
        Serial.println(WiFi.localIP());
    } else {
        Serial.println();
        Serial.println(F("[WIFI] Connection attempt timed out. Will retry in loop."));
    }
}

/**
Ensures active connection to the Eclipse Mosquitto MQTT broker.
*/
void ensureMqttConnected() {
    if (WiFi.status() != WL_CONNECTED) {
        connectToWiFi();
        if (WiFi.status() != WL_CONNECTED) return;
    }

    while (!mqttClient.connected()) {
        Serial.print(F("[MQTT] Attempting connection to broker ("));
        Serial.print(MQTT_BROKER);
        Serial.print(F(":"));
        Serial.print(MQTT_PORT);
        Serial.print(F(")... "));

        if (mqttClient.connect(MQTT_CLIENT_ID)) {
            Serial.println(F("CONNECTED."));
        } else {
            Serial.print(F("FAILED, rc="));
            Serial.print(mqttClient.state());
            Serial.println(F(". Retrying in 2000 ms..."));
            delay(2000);
        }
    }
}
// 7. HARDWARE SENSOR ACQUISITION & STATISTICAL FUNCTIONS

/**
Reads 12-bit ADC value from GPIO36 and converts it to Voltage (0.0 - 3.3V).
*/
float readSensorVoltage() {
    uint16_t rawAdc = analogRead(PIN_ANALOG_SENSOR);
    return (static_cast<float>(rawAdc) / ADC_MAX_VALUE) * ADC_REF_VOLTAGE;
}

/**
Computes the standardized Z-score: Z = (x - mu) / sigma.
Uses EPSILON floor to prevent division-by-zero on constant signals.
*/
float computeZScore(float sample, float mean, float stdDev) {
    float safeStdDev = (stdDev < EPSILON) ? EPSILON : stdDev;
    return (sample - mean) / safeStdDev;
}

/**
Serializes telemetry payload into JSON and publishes via MQTT.
*/
bool publishTelemetryPayload(
    uint32_t sampleId,
    float voltage,
    float mean,
    float stdDev,
    float zScore,
    const char* statusLabel
) {
    JsonDocument doc;
    doc["node_id"]     = MQTT_CLIENT_ID;
    doc["sample_id"]   = sampleId;
    doc["timestamp"]   = millis();
    doc["voltage_v"]   = serialized(String(voltage, 4));
    doc["rolling_mu"]  = serialized(String(mean, 4));
    doc["rolling_std"] = serialized(String(stdDev, 4));
    doc["z_score"]     = serialized(String(zScore, 3));
    doc["threshold"]   = Z_THRESHOLD;
    doc["status"]      = statusLabel;
    doc["published"]   = totalPacketsPublished + 1;
    doc["dropped"]     = totalPacketsDropped;

    char payloadBuffer[320];
    size_t payloadSize = serializeJson(doc, payloadBuffer, sizeof(payloadBuffer));

    if (payloadSize == 0) {
        Serial.println(F("[ERROR] JSON serialization failed."));
        return false;
    }

    bool success = mqttClient.publish(MQTT_TOPIC, payloadBuffer, false);
    if (success) {
        totalPacketsPublished++;
    }
    return success;
}

// 8. ARDUINO LIFECYCLE: SETUP & MAIN LOOP

void setup() {
    Serial.begin(115200);
    delay(500);

    Serial.println(F("============================================================"));
    Serial.println(F(" EEAIR Coursework: Edge Anomaly Stream Filter"));
    Serial.println(F(" Author: Sulaimanov Baibol (GitHub: CilaNim / cilan1m0)"));
    Serial.println(F("============================================================"));

    // Configure GPIO pins
    pinMode(PIN_STATUS_LED, OUTPUT);
    digitalWrite(PIN_STATUS_LED, LOW);
    pinMode(PIN_ANALOG_SENSOR, INPUT);

    // Configure ESP32 12-bit SAR ADC resolution and 11dB attenuation (full 0-3.3V range)
    analogReadResolution(12);
    analogSetPinAttenuation(PIN_ANALOG_SENSOR, ADC_11db);

    // Initialize static ring buffer
    sensorBuffer.init();

    // Initialize Wi-Fi and MQTT Broker settings
    connectToWiFi();
    mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
    mqttClient.setBufferSize(512);
}

void loop() {
    // Maintain MQTT broker connection
    ensureMqttConnected();
    mqttClient.loop();

    uint32_t currentMillis = millis();

    // Non-blocking turn-off check for Anomaly Status LED (GPIO2)
    if (isLedActive && (currentMillis >= ledTurnOffTimestamp)) {
        digitalWrite(PIN_STATUS_LED, LOW);
        isLedActive = false;
    }

    // Execute periodic sensor sampling and edge filtering
    if (currentMillis - lastSampleTimestamp >= SAMPLE_PERIOD_MS) {
        lastSampleTimestamp = currentMillis;
        totalSamplesProcessed++;

        float currentVoltage = readSensorVoltage();

        // PHASE A: BUFFER WARM-UP (Samples 1 to 50)
        if (!sensorBuffer.isFull()) {
            sensorBuffer.push(currentVoltage);
            float warmMean = sensorBuffer.computeMean();
            float warmStd  = sensorBuffer.computeStdDev(warmMean);

            publishTelemetryPayload(
                totalSamplesProcessed,
                currentVoltage,
                warmMean,
                warmStd,
                0.0f,
                "WARMUP"
            );

            Serial.printf(
                "[WARMUP %02u/%02u] V=%.4fV | mu=%.4fV | sigma=%.4fV -> PUBLISHED\n",
                static_cast<unsigned int>(sensorBuffer.count),
                static_cast<unsigned int>(WINDOW_SIZE),
                currentVoltage,
                warmMean,
                warmStd
            );
            return;
        }
        // PHASE B: REAL-TIME Z-SCORE EVALUATION AGAINST HISTORICAL BASELINE
        // Compute baseline statistics prior to inserting the new observation
        // so that an extreme spike does not corrupt its own baseline mean/std.
        float rollingMean   = sensorBuffer.computeMean();
        float rollingStdDev = sensorBuffer.computeStdDev(rollingMean);
        float zScore        = computeZScore(currentVoltage, rollingMean, rollingStdDev);
        float absZScore     = fabsf(zScore);

        // Update ring buffer with current observation for subsequent window
        sensorBuffer.push(currentVoltage);

        // Evaluate dynamic anomaly condition: |Z| >= Z_THRESHOLD
        if (absZScore >= Z_THRESHOLD) {
            // Trigger GPIO2 Status LED to indicate anomaly transmission
            digitalWrite(PIN_STATUS_LED, HIGH);
            isLedActive = true;
            ledTurnOffTimestamp = currentMillis + LED_HOLD_MS;

            publishTelemetryPayload(
                totalSamplesProcessed,
                currentVoltage,
                rollingMean,
                rollingStdDev,
                zScore,
                "ANOMALY"
            );

            float savingsPct = (static_cast<float>(totalPacketsDropped) /
                                static_cast<float>(totalSamplesProcessed)) * 100.0f;

            Serial.printf(
                "[ANOMALY] Sample #%lu | V=%.4fV | mu=%.4fV | sigma=%.4fV | Z=%+.2f >= %.2f -> SENT (Saved: %.1f%%)\n",
                totalSamplesProcessed,
                currentVoltage,
                rollingMean,
                rollingStdDev,
                zScore,
                Z_THRESHOLD,
                savingsPct
            );
        } else {
            // Drop routine sample at the edge node to conserve bandwidth
            totalPacketsDropped++;

            float savingsPct = (static_cast<float>(totalPacketsDropped) /
                                static_cast<float>(totalSamplesProcessed)) * 100.0f;

            Serial.printf(
                "[FILTERED] Sample #%lu | V=%.4fV | Z=%+.2f < %.2f -> DROPPED AT EDGE (Saved: %.1f%%)\n",
                totalSamplesProcessed,
                currentVoltage,
                zScore,
                Z_THRESHOLD,
                savingsPct
            );
        }
    }
}
