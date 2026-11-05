#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <math.h>

static const char* WIFI_SSID     = "Wokwi-GUEST";
static const char* WIFI_PASSWORD = "";
static const char*    MQTT_HOST      = "test.mosquitto.org";
static const uint16_t MQTT_PORT      = 1883;
static const char*    MQTT_CLIENT_ID = "esp32-edge-node-cilan1m0";

static const char* TOPIC_RAW     = "eeair/telemetry/raw";     
static const char* TOPIC_ANOMALY = "eeair/telemetry/anomaly"; 
static const char* TOPIC_STATUS  = "eeair/telemetry/status";  
static const uint8_t PIN_ADC = 36;  
static const uint8_t PIN_LED = 2; 
static const float ADC_VREF      = 3.3f;   
static const float ADC_MAX_COUNT = 4095.0f; 
static const uint16_t RING_SIZE     = 50;     
static const float    Z_THRESHOLD   = 2.5f;    
static const float    SIGMA_EPSILON = 0.0001f; 
static const uint32_t SAMPLE_PERIOD_MS = 20; 
static const uint32_t LED_PULSE_MS     = 150; 
static const uint32_t WIFI_RETRY_MS    = 5000;
static const uint32_t MQTT_RETRY_MS    = 5000;
struct RingBuffer {
  float    data[RING_SIZE];  
  uint16_t head  = 0;      
  uint16_t count = 0;       
  void push(float value) {
    data[head] = value;
    head = (uint16_t)((head + 1u) % RING_SIZE);
    if (count < RING_SIZE) {
      ++count;
    }
  }
  float at(uint16_t i) const {
    const uint16_t start = (count < RING_SIZE) ? 0u : head;
    return data[(uint16_t)((start + i) % RING_SIZE)];
  }

  bool full() const { return count == RING_SIZE; }
  void clear() { head = 0; count = 0; }
};
static RingBuffer       g_ring;
static WiFiClient       g_wifiClient;
static PubSubClient     g_mqtt(g_wifiClient);

static uint32_t g_lastSampleMs = 0;
static uint32_t g_lastWifiMs   = 0;
static uint32_t g_lastMqttMs   = 0;
static uint32_t g_ledOffAt     = 0;

static uint32_t g_txRawFrames     = 0;
static uint32_t g_txAnomalyFrames = 0;
static uint32_t g_txBytes         = 0;


static void computeStats(const RingBuffer& rb, float& mean, float& sigma) {
  if (rb.count == 0) { mean = 0.0f; sigma = 0.0f; return; }

  float sum = 0.0f;
  for (uint16_t i = 0; i < rb.count; ++i) sum += rb.at(i);
  mean = sum / (float)rb.count;

  if (rb.count < 2) { sigma = 0.0f; return; }

  float acc = 0.0f;
  for (uint16_t i = 0; i < rb.count; ++i) {
    const float d = rb.at(i) - mean;
    acc += d * d;
  }
  sigma = sqrtf(acc / (float)(rb.count - 1));  
}

static void ensureWifi() {
  if (WiFi.status() == WL_CONNECTED) return;

  const uint32_t now = millis();
  if (now - g_lastWifiMs < WIFI_RETRY_MS) return;
  g_lastWifiMs = now;

  Serial.printf("[WIFI] connecting to \"%s\" ...\n", WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
}

static void ensureMqtt() {
  if (WiFi.status() != WL_CONNECTED) return;
  if (g_mqtt.connected()) return;

  const uint32_t now = millis();
  if (now - g_lastMqttMs < MQTT_RETRY_MS) return;
  g_lastMqttMs = now;

  Serial.printf("[MQTT] connecting to %s:%u ...\n", MQTT_HOST, MQTT_PORT);
  const bool ok = g_mqtt.connect(
      MQTT_CLIENT_ID,
      TOPIC_STATUS, 1, true, "offline");

  if (ok) {
    Serial.println("[MQTT] connected.");
    g_mqtt.publish(TOPIC_STATUS, "online", true);
    g_mqtt.publish(TOPIC_STATUS,
                   "{\"node\":\"esp32-edge\",\"filter\":\"zscore\","
                   "\"n\":50,\"z\":2.50,\"state\":\"boot\"}",
                   true);
  } else {
    Serial.printf("[MQTT] failed, rc=%d\n", g_mqtt.state());
  }
}
static void publishRaw(uint32_t tMs, float voltage) {
  StaticJsonDocument<128> doc;
  doc["t"] = tMs;
  doc["v"] = serialized(String(voltage, 4));
  doc["src"] = "esp32-warmup";

  char payload[128];
  const size_t len = serializeJson(doc, payload, sizeof(payload));

  if (g_mqtt.publish(TOPIC_RAW, payload, len)) {
    ++g_txRawFrames;
    g_txBytes += len;
  }
}
static void publishAnomaly(uint32_t tMs, float voltage,
                           float mean, float sigma, float z) {
  StaticJsonDocument<192> doc;
  doc["t"] = tMs;
  doc["v"] = serialized(String(voltage, 4));
  doc["z"] = serialized(String(z, 2));
  doc["m"] = serialized(String(mean, 4));
  doc["s"] = serialized(String(sigma, 4));

  char payload[192];
  const size_t len = serializeJson(doc, payload, sizeof(payload));

  if (g_mqtt.publish(TOPIC_ANOMALY, payload, len)) {
    ++g_txAnomalyFrames;
    g_txBytes += len;
    Serial.printf("[ANOMALY] v=%.4f mu=%.4f sd=%.4f z=%+.2f\n",
                  voltage, mean, sigma, z);
  }
}
static void serviceLed() {
  if (g_ledOffAt != 0 && (int32_t)(millis() - g_ledOffAt) >= 0) {
    digitalWrite(PIN_LED, LOW);
    g_ledOffAt = 0;
  }
}

static void pulseLed() {
  digitalWrite(PIN_LED, HIGH);
  g_ledOffAt = millis() + LED_PULSE_MS;
}
void setup() {
  Serial.begin(115200);
  delay(200);

  pinMode(PIN_LED, OUTPUT);
  digitalWrite(PIN_LED, LOW);
  analogReadResolution(12);
  analogSetWidth(12);
  analogSetPinAttenuation(PIN_ADC, ADC_11db);

  g_ring.clear();
  g_mqtt.setServer(MQTT_HOST, MQTT_PORT);
  g_mqtt.setKeepAlive(30);
  g_mqtt.setBufferSize(512);

  Serial.println();
  Serial.println("==============================================");
  Serial.println(" Edge Anomaly Stream Filter  |  EEAIR 2026");
  Serial.println(" Sulaimanov Baibol  (CilaNim / cilan1m0)");
  Serial.println("==============================================");
  Serial.printf(" N=%u  Z_THRESHOLD=%.2f  fs=%.0f Hz\n",
                RING_SIZE, (double)Z_THRESHOLD, 1000.0 / SAMPLE_PERIOD_MS);

  ensureWifi();
}
void loop() {
  ensureWifi();
  ensureMqtt();
  if (g_mqtt.connected()) g_mqtt.loop();
  serviceLed();
  const uint32_t now = millis();
  if (now - g_lastSampleMs < SAMPLE_PERIOD_MS) return;
  g_lastSampleMs = now;
  const int   rawCount = analogRead(PIN_ADC);              
  const float voltage  = (rawCount * ADC_VREF) / ADC_MAX_COUNT; 
  g_ring.push(voltage);

  if (!g_mqtt.connected()) return;   
  if (!g_ring.full()) {
    publishRaw(now, voltage);
    return;
  }
  float mean = 0.0f, sigma = 0.0f;
  computeStats(g_ring, mean, sigma);
  if (sigma < SIGMA_EPSILON) {
    return;
  }
  const float z = (voltage - mean) / sigma;

  if (fabsf(z) >= Z_THRESHOLD) {
    publishAnomaly(now, voltage, mean, sigma, z);
    pulseLed();           
  }
  static uint32_t lastReportMs = 0;
  if (now - lastReportMs >= 10000) {
    lastReportMs = now;
    Serial.printf("[STAT] win=%u/%u  mu=%.4f sd=%.4f  raw=%lu anom=%lu bytes=%lu\n",
                  g_ring.count, RING_SIZE, mean, sigma,
                  (unsigned long)g_txRawFrames,
                  (unsigned long)g_txAnomalyFrames,
                  (unsigned long)g_txBytes);
  }
}
