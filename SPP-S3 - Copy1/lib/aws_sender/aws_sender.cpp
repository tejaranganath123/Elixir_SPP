#include "aws_sender.h"
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include "FS.h"
#include "SD.h"
#include "esp_task_wdt.h"
#include <certs.h>
#include "config.h"
#include "shared_tasks.h"
#include "status_feedback.h"

// ===== Externs =====
extern SemaphoreHandle_t sdMutex;
extern SemaphoreHandle_t wifiMutex;
extern SemaphoreHandle_t awsDrainerLock;         // <- make sure this exists in your project
extern QueueHandle_t senderQueue;                // trigger queue from RS485 task


static const char* aws_endpoint = "a1j6n3szeyh3xa-ats.iot.ap-south-1.amazonaws.com";
static const int aws_port = 8883;
static const char* thingName = "poc-001";

// ===== Helpers =====
static bool mqttPublishJSON(const String& topic, const String& payload) {
  bool ok = false;
  if (xSemaphoreTake(wifiMutex, pdMS_TO_TICKS(500)) == pdTRUE) {
    ok = client.publish(topic.c_str(), payload.c_str());
    xSemaphoreGive(wifiMutex);
  }
  return ok;
}

static void buildPayloadFromLine(const String& line, String& outPayload) {
  // line: Date,Time,KWH1, ... (total 36 values after the first two)
  int firstComma = line.indexOf(',');
  String date = line.substring(0, firstComma);
  String time = line.substring(firstComma + 1, line.indexOf(',', firstComma + 1));
  String values = line.substring(line.indexOf(',', firstComma + 1) + 1);

  int rssi = 0;
  if (xSemaphoreTake(wifiMutex, pdMS_TO_TICKS(300)) == pdTRUE) {
    rssi = WiFi.RSSI();
    xSemaphoreGive(wifiMutex);
  }

  outPayload = "{\"TS\":\"" + date + "T" + time + "\",\"DID\":\"" + String(DEVICE_UID) +
               "\",\"FID\":\"" + String(FirmwareID) + "\",\"values\":[" + String(rssi) + ",";

  for (int i = 0; i < 36; i++) {
    int commaIndex = values.indexOf(',');
    String val = (i < 35) ? values.substring(0, commaIndex) : values;
    val.trim();
    if (val == "" || val == "NaN" || val == "null") val = "N";
    outPayload += val;
    if (i < 35) outPayload += ",";
    values = (commaIndex >= 0) ? values.substring(commaIndex + 1) : "";
  }
  outPayload += "]}";
}

bool initAWS() {
  net.setCACert(rootCA);
  net.setCertificate(cert);
  net.setPrivateKey(privateKey);
  client.setServer(aws_endpoint, aws_port);
  client.setBufferSize(512); 

  uint32_t startMs = millis();
  while (!client.connected()) {
    esp_task_wdt_reset();
    
    if (client.connect(thingName)) {
      Serial.println("Connected to AWS IoT");
      esp_task_wdt_reset();
      return true;
    } else {
      Serial.print("AWS MQTT Connect failed: ");
      Serial.println(client.state());
      esp_task_wdt_reset();
      delay(2000);
    }
    // optional safety timeout (e.g., 30s)
    if (millis() - startMs > 30000UL) break;
  }
  return client.connected();
}



void awsSenderTaskprocess() {
  esp_task_wdt_reset();  // Initial feed

  // avoid clash with any drainer using the same files
  if (!xSemaphoreTake(awsDrainerLock, pdMS_TO_TICKS(100))) {
    Serial.println("[AWS Sender] Skipping — awsDrainer is running.");
    return;
  }

  bool wifiConnected = false;
  bool mqttConnected = false;

  // -------- Wi-Fi and MQTT Checks --------
  if (xSemaphoreTake(wifiMutex, portMAX_DELAY) == pdTRUE) {
    esp_task_wdt_reset();

    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[AWS Sender] WiFi disconnected.");
    } else {
      Serial.println("[AWS_SENDER FN] WiFi connected.");
      wifiConnected = true;

      if (!client.connected()) {
        Serial.println("[AWS Sender] MQTT reconnecting...");
        esp_task_wdt_reset();
        mqttConnected = initAWS();
        Serial.println(mqttConnected ? "[AWS Sender] MQTT connected." : "[AWS Sender] MQTT reconnect failed.");
      } else {
        mqttConnected = true;
      }

      // ✅ Call client.loop() only when connected
      if (mqttConnected) client.loop();
    }

    xSemaphoreGive(wifiMutex);
  } else {
    Serial.println("[AWS Sender] Could not take wifiMutex.");
  }

  bool canSend = wifiConnected && mqttConnected;

  // -------- SD access --------
  if (xSemaphoreTake(sdMutex, portMAX_DELAY) != pdTRUE) {
    Serial.println("[AWS Sender] Failed to take sdMutex.");
    xSemaphoreGive(awsDrainerLock);
    return;
  }

  // ---------- OFFLINE PATH: move/merge temp -> temp2 once, no heavy work ----------
  if (!canSend) {
  File tempFile = SD.open("/temp.csv", FILE_READ);
  if (!tempFile || tempFile.size() == 0) {
    Serial.println("[AWS Sender] temp.csv is empty or not found.");
    if (tempFile) tempFile.close();
    xSemaphoreGive(sdMutex);
    xSemaphoreGive(awsDrainerLock);
    vTaskDelay(30000 / portTICK_PERIOD_MS);
    return;
  }

    // check if temp2 is empty to decide between rename (O(1)) vs append
    File temp2Check = SD.open("/temp2.csv", FILE_READ);
    bool temp2Empty = !temp2Check || (temp2Check.size() == 0);
    if (temp2Check) temp2Check.close();

    if (temp2Empty) {
    tempFile.close();
      if (SD.rename("/temp.csv", "/temp2.csv")) {
        Serial.println("[AWS Sender] Offline: moved temp.csv -> temp2.csv");
      } else {
        // fallback append if FS doesn't support rename
        File src = SD.open("/temp.csv", FILE_READ);
        File dst = SD.open("/temp2.csv", FILE_WRITE);
        while (src && dst && src.available()) {
          String line = src.readStringUntil('\n');
          dst.println(line);
        }
        if (src) src.close();
        if (dst) dst.close();
        SD.remove("/temp.csv");
        Serial.println("[AWS Sender] Offline: appended temp.csv -> temp2.csv, removed temp.csv");
      }
    } else {
      // append then remove temp.csv
      File src = SD.open("/temp.csv", FILE_READ);
      File dst = SD.open("/temp2.csv", FILE_APPEND);
      bool headerSeen = true; // if temp2 not empty, assume header exists
      while (src && dst && src.available()) {
        String line = src.readStringUntil('\n'); line.trim();
        if (line.isEmpty()) continue;
        if (line.startsWith("Date,Time,KWH1")) {
          if (!headerSeen) { dst.println(line); headerSeen = true; }
        } else {
          dst.println(line);
        }
      }
      if (src) src.close();
      if (dst) dst.close();
      SD.remove("/temp.csv");
      Serial.println("[AWS Sender] Offline: merged temp into temp2, cleared temp.csv");
    }

    xSemaphoreGive(sdMutex);
    xSemaphoreGive(awsDrainerLock);
    vTaskDelay(30000 / portTICK_PERIOD_MS);
    return;
  }

  // ---------- ONLINE PATH: send temp.csv then drain temp2.csv ----------
  File sentFile = SD.open("/sent.csv", FILE_APPEND);
  if (!sentFile) {
    Serial.println("[AWS Sender] Could not open sent.csv");
    xSemaphoreGive(sdMutex);
    xSemaphoreGive(awsDrainerLock);
    vTaskDelay(3000 / portTICK_PERIOD_MS);
    return;
  }

  // 1) send fresh data from temp.csv (if any)
  {
    File tempFile = SD.open("/temp.csv", FILE_READ);
    File temp2Append = SD.open("/temp2.csv", FILE_APPEND); // failures go here

    if (tempFile) {
  while (tempFile.available()) {
    esp_task_wdt_reset();

    String line = tempFile.readStringUntil('\n');
    line.trim();
    if (line.isEmpty()) continue;
        if (line.startsWith("Date,Time,KWH1")) continue; // skip header

        String payload;
        buildPayloadFromLine(line, payload);

        Serial.println("---- Payload (temp) ----");
    Serial.println(payload);
        Serial.print("Payload size: "); Serial.println(payload.length());
        Serial.println("------------------------");

        bool ok = mqttPublishJSON("esp32/topic", payload);
        if (ok) {
          Serial.println("Sent (temp) → " + payload);
      setSystemState(SYS_DATA_SENT);
      sentFile.println(line);
      sentFile.flush();
      setSystemState(SYS_NORMAL);
    } else {
          Serial.println("Retry Later (temp) → " + payload);
          if (temp2Append) temp2Append.println(line);
      setSystemState(SYS_NORMAL);
          // optional: break early on failure to avoid thrashing
          break;
    }

    delay(300);  // throttle MQTT bursts
  }
  tempFile.close();
      SD.remove("/temp.csv"); // clear fresh buffer
    }

    if (temp2Append) temp2Append.close();
  }

  // 2) drain backlog from temp2.csv using swap file to keep leftovers
  {
    File backlog = SD.open("/temp2.csv", FILE_READ);
    if (backlog && backlog.size() > 0) {
      File temp2New = SD.open("/temp2_new.csv", FILE_WRITE); // leftovers and header (once)
    bool headerWritten = false;

      while (backlog.available()) {
        esp_task_wdt_reset();

        String line = backlog.readStringUntil('\n');
        line.trim();
        if (line.isEmpty()) continue;

        if (line.startsWith("Date,Time,KWH1")) {
          if (!headerWritten) {
            temp2New.println(line);
            headerWritten = true;
          }
          continue;
        }

        String payload;
        buildPayloadFromLine(line, payload);

        Serial.println("---- Payload (backlog) ----");
        Serial.println(payload);
        Serial.print("Payload size: "); Serial.println(payload.length());
        Serial.println("---------------------------");

        bool ok = mqttPublishJSON("esp32/topic", payload);
        if (ok) {
          Serial.println("Sent (backlog) → " + payload);
          setSystemState(SYS_DATA_SENT);
          sentFile.println(line);
          sentFile.flush();
          setSystemState(SYS_NORMAL);
        } else {
          Serial.println("Keep (backlog) → " + payload);
          temp2New.println(line);   // keep this line for next time
          setSystemState(SYS_NORMAL);
          // optional: break; // if you want to stop on first failure
        }

        delay(300);
      }

      backlog.close();
      if (temp2New) temp2New.close();

      // swap the new backlog into place (atomic-ish)
      SD.remove("/temp2.csv");
      SD.rename("/temp2_new.csv", "/temp2.csv");
    } else {
      if (backlog) backlog.close();
      // if file existed but was empty, you can truncate anyway
      // SD.remove("/temp2.csv");
    }
    }

  if (sentFile) sentFile.close();

  xSemaphoreGive(sdMutex);
  xSemaphoreGive(awsDrainerLock);
  Serial.println("[AWS SENDER] : Done. No rebuild step.");
  vTaskDelay(3000 / portTICK_PERIOD_MS);
}



void awsSenderTask(void *parameter) {
  while (true) {
    //ulTaskNotifyTake(pdTRUE, portMAX_DELAY); //Wait for Modbus task to trigger
    int sendTrigger;
    if (xQueueReceive(senderQueue, &sendTrigger, portMAX_DELAY) == pdTRUE) {
      esp_task_wdt_reset();
      Serial.println("[awsSenderTask] Received trigger from RS485 task");
    Serial.println("[AWS Task] Notified, sending data...");
    esp_task_wdt_reset();
    awsSenderTaskprocess();
    esp_task_wdt_reset();
    }
  }
}

