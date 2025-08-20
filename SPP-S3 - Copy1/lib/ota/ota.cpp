#include "ota.h"
#include <WiFi.h>
#include <HTTPClient.h>
#include <HTTPUpdate.h>
#include <Arduino.h>
#include <WiFiClientSecure.h>
#include <ArduinoJson.h>
#include "config.h"
#include "shared_tasks.h"
#include "esp_task_wdt.h"

extern SemaphoreHandle_t wifiMutex;
extern SemaphoreHandle_t nvsMutex;



Preferences prefs;

// Firmware version and OTA state
const char* FirmwareVer = "1.0001-rc1";
bool isUpdateAvailable = false;
String ota_url = "";

// API URL to check for OTA update
String check_url = "http://3.7.222.84:9610/api/v1/ota/check?id=sppa00001&version=1.0001-rc1";

// Extern task handles
extern TaskHandle_t WiFiResetTaskHandle ;
extern TaskHandle_t SchedularTaskHandle ;
extern TaskHandle_t awsDrainerTaskHandle;

// Suspend all system tasks before OTA
void suspendAllTasksBeforeOTA() {
   if (readRS485TaskHandle) vTaskSuspend(readRS485TaskHandle);
   if (awsSenderTaskHandle) vTaskSuspend(awsSenderTaskHandle);
   //vTaskSuspend(awsDrainerTaskHandle);
   //if (FileMonitorTaskHandle) vTaskSuspend(FileMonitorTaskHandle);
    
}

// Resume tasks if OTA fails or not needed
void resumeAllTasksAfterOTA() {
    vTaskResume(readRS485TaskHandle);
    vTaskResume(awsSenderTaskHandle);
    //vTaskResume(awsDrainerTaskHandle);
    //vTaskResume(FileMonitorTaskHandle);    
}

// Check firmware version via API
bool checkFirmwareVersion() {
    //if (isUpdateAvailable) return true;
    xSemaphoreTake(wifiMutex, portMAX_DELAY);
    HTTPClient http;
    http.begin(check_url.c_str());
    int httpCode = http.GET();

    if (httpCode > 0) {
        String payload = http.getString();
        StaticJsonDocument<512> doc;
        DeserializationError error = deserializeJson(doc, payload);

        if (error) {
            Serial.printf("JSON parse error: %s\n", error.c_str());
            http.end();
            return false;
        }

        bool status = doc["status"];
        const char* link = doc["link"];
        int duration = doc["interval"].as<int>();
        Serial.printf("OTA Check Status: %d | Link: %s | Interval: %d ms\n", status, link, duration);
        xSemaphoreGive(wifiMutex);  // ✅ release after GET
        // -------- Write to NVS if interval is valid --------
        if (duration>0){
            xSemaphoreTake(nvsMutex, portMAX_DELAY);
              if (prefs.begin("spp", false)) {

                int val = prefs.getInt("read_delay", 5000); // default fallback
                prefs.end();
                xSemaphoreGive(nvsMutex);
                } else {
                    Serial.println("Failed to open NVS namespace 'spp'");
                    xSemaphoreGive(nvsMutex);
                }
              Serial.printf("Saved sensor_read_delay to prefs: %d ms\n", duration);
              xSemaphoreGive(nvsMutex);
        }

        if (status && link) {
            ota_url = String(link);
            isUpdateAvailable = true;
            http.end();
            xSemaphoreGive(wifiMutex);  // ✅ always release WiFi mutex
            return true;
            
        } else {
            Serial.println("No update available.");
            xSemaphoreGive(wifiMutex);  // ✅ always release WiFi mutex
            return false;
            
        }
    } 
    else {
        Serial.printf("HTTP GET failed: %s\n", http.errorToString(httpCode).c_str());
        xSemaphoreGive(wifiMutex);  // ✅ always release WiFi mutex
        return false;
    }

    http.end();
    return false;
}

void stopAllTasksForOTA() {
  Serial.println("🔁 Stopping all tasks for OTA...");

  // Step 1: Release all known mutexes
  xSemaphoreGive(sdMutex);
  xSemaphoreGive(wifiMutex);
  xSemaphoreGive(nvsMutex);

  // Step 2: Delete WDT from all known problematic tasks
  esp_task_wdt_delete(awsDrainerTaskHandle);
  esp_task_wdt_delete(FileMonitorTaskHandle);
  esp_task_wdt_delete(WiFiResetTaskHandle);

  // Step 3: Suspend or kill tasks
  if (awsDrainerTaskHandle) {
    vTaskDelete(awsDrainerTaskHandle);
    awsDrainerTaskHandle = NULL;
    Serial.println("awsDrainerTaskHandle killed");
  }

  if (FileMonitorTaskHandle) {
    vTaskDelete(FileMonitorTaskHandle);
    FileMonitorTaskHandle = NULL;
    Serial.println("FileMonitorTaskHandle killed");
  }

  if (readRS485TaskHandle) {
    vTaskDelete(readRS485TaskHandle);
    readRS485TaskHandle = NULL;
    Serial.println("readRS485TaskHandle killed");
  }

  Serial.println("✅ All tasks stopped, mutexes released, proceeding to OTA...");
}


// Perform OTA update
void firmwareUpdate() {
    Serial.println("Starting OTA update...1");
    //suspendAllTasksBeforeOTA();
    Serial.println("Suspended all tasks before ota");
    stopAllTasksForOTA();
    WiFiClientSecure client;
    client.setInsecure();
    otainprogress = true ;
    Serial.println("Starting OTA update...2");
    t_httpUpdate_return ret = httpUpdate.update(client, ota_url);
    
    switch (ret) {
        case HTTP_UPDATE_FAILED:
            esp_task_wdt_reset();
            esp_task_wdt_add(NULL);     // Restore watchdog
            Serial.printf("OTA FAILED (%d): %s\n", httpUpdate.getLastError(), httpUpdate.getLastErrorString().c_str());
            resumeAllTasksAfterOTA();
            break;

        case HTTP_UPDATE_NO_UPDATES:
            esp_task_wdt_reset();
            esp_task_wdt_add(NULL);     // Restore watchdog
            Serial.println("No OTA update available.");
            resumeAllTasksAfterOTA();
            break;

        case HTTP_UPDATE_OK:
            esp_task_wdt_reset();
            esp_task_wdt_add(NULL);     // Restore watchdog
            Serial.println("OTA update complete. Rebooting...");
            break;
    }
}

void checkforotamanual(){
    if (checkFirmwareVersion()) {
        firmwareUpdate();
    } else {
        Serial.println("No update required.");
    }
}

void otaTaskprocess(){
    Serial.println("ota task run");
    esp_task_wdt_reset();
    if (checkFirmwareVersion()) {
        firmwareUpdate();
    } else {
        Serial.println("No update required.");
    }
}

// Task wrapper
void otaTask(void* parameter) {
     while (true) {
    struct tm timeinfo;
    Serial.println("[OTA TASK] Running the OTA task for check");
    if (getLocalTime(&timeinfo)) {
      if (timeinfo.tm_hour == 3 && timeinfo.tm_min <= 9) { // runs at only b/w  3:00 to 3:09 AM
        otaTaskprocess();
      }
      else{
        Serial.print("not the time to run ota");
      }
    }
    vTaskDelay(pdMS_TO_TICKS(60000*3));  // Check every 3 minute
  }
}
