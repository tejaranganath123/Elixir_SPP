#include "fileMonitorTask.h"
#include <Arduino.h>
#include "FS.h"
#include "SD.h"
#include "SPI.h"
#include "esp_task_wdt.h"
#include "config.h"
extern SemaphoreHandle_t sdMutex;


#define SENT_FILE_PATH "/sent.csv"
#define MAX_FILE_SIZE 2000000 * 1024  // 2000000 KB || 2GB
#define FILE_CHECK_INTERVAL 60000 // Check every 60s

void fileMonitorTaskprocess(){
  esp_task_wdt_reset();  // Feed watchdog
    if (SD.begin()) {
      xSemaphoreTake(sdMutex, portMAX_DELAY);
      File file = SD.open(SENT_FILE_PATH, FILE_READ);
      File file2 = SD.open("/temp2.csv");
      if (file) {
        size_t fileSize = file.size();
        file.close();
        esp_task_wdt_reset();  // Feed watchdog

        if (fileSize > MAX_FILE_SIZE) {
          Serial.printf("[FileMonitorTask] %s size = %d > %d. Deleting...\n", SENT_FILE_PATH, fileSize, MAX_FILE_SIZE);
          SD.remove(SENT_FILE_PATH);
          xSemaphoreGive(sdMutex);  // ✅ Always release
          esp_task_wdt_reset(); 
        } else {
          Serial.printf("[FileMonitorTask] %s size OK: %d bytes\n", SENT_FILE_PATH, fileSize);
          xSemaphoreGive(sdMutex);  // ✅ Always release
          esp_task_wdt_reset(); 
        }
      } 
      if (file2) {
        size_t file2Size = file2.size();
        file2.close();
        esp_task_wdt_reset();
        Serial.printf("[FileMonitorTask] temp2 size: %d bytes\n", file2Size);

      }
      else {
        Serial.println("[FileMonitorTask] sent.csv not found.");
        xSemaphoreGive(sdMutex);  // ✅ Always release
        esp_task_wdt_reset(); 
      }
      
    } 
    
    else {
      Serial.println("[FileMonitorTask] SD card not initialized.");
      xSemaphoreGive(sdMutex);  // ✅ Always release
      esp_task_wdt_reset(); 
    }
     esp_task_wdt_reset();  // Final watchdog reset (optional here)
}

void fileMonitorTask(void *parameter) {
  while (true) {
    if (otainprogress) {
      Serial.println("OTA started, awsDrainerTask exiting...");
      xSemaphoreGive(sdMutex);
      vTaskDelete(NULL);  // ✅ Kill itself
    }
    fileMonitorTaskprocess();
    esp_task_wdt_reset(); 
    vTaskDelay(FILE_CHECK_INTERVAL / portTICK_PERIOD_MS);
  }
}
