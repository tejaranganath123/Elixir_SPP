#include <Arduino.h>
#include <time.h>
#include "Preferences.h"
#include "modbus_task.h"
#include "aws_sender.h"
#include "fileMonitorTask.h"
#include "mutexWatchdog.h"
#include "ota.h"
#include <vector>
#include "esp_task_wdt.h"

// --- Global Sync Flags ---
int lastOTADay = -1;
bool otaDoneToday = false;

// --- Dynamic read delay ---
int getReadDelay() {
  Preferences prefs;
  prefs.begin("spp", true); // read-only
  int delay = prefs.getInt("read_delay", 1); // default 1 minute
  prefs.end();
  return delay;
}

uint8_t slaveIDs[] = {1, 2};

// --- Scheduler Task ---
void SchedulerTask(void *parameter) {
  esp_task_wdt_init(15, true);        // 15 sec timeout, reset chip if task stalls
  esp_task_wdt_add(NULL);             // Register current task (SchedulerTask)
  TickType_t xLastWakeTime = xTaskGetTickCount();
  const TickType_t oneMinTick = pdMS_TO_TICKS(60000);

  while (true) {
    int delayMin = getReadDelay();
    TickType_t delayTicks = pdMS_TO_TICKS(delayMin * 60000);

    // --- Periodic RS485 + AWS + File Monitor ---
    Serial.println("[Scheduler] Running RS485 → AWS → File Monitor");
    readRS485process(slaveIDs,2);
    esp_task_wdt_reset();
    Serial.println("[Scheduler] Starting AWS Sender");
    awsSenderTaskprocess();
    esp_task_wdt_reset();
    // Serial.println("[Scheduler] Starting File Monitor Task");
    // fileMonitorTaskprocess();

    // --- Check for 3AM OTA ---
    struct tm timeinfo;
    if (getLocalTime(&timeinfo)) {

      int hour = timeinfo.tm_hour;
      int minute = timeinfo.tm_min;
      int today = timeinfo.tm_mday;

      if (hour == 3 && minute <= 30 && today != lastOTADay) {
        if (!otaDoneToday || timeinfo.tm_mday != lastOTADay) {
          Serial.println("[Scheduler] Running OTA at 3:00 AM");
          otaTaskprocess();  // defined OTA function
          esp_task_wdt_reset();
          otaDoneToday = true;
          lastOTADay = today;
        }
      } else {
        otaDoneToday = false;  // reset if hour != 3
      }
    } else {
      Serial.println("[Scheduler] Failed to get RTC time");
    }

    // --- Delay precisely until next X min ---
    vTaskDelayUntil(&xLastWakeTime, delayTicks);
  }
}
