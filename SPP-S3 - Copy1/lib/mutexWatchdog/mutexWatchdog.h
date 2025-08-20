#ifndef MUTEXWATCHDOG_H
#define MUTEXWATCHDOG_H

#include <Arduino.h>
#include "freertos/semphr.h"

inline bool takeMutex(SemaphoreHandle_t mutex, const char* tag, TickType_t timeout = pdMS_TO_TICKS(3000)) {
  if (xSemaphoreTake(mutex, timeout) == pdTRUE) {
    return true;
  } else {
    Serial.printf("[Watchdog] Mutex blocked: %s\n", tag);
    // TODO: optional LED or log to SD here
    return false;
  }
}

inline void giveMutex(SemaphoreHandle_t mutex) {
  xSemaphoreGive(mutex);
}

#endif