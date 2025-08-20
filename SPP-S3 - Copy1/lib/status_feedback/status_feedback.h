#ifndef STATUS_FEEDBACK_H
#define STATUS_FEEDBACK_H

#include <Arduino.h>

// ----------- System State Enum -----------
enum SystemState {
  SYS_NORMAL,
  SYS_WIFI_PORTAL,
  SYS_WIFI_CONNECTED,
  SYS_WIFI_WEAK,
  SYS_WIFI_DISCONNECTED,
  SYS_DATA_SENT,
  SYS_RESET_SEQUENCE,
  SYS_WIFI_RESET,
  SYS_DRAIN_SEQUENCE,
  SYS_START,
  SYS_OTA
};

extern SystemState currentSystemState;

// ----------- Buzzer Pin -----------
#define BUZZER_PIN 4  // Updated to GPIO4 for ESP32-S3

// ----------- Buzzer Frequencies -----------
#define BUZZ_FREQ_RESET 1535
#define BUZZ_FREQ_OTA   2000

// ----------- Functions -----------
void setSystemState(SystemState newState);
void statusFeedbackTask(void *param);

#endif // STATUS_FEEDBACK_H