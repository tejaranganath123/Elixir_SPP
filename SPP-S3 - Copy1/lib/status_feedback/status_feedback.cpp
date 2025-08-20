
/*
setSystemState(SYS_WIFI_RESET);    // Yellow blink + 1kHz beep
setSystemState(SYS_OTA);           // Blue blink + 2kHz beep
setSystemState(SYS_DATA_SENT);     // Blue flash
setSystemState(SYS_WIFI_CONNECTED); // Solid green
 */
#include "status_feedback.h"
#include "config.h"

SystemState currentSystemState = SYS_WIFI_PORTAL;

unsigned long lastBlinkTime = 0;
bool buzzerOn = false;
unsigned long lastBuzzerToggle = 0;

void setRGB(int r, int g, int b) {
  analogWrite(RED_PIN, r);
  analogWrite(GREEN_PIN, g);
  analogWrite(BLUE_PIN, b);
}

void flashColor(int r, int g, int b, int duration = 100) {
  setRGB(r, g, b);
  delay(duration);
  setRGB(0, 0, 0);
  delay(duration);
}

void blinkResetSequence() {
  for (int i = 0; i < 2; i++) flashColor(0, 255, 0);
  for (int i = 0; i < 2; i++) flashColor(255, 0, 0);
  for (int i = 0; i < 2; i++) flashColor(0, 0, 255);
  ESP.restart();
}

void setSystemState(SystemState newState) {
  currentSystemState = newState;
  buzzerOn = false;
  lastBuzzerToggle = millis();
}

void statusFeedbackTask(void *param) {
  pinMode(RED_PIN, OUTPUT);
  pinMode(GREEN_PIN, OUTPUT);
  pinMode(BLUE_PIN, OUTPUT);
  pinMode(BUZZER_PIN, OUTPUT);

  while (true) {
    switch (currentSystemState) {
      case SYS_WIFI_PORTAL:
        if (millis() - lastBlinkTime > 1000) {
          setRGB(0, 255, 0); vTaskDelay(pdMS_TO_TICKS(100));;
          setRGB(0, 0, 0);  vTaskDelay(pdMS_TO_TICKS(900));;
          lastBlinkTime = millis();
        }
        noTone(BUZZER_PIN);
        break;

      case SYS_WIFI_CONNECTED:
        setRGB(0, 255, 0);
        noTone(BUZZER_PIN);
        break;

      case SYS_START:
        tone(BUZZER_PIN, 5000);
        delay(1000);
        noTone(BUZZER_PIN);
        break;

      case SYS_DRAIN_SEQUENCE:
        setRGB(0, 255, 0);
        noTone(BUZZER_PIN);
        break;

      case SYS_WIFI_WEAK:
        setRGB(0, 255, 0); delay(200); setRGB(0, 0, 0); vTaskDelay(pdMS_TO_TICKS(100));;
        setRGB(255, 0, 0); delay(200); setRGB(0, 0, 0); vTaskDelay(pdMS_TO_TICKS(500));;
        noTone(BUZZER_PIN);
        break;

      case SYS_WIFI_DISCONNECTED:
        setRGB(255, 0, 0); delay(200); setRGB(0, 0, 0); vTaskDelay(pdMS_TO_TICKS(200));;
        noTone(BUZZER_PIN);
        break;

      case SYS_DATA_SENT:
        setRGB(0, 0, 255); delay(100); setRGB(0, 0, 0); vTaskDelay(pdMS_TO_TICKS(100));;
        // setSystemState(SYS_WIFI_CONNECTED);
        noTone(BUZZER_PIN);
        break;

      case SYS_RESET_SEQUENCE:
      // if (millis() - lastBuzzerToggle > 200) {
          // buzzerOn = !buzzerOn;
          tone(BUZZER_PIN, BUZZ_FREQ_RESET);
          // else noTone(BUZZER_PIN);
          // lastBuzzerToggle = millis();
        // }
        blinkResetSequence();

        break;

      case SYS_WIFI_RESET:
        if (millis() - lastBuzzerToggle > 200) {
          buzzerOn = !buzzerOn;
          if (buzzerOn) tone(BUZZER_PIN, BUZZ_FREQ_RESET);
          else noTone(BUZZER_PIN);
          lastBuzzerToggle = millis();
        }
        setRGB(255, 255, 0); delay(150); setRGB(0, 0, 0); delay(150);
        break;

      case SYS_OTA:
        // if (millis() - lastBuzzerToggle > 150) {
        //   buzzerOn = !buzzerOn;
          tone(BUZZER_PIN, BUZZ_FREQ_OTA);
        //   else noTone(BUZZER_PIN);
        //   lastBuzzerToggle = millis();
        // }
        setRGB(0, 0, 255); delay(100); setRGB(0, 0, 0); vTaskDelay(pdMS_TO_TICKS(100));;
        // break;

      case SYS_NORMAL:
      default:
        setRGB(0, 0, 0);
        noTone(BUZZER_PIN);
        break;
    }

    delay(20);
  }
}
