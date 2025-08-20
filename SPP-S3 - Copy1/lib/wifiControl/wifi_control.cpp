
#include <Preferences.h>
#include <WiFi.h>
#include <esp_system.h>
#include "config.h"
#include "nvs_flash.h"
#include "WiFiProv.h"
#include "WiFi.h"
#include "ota.h"
#include "ota.h"
#include "status_feedback.h"
#include "esp_task_wdt.h"

#define WIFI_SSID_KEY "ssid"
#define WIFI_PASS_KEY "pass"
#define WIFI_PREF_NS  "wifi"

unsigned long pressStartTime = 0;
bool isPressed = false;
bool tonePlayed3s = false;
bool tonePlayed10s = false;

const char *pop = "abcd1234";
const char *service_name = "PROV_123";
const char *service_key = NULL;

void SysProvEvent(arduino_event_t *sys_event) {
  switch (sys_event->event_id) {
    case ARDUINO_EVENT_WIFI_STA_GOT_IP:
      Serial.print("\nConnected IP address : ");
      Serial.println(IPAddress(sys_event->event_info.got_ip.ip_info.ip.addr));
      break;
    case ARDUINO_EVENT_WIFI_STA_DISCONNECTED:
      Serial.println("\nDisconnected. Reconnecting...");
      break;
    case ARDUINO_EVENT_PROV_START:
      Serial.println("\nProvisioning started.");
      break;
    case ARDUINO_EVENT_PROV_CRED_RECV: {
      Preferences prefs;
      Serial.println("\nReceived Wi-Fi credentials");
      Serial.print("\tSSID : ");
      Serial.println((const char *)sys_event->event_info.prov_cred_recv.ssid);
      Serial.print("\tPassword : ");
      Serial.println((char const *)sys_event->event_info.prov_cred_recv.password);
      prefs.begin(WIFI_PREF_NS, false);
      prefs.putString(WIFI_SSID_KEY, WiFi.SSID());
      prefs.putString(WIFI_PASS_KEY, WiFi.psk());
      prefs.end();
      setSystemState( SYS_WIFI_CONNECTED);  // wifi connected

      break;
    }
    case ARDUINO_EVENT_PROV_CRED_FAIL:
    {
      Serial.println("\nProvisioning failed!\nProceeding to factory and retry provisioning\n");
      wifi_prov_mgr_deinit();
      wifi_prov_mgr_reset_provisioning();
      esp_restart();
      if (sys_event->event_info.prov_fail_reason == WIFI_PROV_STA_AUTH_ERROR) {
        Serial.println("\nWi-Fi AP password incorrect");
      } else {
        //nvs_flash_erase();
        Serial.println("\nWi-Fi AP not found....Add API \" nvs_flash_erase() \" before beginProvision()");
      }
      break;
    }
    case ARDUINO_EVENT_PROV_CRED_SUCCESS: Serial.println("\nProvisioning Successful"); break;
    case ARDUINO_EVENT_PROV_END:          Serial.println("\nProvisioning Ends"); break;
    default:                              break;
  }
}

void startWiFiManagerIfNeeded() {
  Preferences prefs;
  prefs.begin(WIFI_PREF_NS, false);
  String ssid = prefs.getString(WIFI_SSID_KEY, "");
  String pass = prefs.getString(WIFI_PASS_KEY, "");
  prefs.end();

  if (ssid.length() > 0) {
    Serial.println("[WiFi] Found saved credentials. Connecting...");
    WiFi.begin(ssid.c_str(), pass.c_str());
    unsigned long start = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - start < 10000) {
      delay(500);
      Serial.print(".");
    }
    Serial.println();
    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("[WiFi] Connected successfully.");
      return;
    }
    Serial.println("[WiFi] Failed. Starting provisioning...");
  }
  WiFi.onEvent(SysProvEvent);
  WiFiProv.beginProvision(WIFI_PROV_SCHEME_SOFTAP, WIFI_PROV_SCHEME_HANDLER_NONE, WIFI_PROV_SECURITY_1, pop, service_name, service_key);
  setSystemState(SYS_WIFI_PORTAL);
}

void wifiResetMonitorTask(void *parameter) {
  bool lastState = HIGH;
  unsigned long pressStartTime = 0;
  bool isPressed = false;

while (true) {
    bool currentState = digitalRead(BUTTON_PIN);
    esp_task_wdt_reset();
    if (currentState != lastState) {
      vTaskDelay(50 / portTICK_PERIOD_MS);
      currentState = digitalRead(BUTTON_PIN);
    }

    if (!isPressed && currentState == HIGH) {
      pressStartTime = millis();
      isPressed = true;
      Serial.println("Button press started");
    }

    if (isPressed && currentState == HIGH) {
      unsigned long heldTime = millis() - pressStartTime;
      
      while (digitalRead(BUTTON_PIN) == HIGH) {
        esp_task_wdt_reset();
        unsigned long heldTime = millis() - pressStartTime;

        if (heldTime >= 3000 && heldTime <= 10000) {
          esp_task_wdt_reset();
          if(!tonePlayed3s){//setSystemState(SYS_RESET_SEQUENCE); 
            tonePlayed3s = true ;Serial.println("WiFi reset tone");}
        } else if (heldTime > 10000) {
          esp_task_wdt_reset();
          if(!tonePlayed10s){//setSystemState(SYS_OTA); 
            tonePlayed10s = true ;Serial.println("OTA tone");}
        } else {
          Serial.println("No tone below 3s");
          //setSystemState(SYS_NORMAL);
          
        }
        vTaskDelay(50 / portTICK_PERIOD_MS);  // Small delay for responsiveness
      }
    }

    if (isPressed && currentState == LOW) {
      unsigned long pressDuration = millis() - pressStartTime;
      Serial.printf("Button released after %lu ms\n", pressDuration);
      //setSystemState(SYS_NORMAL);

      if (pressDuration >= 3000 && pressDuration <= 10000) {
        Serial.println("[WiFi Reset] Triggered");
        //setSystemState(SYS_RESET_SEQUENCE);
        Preferences prefs;
        prefs.begin(WIFI_PREF_NS, false);
        prefs.clear();
        prefs.end();
        prefs.begin("system", false);
        prefs.putBool("rtc_set_done", false);
        prefs.end();

        esp_err_t err = nvs_flash_erase();
        if (err == ESP_OK) {
          nvs_flash_init();
          wifi_prov_mgr_reset_provisioning();
          Serial.println("[NVS] WiFi reset successful");
        } else {
          Serial.printf("[NVS] Flash erase failed: 0x%X\n", err);
        }
        delay(1000);
        ESP.restart();
      }
      else if (pressDuration > 10000) {
        Serial.println("[OTA] Triggered");
        checkforotamanual();
      }
      else {
        Serial.println("[Button] Short press. No action.");
      }

      isPressed = false;
    }

    lastState = currentState;
    vTaskDelay(20 / portTICK_PERIOD_MS);
   }
 }
