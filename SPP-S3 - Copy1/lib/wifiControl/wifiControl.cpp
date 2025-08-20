/* Using the latest file wifi_control */

// #include "wifiControl.h"
// #include <Preferences.h>
// #include <WiFi.h>
// #include <esp_system.h>
// #include "rgb_led.h"
// #include "config.h"
// #include "nvs_flash.h"
// #include "WiFiProv.h"
// #include "WiFi.h"
// #include "ota.h"
// #include "buzzer.h"

// #define WIFI_SSID_KEY "ssid"
// #define WIFI_PASS_KEY "pass"
// #define WIFI_PREF_NS  "wifi"

// // #define USE_SOFT_AP // Uncomment if you want to enforce using the Soft AP method instead of BLE
// const char *pop = "abcd1234";           // Proof of possession - otherwise called a PIN - string provided by the device, entered by the user in the phone app
// const char *service_name = "PROV_123";  // Name of your device (the Espressif apps expects by default device name starting with "Prov_")
// const char *service_key = NULL;         // Password used for SofAP method (NULL = no password needed)
// bool reset_provisioned = true;          // When true the library will automatically delete previously provisioned data.


// //#define PRESS_TIMEOUT_MS 3000  // Timeout to finalize press count
// //#define WIFI_RESET_COUNT 3
// //#define OTA_CHECK_COUNT 5

// //unsigned long lastPressTime = 0;
// //int pressCount = 0;
// //bool lastState = HIGH;  // Assuming active LOW button

// void SysProvEvent(arduino_event_t *sys_event) {
//   switch (sys_event->event_id) {
//     case ARDUINO_EVENT_WIFI_STA_GOT_IP:
//       Serial.print("\nConnected IP address : ");
//       Serial.println(IPAddress(sys_event->event_info.got_ip.ip_info.ip.addr));
//       break;
//     case ARDUINO_EVENT_WIFI_STA_DISCONNECTED: Serial.println("\nDisconnected. Connecting to the AP again... "); break;
//     case ARDUINO_EVENT_PROV_START:            Serial.println("\nProvisioning started\nGive Credentials of your access point using smartphone app"); break;
//     case ARDUINO_EVENT_PROV_CRED_RECV:
//     {
//       Preferences prefs;
//       Serial.println("\nReceived Wi-Fi credentials");
//       Serial.print("\tSSID : ");
//       Serial.println((const char *)sys_event->event_info.prov_cred_recv.ssid);
//       Serial.print("\tPassword : ");
//       Serial.println((char const *)sys_event->event_info.prov_cred_recv.password);
//       prefs.begin(WIFI_PREF_NS, false);
//       prefs.putString(WIFI_SSID_KEY, WiFi.SSID());
//       prefs.putString(WIFI_PASS_KEY, WiFi.psk());
//       setLedState(WIFI_CONNECTED);
//       prefs.end();
//       break;
//     }
//     case ARDUINO_EVENT_PROV_CRED_FAIL:
//     {
//       Serial.println("\nProvisioning failed!\nPlease reset to factory and retry provisioning\n");
//       if (sys_event->event_info.prov_fail_reason == WIFI_PROV_STA_AUTH_ERROR) {
//         Serial.println("\nWi-Fi AP password incorrect");
//       } else {
//         //nvs_flash_erase();
//         Serial.println("\nWi-Fi AP not found....Add API \" nvs_flash_erase() \" before beginProvision()");
//       }
//       break;
//     }
//     case ARDUINO_EVENT_PROV_CRED_SUCCESS: Serial.println("\nProvisioning Successful"); break;
//     case ARDUINO_EVENT_PROV_END:          Serial.println("\nProvisioning Ends"); break;
//     default:                              break;
//   }
// }

// void startWiFiManagerIfNeeded() {
//   Preferences prefs;
//   prefs.begin(WIFI_PREF_NS, false);
//   String ssid = prefs.getString(WIFI_SSID_KEY, "");
//   String pass = prefs.getString(WIFI_PASS_KEY, "");
//   prefs.end();

//   if (ssid.length() > 0) {
//     Serial.println("[WiFi] Found saved credentials. Connecting...");
//     WiFi.begin(ssid.c_str(), pass.c_str());
//     unsigned long start = millis();
//     while (WiFi.status() != WL_CONNECTED && millis() - start < 10000) {
//       delay(500);
//       Serial.print(".");
//     }
//     Serial.println();
//     if (WiFi.status() == WL_CONNECTED) {
//       Serial.println("[WiFi] Connected successfully.");
//       return;
//     }
//     Serial.println("[WiFi] Failed. Starting WiFi Manager...");
//   }
//   WiFi.begin();  // no SSID/PWD - get it from the Provisioning APP or from NVS (last successful connection)
//   WiFi.onEvent(SysProvEvent);
//   Serial.println("Manually scanning WiFi networks...");
//   delay(3000);
//   Serial.println("Begin Provisioning using Soft AP");
//   WiFiProv.beginProvision(WIFI_PROV_SCHEME_SOFTAP, WIFI_PROV_SCHEME_HANDLER_NONE, WIFI_PROV_SECURITY_1, pop, service_name, service_key);
  
//   // WiFiManager wm;
//   // if (wm.autoConnect("SPP-Device", "admin123")) {
//   //   setLedState(WIFI_PORTAL);
//   //   Serial.println("[WiFiManager] Connected.");
//   //   prefs.begin(WIFI_PREF_NS, false);
//   //   prefs.putString(WIFI_SSID_KEY, WiFi.SSID());
//   //   prefs.putString(WIFI_PASS_KEY, WiFi.psk());
//   //   setLedState(WIFI_CONNECTED);
//   //   prefs.end();
//   // } else {
//   //   Serial.println("[WiFiManager] Failed. Rebooting...");
//   //   ESP.restart();
//   // }
// }

// void wifiResetMonitorTask(void *parameter) {
//   bool lastState = HIGH;
//   unsigned long pressStartTime = 0;
//   bool isPressed = false;
  
//   while (true) {
//     bool currentState = digitalRead(BUTTON_PIN);

//     if (currentState != lastState) {
//       vTaskDelay(50 / portTICK_PERIOD_MS);
//       currentState = digitalRead(BUTTON_PIN);
//     }

//     if (!isPressed && currentState == HIGH) {
//       pressStartTime = millis();
//       isPressed = true;
//       Serial.println("Button press started");
//     }

//     if (isPressed && currentState == HIGH) {
//       unsigned long heldTime = millis() - pressStartTime;
      
//       while (digitalRead(BUTTON_PIN) == HIGH) {
//         unsigned long heldTime = millis() - pressStartTime;

//         if (heldTime >= 3000 && heldTime <= 10000) {
//           Serial.println("WiFi reset tone");
//           ledcWriteTone(0, 1000);  // WiFi reset tone
//         } else if (heldTime > 10000) {
//           Serial.println("OTA tone");
//           ledcWriteTone(0, 2000);  // OTA tone
//         } else {
//           Serial.println("No tone below 3s");
//           ledcWriteTone(0, 0);     // No tone below 3s
//         }

//         vTaskDelay(50 / portTICK_PERIOD_MS);  // Small delay for responsiveness
//       }
//     }

//     if (isPressed && currentState == LOW) {
//       unsigned long pressDuration = millis() - pressStartTime;
//       Serial.printf("Button released after %lu ms\n", pressDuration);
//       setBuzzerState(BUZZER_OFF);

//       if (pressDuration >= 3000 && pressDuration <= 10000) {
//         Serial.println("[WiFi Reset] Triggered");
//         setLedState(RESET_SEQUENCE);
//         Preferences prefs;
//         prefs.begin(WIFI_PREF_NS, false);
//         prefs.clear();
//         prefs.end();
//         prefs.begin("system", false);
//         prefs.putBool("rtc_set_done", false);
//         prefs.end();

//         esp_err_t err = nvs_flash_erase();
//         if (err == ESP_OK) {
//           nvs_flash_init();
//           wifi_prov_mgr_reset_provisioning();
//           Serial.println("[NVS] WiFi reset successful");
//         } else {
//           Serial.printf("[NVS] Flash erase failed: 0x%X\n", err);
//         }
//         delay(1000);
//         ESP.restart();
//       }
//       else if (pressDuration > 10000) {
//         Serial.println("[OTA] Triggered");
//         checkforotamanual();
//       }
//       else {
//         Serial.println("[Button] Short press. No action.");
//       }

//       isPressed = false;
//     }

//     lastState = currentState;
//     vTaskDelay(20 / portTICK_PERIOD_MS);
//   }
// }
