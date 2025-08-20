#include "rtcSync.h"
#include <Preferences.h>
#include <time.h>
#include <RTClib.h>
#include <WiFi.h>
#include "config.h"

RTC_DS3231 rtc;

void syncRTCOnceAfterWiFi() {
  Preferences prefs;
  prefs.begin("system", false);
  bool isSet = prefs.getBool("rtc_set_done", false);
  prefs.end();

  if (!isSet) {
    Serial.println("[RTC] Syncing from NTP...");
    // Wait until WiFi is connected
  while (WiFi.status() != WL_CONNECTED) {
    Serial.println("[RTC] WiFi not connected. Waiting...");
    delay(1000);
  }

  Serial.println("[RTC] WiFi connected. Syncing RTC from NTP...");

    configTime(19800, 0, "pool.ntp.org", "time.nist.gov");
    struct tm timeinfo;
    int retry = 0;
    while (!getLocalTime(&timeinfo) && retry++ < 20) {
      delay(500);
    }
    if (retry < 20 && rtc.begin()) {
      rtc.adjust(DateTime(timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday,
                          timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec));
      prefs.begin("system", false);
      prefs.putBool("rtc_set_done", true);
      prefs.end();
      Serial.println("[RTC] RTC updated.");
      prefs.begin("spp", true);  // Read-only mode
      int savedDelay = prefs.getInt("read_delay", 1000);  // Default 1000 ms
      prefs.end();
      sensor_read_delay = savedDelay;
      Serial.printf("Loaded sensor_read_delay from prefs: %d ms\n", sensor_read_delay);
}
     else {
      Serial.println("[RTC] Sync failed.");
    }
  } else {
    Serial.println("[RTC] Already synced.");
  }
}