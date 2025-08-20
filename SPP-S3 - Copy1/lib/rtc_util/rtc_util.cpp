#include "rtc_util.h"
#include "RTClib.h"
#include "Preferences.h"
#include "rtcSync.h"


void setupRTC() {
  if (!rtc.begin()) {
    
    Serial.println("Couldn't find RTC");
    while (1) delay(10);
  }

  if (!rtc.isrunning()) {
    Serial.println("RTC is not running, Calling NTP time FN...");
    //rtc.adjust(DateTime(2025, 6, 20, 12, 0, 0));  // Set time here once
    Preferences prefs;
    prefs.begin("system", false);
      prefs.putBool("rtc_set_done", false);
      prefs.end();
    syncRTCOnceAfterWiFi();
  }
}

String getTimestamp() {
  DateTime now = rtc.now();
  char buffer[20];
  snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d,%02d:%02d:%02d",
           now.year(), now.month(), now.day(),
           now.hour(), now.minute(), now.second());
  return String(buffer);
}
