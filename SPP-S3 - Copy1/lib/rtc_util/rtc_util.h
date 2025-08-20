#ifndef RTC_UTIL_H
#define RTC_UTIL_H
#include <RTClib.h>

void setupRTC();
String getTimestamp();

extern RTC_DS1307 rtc;
#endif