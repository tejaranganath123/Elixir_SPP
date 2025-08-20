#ifndef CONFIG_H
#define CONFIG_H

#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <Arduino.h>
#include <Preferences.h>

// Declare global instances — defined in main.cpp

extern WiFiClientSecure net;
extern PubSubClient client;

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

// Global mutex handles
extern SemaphoreHandle_t sdMutex;
extern SemaphoreHandle_t nvsMutex;
extern SemaphoreHandle_t wifiMutex;



extern Preferences prefs;

#define RED_PIN 19
#define GREEN_PIN  20
#define BLUE_PIN 21

extern const String DEVICE_UID;
extern const String FirmwareID;
extern bool otainprogress;

#define BUTTON_PIN 1
#define MAX485_DE 18
#define MAX485_RE_NEG 18
#define SLAVE_ID 1

extern const int app_cpu;
extern  int sensor_read_delay ;
#endif