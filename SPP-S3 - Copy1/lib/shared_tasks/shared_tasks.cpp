#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include "freertos/queue.h"
#include "freertos/semphr.h"

#include <Arduino.h>
TaskHandle_t WiFiResetTaskHandle = NULL;
TaskHandle_t SchedularTaskHandle = NULL;
TaskHandle_t awsDrainerTaskHandle = NULL;
TaskHandle_t FileMonitorTaskHandle = NULL;
TaskHandle_t readRS485TaskHandle = NULL;
TaskHandle_t awsSenderTaskHandle = NULL;
TaskHandle_t otaTaskhandle = NULL;
SemaphoreHandle_t awsDrainerLock = xSemaphoreCreateBinary();