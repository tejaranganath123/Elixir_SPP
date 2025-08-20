#ifndef SYSTEM_TASKS_H
#define SYSTEM_TASKS_H

#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include "freertos/queue.h"
#include "freertos/semphr.h"

extern SemaphoreHandle_t awsDrainerLock;
extern TaskHandle_t WiFiResetTaskHandle ;
extern TaskHandle_t SchedularTaskHandle ;
extern TaskHandle_t awsDrainerTaskHandle;
extern TaskHandle_t FileMonitorTaskHandle;
extern TaskHandle_t readRS485TaskHandle ;
extern TaskHandle_t awsSenderTaskHandle ;
extern TaskHandle_t otaTaskhandle ;
extern QueueHandle_t senderQueue;
#endif
