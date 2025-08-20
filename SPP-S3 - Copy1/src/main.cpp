/*
Author : P TEJA RANGANATH
Company : Elixir SM 
Date :  
Product : SPP
Version : 1.0.1
C.Language : Embedded C - PlatformIO
File : 
Consists: 
*/
#include <Arduino.h>
#include "status_feedback.h"
#include "config.h"
#include "modbus_task.h"
//#include "rgb_led.h"
#include "sensor_data.h"
#include "rtc_util.h"
#include "sd_logger.h"
#include "fileMonitorTask.h"
#include "aws_sender.h"
#include "wifiControl.h"
#include "rtcSync.h"
#include "ota.h"  
#include "shared_tasks.h"
//#include "buzzer.h"
#include "awsDrainerTask.h"
#include "SchedulerTask.h"
#include "esp_task_wdt.h"
#include <WiFiClientSecure.h>
#include <PubSubClient.h>

WiFiClientSecure net;
PubSubClient client(net);

SensorData sensorData;

SemaphoreHandle_t sdMutex = NULL;
SemaphoreHandle_t wifiMutex = NULL;
SemaphoreHandle_t nvsMutex = NULL;
QueueHandle_t senderQueue = NULL; 



void setup() {
  Serial.begin(9600);
  // I/O pin setup
  pinMode(BUTTON_PIN, INPUT_PULLUP);
  pinMode(RED_PIN, OUTPUT);
  pinMode(GREEN_PIN, OUTPUT);
  pinMode(BLUE_PIN, OUTPUT);
  pinMode(MAX485_RE_NEG, OUTPUT);
  pinMode(MAX485_DE, OUTPUT);
  Serial.println(FirmwareID);
  if (!initSD()) Serial.println("SD Failed"); 
  sdMutex = xSemaphoreCreateMutex();
  wifiMutex = xSemaphoreCreateMutex();
  nvsMutex = xSemaphoreCreateMutex();
  bool isOTAInProgress = false;
  xSemaphoreGive(awsDrainerLock);  // Allow first access Initializing Once
  senderQueue = xQueueCreate(10, sizeof(int));
  if (senderQueue == NULL) {
    Serial.println("Queue creation failed!");
  }
  esp_task_wdt_init(25, true);  // <--- WDT setup here
  
  // Start WiFiManager or auto-connect from Preferences
  startWiFiManagerIfNeeded(); 
  // Your original rtc_util function 
  setupRTC();  
  // RTC Sync once if not done
  //syncRTCOnceAfterWiFi(); 
  // RGB Monitor 
  //xTaskCreatePinnedToCore(ledStatusTask, "LED Status", 2048, NULL, 1, NULL, 1);

  // RGB & Buzzer Monitor
  xTaskCreatePinnedToCore(statusFeedbackTask,"Status Feedback Task",4096,NULL,1,NULL,1);
  setSystemState(SYS_START);
  // button Monitor (button press)
  xTaskCreatePinnedToCore(wifiResetMonitorTask,"WiFiResetMonitor", 4096,NULL,1,&WiFiResetTaskHandle,0);    
  
  setupModbus(); // Basic Modbus Startup
  
  
  //if (!initAWS()) Serial.println("AWS Connection Failed");
  //Task for RS485 Read
  xTaskCreatePinnedToCore(readRS485, "RS485 Read", 8192, NULL, 3, &readRS485TaskHandle, 1);
  //Task for AWS Sender
  xTaskCreatePinnedToCore(awsSenderTask, "AWS Sender", 12288, NULL, 3, &awsSenderTaskHandle, 1);
  //Task for FileMonitorTask
  xTaskCreatePinnedToCore(fileMonitorTask, "FileMonitorTask", 4096, NULL, 2, &FileMonitorTaskHandle, 1);
  //Task for OtaTask
  xTaskCreatePinnedToCore(otaTask,"OtaTask",8192,NULL,3, &otaTaskhandle,1);
  //Task for awsDrainerTask
  //xTaskCreatePinnedToCore(awsDrainerTask, "awsDrainerTask", 8192, NULL, 1, &awsDrainerTaskHandle, 0);
  //Task for Buzzer
  //xTaskCreatePinnedToCore(buzzerTask, "Buzzer Task", 2048, NULL, 1, NULL, 0); 
    //xTaskCreatePinnedToCore(SchedulerTask, "SchedularTask", 8192, NULL, 2, &SchedularTaskHandle, 0);
  // Kill setup task if running under FreeRTOS
  vTaskDelete(NULL);
}

void loop() {
  // Nothing to do here — handled by FreeRTOS tasks
}
