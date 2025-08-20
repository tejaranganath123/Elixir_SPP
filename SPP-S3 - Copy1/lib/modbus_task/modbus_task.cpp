#include "modbus_task.h"
#include "config.h"
#include "sensor_data.h"
#include "utils.h"
#include "rtc_util.h"
#include "sd_logger.h"
#include "esp_task_wdt.h"
#include "shared_tasks.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

#include <ModbusMaster.h>

extern SemaphoreHandle_t sdMutex;
extern SemaphoreHandle_t nvsMutex; 
extern QueueHandle_t senderQueue;

ModbusMaster node;

uint16_t regList[] = {
  0x0000, 0x0002,       // KWH1, KWH2
  0x0004, 0x0006,       // KVAH1, KVAH2
  0x0008, 0x000A,       // KVARH1, KVARH2
  0x000C, 0x0010, 0x0014, 0x0018,  // VR, VY, VB, AVG_VLN
  0x001C, 0x0020, 0x0024, 0x0028,  // VRY, VYB, VBR, AVG_VLL
  0x002C, 0x0030, 0x0034, 0x0038,  // IR, IY, IB, AVG_I
  0x003C, 0x003E, 0x0040, 0x0044,  // R_PF, Y_PF, B_PF, AVG_PF
  0x0046,                         // FREQUENCY
  0x0048, 0x004C, 0x0050, 0x0054,  // R_KW, Y_KW, B_KW, TOTAL_KW
  0x0058, 0x005C, 0x0060, 0x0064,  // R_KVA, Y_KVA, B_KVA, TOTAL_KVA
  0x0068, 0x006C, 0x0070, 0x0074   // R_KVAR, Y_KVAR, B_KVAR, TOTAL_KVAR
};

#define NUM_PARAMS 35
#define MAX_SLAVES 5

void preTransmission() {
  digitalWrite(MAX485_RE_NEG, 1);
  digitalWrite(MAX485_DE, 1);
}

void postTransmission() {
  digitalWrite(MAX485_RE_NEG, 0);
  digitalWrite(MAX485_DE, 0);
}

void setupModbus() {
  pinMode(MAX485_RE_NEG, OUTPUT);
  pinMode(MAX485_DE, OUTPUT);
  digitalWrite(MAX485_RE_NEG, 0);
  digitalWrite(MAX485_DE, 0);
  
  Serial2.begin(9600, SERIAL_8N1, 16, 17);
  node.begin(SLAVE_ID, Serial2);
  node.preTransmission(preTransmission);
  node.postTransmission(postTransmission);
}

void readRS485process(uint8_t* slaveIDs, int count, float accumulators[][NUM_PARAMS], int* readCount) {
  // xSemaphoreTake(nvsMutex, portMAX_DELAY);
  // prefs.begin("spp", true);
  // sensor_read_delay = prefs.getInt("read_delay", 1000);
  // prefs.end();
  // xSemaphoreGive(nvsMutex);
  Serial.println("Starting process");

  for (int s = 0; s < count; s++) {
    esp_task_wdt_reset();
    uint8_t slaveID = slaveIDs[s];
    node.begin(slaveID, Serial2);
    node.preTransmission(preTransmission);
    node.postTransmission(postTransmission);

    for (int i = 0; i < NUM_PARAMS; i++) {
      uint8_t result = node.readInputRegisters(regList[i], 2);
      float value = 0;

      if (result == node.ku8MBSuccess) {
        uint16_t data0 = node.getResponseBuffer(0);
        uint16_t data1 = node.getResponseBuffer(1);
        value = InttoFloat(data1, data0);
        if (i <= 5) {
            accumulators[s][i] = value;  // ✅ overwrite with latest
          } else {
            accumulators[s][i] += value; // ✅ accumulate for avg
          }
          //Serial.printf("[Read] Slave %d | Param %d | Value: %.2f\n", slaveID, i, value);
          delay(100);

      }
      else {
        accumulators[s][i] = 99.99;
        //Serial.printf("[Error Read] Slave %d | at Param %d | D.Value: %.2f\n", slaveID, i, accumulators[s][i]);
        delay(100);
      }

      vTaskDelay(10 / portTICK_PERIOD_MS);
      esp_task_wdt_reset();
    }

    readCount[s]++;
    Serial.println(readCount[s]);
  }
}

void readRS485(void *parameter) {
  uint8_t slaveIDs[] = {1,1,1};  // 👈 Add your slave IDs here
  int numSlaves = sizeof(slaveIDs) / sizeof(slaveIDs[0]);

  float accumulators[MAX_SLAVES][NUM_PARAMS] = {0};
  int readCount[MAX_SLAVES] = {0};

  TickType_t duration = pdMS_TO_TICKS(5 * 60000);  // 5 min
  TickType_t start, now;

  while (1) {
    memset(accumulators, 0, sizeof(accumulators));
    memset(readCount, 0, sizeof(readCount));

    start = xTaskGetTickCount();
    now = start;

    while ((now - start) < duration) {
      Serial.println(now - start);
      readRS485process(slaveIDs, numSlaves, accumulators, readCount);
      now = xTaskGetTickCount();
    }

    for (int s = 0; s < numSlaves; s++) {
      if (readCount[s] > 0) {
        Serial.printf("---- [Pre-Avg] Slave %d | Samples: %d ----\n", slaveIDs[s], readCount[s]);
        for (int i = 0; i < NUM_PARAMS; i++) {
          Serial.printf("Param %02d Raw Sum: %.2f\n", i, accumulators[s][i]);
           if (i > 5 && readCount[s] > 0) {  // ✅ skip averaging for energy counters
              accumulators[s][i] /= readCount[s];
            }
        }

        // Map to sensorData
        sensorData.KWH1 = accumulators[s][0];
        sensorData.KWH2 = accumulators[s][1];
        sensorData.KVAH1 = accumulators[s][2];
        sensorData.KVAH2 = accumulators[s][3];
        sensorData.KVARH1 = accumulators[s][4];
        sensorData.KVARH2 = accumulators[s][5];
        sensorData.VR = accumulators[s][6];
        sensorData.VY = accumulators[s][7];
        sensorData.VB = accumulators[s][8];
        sensorData.AVG_VLN = accumulators[s][9];
        sensorData.VRY = accumulators[s][10];
        sensorData.VYB = accumulators[s][11];
        sensorData.VBR = accumulators[s][12];
        sensorData.AVG_VLL = accumulators[s][13];
        sensorData.IR = accumulators[s][14];
        sensorData.IY = accumulators[s][15];
        sensorData.IB = accumulators[s][16];
        sensorData.AVG_I = accumulators[s][17];
        sensorData.R_PF = accumulators[s][18];
        sensorData.Y_PF = accumulators[s][19];
        sensorData.B_PF = accumulators[s][20];
        sensorData.AVG_PF = accumulators[s][21];
        sensorData.FREQUENCY = accumulators[s][22];
        sensorData.R_KW = accumulators[s][23];
        sensorData.Y_KW = accumulators[s][24];
        sensorData.B_KW = accumulators[s][25];
        sensorData.TOTAL_KW = accumulators[s][26];
        sensorData.R_KVA = accumulators[s][27];
        sensorData.Y_KVA = accumulators[s][28];
        sensorData.B_KVA = accumulators[s][29];
        sensorData.TOTAL_KVA = accumulators[s][30];
        sensorData.R_KVAR = accumulators[s][31];
        sensorData.Y_KVAR = accumulators[s][32];
        sensorData.B_KVAR = accumulators[s][33];
        sensorData.TOTAL_KVAR = accumulators[s][34];
        while (xSemaphoreTake(awsDrainerLock, pdMS_TO_TICKS(100)) != pdTRUE) {
          Serial.println("[modbusTask] : Waiting for awsDrainer to finish to log data to sdcard...");
          esp_task_wdt_reset();  // Keep watchdog alive
        }
        Serial.println("[modbusTask] : Received awsDrainer to finish waiting sdcard...");
        TaskHandle_t owner = xSemaphoreGetMutexHolder(sdMutex);
        if (owner != NULL) {
           Serial.print("Mutex is owned by: ");
           Serial.println(pcTaskGetName(owner));
           } else {
                 Serial.println("Mutex is not held by any task");
           }
        if (xSemaphoreTake(sdMutex, portMAX_DELAY) == pdTRUE) {
          Serial.println("[modbusTask] : Received SD Mutex to finish to log data to sdcard...");
          String timestamp = getTimestamp();
          logSensorData(sensorData, timestamp, slaveIDs[s]);
          xSemaphoreGive(sdMutex);
          xSemaphoreGive(awsDrainerLock);  // Release for AWS drain if needed
          Serial.println("[modbusTask]: Released access to the AWS drain");
        }
      }
    }

    // Notify AWS Sender
    int sendTrigger = 1;
    if (senderQueue != NULL) {
      xQueueSend(senderQueue, &sendTrigger, 0);
    }
  }
}
