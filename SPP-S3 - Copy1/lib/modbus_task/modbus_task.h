#ifndef MODEBUS_TASK_H
#define MODEBUS_TASK_H
#include <Arduino.h>
void readRS485(void *parameter);
void setupModbus();
void readRS485process(uint8_t* slaveIDs, int count);
#endif