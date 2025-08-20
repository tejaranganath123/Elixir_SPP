#ifndef SD_LOGGER_H
#define SD_LOGGER_H
#include "FS.h"
#include "SD.h"
#include "SPI.h"
#include "sensor_data.h"

bool initSD();
void logSensorData(const SensorData& data, const String& timestamp, uint8_t slaveid);
#endif