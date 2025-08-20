#include "sd_logger.h"

#define SD_CS_PIN 10 // Customize if needed
File logFile;

bool initSD() {
  SPI.begin(13, 12, 11, SD_CS_PIN);
  if (!SD.begin(SD_CS_PIN)) {
    Serial.println("SD card init failed!");
    return false;
  }

  // Create log file with header if it doesn't exist
  if (!SD.exists("/temp.csv")) {
    logFile = SD.open("/temp.csv", FILE_WRITE);
    if (logFile) {
      logFile.println("Date,Time,KWH1,KWH2,KVAH1,KVAH2,KVARH1,KVARH2,VR,VY,VB,AVG_VLN,VRY,VYB,VBR,AVG_VLL,IR,IY,IB,AVG_I,"
                      "R_PF,Y_PF,B_PF,AVG_PF,FREQUENCY,R_KW,Y_KW,B_KW,TOTAL_KW,"
                      "R_KVA,Y_KVA,B_KVA,TOTAL_KVA,R_KVAR,Y_KVAR,B_KVAR,TOTAL_KVAR");
      logFile.close();
    }
  }
  return true;
}

void logSensorData(const SensorData& data, const String& timestamp, uint8_t slaveid) {
  logFile = SD.open("/temp.csv", FILE_APPEND);
  if (!logFile) {
    Serial.println("Failed to open file for appending");
    return;
  }

  logFile.printf("%s,%u,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,"
                 "%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,"
                 "%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n",
                 timestamp.c_str(), slaveid,
                 data.KWH1,data.KWH2, data.KVAH1, data.KVAH2,data.KVARH1,data.KVARH2,
                 data.VR, data.VY, data.VB, data.AVG_VLN,
                 data.VRY, data.VYB, data.VBR, data.AVG_VLL,
                 data.IR, data.IY, data.IB, data.AVG_I,
                 data.R_PF, data.Y_PF, data.B_PF, data.AVG_PF,
                 data.FREQUENCY,
                 data.R_KW, data.Y_KW, data.B_KW, data.TOTAL_KW,
                 data.R_KVA, data.Y_KVA, data.B_KVA, data.TOTAL_KVA,
                 data.R_KVAR, data.Y_KVAR, data.B_KVAR, data.TOTAL_KVAR);
  Serial.println("Data Logged");
  Serial.printf("%s,%u,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,"
                 "%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,"
                 "%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n",
                 timestamp.c_str(),slaveid,
                 data.KWH1,data.KWH2, data.KVAH1, data.KVAH2,data.KVARH1,data.KVARH2,
                 data.VR, data.VY, data.VB, data.AVG_VLN,
                 data.VRY, data.VYB, data.VBR, data.AVG_VLL,
                 data.IR, data.IY, data.IB, data.AVG_I,
                 data.R_PF, data.Y_PF, data.B_PF, data.AVG_PF,
                 data.FREQUENCY,
                 data.R_KW, data.Y_KW, data.B_KW, data.TOTAL_KW,
                 data.R_KVA, data.Y_KVA, data.B_KVA, data.TOTAL_KVA,
                 data.R_KVAR, data.Y_KVAR, data.B_KVAR, data.TOTAL_KVAR);
  logFile.close();
}
