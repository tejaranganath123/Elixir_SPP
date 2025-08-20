#ifndef SENSOR_DATA_H 
#define SENSOR_DATA_H

struct SensorData {
  float KWH1,KWH2, KVAH1,KVAH2, KVARH1,KVARH2;
  float VR, VY, VB, AVG_VLN;
  float VRY, VYB, VBR, AVG_VLL;
  float IR, IY, IB, AVG_I;
  float R_PF, Y_PF, B_PF, AVG_PF;
  float FREQUENCY;
  float R_KW, Y_KW, B_KW, TOTAL_KW;
  float R_KVA, Y_KVA, B_KVA, TOTAL_KVA;
  float R_KVAR, Y_KVAR, B_KVAR, TOTAL_KVAR;
};

extern SensorData sensorData;

#endif