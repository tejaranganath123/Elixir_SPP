#ifndef AWS_SENDER_H
#define AWS_SENDER_H
bool initAWS();
void awsdrain();
void awsSenderTask(void *parameter);  // FreeRTOS task entry
void awsSenderTaskprocess();
#endif