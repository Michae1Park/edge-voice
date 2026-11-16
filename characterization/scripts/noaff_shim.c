#define _GNU_SOURCE
#include <pthread.h>
#include <sched.h>
int pthread_setaffinity_np(pthread_t t, size_t s, const cpu_set_t *c){return 0;}
