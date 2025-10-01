#include "expiry.h"

#include <pthread.h>
#include <stdbool.h>
#include <time.h>

#define EXPIRY_PRUNE_BATCH 128
#define EXPIRY_SLEEP_MICROS 100000

static pthread_t expiry_thread;
static pthread_mutex_t expiry_mutex = PTHREAD_MUTEX_INITIALIZER;
static bool expiry_thread_running = false;
static bool expiry_thread_should_run = false;

static void sleep_interval(void) {
    struct timespec ts = {0};
    ts.tv_sec = EXPIRY_SLEEP_MICROS / 1000000;
    ts.tv_nsec = (EXPIRY_SLEEP_MICROS % 1000000) * 1000;
    nanosleep(&ts, NULL);
}

static void *expiry_loop(void *arg) {
    redis_store_t *store = (redis_store_t *)arg;
    while (true) {
        pthread_mutex_lock(&expiry_mutex);
        bool should_run = expiry_thread_should_run;
        pthread_mutex_unlock(&expiry_mutex);
        if (!should_run) {
            break;
        }
        datastore_prune_expired(store, EXPIRY_PRUNE_BATCH);
        sleep_interval();
    }
    return NULL;
}

int expiry_start(redis_store_t *store) {
    if (!store) {
        return -1;
    }

    pthread_mutex_lock(&expiry_mutex);
    if (expiry_thread_running) {
        pthread_mutex_unlock(&expiry_mutex);
        return 0;
    }
    expiry_thread_should_run = true;
    if (pthread_create(&expiry_thread, NULL, expiry_loop, store) != 0) {
        expiry_thread_should_run = false;
        pthread_mutex_unlock(&expiry_mutex);
        return -1;
    }
    expiry_thread_running = true;
    pthread_mutex_unlock(&expiry_mutex);
    return 0;
}

void expiry_stop(void) {
    pthread_mutex_lock(&expiry_mutex);
    if (!expiry_thread_running) {
        pthread_mutex_unlock(&expiry_mutex);
        return;
    }
    expiry_thread_should_run = false;
    pthread_mutex_unlock(&expiry_mutex);

    pthread_join(expiry_thread, NULL);

    pthread_mutex_lock(&expiry_mutex);
    expiry_thread_running = false;
    pthread_mutex_unlock(&expiry_mutex);
}
