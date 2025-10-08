#ifndef RUNTIME_H
#define RUNTIME_H

#include "command.h"
#include "config.h"
#include "datastore.h"

typedef enum {
    RUNTIME_OK = 0,
    RUNTIME_ERROR_CONFIG = -1,
    RUNTIME_ERROR_DATASTORE = -2,
    RUNTIME_ERROR_RDB = -3,
    RUNTIME_ERROR_EXPIRY = -4,
    RUNTIME_ERROR_REPLICATION = -5,
    RUNTIME_ERROR_SERVER = -6,
} runtime_status_t;

typedef struct {
    redis_config_t config;
    redis_store_t store;
    command_context_t command_ctx;
    int server_status;
    int expiry_started;
    int replication_started;
    int command_ctx_initialized;
    int store_initialized;
} runtime_t;

void runtime_init(runtime_t *rt);
void runtime_shutdown(runtime_t *rt);
runtime_status_t runtime_bootstrap(runtime_t *rt, int argc, char **argv);
runtime_status_t runtime_start(runtime_t *rt);

#endif /* RUNTIME_H */
