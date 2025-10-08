#ifndef RUNTIME_H
#define RUNTIME_H

#include <stddef.h>

#include "command.h"
#include "config.h"
#include "datastore.h"

#define RUNTIME_MAX_SUBSYSTEMS 16

typedef enum {
    RUNTIME_OK = 0,
    RUNTIME_ERROR_CONFIG = -1,
    RUNTIME_ERROR_DATASTORE = -2,
    RUNTIME_ERROR_RDB = -3,
    RUNTIME_ERROR_EXPIRY = -4,
    RUNTIME_ERROR_REPLICATION = -5,
    RUNTIME_ERROR_SERVER = -6,
} runtime_status_t;

typedef struct runtime runtime_t;

typedef runtime_status_t (*runtime_subsystem_start_fn)(runtime_t *rt);
typedef void (*runtime_subsystem_stop_fn)(runtime_t *rt);

typedef struct {
    const char *name;
    runtime_subsystem_start_fn start;
    runtime_subsystem_stop_fn stop;
    runtime_status_t failure_code;
    int optional;
    int started;
} runtime_subsystem_entry_t;

struct runtime {
    redis_config_t config;
    redis_store_t store;
    command_context_t command_ctx;
    int server_status;
    int command_ctx_initialized;
    int store_initialized;
    runtime_subsystem_entry_t subsystems[RUNTIME_MAX_SUBSYSTEMS];
    size_t subsystem_count;
};

void runtime_init(runtime_t *rt);
void runtime_shutdown(runtime_t *rt);
runtime_status_t runtime_bootstrap(runtime_t *rt, int argc, char **argv);
runtime_status_t runtime_start(runtime_t *rt);
int runtime_register_subsystem(runtime_t *rt,
                               const char *name,
                               runtime_subsystem_start_fn start,
                               runtime_subsystem_stop_fn stop,
                               runtime_status_t failure_code,
                               int optional);

#endif /* RUNTIME_H */
