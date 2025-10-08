#include "runtime.h"

#include "expiry.h"
#include "persistence.h"
#include "replication.h"
#include "rdb.h"
#include "server.h"

#include <stdio.h>
#include <stdlib.h>

static void reset_flags(runtime_t *rt) {
    rt->server_status = 0;
    rt->expiry_started = 0;
    rt->replication_started = 0;
    rt->command_ctx_initialized = 0;
    rt->store_initialized = 0;
}

void runtime_init(runtime_t *rt) {
    if (!rt) {
        return;
    }
    reset_flags(rt);
    config_init(&rt->config);
    datastore_init(&rt->store);
    rt->store_initialized = 1;
}

static void runtime_stop_subsystems(runtime_t *rt) {
    if (!rt) {
        return;
    }
    if (rt->expiry_started) {
        expiry_stop();
        rt->expiry_started = 0;
    }
    if (rt->command_ctx_initialized) {
        command_context_deinit(&rt->command_ctx);
        rt->command_ctx_initialized = 0;
    }
    if (rt->store_initialized) {
        datastore_free(&rt->store);
        rt->store_initialized = 0;
    }
}

void runtime_shutdown(runtime_t *rt) {
    runtime_stop_subsystems(rt);
}

runtime_status_t runtime_bootstrap(runtime_t *rt, int argc, char **argv) {
    if (!rt) {
        return RUNTIME_ERROR_CONFIG;
    }

    if (config_parse_args(&rt->config, argc, argv) != 0) {
        fprintf(stderr, "Failed to parse command-line arguments\n");
        return RUNTIME_ERROR_CONFIG;
    }

    if (command_context_init(&rt->command_ctx, &rt->store, &rt->config) != 0) {
        fprintf(stderr, "Failed to initialize command context\n");
        return RUNTIME_ERROR_DATASTORE;
    }
    rt->command_ctx_initialized = 1;

    if (rdb_load(&rt->config, &rt->store) != 0) {
        fprintf(stderr, "Failed to load RDB file\n");
        /* Continue as Redis does when RDB fails */
    }

    if (expiry_start(&rt->store) != 0) {
        fprintf(stderr, "Warning: failed to start expiration scheduler\n");
    } else {
        rt->expiry_started = 1;
    }

    if (replication_start(&rt->command_ctx) != 0) {
        fprintf(stderr, "Warning: replication thread failed to start\n");
    } else {
        rt->replication_started = 1;
    }

    return RUNTIME_OK;
}

runtime_status_t runtime_start(runtime_t *rt) {
    if (!rt) {
        return RUNTIME_ERROR_SERVER;
    }

    rt->server_status = server_run(&rt->command_ctx);
    if (rt->server_status != 0) {
        fprintf(stderr, "Server exited with status %d\n", rt->server_status);
        return RUNTIME_ERROR_SERVER;
    }

    return RUNTIME_OK;
}
