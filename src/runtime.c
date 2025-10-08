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
    rt->command_ctx_initialized = 0;
    rt->store_initialized = 0;
    rt->subsystem_count = 0;
    for (size_t i = 0; i < RUNTIME_MAX_SUBSYSTEMS; ++i) {
        rt->subsystems[i].name = NULL;
        rt->subsystems[i].start = NULL;
        rt->subsystems[i].stop = NULL;
        rt->subsystems[i].failure_code = RUNTIME_OK;
        rt->subsystems[i].optional = 0;
        rt->subsystems[i].started = 0;
    }
}

static runtime_status_t start_expiry_subsystem(runtime_t *rt) {
    if (expiry_start(&rt->store) != 0) {
        return RUNTIME_ERROR_EXPIRY;
    }
    return RUNTIME_OK;
}

static void stop_expiry_subsystem(runtime_t *rt) {
    (void)rt;
    expiry_stop();
}

static runtime_status_t start_replication_subsystem(runtime_t *rt) {
    if (replication_start(&rt->command_ctx) != 0) {
        return RUNTIME_ERROR_REPLICATION;
    }
    return RUNTIME_OK;
}

static void stop_replication_subsystem(runtime_t *rt) {
    command_context_remove_listeners(&rt->command_ctx);
    replication_backlog_clear();
}

static void register_default_subsystems(runtime_t *rt) {
    runtime_register_subsystem(rt,
                               "expiry",
                               start_expiry_subsystem,
                               stop_expiry_subsystem,
                               RUNTIME_ERROR_EXPIRY,
                               1);
    runtime_register_subsystem(rt,
                               "replication",
                               start_replication_subsystem,
                               stop_replication_subsystem,
                               RUNTIME_ERROR_REPLICATION,
                               1);
}

static void runtime_stop_registered_subsystems(runtime_t *rt, size_t count) {
    if (!rt) {
        return;
    }
    if (count > rt->subsystem_count) {
        count = rt->subsystem_count;
    }
    for (size_t i = count; i > 0; --i) {
        runtime_subsystem_entry_t *entry = &rt->subsystems[i - 1];
        if (entry->started && entry->stop) {
            entry->stop(rt);
        }
        entry->started = 0;
    }
}

void runtime_init(runtime_t *rt) {
    if (!rt) {
        return;
    }
    reset_flags(rt);
    config_init(&rt->config);
    datastore_init(&rt->store);
    rt->store_initialized = 1;
    register_default_subsystems(rt);
}

void runtime_shutdown(runtime_t *rt) {
    if (!rt) {
        return;
    }
    runtime_stop_registered_subsystems(rt, rt->subsystem_count);
    if (rt->command_ctx_initialized) {
        command_context_deinit(&rt->command_ctx);
        rt->command_ctx_initialized = 0;
    }
    if (rt->store_initialized) {
        datastore_free(&rt->store);
        rt->store_initialized = 0;
    }
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

    for (size_t i = 0; i < rt->subsystem_count; ++i) {
        runtime_subsystem_entry_t *entry = &rt->subsystems[i];
        if (!entry->start) {
            continue;
        }
        runtime_status_t rc = entry->start(rt);
        if (rc != RUNTIME_OK) {
            if (entry->optional) {
                fprintf(stderr,
                        "Warning: optional subsystem '%s' failed to start (%d)\n",
                        entry->name ? entry->name : "unknown",
                        rc);
                continue;
            }
            fprintf(stderr,
                    "Failed to start subsystem '%s' (code %d)\n",
                    entry->name ? entry->name : "unknown",
                    rc);
            runtime_stop_registered_subsystems(rt, i);
            return (entry->failure_code != RUNTIME_OK) ? entry->failure_code : rc;
        }
        entry->started = 1;
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

int runtime_register_subsystem(runtime_t *rt,
                               const char *name,
                               runtime_subsystem_start_fn start,
                               runtime_subsystem_stop_fn stop,
                               runtime_status_t failure_code,
                               int optional) {
    if (!rt) {
        return -1;
    }
    if (rt->subsystem_count >= RUNTIME_MAX_SUBSYSTEMS) {
        return -1;
    }
    runtime_subsystem_entry_t *entry = &rt->subsystems[rt->subsystem_count++];
    entry->name = name;
    entry->start = start;
    entry->stop = stop;
    entry->failure_code = failure_code;
    entry->optional = optional ? 1 : 0;
    entry->started = 0;
    return 0;
}
