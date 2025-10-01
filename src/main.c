#include "command.h"
#include "config.h"
#include "datastore.h"
#include "rdb.h"
#include "replication.h"
#include "server.h"

#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    redis_config_t config;
    config_init(&config);
    if (config_parse_args(&config, argc, argv) != 0) {
        fprintf(stderr, "Failed to parse command-line arguments\n");
        return EXIT_FAILURE;
    }

    redis_store_t store;
    datastore_init(&store);

    command_context_t cmd_ctx;
    command_context_init(&cmd_ctx, &store, &config);

    if (rdb_load(&config, &store) != 0) {
        fprintf(stderr, "Failed to load RDB file\n");
    }

    if (replication_start(&cmd_ctx) != 0) {
        fprintf(stderr, "Warning: replication thread failed to start\n");
    }

    int server_status = server_run(&cmd_ctx);

    datastore_free(&store);
    return (server_status == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
