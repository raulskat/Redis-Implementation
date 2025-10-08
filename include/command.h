#ifndef COMMAND_H
#define COMMAND_H

#include "command_dispatcher.h"
#include "config.h"
#include "datastore.h"
#include "resp.h"

typedef struct command_context_t {
    redis_store_t *store;
    redis_config_t *config;
    command_dispatcher_t dispatcher;
} command_context_t;

int command_context_init(command_context_t *ctx, redis_store_t *store, redis_config_t *config);
void command_context_deinit(command_context_t *ctx);
void command_handle(int client_fd, const resp_command_t *cmd, command_context_t *ctx);

#endif // COMMAND_H
