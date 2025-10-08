#ifndef COMMAND_H
#define COMMAND_H

#include "command_dispatcher.h"
#include "command_events.h"
#include "config.h"
#include "datastore.h"
#include "resp.h"

typedef struct command_context_t {
    redis_store_t *store;
    redis_config_t *config;
    command_dispatcher_t dispatcher;
    command_event_dispatcher_t event_dispatcher;
} command_context_t;

int command_context_init(command_context_t *ctx, redis_store_t *store, redis_config_t *config);
void command_context_deinit(command_context_t *ctx);
int command_context_add_listener(command_context_t *ctx,
                                 command_event_listener_fn fn,
                                 void *userdata);
void command_context_remove_listeners(command_context_t *ctx);
void command_handle(int client_fd, const resp_command_t *cmd, command_context_t *ctx);

#endif // COMMAND_H
