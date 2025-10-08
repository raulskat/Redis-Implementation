#ifndef COMMAND_H
#define COMMAND_H

#include "acl.h"
#include "command_dispatcher.h"
#include "command_events.h"
#include "config.h"
#include "datastore.h"
#include "resp.h"

#define REDIS_CONNECTION_NAME_MAX 128

typedef struct command_context_t {
    redis_store_t *store;
    redis_config_t *config;
    command_dispatcher_t dispatcher;
    command_event_dispatcher_t event_dispatcher;
    acl_state_t acl;
} command_context_t;

typedef struct command_session_t {
    command_context_t *ctx;
    acl_state_t acl;
    int resp_version;
    char connection_name[REDIS_CONNECTION_NAME_MAX];
    char persisted_connection_name[REDIS_CONNECTION_NAME_MAX];
    bool has_persisted_name;
} command_session_t;

int command_context_init(command_context_t *ctx, redis_store_t *store, redis_config_t *config);
void command_context_deinit(command_context_t *ctx);
int command_context_add_listener(command_context_t *ctx,
                                 command_event_listener_fn fn,
                                 void *userdata);
void command_context_remove_listeners(command_context_t *ctx);
void command_session_init(command_session_t *session, command_context_t *ctx);
void command_session_reset(command_session_t *session);
acl_state_t *command_session_acl(command_session_t *session);
void command_handle(int client_fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);

#endif // COMMAND_H
