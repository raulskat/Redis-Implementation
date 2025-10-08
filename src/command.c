#include "command.h"
#include "command_handlers.h"
#include "command_utils.h"
#include "resp.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define CMD_DEF(NAME, HANDLER, FLAGS, MIN, MAX) \
    { .name = NAME, .handler = HANDLER, .validators = NULL, .validator_count = 0, .flags = FLAGS, .min_arity = MIN, .max_arity = MAX }

static int validate_config_command(const resp_command_t *cmd,
                                   const command_context_t *ctx,
                                   const command_session_t *session,
                                   char *error_buf,
                                   size_t error_buf_len) {
    (void)ctx;
    (void)session;
    if (cmd->argc < 2) {
        return 0;
    }
    if (command_str_icmp(cmd->argv[1], "GET") != 0) {
        snprintf(error_buf, error_buf_len, "ERR unknown subcommand");
        return -1;
    }
    if (cmd->argc < 3) {
        snprintf(error_buf, error_buf_len, "ERR wrong number of arguments for 'config get'");
        return -1;
    }
    return 0;
}

static int validate_info_command(const resp_command_t *cmd,
                                 const command_context_t *ctx,
                                 const command_session_t *session,
                                 char *error_buf,
                                 size_t error_buf_len) {
    (void)ctx;
    (void)session;
    if (cmd->argc <= 1) {
        return 0;
    }
    if (command_str_icmp(cmd->argv[1], "replication") != 0) {
        snprintf(error_buf, error_buf_len, "ERR unsupported INFO section");
        return -1;
    }
    return 0;
}

static const command_validator_t config_validators[] = {
    {.fn = validate_config_command},
};

static const command_validator_t info_validators[] = {
    {.fn = validate_info_command},
};

static const command_spec_t builtin_commands[] = {
    CMD_DEF("PING", handle_ping_command, CMD_FLAG_ALLOW_UNAUTH | CMD_FLAG_READONLY, 1, 2),
    CMD_DEF("ECHO", handle_echo_command, CMD_FLAG_ALLOW_UNAUTH | CMD_FLAG_READONLY, 2, 2),
    CMD_DEF("HELLO", handle_hello_command, CMD_FLAG_ALLOW_UNAUTH | CMD_FLAG_READONLY, 1, -1),
    CMD_DEF("AUTH", handle_auth_command, CMD_FLAG_ALLOW_UNAUTH, 2, 3),
    CMD_DEF("SET", handle_set_command, CMD_FLAG_WRITE, 3, -1),
    CMD_DEF("GET", handle_get_command, CMD_FLAG_READONLY, 2, 2),
    {.name = "CONFIG",
     .handler = handle_config_command,
     .validators = config_validators,
     .validator_count = sizeof(config_validators) / sizeof(config_validators[0]),
     .flags = CMD_FLAG_ADMIN,
     .min_arity = 3,
     .max_arity = -1},
    CMD_DEF("KEYS", handle_keys_command, CMD_FLAG_READONLY, 1, -1),
    CMD_DEF("EXPIRE", handle_expire_command, CMD_FLAG_WRITE | CMD_FLAG_EXPIRY, 3, 3),
    CMD_DEF("PEXPIRE", handle_pexpire_command, CMD_FLAG_WRITE | CMD_FLAG_EXPIRY, 3, 3),
    CMD_DEF("TTL", handle_ttl_command, CMD_FLAG_READONLY, 2, 2),
    CMD_DEF("PTTL", handle_pttl_command, CMD_FLAG_READONLY, 2, 2),
    CMD_DEF("PERSIST", handle_persist_command, CMD_FLAG_WRITE | CMD_FLAG_EXPIRY, 2, 2),
    {.name = "INFO",
     .handler = handle_info_command,
     .validators = info_validators,
     .validator_count = sizeof(info_validators) / sizeof(info_validators[0]),
     .flags = CMD_FLAG_READONLY,
     .min_arity = 1,
     .max_arity = 2},
    CMD_DEF("FLUSHALL", handle_flush_command, CMD_FLAG_ADMIN | CMD_FLAG_WRITE | CMD_FLAG_DELETE, 1, 1),
    CMD_DEF("FLUSHDB", handle_flush_command, CMD_FLAG_ADMIN | CMD_FLAG_WRITE | CMD_FLAG_DELETE, 1, 1),
    CMD_DEF("SAVE", handle_save_command, CMD_FLAG_ADMIN, 1, 1),
    CMD_DEF("BGSAVE", handle_bgsave_command, CMD_FLAG_ADMIN, 1, 1),
    CMD_DEF("REPLCONF", handle_replconf_command, CMD_FLAG_ADMIN | CMD_FLAG_ALLOW_UNAUTH, 1, -1),
    CMD_DEF("PSYNC", handle_psync_command, CMD_FLAG_ADMIN | CMD_FLAG_ALLOW_UNAUTH, 1, -1),
};

static int register_builtin_commands(command_dispatcher_t *dispatcher) {
    size_t count = sizeof(builtin_commands) / sizeof(builtin_commands[0]);
    for (size_t i = 0; i < count; ++i) {
        if (command_dispatcher_register(dispatcher, &builtin_commands[i]) != 0) {
            return -1;
        }
    }
    return 0;
}

void command_session_init(command_session_t *session, command_context_t *ctx) {
    if (!session) {
        return;
    }
    session->ctx = ctx;
    if (ctx) {
        session->acl = ctx->acl;
    } else {
        acl_init(&session->acl, NULL, 0, 0);
    }
    acl_reset_session(&session->acl);
    session->resp_version = 2;
    session->connection_name[0] = '\0';
    session->persisted_connection_name[0] = '\0';
    session->has_persisted_name = false;
}

void command_session_reset(command_session_t *session) {
    if (!session) {
        return;
    }
    acl_reset_session(&session->acl);
    session->resp_version = 2;
    bool persist = false;
    if (session->ctx && session->ctx->config) {
        persist = session->ctx->config->persist_connection_names;
    }
    if (persist && session->has_persisted_name) {
        snprintf(session->connection_name,
                 sizeof(session->connection_name),
                 "%s",
                 session->persisted_connection_name);
    } else {
        session->connection_name[0] = '\0';
        if (!persist) {
            session->persisted_connection_name[0] = '\0';
            session->has_persisted_name = false;
        }
    }
}

acl_state_t *command_session_acl(command_session_t *session) {
    return session ? &session->acl : NULL;
}

int command_context_init(command_context_t *ctx, redis_store_t *store, redis_config_t *config) {
    if (!ctx) {
        return -1;
    }
    ctx->store = store;
    ctx->config = config;
    command_dispatcher_init(&ctx->dispatcher);
    command_event_dispatcher_init(&ctx->event_dispatcher);
    const redis_acl_user_t *users = (config && config->acl_user_count > 0) ? config->acl_users : NULL;
    size_t user_count = config ? config->acl_user_count : 0;
    size_t default_index = config ? config->acl_default_user : 0;
    acl_init(&ctx->acl, users, user_count, default_index);
    if (register_builtin_commands(&ctx->dispatcher) != 0) {
        command_dispatcher_free(&ctx->dispatcher);
        command_event_dispatcher_deinit(&ctx->event_dispatcher);
        return -1;
    }
    return 0;
}

void command_context_deinit(command_context_t *ctx) {
    if (!ctx) {
        return;
    }
    command_context_remove_listeners(ctx);
    command_event_dispatcher_deinit(&ctx->event_dispatcher);
    command_dispatcher_free(&ctx->dispatcher);
    ctx->store = NULL;
    ctx->config = NULL;
}

int command_context_add_listener(command_context_t *ctx,
                                 command_event_listener_fn fn,
                                 void *userdata) {
    if (!ctx) {
        return -1;
    }
    return command_event_dispatcher_add(&ctx->event_dispatcher, fn, userdata);
}

void command_context_remove_listeners(command_context_t *ctx) {
    if (!ctx) {
        return;
    }
    command_event_dispatcher_remove_all(&ctx->event_dispatcher);
}

static command_event_type_t identify_event_type(const command_spec_t *spec) {
    if (!spec) {
        return COMMAND_EVENT_GENERIC;
    }
    if (spec->flags & CMD_FLAG_DELETE) {
        return COMMAND_EVENT_DELETE;
    }
    if (spec->flags & CMD_FLAG_WRITE) {
        return COMMAND_EVENT_WRITE;
    }
    if (spec->flags & CMD_FLAG_EXPIRY) {
        return COMMAND_EVENT_EXPIRY;
    }
    return COMMAND_EVENT_GENERIC;
}

void command_handle(int client_fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
    if (!ctx || !cmd || cmd->argc == 0) {
        resp_send_error(client_fd, "ERR empty command");
        return;
    }

    command_session_t temp_session;
    if (!session) {
        command_session_init(&temp_session, ctx);
        session = &temp_session;
    }

    const command_spec_t *spec = command_dispatcher_find(&ctx->dispatcher, cmd->argv[0]);
    if (!spec) {
        resp_send_error(client_fd, "ERR unknown command");
        return;
    }

    if (command_spec_validate(spec, client_fd, cmd, ctx, session) != 0) {
        return;
    }

    int handler_rc = spec->handler(client_fd, cmd, ctx, session);

    command_event_t event = {
        .type = identify_event_type(spec),
        .command_name = spec->name,
        .command = cmd,
        .context = ctx,
        .handler_result = handler_rc,
    };
    command_event_dispatcher_dispatch(&ctx->event_dispatcher, &event);
}


