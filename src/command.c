#include "command.h"
#include "command_handlers.h"
#include "command_utils.h"
#include "resp.h"

#include <stdio.h>
#include <string.h>

#define COMMAND_DEF(NAME, HANDLER, MIN, MAX) \
    { .name = NAME, .handler = HANDLER, .validators = NULL, .validator_count = 0, .min_arity = MIN, .max_arity = MAX }

static int validate_config_command(const resp_command_t *cmd,
                                   const command_context_t *ctx,
                                   char *error_buf,
                                   size_t error_buf_len) {
    (void)ctx;
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
                                 char *error_buf,
                                 size_t error_buf_len) {
    (void)ctx;
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
    COMMAND_DEF("PING", handle_ping_command, 1, 2),
    COMMAND_DEF("ECHO", handle_echo_command, 2, 2),
    COMMAND_DEF("SET", handle_set_command, 3, -1),
    COMMAND_DEF("GET", handle_get_command, 2, 2),
    {.name = "CONFIG",
     .handler = handle_config_command,
     .validators = config_validators,
     .validator_count = sizeof(config_validators) / sizeof(config_validators[0]),
     .min_arity = 3,
     .max_arity = -1},
    COMMAND_DEF("KEYS", handle_keys_command, 1, -1),
    COMMAND_DEF("EXPIRE", handle_expire_command, 3, 3),
    COMMAND_DEF("PEXPIRE", handle_pexpire_command, 3, 3),
    COMMAND_DEF("TTL", handle_ttl_command, 2, 2),
    COMMAND_DEF("PTTL", handle_pttl_command, 2, 2),
    COMMAND_DEF("PERSIST", handle_persist_command, 2, 2),
    {.name = "INFO",
     .handler = handle_info_command,
     .validators = info_validators,
     .validator_count = sizeof(info_validators) / sizeof(info_validators[0]),
     .min_arity = 1,
     .max_arity = 2},
    COMMAND_DEF("FLUSHALL", handle_flush_command, 1, 1),
    COMMAND_DEF("FLUSHDB", handle_flush_command, 1, 1),
    COMMAND_DEF("SAVE", handle_save_command, 1, 1),
    COMMAND_DEF("BGSAVE", handle_bgsave_command, 1, 1),
    COMMAND_DEF("REPLCONF", handle_replconf_command, 1, -1),
    COMMAND_DEF("PSYNC", handle_psync_command, 1, -1),
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

int command_context_init(command_context_t *ctx, redis_store_t *store, redis_config_t *config) {
    if (!ctx) {
        return -1;
    }
    ctx->store = store;
    ctx->config = config;
    command_dispatcher_init(&ctx->dispatcher);
    command_event_dispatcher_init(&ctx->event_dispatcher);
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

static command_event_type_t identify_event_type(const char *command_name) {
    if (!command_name) {
        return COMMAND_EVENT_GENERIC;
    }
    if (command_str_icmp(command_name, "SET") == 0) {
        return COMMAND_EVENT_WRITE;
    }
    if (command_str_icmp(command_name, "FLUSHALL") == 0 ||
        command_str_icmp(command_name, "FLUSHDB") == 0) {
        return COMMAND_EVENT_DELETE;
    }
    if (command_str_icmp(command_name, "EXPIRE") == 0 ||
        command_str_icmp(command_name, "PEXPIRE") == 0 ||
        command_str_icmp(command_name, "PERSIST") == 0) {
        return COMMAND_EVENT_EXPIRY;
    }
    return COMMAND_EVENT_GENERIC;
}

void command_handle(int client_fd, const resp_command_t *cmd, command_context_t *ctx) {
    if (!ctx || !cmd || cmd->argc == 0) {
        resp_send_error(client_fd, "ERR empty command");
        return;
    }

    const command_spec_t *spec = command_dispatcher_find(&ctx->dispatcher, cmd->argv[0]);
    if (!spec) {
        resp_send_error(client_fd, "ERR unknown command");
        return;
    }

    if (command_spec_validate(spec, client_fd, cmd, ctx) != 0) {
        return;
    }

    int handler_rc = spec->handler(client_fd, cmd, ctx);

    command_event_t event = {
        .type = identify_event_type(spec->name),
        .command_name = spec->name,
        .command = cmd,
        .context = ctx,
        .handler_result = handler_rc,
    };
    command_event_dispatcher_dispatch(&ctx->event_dispatcher, &event);
}
