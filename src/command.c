#include "command.h"
#include "command_handlers.h"
#include "command_utils.h"
#include "resp.h"

#include <stdio.h>
#include <string.h>

#define COMMAND_DEF(NAME, HANDLER, MIN, MAX) \
    { .name = NAME, .handler = HANDLER, .validator = NULL, .min_arity = MIN, .max_arity = MAX }
#define COMMAND_DEF_WITH_VALIDATOR(NAME, HANDLER, MIN, MAX, VALIDATOR) \
    { .name = NAME, .handler = HANDLER, .validator = VALIDATOR, .min_arity = MIN, .max_arity = MAX }

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

static const command_spec_t builtin_commands[] = {
    COMMAND_DEF("PING", handle_ping_command, 1, 2),
    COMMAND_DEF("ECHO", handle_echo_command, 2, 2),
    COMMAND_DEF("SET", handle_set_command, 3, -1),
    COMMAND_DEF("GET", handle_get_command, 2, 2),
    COMMAND_DEF_WITH_VALIDATOR("CONFIG", handle_config_command, 3, -1, validate_config_command),
    COMMAND_DEF("KEYS", handle_keys_command, 1, -1),
    COMMAND_DEF("EXPIRE", handle_expire_command, 3, 3),
    COMMAND_DEF("PEXPIRE", handle_pexpire_command, 3, 3),
    COMMAND_DEF("TTL", handle_ttl_command, 2, 2),
    COMMAND_DEF("PTTL", handle_pttl_command, 2, 2),
    COMMAND_DEF("PERSIST", handle_persist_command, 2, 2),
    COMMAND_DEF_WITH_VALIDATOR("INFO", handle_info_command, 1, 2, validate_info_command),
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
    if (register_builtin_commands(&ctx->dispatcher) != 0) {
        command_dispatcher_free(&ctx->dispatcher);
        return -1;
    }
    return 0;
}

void command_context_deinit(command_context_t *ctx) {
    if (!ctx) {
        return;
    }
    command_dispatcher_free(&ctx->dispatcher);
    ctx->store = NULL;
    ctx->config = NULL;
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

    spec->handler(client_fd, cmd, ctx);
}
