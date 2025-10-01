#include "command.h"
#include "command_handlers.h"
#include "command_utils.h"
#include "resp.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void command_context_init(command_context_t *ctx, redis_store_t *store, redis_config_t *config) {
    if (!ctx) {
        return;
    }
    ctx->store = store;
    ctx->config = config;
}

typedef struct {
    const char *name;
    command_handler_fn handler;
    int min_arity;
    int max_arity; // -1 means unlimited
} command_entry_t;

static const command_entry_t command_table[] = {
    {"PING", handle_ping_command, 1, 2},
    {"ECHO", handle_echo_command, 2, 2},
    {"SET", handle_set_command, 3, -1},
    {"GET", handle_get_command, 2, 2},
    {"CONFIG", handle_config_command, 2, -1},
    {"KEYS", handle_keys_command, 1, -1},
    {"INFO", handle_info_command, 1, 2},
    {"FLUSHALL", handle_flush_command, 1, 1},
    {"FLUSHDB", handle_flush_command, 1, 1},
    {"SAVE", handle_save_command, 1, 1},
    {"BGSAVE", handle_bgsave_command, 1, 1},
    {"REPLCONF", handle_replconf_command, 1, -1},
    {"PSYNC", handle_psync_command, 1, -1},
};

static const size_t command_count = sizeof(command_table) / sizeof(command_table[0]);

static void send_wrong_arity(int fd, const char *cmd_name) {
    char lower[64];
    size_t len = strlen(cmd_name);
    if (len >= sizeof(lower)) {
        len = sizeof(lower) - 1;
    }
    for (size_t i = 0; i < len; ++i) {
        lower[i] = (char)tolower((unsigned char)cmd_name[i]);
    }
    lower[len] = '\0';

    char message[128];
    snprintf(message, sizeof(message), "ERR wrong number of arguments for '%s' command", lower);
    resp_send_error(fd, message);
}

void command_handle(int client_fd, const resp_command_t *cmd, command_context_t *ctx) {
    if (!cmd || cmd->argc == 0) {
        resp_send_error(client_fd, "ERR empty command");
        return;
    }

    const command_entry_t *selected = NULL;
    for (size_t i = 0; i < command_count; ++i) {
        if (command_str_icmp(cmd->argv[0], command_table[i].name) == 0) {
            selected = &command_table[i];
            break;
        }
    }

    if (!selected) {
        resp_send_error(client_fd, "ERR unknown command");
        return;
    }

    if (selected->min_arity >= 0 && (int)cmd->argc < selected->min_arity) {
        send_wrong_arity(client_fd, selected->name);
        return;
    }
    if (selected->max_arity >= 0 && (int)cmd->argc > selected->max_arity) {
        send_wrong_arity(client_fd, selected->name);
        return;
    }

    selected->handler(client_fd, cmd, ctx);
}
