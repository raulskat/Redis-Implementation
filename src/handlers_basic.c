#include "command_handlers.h"

#include "command_utils.h"
#include "resp.h"

#include <stdio.h>

int handle_ping_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    (void)ctx;
    if (cmd->argc > 1) {
        return resp_send_bulk_string(fd, cmd->argv[1]);
    }
    return resp_send_simple_string(fd, "PONG");
}

int handle_echo_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    (void)ctx;
    if (cmd->argc < 2) {
        return resp_send_error(fd, "ERR wrong number of arguments for 'echo' command");
    }
    return resp_send_bulk_string(fd, cmd->argv[1]);
}

int handle_info_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    const char *section = (cmd->argc > 1) ? cmd->argv[1] : NULL;
    if (section && command_str_icmp(section, "replication") != 0) {
        return resp_send_error(fd, "ERR unsupported INFO section");
    }
    char buffer[512];
    if (ctx->config->is_slave) {
        snprintf(buffer, sizeof(buffer),
                 "role:slave\nmaster_host:%s\nmaster_port:%d\n",
                 ctx->config->master_host,
                 ctx->config->master_port);
    } else {
        snprintf(buffer, sizeof(buffer),
                 "role:master\nmaster_replid:0000000000000000000000000000000000000000\nmaster_repl_offset:0\n");
    }
    return resp_send_bulk_string(fd, buffer);
}
