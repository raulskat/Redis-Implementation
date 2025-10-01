#include "command_handlers.h"

#include "command_utils.h"
#include "datastore.h"
#include "persistence.h"
#include "resp.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

int handle_config_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    if (cmd->argc < 2) {
        return resp_send_error(fd, "ERR wrong number of arguments for 'config' command");
    }
    const char *sub = cmd->argv[1];
    if (command_str_icmp(sub, "GET") != 0) {
        return resp_send_error(fd, "ERR unknown subcommand");
    }
    if (cmd->argc < 3) {
        return resp_send_error(fd, "ERR wrong number of arguments for 'config get'");
    }
    const char *param = cmd->argv[2];
    const char *items[2] = {NULL, NULL};

    if (command_str_icmp(param, "dir") == 0) {
        items[0] = "dir";
        items[1] = ctx->config->dir;
        return resp_send_array(fd, items, 2);
    }
    if (command_str_icmp(param, "dbfilename") == 0) {
        items[0] = "dbfilename";
        items[1] = ctx->config->db_filename;
        return resp_send_array(fd, items, 2);
    }
    return resp_send_array(fd, NULL, 0);
}

int handle_flush_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    (void)cmd;
    datastore_flush(ctx->store);
    return resp_send_simple_string(fd, "OK");
}

int handle_save_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    (void)cmd;
    int rc = persistence_save_sync(ctx->store, ctx->config);
    if (rc == 0) {
        return resp_send_simple_string(fd, "OK");
    }
    if (rc == EBUSY) {
        return resp_send_error(fd, "ERR Background save already in progress");
    }
    return resp_send_error(fd, "ERR save failed");
}

int handle_bgsave_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    (void)cmd;
    int rc = persistence_save_async(ctx->store, ctx->config);
    if (rc == 0) {
        return resp_send_simple_string(fd, "Background saving started");
    }
    if (rc == EBUSY) {
        return resp_send_error(fd, "ERR Background save already in progress");
    }
    return resp_send_error(fd, "ERR background save failed");
}
