#include "command_handlers.h"

#include "acl.h"
#include "command_utils.h"
#include "datastore.h"
#include "persistence.h"
#include "resp.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

int handle_auth_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
    if (!ctx || !session) {
        return resp_send_error(fd, "ERR internal error");
    }
    if (cmd->argc != 2 && cmd->argc != 3) {
        return resp_send_error(fd, "ERR wrong number of arguments for 'auth' command");
    }

    const char *default_user =
        (ctx->config && ctx->config->requireuser[0] != '\0') ? ctx->config->requireuser : ACL_DEFAULT_USERNAME;

    const char *username = NULL;
    const char *password = NULL;
    if (cmd->argc == 2) {
        username = default_user;
        password = cmd->argv[1];
    } else {
        username = cmd->argv[1];
        password = cmd->argv[2];
    }

    if (!password) {
        return resp_send_error(fd, "ERR invalid password");
    }

    if (acl_authenticate(&session->acl, username, password) == 0) {
        return resp_send_simple_string(fd, "OK");
    }
    return resp_send_error(fd, "ERR invalid username-password pair");
}

int handle_config_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
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
    const char *keys[1] = {NULL};
    const char *values[1] = {NULL};
    int resp_version = session ? session->resp_version : 2;
    if (command_str_icmp(param, "dir") == 0) {
        keys[0] = "dir";
        values[0] = ctx->config->dir;
        return resp_send_string_map(fd, keys, values, 1, resp_version);
    }
    if (command_str_icmp(param, "dbfilename") == 0) {
        keys[0] = "dbfilename";
        values[0] = ctx->config->db_filename;
        return resp_send_string_map(fd, keys, values, 1, resp_version);
    }
    return resp_send_string_map(fd, NULL, NULL, 0, resp_version);
}

int handle_flush_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
    (void)cmd;
    (void)session;
    datastore_flush(ctx->store);
    return resp_send_simple_string(fd, "OK");
}

int handle_save_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
    (void)cmd;
    (void)session;
    int rc = persistence_save(ctx->store, ctx->config, PERSISTENCE_MODE_SYNC);
    if (rc == 0) {
        return resp_send_simple_string(fd, "OK");
    }
    if (rc == EBUSY) {
        return resp_send_error(fd, "ERR Background save already in progress");
    }
    return resp_send_error(fd, "ERR save failed");
}

int handle_bgsave_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
    (void)cmd;
    (void)session;
    int rc = persistence_save(ctx->store, ctx->config, PERSISTENCE_MODE_ASYNC);
    if (rc == 0) {
        return resp_send_simple_string(fd, "Background saving started");
    }
    if (rc == EBUSY) {
        return resp_send_error(fd, "ERR Background save already in progress");
    }
    return resp_send_error(fd, "ERR background save failed");
}
