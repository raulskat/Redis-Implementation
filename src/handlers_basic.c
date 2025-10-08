#include "command_handlers.h"

#include "acl.h"
#include "command_utils.h"
#include "resp.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

static int send_all(int fd, const char *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t rc = send(fd, buf + sent, len - sent, 0);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (rc == 0) {
            return -1;
        }
        sent += (size_t)rc;
    }
    return 0;
}

static int send_resp3_hello(int fd, command_session_t *session, command_context_t *ctx) {
    const char *role = (ctx && ctx->config && ctx->config->is_slave) ? "slave" : "master";
    const char *mode = "standalone";
    const char *username = session ? session->acl.username : ACL_DEFAULT_USERNAME;
    int authenticated = acl_is_authenticated(session ? &session->acl : NULL) ? 1 : 0;
    const char *connection_name =
        (session && session->connection_name[0] != '\0') ? session->connection_name : "";

    char buffer[512];
    int len = snprintf(buffer, sizeof(buffer),
                       "%%8\r\n"
                       "$5\r\nproto\r\n:%d\r\n"
                       "$6\r\nserver\r\n$5\r\nredis\r\n"
                       "$7\r\nversion\r\n$3\r\n0.1\r\n"
                       "$4\r\nrole\r\n$%zu\r\n%s\r\n"
                       "$4\r\nmode\r\n$%zu\r\n%s\r\n"
                       "$12\r\nauthenticated\r\n:%d\r\n"
                       "$8\r\nusername\r\n$%zu\r\n%s\r\n"
                       "$13\r\nconnection_name\r\n$%zu\r\n%s\r\n"
                       "$2\r\nid\r\n:0\r\n",
                       session ? session->resp_version : 3,
                       strlen(role), role,
                       strlen(mode), mode,
                       authenticated,
                       strlen(username), username,
                       strlen(connection_name), connection_name);
    if (len < 0 || len >= (int)sizeof(buffer)) {
        return -1;
    }
    return send_all(fd, buffer, (size_t)len);
}

static int send_resp3_info(int fd, const command_context_t *ctx) {
    const char *role = (ctx && ctx->config && ctx->config->is_slave) ? "slave" : "master";
    const char *host = (ctx && ctx->config && ctx->config->is_slave) ? ctx->config->master_host : "";
    int port = (ctx && ctx->config && ctx->config->is_slave) ? ctx->config->master_port : 0;

    char buffer[512];
    int len = snprintf(buffer, sizeof(buffer),
                       "%%3\r\n"
                       "$4\r\nrole\r\n$%zu\r\n%s\r\n"
                       "$11\r\nmaster_host\r\n$%zu\r\n%s\r\n"
                       "$11\r\nmaster_port\r\n:%d\r\n",
                       strlen(role), role,
                       strlen(host), host,
                       port);
    if (len < 0 || len >= (int)sizeof(buffer)) {
        return -1;
    }
    return send_all(fd, buffer, (size_t)len);
}

int handle_ping_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
    (void)ctx;
    (void)session;
    if (cmd->argc > 1) {
        return resp_send_bulk_string(fd, cmd->argv[1]);
    }
    return resp_send_simple_string(fd, "PONG");
}

int handle_echo_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
    (void)ctx;
    (void)session;
    if (cmd->argc < 2) {
        return resp_send_error(fd, "ERR wrong number of arguments for 'echo' command");
    }
    return resp_send_bulk_string(fd, cmd->argv[1]);
}

int handle_hello_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
    if (!session) {
        return resp_send_error(fd, "ERR internal error");
    }

    int protocol = 2;
    if (cmd->argc >= 2) {
        char *endptr = NULL;
        long parsed = strtol(cmd->argv[1], &endptr, 10);
        if (!endptr || *endptr != '\0') {
            return resp_send_error(fd, "ERR invalid protocol version");
        }
        protocol = (int)parsed;
    }
    if (protocol != 2 && protocol != 3) {
        return resp_send_error(fd, "ERR unsupported protocol version");
    }

    size_t i = (cmd->argc >= 2) ? 2 : 1;
    while (i < cmd->argc) {
        const char *option = cmd->argv[i++];
        if (command_str_icmp(option, "AUTH") == 0) {
            const char *username = NULL;
            const char *password = NULL;
            if (i < cmd->argc) {
                username = cmd->argv[i++];
            } else {
                return resp_send_error(fd, "ERR HELLO missing username");
            }
            if (i < cmd->argc) {
                password = cmd->argv[i++];
            } else {
                return resp_send_error(fd, "ERR HELLO missing password");
            }
            if (acl_authenticate(&session->acl, username, password) != 0) {
                return resp_send_error(fd, "WRONGPASS invalid username-password pair");
            }
        } else if (command_str_icmp(option, "SETNAME") == 0) {
            if (i >= cmd->argc) {
                return resp_send_error(fd, "ERR HELLO missing connection name");
            }
            const char *name = cmd->argv[i++];
            if (session) {
                snprintf(session->connection_name, sizeof(session->connection_name), "%s", name);
                snprintf(session->persisted_connection_name,
                         sizeof(session->persisted_connection_name),
                         "%s",
                         name);
                session->has_persisted_name = (name && name[0] != '\0');
            }
        } else {
            if (i < cmd->argc) {
                ++i;
            }
        }
    }

    session->resp_version = protocol;

    if (protocol == 3) {
        return send_resp3_hello(fd, session, ctx);
    }

    char buffer[32];
    snprintf(buffer, sizeof(buffer), "HELLO %d", protocol);
    return resp_send_simple_string(fd, buffer);
}

int handle_info_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
    const char *section = (cmd->argc > 1) ? cmd->argv[1] : NULL;
    if (section && command_str_icmp(section, "replication") != 0) {
        return resp_send_error(fd, "ERR unsupported INFO section");
    }

    if (session && session->resp_version == 3) {
        return send_resp3_info(fd, ctx);
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
