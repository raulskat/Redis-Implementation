#include "command.h"
#include "persistence.h"

#include <errno.h>
#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#ifndef _WIN32
#include <strings.h>
#endif
#include <string.h>

static int str_icmp(const char *a, const char *b) {
#ifdef _WIN32
    return _stricmp(a, b);
#else
    return strcasecmp(a, b);
#endif
}

static const char *safe_arg(const resp_command_t *cmd, size_t index) {
    return (index < cmd->argc) ? cmd->argv[index] : NULL;
}

void command_context_init(command_context_t *ctx, redis_store_t *store, redis_config_t *config) {
    if (!ctx) {
        return;
    }
    ctx->store = store;
    ctx->config = config;
}

static int handle_ping(int fd, const resp_command_t *cmd) {
    if (cmd->argc > 1) {
        return resp_send_bulk_string(fd, cmd->argv[1]);
    }
    return resp_send_simple_string(fd, "PONG");
}

static int handle_echo(int fd, const resp_command_t *cmd) {
    if (cmd->argc < 2) {
        return resp_send_error(fd, "ERR wrong number of arguments for 'echo' command");
    }
    return resp_send_bulk_string(fd, cmd->argv[1]);
}

static int handle_set(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    if (cmd->argc < 3) {
        return resp_send_error(fd, "ERR wrong number of arguments for 'set' command");
    }

    const char *key = cmd->argv[1];
    const char *value = cmd->argv[2];
    uint64_t expiry_ms = 0;

    for (size_t i = 3; i < cmd->argc; ++i) {
        const char *option = cmd->argv[i];
        if (str_icmp(option, "PX") == 0) {
            if (i + 1 >= cmd->argc) {
                return resp_send_error(fd, "ERR syntax error");
            }
            long long ttl_ms = atoll(cmd->argv[++i]);
            if (ttl_ms <= 0) {
                return resp_send_error(fd, "ERR invalid expire time in set");
            }
            uint64_t now = datastore_current_time_ms();
            expiry_ms = now + (uint64_t)ttl_ms;
        } else if (str_icmp(option, "EX") == 0) {
            if (i + 1 >= cmd->argc) {
                return resp_send_error(fd, "ERR syntax error");
            }
            long long ttl_s = atoll(cmd->argv[++i]);
            if (ttl_s <= 0) {
                return resp_send_error(fd, "ERR invalid expire time in set");
            }
            uint64_t now = datastore_current_time_ms();
            expiry_ms = now + (uint64_t)ttl_s * 1000ULL;
        } else {
            return resp_send_error(fd, "ERR unsupported option for SET");
        }
    }

    if (datastore_set(ctx->store, key, value, expiry_ms) != 0) {
        return resp_send_error(fd, "ERR failed to set key");
    }
    return resp_send_simple_string(fd, "OK");
}

static int handle_get(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    if (cmd->argc < 2) {
        return resp_send_error(fd, "ERR wrong number of arguments for 'get' command");
    }
    char *value = NULL;
    if (datastore_get(ctx->store, cmd->argv[1], &value) != 0) {
        return resp_send_null_bulk_string(fd);
    }
    int result = resp_send_bulk_string(fd, value);
    free(value);
    return result;
}

static int handle_config(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    if (cmd->argc < 2) {
        return resp_send_error(fd, "ERR wrong number of arguments for 'config' command");
    }
    const char *sub = cmd->argv[1];
    if (str_icmp(sub, "GET") != 0) {
        return resp_send_error(fd, "ERR unknown subcommand");
    }
    if (cmd->argc < 3) {
        return resp_send_error(fd, "ERR wrong number of arguments for 'config get'");
    }
    const char *param = cmd->argv[2];
    const char *items[2] = {NULL, NULL};

    if (str_icmp(param, "dir") == 0) {
        items[0] = "dir";
        items[1] = ctx->config->dir;
        return resp_send_array(fd, items, 2);
    }
    if (str_icmp(param, "dbfilename") == 0) {
        items[0] = "dbfilename";
        items[1] = ctx->config->db_filename;
        return resp_send_array(fd, items, 2);
    }
    return resp_send_array(fd, NULL, 0);
}

static int handle_keys(int fd, command_context_t *ctx) {
    char **keys = NULL;
    size_t count = 0;
    if (datastore_keys(ctx->store, &keys, &count) != 0) {
        return resp_send_error(fd, "ERR failed to enumerate keys");
    }
    int status = 0;
    if (count == 0) {
        status = resp_send_array(fd, NULL, 0);
    } else {
        const char **items = (const char **)keys;
        status = resp_send_array(fd, items, count);
    }
    for (size_t i = 0; i < count; ++i) {
        free(keys[i]);
    }
    free(keys);
    return status;
}

static int handle_info(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    const char *section = safe_arg(cmd, 1);
    if (section && str_icmp(section, "replication") != 0) {
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

static int handle_flush(int fd, command_context_t *ctx) {
    datastore_flush(ctx->store);
    return resp_send_simple_string(fd, "OK");
}

static int handle_save(int fd, command_context_t *ctx) {
    int rc = persistence_save_sync(ctx->store, ctx->config);
    if (rc == 0) {
        return resp_send_simple_string(fd, "OK");
    }
    if (rc == EBUSY) {
        return resp_send_error(fd, "ERR Background save already in progress");
    }
    return resp_send_error(fd, "ERR save failed");
}

static int handle_bgsave(int fd, command_context_t *ctx) {
    int rc = persistence_save_async(ctx->store, ctx->config);
    if (rc == 0) {
        return resp_send_simple_string(fd, "Background saving started");
    }
    if (rc == EBUSY) {
        return resp_send_error(fd, "ERR Background save already in progress");
    }
    return resp_send_error(fd, "ERR background save failed");
}

static int handle_replconf(int fd) {
    return resp_send_simple_string(fd, "OK");
}

static int handle_psync(int fd) {
    if (resp_send_simple_string(fd, "FULLRESYNC 0000000000000000000000000000000000000000 0") != 0) {
        return -1;
    }
    // Empty RDB payload to complete replication handshake
    return resp_send_bulk_string(fd, "");
}

void command_handle(int client_fd, const resp_command_t *cmd, command_context_t *ctx) {
    if (!cmd || cmd->argc == 0) {
        resp_send_error(client_fd, "ERR empty command");
        return;
    }

    const char *verb = cmd->argv[0];
    if (str_icmp(verb, "PING") == 0) {
        handle_ping(client_fd, cmd);
        return;
    }
    if (str_icmp(verb, "ECHO") == 0) {
        handle_echo(client_fd, cmd);
        return;
    }
    if (str_icmp(verb, "SET") == 0) {
        handle_set(client_fd, cmd, ctx);
        return;
    }
    if (str_icmp(verb, "GET") == 0) {
        handle_get(client_fd, cmd, ctx);
        return;
    }
    if (str_icmp(verb, "CONFIG") == 0) {
        handle_config(client_fd, cmd, ctx);
        return;
    }
    if (str_icmp(verb, "KEYS") == 0) {
        handle_keys(client_fd, ctx);
        return;
    }
    if (str_icmp(verb, "INFO") == 0) {
        handle_info(client_fd, cmd, ctx);
        return;
    }
    if (str_icmp(verb, "FLUSHALL") == 0 || str_icmp(verb, "FLUSHDB") == 0) {
        if (cmd->argc != 1) {
            const char *name = str_icmp(verb, "FLUSHALL") == 0 ? "flushall" : "flushdb";
            char err[128];
            snprintf(err, sizeof(err), "ERR wrong number of arguments for '%s' command", name);
            resp_send_error(client_fd, err);
        } else {
            handle_flush(client_fd, ctx);
        }
        return;
    }
    if (str_icmp(verb, "SAVE") == 0) {
        if (cmd->argc != 1) {
            resp_send_error(client_fd, "ERR wrong number of arguments for 'save' command");
        } else {
            handle_save(client_fd, ctx);
        }
        return;
    }
    if (str_icmp(verb, "BGSAVE") == 0) {
        if (cmd->argc != 1) {
            resp_send_error(client_fd, "ERR wrong number of arguments for 'bgsave' command");
        } else {
            handle_bgsave(client_fd, ctx);
        }
        return;
    }
    if (str_icmp(verb, "REPLCONF") == 0) {
        handle_replconf(client_fd);
        return;
    }
    if (str_icmp(verb, "PSYNC") == 0) {
        handle_psync(client_fd);
        return;
    }

    resp_send_error(client_fd, "ERR unknown command");
}
