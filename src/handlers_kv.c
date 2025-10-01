#include "command_handlers.h"

#include "command_utils.h"
#include "datastore.h"
#include "resp.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int handle_set_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    if (cmd->argc < 3) {
        return resp_send_error(fd, "ERR wrong number of arguments for 'set' command");
    }

    const char *key = cmd->argv[1];
    const char *value = cmd->argv[2];
    uint64_t expiry_ms = 0;

    for (size_t i = 3; i < cmd->argc; ++i) {
        const char *option = cmd->argv[i];
        if (command_str_icmp(option, "PX") == 0) {
            if (i + 1 >= cmd->argc) {
                return resp_send_error(fd, "ERR syntax error");
            }
            long long ttl_ms = atoll(cmd->argv[++i]);
            if (ttl_ms <= 0) {
                return resp_send_error(fd, "ERR invalid expire time in set");
            }
            uint64_t now = datastore_current_time_ms();
            expiry_ms = now + (uint64_t)ttl_ms;
        } else if (command_str_icmp(option, "EX") == 0) {
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

int handle_get_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
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

int handle_keys_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    (void)cmd;
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
