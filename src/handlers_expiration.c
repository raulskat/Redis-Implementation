#include "command_handlers.h"

#include "command_utils.h"
#include "datastore.h"
#include "resp.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

static int respond_with_integer(int fd, int value) {
    return resp_send_integer(fd, value);
}

int handle_expire_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    long long seconds = atoll(cmd->argv[2]);
    if (seconds <= 0) {
        int removed = datastore_expire_at(ctx->store, cmd->argv[1], datastore_current_time_ms());
        return respond_with_integer(fd, removed);
    }
    uint64_t ttl_ms = (uint64_t)seconds * 1000ULL;
    int updated = datastore_expire_in(ctx->store, cmd->argv[1], ttl_ms);
    return respond_with_integer(fd, updated);
}

int handle_pexpire_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    long long ttl = atoll(cmd->argv[2]);
    if (ttl <= 0) {
        int removed = datastore_expire_at(ctx->store, cmd->argv[1], datastore_current_time_ms());
        return respond_with_integer(fd, removed);
    }
    int updated = datastore_expire_in(ctx->store, cmd->argv[1], (uint64_t)ttl);
    return respond_with_integer(fd, updated);
}

static int send_ttl_response(int fd, long long ttl_ms, bool in_seconds) {
    if (ttl_ms < 0) {
        return resp_send_integer(fd, ttl_ms);
    }
    if (in_seconds) {
        ttl_ms /= 1000LL;
    }
    return resp_send_integer(fd, ttl_ms);
}

int handle_ttl_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    long long ttl_ms = datastore_ttl_ms(ctx->store, cmd->argv[1]);
    return send_ttl_response(fd, ttl_ms, true);
}

int handle_pttl_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    long long ttl_ms = datastore_ttl_ms(ctx->store, cmd->argv[1]);
    return resp_send_integer(fd, ttl_ms);
}

int handle_persist_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    int updated = datastore_persist_key(ctx->store, cmd->argv[1]);
    return respond_with_integer(fd, updated);
}
