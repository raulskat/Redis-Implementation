#include "command_dispatcher.h"

#include "command.h"
#include "command_utils.h"
#include "resp.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define COMMAND_DISPATCHER_GROWTH 8

static int ensure_capacity(command_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return -1;
    }
    if (dispatcher->count < dispatcher->capacity) {
        return 0;
    }
    size_t new_capacity = dispatcher->capacity == 0 ? COMMAND_DISPATCHER_GROWTH
                                                    : dispatcher->capacity * 2;
    command_spec_t *new_entries = realloc(dispatcher->entries,
                                          new_capacity * sizeof(*new_entries));
    if (!new_entries) {
        return -1;
    }
    dispatcher->entries = new_entries;
    dispatcher->capacity = new_capacity;
    return 0;
}

void command_dispatcher_init(command_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return;
    }
    dispatcher->entries = NULL;
    dispatcher->count = 0;
    dispatcher->capacity = 0;
}

void command_dispatcher_free(command_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return;
    }
    free(dispatcher->entries);
    dispatcher->entries = NULL;
    dispatcher->count = 0;
    dispatcher->capacity = 0;
}

int command_dispatcher_register(command_dispatcher_t *dispatcher, const command_spec_t *spec) {
    if (!dispatcher || !spec || !spec->name || !spec->handler) {
        return -1;
    }
    for (size_t i = 0; i < dispatcher->count; ++i) {
        if (command_str_icmp(dispatcher->entries[i].name, spec->name) == 0) {
            return -1;
        }
    }
    if (ensure_capacity(dispatcher) != 0) {
        return -1;
    }
    dispatcher->entries[dispatcher->count] = *spec;
    ++dispatcher->count;
    return 0;
}

const command_spec_t *command_dispatcher_find(const command_dispatcher_t *dispatcher, const char *name) {
    if (!dispatcher || !name) {
        return NULL;
    }
    for (size_t i = 0; i < dispatcher->count; ++i) {
        if (command_str_icmp(dispatcher->entries[i].name, name) == 0) {
            return &dispatcher->entries[i];
        }
    }
    return NULL;
}

static void send_wrong_arity(int client_fd, const char *cmd_name) {
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
    snprintf(message, sizeof(message),
             "ERR wrong number of arguments for '%s' command", lower);
    resp_send_error(client_fd, message);
}

int command_spec_validate(const command_spec_t *spec,
                          int client_fd,
                          const resp_command_t *cmd,
                          const command_context_t *ctx,
                          const command_session_t *session) {
    if (!spec || !cmd) {
        return -1;
    }

    if (spec->min_arity >= 0 && (int)cmd->argc < spec->min_arity) {
        send_wrong_arity(client_fd, spec->name);
        return -1;
    }
    if (spec->max_arity >= 0 && (int)cmd->argc > spec->max_arity) {
        send_wrong_arity(client_fd, spec->name);
        return -1;
    }

    const acl_state_t *acl = NULL;
    if (session) {
        acl = &session->acl;
    } else if (ctx) {
        acl = &ctx->acl;
    }

    bool allow_unauthed = (spec->flags & CMD_FLAG_ALLOW_UNAUTH) != 0;
    if (!allow_unauthed) {
        if (!acl || (acl_requires_auth(acl) && !acl_is_authenticated(acl))) {
            resp_send_error(client_fd, "NOAUTH Authentication required.");
            return -1;
        }
    }

    unsigned int required_roles = 0;
    if (spec->flags & CMD_FLAG_ADMIN) {
        required_roles |= ACL_ROLE_ADMIN;
    }
    if (spec->flags & CMD_FLAG_WRITE) {
        required_roles |= ACL_ROLE_WRITE;
    }
    if (spec->flags & CMD_FLAG_READONLY) {
        required_roles |= ACL_ROLE_READ;
    }

    if (required_roles && acl) {
        if (!acl_is_authenticated(acl)) {
            if (!allow_unauthed) {
                resp_send_error(client_fd, "NOAUTH Authentication required.");
                return -1;
            }
        } else if (!acl_has_role(acl, required_roles)) {
            resp_send_error(client_fd, "NOPERM this user has no access to the command");
            return -1;
        }
    }

    if (spec->validator_count > 0 && spec->validators) {
        for (size_t i = 0; i < spec->validator_count; ++i) {
            const command_validator_t *validator = &spec->validators[i];
            if (!validator->fn) {
                continue;
            }
            char error_buf[128] = {0};
            int rc = validator->fn(cmd, ctx, session, error_buf, sizeof(error_buf));
            if (rc != 0) {
                if (error_buf[0] == '\0') {
                    snprintf(error_buf, sizeof(error_buf), "ERR invalid arguments");
                }
                resp_send_error(client_fd, error_buf);
                return -1;
            }
        }
    }
    return 0;
}
