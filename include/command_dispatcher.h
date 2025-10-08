#ifndef COMMAND_DISPATCHER_H
#define COMMAND_DISPATCHER_H

#include <stddef.h>
#include <stdint.h>

#include "resp.h"

typedef struct command_context_t command_context_t;
typedef struct command_session_t command_session_t;

typedef int (*command_handler_fn)(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);

typedef int (*command_validator_fn)(const resp_command_t *cmd,
                                    const command_context_t *ctx,
                                    const command_session_t *session,
                                    char *error_buf,
                                    size_t error_buf_len);

/* Command capability flags */
#define CMD_FLAG_WRITE (1u << 0)
#define CMD_FLAG_ADMIN (1u << 1)
#define CMD_FLAG_ALLOW_UNAUTH (1u << 2)
#define CMD_FLAG_EXPIRY (1u << 3)
#define CMD_FLAG_READONLY (1u << 4)
#define CMD_FLAG_DELETE (1u << 5)

typedef struct {
    command_validator_fn fn;
} command_validator_t;

typedef struct {
    const char *name;
    command_handler_fn handler;
    const command_validator_t *validators;
    size_t validator_count;
    uint32_t flags;
    int min_arity;
    int max_arity; /* -1 means unlimited */
} command_spec_t;

typedef struct {
    command_spec_t *entries;
    size_t count;
    size_t capacity;
} command_dispatcher_t;

void command_dispatcher_init(command_dispatcher_t *dispatcher);
void command_dispatcher_free(command_dispatcher_t *dispatcher);
int command_dispatcher_register(command_dispatcher_t *dispatcher, const command_spec_t *spec);
const command_spec_t *command_dispatcher_find(const command_dispatcher_t *dispatcher, const char *name);

int command_spec_validate(const command_spec_t *spec,
                          int client_fd,
                          const resp_command_t *cmd,
                          const command_context_t *ctx,
                          const command_session_t *session);

#endif /* COMMAND_DISPATCHER_H */
