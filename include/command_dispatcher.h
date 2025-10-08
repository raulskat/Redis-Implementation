#ifndef COMMAND_DISPATCHER_H
#define COMMAND_DISPATCHER_H

#include <stddef.h>

#include "resp.h"

typedef struct command_context_t command_context_t;

typedef int (*command_handler_fn)(int fd, const resp_command_t *cmd, command_context_t *ctx);

typedef int (*command_validator_fn)(const resp_command_t *cmd,
                                    const command_context_t *ctx,
                                    char *error_buf,
                                    size_t error_buf_len);

typedef struct {
    const char *name;
    command_handler_fn handler;
    command_validator_fn validator;
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
                          const command_context_t *ctx);

#endif /* COMMAND_DISPATCHER_H */
