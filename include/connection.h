#ifndef CONNECTION_H
#define CONNECTION_H

#include "command.h"
#include "reactor.h"

typedef struct connection_state connection_state_t;

connection_state_t *connection_state_create(int fd, command_context_t *ctx);
void connection_state_destroy(connection_state_t *state);
reactor_event_result_t connection_handle_event(int fd, short events, void *userdata);

#endif // CONNECTION_H
