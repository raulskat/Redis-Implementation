#ifndef CONNECTION_H
#define CONNECTION_H

#include "command.h"

int connection_serve(int client_fd, command_context_t *ctx);

#endif // CONNECTION_H
