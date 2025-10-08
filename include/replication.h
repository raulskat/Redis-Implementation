#ifndef REPLICATION_H
#define REPLICATION_H

#include "command.h"

int replication_start(command_context_t *ctx);
int replication_attach(command_context_t *ctx);

#endif // REPLICATION_H
