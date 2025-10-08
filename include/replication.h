#ifndef REPLICATION_H
#define REPLICATION_H

#include "command.h"

int replication_start(command_context_t *ctx);
int replication_attach(command_context_t *ctx);
size_t replication_backlog_command_count(void);
size_t replication_backlog_bytes(void);
void replication_backlog_clear(void);
int replication_backlog_stream(int fd);

#endif // REPLICATION_H
