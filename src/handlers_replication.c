#include "command_handlers.h"
#include "replication.h"

#include "resp.h"

int handle_replconf_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
    (void)cmd;
    (void)ctx; (void)session;
    return resp_send_simple_string(fd, "OK");
}

int handle_psync_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session) {
    (void)session;
    (void)ctx;
    (void)cmd;
    if (resp_send_simple_string(fd, "FULLRESYNC 0000000000000000000000000000000000000000 0") != 0) {
        return -1;
    }
    if (resp_send_bulk_string(fd, "") != 0) {
        return -1;
    }
    if (replication_backlog_stream(fd) != 0) {
        return -1;
    }
    return 0;
}
