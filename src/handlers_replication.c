#include "command_handlers.h"

#include "resp.h"

int handle_replconf_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    (void)cmd;
    (void)ctx;
    return resp_send_simple_string(fd, "OK");
}

int handle_psync_command(int fd, const resp_command_t *cmd, command_context_t *ctx) {
    (void)cmd;
    (void)ctx;
    if (resp_send_simple_string(fd, "FULLRESYNC 0000000000000000000000000000000000000000 0") != 0) {
        return -1;
    }
    return resp_send_bulk_string(fd, "");
}
