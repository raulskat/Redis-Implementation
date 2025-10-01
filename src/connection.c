#include "connection.h"

#include "resp.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define CONNECTION_BUFFER_SIZE 8192

int connection_serve(int client_fd, command_context_t *ctx) {
    if (client_fd < 0 || !ctx) {
        return -1;
    }

    char buffer[CONNECTION_BUFFER_SIZE];
    size_t buffered = 0;

    while (1) {
        if (buffered == sizeof(buffer)) {
            resp_send_error(client_fd, "ERR command too large");
            break;
        }

        ssize_t received = recv(client_fd, buffer + buffered, sizeof(buffer) - buffered, 0);
        if (received == 0) {
            break; // client closed connection
        }
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("recv");
            break;
        }

        buffered += (size_t)received;
        size_t offset = 0;

        while (offset < buffered) {
            resp_command_t cmd = {0};
            size_t consumed = 0;
            int parse_status = resp_parse(buffer + offset, buffered - offset, &cmd, &consumed);

            if (parse_status == RESP_PARSE_ERROR) {
                resp_send_error(client_fd, "ERR protocol error");
                resp_command_free(&cmd);
                offset = buffered; // drop buffered data
                break;
            }

            if (parse_status == RESP_PARSE_INCOMPLETE) {
                break; // need more data
            }

            command_handle(client_fd, &cmd, ctx);
            resp_command_free(&cmd);
            offset += consumed;
        }

        if (offset > 0) {
            memmove(buffer, buffer + offset, buffered - offset);
            buffered -= offset;
        }
    }

    close(client_fd);
    return 0;
}
