#include "connection.h"
#include "resp.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define CONNECTION_BUFFER_SIZE 8192

struct connection_state {
    int fd;
    command_context_t *ctx;
    char buffer[CONNECTION_BUFFER_SIZE];
    size_t buffered;
};

static int process_buffer(connection_state_t *state) {
    size_t offset = 0;
    while (offset < state->buffered) {
        resp_command_t cmd = {0};
        size_t consumed = 0;
        int parse_status =
            resp_parse(state->buffer + offset, state->buffered - offset, &cmd, &consumed);

        if (parse_status == RESP_PARSE_ERROR) {
            resp_send_error(state->fd, "ERR protocol error");
            resp_command_free(&cmd);
            return -1;
        }

        if (parse_status == RESP_PARSE_INCOMPLETE) {
            break;
        }

        command_handle(state->fd, &cmd, state->ctx);
        resp_command_free(&cmd);
        offset += consumed;
    }

    if (offset > 0 && offset <= state->buffered) {
        memmove(state->buffer, state->buffer + offset, state->buffered - offset);
        state->buffered -= offset;
    }
    return 0;
}

connection_state_t *connection_state_create(int fd, command_context_t *ctx) {
    if (fd < 0 || !ctx) {
        return NULL;
    }
    connection_state_t *state = calloc(1, sizeof(*state));
    if (!state) {
        return NULL;
    }
    state->fd = fd;
    state->ctx = ctx;
    state->buffered = 0;
    return state;
}

void connection_state_destroy(connection_state_t *state) {
    if (!state) {
        return;
    }
    if (state->fd >= 0) {
        close(state->fd);
        state->fd = -1;
    }
    free(state);
}

static reactor_event_result_t handle_disconnect(connection_state_t *state) {
    connection_state_destroy(state);
    return REACTOR_EVENT_REMOVE;
}

reactor_event_result_t connection_handle_event(int fd, short events, void *userdata) {
    (void)fd;
    connection_state_t *state = (connection_state_t *)userdata;
    if (!state) {
        return REACTOR_EVENT_REMOVE;
    }

    if (events & (POLLHUP | POLLERR | POLLNVAL)) {
        return handle_disconnect(state);
    }

    while (1) {
        if (state->buffered == sizeof(state->buffer)) {
            resp_send_error(state->fd, "ERR command too large");
            return handle_disconnect(state);
        }

        ssize_t received =
            recv(state->fd,
                 state->buffer + state->buffered,
                 sizeof(state->buffer) - state->buffered,
                 0);
        if (received == 0) {
            return handle_disconnect(state);
        }
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            perror("recv");
            return handle_disconnect(state);
        }

        state->buffered += (size_t)received;
        if (process_buffer(state) != 0) {
            return handle_disconnect(state);
        }
    }

    return REACTOR_EVENT_CONTINUE;
}
