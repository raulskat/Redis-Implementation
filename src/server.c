#include "server.h"
#include "connection.h"
#include "reactor.h"

#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct {
    reactor_t *reactor;
    command_context_t *ctx;
} server_acceptor_t;

static int make_socket_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        return -1;
    }
    return 0;
}

static reactor_event_result_t server_accept_handler(int fd, short events, void *userdata) {
    server_acceptor_t *acceptor = (server_acceptor_t *)userdata;
    if (!acceptor || !acceptor->reactor || !acceptor->ctx) {
        return REACTOR_EVENT_REMOVE;
    }

    if (events & (POLLERR | POLLNVAL | POLLHUP)) {
        perror("acceptor socket error");
        return REACTOR_EVENT_REMOVE;
    }

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            perror("accept");
            return REACTOR_EVENT_REMOVE;
        }

        if (make_socket_nonblocking(client_fd) != 0) {
            perror("fcntl");
            close(client_fd);
            continue;
        }

        connection_state_t *state = connection_state_create(client_fd, acceptor->ctx);
        if (!state) {
            close(client_fd);
            continue;
        }

        if (reactor_add(acceptor->reactor,
                        client_fd,
                        POLLIN | POLLERR | POLLHUP,
                        connection_handle_event,
                        state) != 0) {
            connection_state_destroy(state);
            continue;
        }
    }

    return REACTOR_EVENT_CONTINUE;
}

int server_run(command_context_t *ctx) {
    if (!ctx || !ctx->config) {
        return -1;
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("socket");
        return -1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt");
        close(server_fd);
        return -1;
    }

    if (make_socket_nonblocking(server_fd) != 0) {
        perror("fcntl");
        close(server_fd);
        return -1;
    }

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons((uint16_t)ctx->config->port),
        .sin_addr = {htonl(INADDR_ANY)},
    };

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(server_fd);
        return -1;
    }

    if (listen(server_fd, 16) < 0) {
        perror("listen");
        close(server_fd);
        return -1;
    }

    printf("Redis server listening on port %d\n", ctx->config->port);

    reactor_t reactor;
    reactor_init(&reactor);

    server_acceptor_t acceptor = {
        .reactor = &reactor,
        .ctx = ctx,
    };

    if (reactor_add(&reactor, server_fd, POLLIN, server_accept_handler, &acceptor) != 0) {
        fprintf(stderr, "Failed to register server socket with reactor\n");
        reactor_deinit(&reactor);
        close(server_fd);
        return -1;
    }

    int rc = reactor_run(&reactor);
    reactor_deinit(&reactor);
    close(server_fd);
    return rc;
}
