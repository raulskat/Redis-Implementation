#include "server.h"

#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define CLIENT_BUFFER_SIZE 8192

typedef struct {
    int fd;
    command_context_t *ctx;
} client_job_t;

static void *client_thread(void *arg) {
    client_job_t *job = (client_job_t *)arg;
    int fd = job->fd;
    command_context_t *ctx = job->ctx;
    free(job);

    char buffer[CLIENT_BUFFER_SIZE];
    size_t buffered = 0;

    while (1) {
        if (buffered == sizeof(buffer)) {
            resp_send_error(fd, "ERR command too large");
            break;
        }
        ssize_t received = recv(fd, buffer + buffered, sizeof(buffer) - buffered, 0);
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
                resp_send_error(fd, "ERR protocol error");
                resp_command_free(&cmd);
                offset = buffered; // drop buffered data
                break;
            }

            if (parse_status == RESP_PARSE_INCOMPLETE) {
                break; // need more data
            }

            command_handle(fd, &cmd, ctx);
            resp_command_free(&cmd);
            offset += consumed;
        }

        if (offset > 0) {
            memmove(buffer, buffer + offset, buffered - offset);
            buffered -= offset;
        }
    }

    close(fd);
    return NULL;
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

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("accept");
            break;
        }

        client_job_t *job = malloc(sizeof(client_job_t));
        if (!job) {
            close(client_fd);
            continue;
        }
        job->fd = client_fd;
        job->ctx = ctx;

        pthread_t thread_id;
        if (pthread_create(&thread_id, NULL, client_thread, job) != 0) {
            perror("pthread_create");
            close(client_fd);
            free(job);
            continue;
        }
        pthread_detach(thread_id);
    }

    close(server_fd);
    return 0;
}
