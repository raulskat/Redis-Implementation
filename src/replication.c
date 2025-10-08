#include "replication.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct {
    command_context_t *ctx;
} replication_args_t;

static int replication_listener_registered = 0;

static int replication_command_listener(const command_event_t *event, void *userdata) {
    (void)userdata;
    if (!event || !event->command_name) {
        return 0;
    }
    if (event->handler_result != 0) {
        return 0;
    }

    switch (event->type) {
    case COMMAND_EVENT_WRITE:
    case COMMAND_EVENT_DELETE:
    case COMMAND_EVENT_EXPIRY:
        printf("Queued command for replication: %s\n", event->command_name);
        break;
    default:
        break;
    }
    return 0;
}

int replication_attach(command_context_t *ctx) {
    if (!ctx) {
        return -1;
    }
    if (replication_listener_registered) {
        return 0;
    }
    if (command_context_add_listener(ctx, replication_command_listener, ctx) != 0) {
        return -1;
    }
    replication_listener_registered = 1;
    return 0;
}

static int send_line(int fd, const char *line) {
    size_t len = strlen(line);
    return (send(fd, line, len, 0) == (ssize_t)len) ? 0 : -1;
}

static void *replication_thread(void *data) {
    replication_args_t *args = (replication_args_t *)data;
    command_context_t *ctx = args->ctx;
    free(args);

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", ctx->config->master_port);

    struct addrinfo *res = NULL;
    int rc = getaddrinfo(ctx->config->master_host, port_str, &hints, &res);
    if (rc != 0) {
        fprintf(stderr, "Failed to resolve master %s:%s: %s\n",
                ctx->config->master_host, port_str, gai_strerror(rc));
        return NULL;
    }

    int master_fd = -1;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        master_fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (master_fd < 0) {
            continue;
        }
        if (connect(master_fd, ai->ai_addr, ai->ai_addrlen) == 0) {
            break;
        }
        close(master_fd);
        master_fd = -1;
    }
    freeaddrinfo(res);

    if (master_fd < 0) {
        fprintf(stderr, "Unable to connect to master %s:%s\n", ctx->config->master_host, port_str);
        return NULL;
    }

    printf("Connected to master %s:%s\n", ctx->config->master_host, port_str);

    const char *ping = "*1\r\n$4\r\nPING\r\n";
    const char *listening = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6379\r\n";
    const char *capa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\nCAPA\r\n$6\r\npsync2\r\n";
    const char *psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";

    const char *commands[] = {ping, listening, capa, psync};
    for (size_t i = 0; i < sizeof(commands) / sizeof(commands[0]); ++i) {
        if (send_line(master_fd, commands[i]) != 0) {
            fprintf(stderr, "Failed to send replication command %zu\n", i);
            close(master_fd);
            return NULL;
        }
        char response[1024];
        ssize_t received = recv(master_fd, response, sizeof(response) - 1, 0);
        if (received <= 0) {
            fprintf(stderr, "Master closed connection during handshake\n");
            close(master_fd);
            return NULL;
        }
        response[received] = '\0';
        printf("Master response: %s\n", response);
    }

    // TODO: stream incoming data and apply to datastore

    close(master_fd);
    return NULL;
}

int replication_start(command_context_t *ctx) {
    if (!ctx || !ctx->config || !ctx->config->is_slave) {
        return replication_attach(ctx);
    }

    replication_args_t *args = malloc(sizeof(replication_args_t));
    if (!args) {
        return -1;
    }
    args->ctx = ctx;

    pthread_t thread_id;
    if (pthread_create(&thread_id, NULL, replication_thread, args) != 0) {
        fprintf(stderr, "Failed to start replication thread\n");
        free(args);
        return -1;
    }
    pthread_detach(thread_id);
    return 0;
}
