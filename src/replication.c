#include "replication.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define REPLICATION_BACKLOG_MAX_BYTES (1024 * 1024)

typedef struct replication_backlog_entry {
    char *payload;
    size_t length;
    command_event_type_t type;
    struct replication_backlog_entry *next;
} replication_backlog_entry_t;

typedef struct {
    command_context_t *ctx;
} replication_args_t;

static pthread_mutex_t backlog_lock = PTHREAD_MUTEX_INITIALIZER;
static replication_backlog_entry_t *backlog_head = NULL;
static replication_backlog_entry_t *backlog_tail = NULL;
static size_t backlog_bytes = 0;
static size_t backlog_count = 0;

static int replication_listener_registered = 0;

static int send_all(int fd, const char *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t rc = send(fd, buf + sent, len - sent, 0);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (rc == 0) {
            return -1;
        }
        sent += (size_t)rc;
    }
    return 0;
}

static void backlog_append(replication_backlog_entry_t *entry) {
    if (!entry) {
        return;
    }
    pthread_mutex_lock(&backlog_lock);
    if (!backlog_tail) {
        backlog_head = backlog_tail = entry;
    } else {
        backlog_tail->next = entry;
        backlog_tail = entry;
    }
    backlog_bytes += entry->length;
    ++backlog_count;

    while (backlog_bytes > REPLICATION_BACKLOG_MAX_BYTES && backlog_head) {
        replication_backlog_entry_t *old = backlog_head;
        backlog_head = old->next;
        if (!backlog_head) {
            backlog_tail = NULL;
        }
        backlog_bytes -= old->length;
        if (backlog_count > 0) {
            --backlog_count;
        }
        free(old->payload);
        free(old);
    }
    pthread_mutex_unlock(&backlog_lock);
}

void replication_backlog_clear(void) {
    pthread_mutex_lock(&backlog_lock);
    replication_backlog_entry_t *entry = backlog_head;
    while (entry) {
        replication_backlog_entry_t *next = entry->next;
        free(entry->payload);
        free(entry);
        entry = next;
    }
    backlog_head = backlog_tail = NULL;
    backlog_bytes = 0;
    backlog_count = 0;
    pthread_mutex_unlock(&backlog_lock);
}

size_t replication_backlog_command_count(void) {
    pthread_mutex_lock(&backlog_lock);
    size_t count = backlog_count;
    pthread_mutex_unlock(&backlog_lock);
    return count;
}

size_t replication_backlog_bytes(void) {
    pthread_mutex_lock(&backlog_lock);
    size_t bytes = backlog_bytes;
    pthread_mutex_unlock(&backlog_lock);
    return bytes;
}

int replication_backlog_stream(int fd) {
    pthread_mutex_lock(&backlog_lock);
    replication_backlog_entry_t *entry = backlog_head;
    while (entry) {
        if (send_all(fd, entry->payload, entry->length) != 0) {
            pthread_mutex_unlock(&backlog_lock);
            return -1;
        }
        entry = entry->next;
    }
    pthread_mutex_unlock(&backlog_lock);
    return 0;
}

static char *serialize_command(const resp_command_t *cmd, size_t *out_len) {
    if (!cmd || cmd->argc == 0) {
        return NULL;
    }
    size_t total = 0;
    total += (size_t)snprintf(NULL, 0, "*%zu\r\n", cmd->argc);
    for (size_t i = 0; i < cmd->argc; ++i) {
        size_t arg_len = strlen(cmd->argv[i]);
        total += (size_t)snprintf(NULL, 0, "$%zu\r\n", arg_len);
        total += arg_len + 2; // argument + CRLF
    }

    char *buffer = malloc(total + 1);
    if (!buffer) {
        return NULL;
    }

    size_t offset = (size_t)snprintf(buffer, total + 1, "*%zu\r\n", cmd->argc);
    for (size_t i = 0; i < cmd->argc; ++i) {
        const char *arg = cmd->argv[i];
        size_t arg_len = strlen(arg);
        offset += (size_t)snprintf(buffer + offset, total + 1 - offset, "$%zu\r\n", arg_len);
        memcpy(buffer + offset, arg, arg_len);
        offset += arg_len;
        buffer[offset++] = '\r';
        buffer[offset++] = '\n';
    }
    buffer[offset] = '\0';
    if (out_len) {
        *out_len = offset;
    }
    return buffer;
}

static void record_event(const command_event_t *event) {
    if (!event || !event->command || event->handler_result != 0) {
        return;
    }
    size_t length = 0;
    char *payload = serialize_command(event->command, &length);
    if (!payload || length == 0) {
        free(payload);
        return;
    }

    replication_backlog_entry_t *entry = malloc(sizeof(*entry));
    if (!entry) {
        free(payload);
        return;
    }
    entry->payload = payload;
    entry->length = length;
    entry->type = event->type;
    entry->next = NULL;

    backlog_append(entry);
    printf("Replication backlog enqueued %s (%zu bytes, total %zu)\n",
           event->command_name ? event->command_name : "unknown",
           length,
           replication_backlog_command_count());
}

static int replication_command_listener(const command_event_t *event, void *userdata) {
    (void)userdata;
    if (!event) {
        return 0;
    }
    switch (event->type) {
    case COMMAND_EVENT_WRITE:
    case COMMAND_EVENT_DELETE:
    case COMMAND_EVENT_EXPIRY:
        record_event(event);
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



