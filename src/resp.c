#include "resp.h"

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static ssize_t find_crlf(const char *buffer, size_t length, size_t offset, size_t *line_len) {
    for (size_t i = offset; i + 1 < length; ++i) {
        if (buffer[i] == '\r' && buffer[i + 1] == '\n') {
            if (line_len) {
                *line_len = i - offset;
            }
            return (ssize_t)(i + 2);
        }
    }
    return -1;
}

static long long parse_number(const char *data, size_t length, int *error) {
    long long value = 0;
    int sign = 1;
    size_t index = 0;

    if (length == 0) {
        if (error) {
            *error = 1;
        }
        return 0;
    }

    if (data[0] == '-') {
        sign = -1;
        index = 1;
    } else if (data[0] == '+') {
        index = 1;
    }

    for (; index < length; ++index) {
        if (!isdigit((unsigned char)data[index])) {
            if (error) {
                *error = 1;
            }
            return 0;
        }
        value = value * 10 + (data[index] - '0');
    }

    if (error) {
        *error = 0;
    }
    return value * sign;
}

int resp_parse(const char *buffer, size_t length, resp_command_t *out, size_t *consumed) {
    if (!buffer || !out) {
        return RESP_PARSE_ERROR;
    }

    if (length == 0) {
        return RESP_PARSE_INCOMPLETE;
    }

    size_t offset = 0;
    if (buffer[offset] != '*') {
        return RESP_PARSE_ERROR;
    }
    ++offset;

    size_t line_len = 0;
    ssize_t next_offset = find_crlf(buffer, length, offset, &line_len);
    if (next_offset < 0) {
        return RESP_PARSE_INCOMPLETE;
    }

    int err = 0;
    long long argc_ll = parse_number(buffer + offset, line_len, &err);
    if (err || argc_ll < 0) {
        return RESP_PARSE_ERROR;
    }
    size_t argc = (size_t)argc_ll;
    offset = (size_t)next_offset;

    char **argv = NULL;
    if (argc > 0) {
        argv = calloc(argc, sizeof(char *));
        if (!argv) {
            return RESP_PARSE_ERROR;
        }
    }

    for (size_t i = 0; i < argc; ++i) {
        if (offset >= length) {
            resp_command_t tmp = {.argv = argv, .argc = i};
            resp_command_free(&tmp);
            return RESP_PARSE_INCOMPLETE;
        }
        if (buffer[offset] != '$') {
            resp_command_t tmp = {.argv = argv, .argc = i};
            resp_command_free(&tmp);
            return RESP_PARSE_ERROR;
        }
        ++offset;

        next_offset = find_crlf(buffer, length, offset, &line_len);
        if (next_offset < 0) {
            resp_command_t tmp = {.argv = argv, .argc = i};
            resp_command_free(&tmp);
            return RESP_PARSE_INCOMPLETE;
        }

        long long bulk_len_ll = parse_number(buffer + offset, line_len, &err);
        if (err || bulk_len_ll < 0) {
            resp_command_t tmp = {.argv = argv, .argc = i};
            resp_command_free(&tmp);
            return RESP_PARSE_ERROR;
        }
        size_t bulk_len = (size_t)bulk_len_ll;
        offset = (size_t)next_offset;

        if (offset + bulk_len + 2 > length) { // +2 for CRLF
            resp_command_t tmp = {.argv = argv, .argc = i};
            resp_command_free(&tmp);
            return RESP_PARSE_INCOMPLETE;
        }

        char *arg = malloc(bulk_len + 1);
        if (!arg) {
            resp_command_t tmp = {.argv = argv, .argc = i};
            resp_command_free(&tmp);
            return RESP_PARSE_ERROR;
        }
        memcpy(arg, buffer + offset, bulk_len);
        arg[bulk_len] = '\0';
        argv[i] = arg;
        offset += bulk_len;

        if (buffer[offset] != '\r' || buffer[offset + 1] != '\n') {
            resp_command_t tmp = {.argv = argv, .argc = i + 1};
            resp_command_free(&tmp);
            return RESP_PARSE_ERROR;
        }
        offset += 2;
    }

    if (consumed) {
        *consumed = offset;
    }
    out->argv = argv;
    out->argc = argc;
    return RESP_PARSE_OK;
}

void resp_command_free(resp_command_t *cmd) {
    if (!cmd || !cmd->argv) {
        return;
    }
    for (size_t i = 0; i < cmd->argc; ++i) {
        free(cmd->argv[i]);
    }
    free(cmd->argv);
    cmd->argv = NULL;
    cmd->argc = 0;
}

static int send_formatted(int fd, const char *fmt, ...) {
    char buffer[512];
    va_list args;
    va_start(args, fmt);
    int len = vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    if (len < 0) {
        return -1;
    }
    if (len >= (int)sizeof(buffer)) {
        char *dynamic_buffer = malloc(len + 1);
        if (!dynamic_buffer) {
            return -1;
        }
        va_start(args, fmt);
        vsnprintf(dynamic_buffer, len + 1, fmt, args);
        va_end(args);
        ssize_t sent = send(fd, dynamic_buffer, len, 0);
        free(dynamic_buffer);
        return (sent == len) ? 0 : -1;
    }
    ssize_t sent = send(fd, buffer, len, 0);
    return (sent == len) ? 0 : -1;
}

int resp_send_simple_string(int fd, const char *msg) {
    return send_formatted(fd, "+%s\r\n", msg ? msg : "");
}

int resp_send_error(int fd, const char *msg) {
    return send_formatted(fd, "-%s\r\n", msg ? msg : "ERR");
}

int resp_send_bulk_string(int fd, const char *msg) {
    if (!msg) {
        return resp_send_null_bulk_string(fd);
    }
    size_t len = strlen(msg);
    return send_formatted(fd, "$%zu\r\n%s\r\n", len, msg);
}

int resp_send_null_bulk_string(int fd) {
    return send_formatted(fd, "$-1\r\n");
}

int resp_send_integer(int fd, long long value) {
    return send_formatted(fd, ":%lld\r\n", value);
}

int resp_send_array(int fd, const char *const *items, size_t count) {
    if (send_formatted(fd, "*%zu\r\n", count) != 0) {
        return -1;
    }
    for (size_t i = 0; i < count; ++i) {
        if (resp_send_bulk_string(fd, items[i]) != 0) {
            return -1;
        }
    }
    return 0;
}

int resp_send_null(int fd, int resp_version) {
    if (resp_version >= 3) {
        return send_formatted(fd, "_\r\n");
    }
    return resp_send_null_bulk_string(fd);
}

int resp_send_set(int fd, const char *const *items, size_t count) {
    if (send_formatted(fd, "~%zu\r\n", count) != 0) {
        return -1;
    }
    for (size_t i = 0; i < count; ++i) {
        if (items && items[i]) {
            if (resp_send_bulk_string(fd, items[i]) != 0) {
                return -1;
            }
        } else {
            if (resp_send_null_bulk_string(fd) != 0) {
                return -1;
            }
        }
    }
    return 0;
}

int resp_send_bool(int fd, int resp_version, bool value) {
    if (resp_version >= 3) {
        return send_formatted(fd, "#%c\r\n", value ? 't' : 'f');
    }
    return resp_send_integer(fd, value ? 1 : 0);
}

int resp_send_string_map(int fd,
                         const char *const *keys,
                         const char *const *values,
                         size_t count,
                         int resp_version) {
    if (resp_version >= 3) {
        if (send_formatted(fd, "%%%zu\r\n", count) != 0) {
            return -1;
        }
    } else {
        if (send_formatted(fd, "*%zu\r\n", count * 2) != 0) {
            return -1;
        }
    }

    for (size_t i = 0; i < count; ++i) {
        const char *key = (keys && keys[i]) ? keys[i] : "";
        const char *value = (values) ? values[i] : NULL;
        if (resp_send_bulk_string(fd, key) != 0) {
            return -1;
        }
        if (resp_send_bulk_string(fd, value) != 0) {
            return -1;
        }
    }
    return 0;
}
