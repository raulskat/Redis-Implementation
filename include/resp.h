#ifndef RESP_H
#define RESP_H

#include <stdbool.h>
#include <stddef.h>

typedef struct {
    char **argv;
    size_t argc;
} resp_command_t;

#define RESP_PARSE_OK 0
#define RESP_PARSE_INCOMPLETE 1
#define RESP_PARSE_ERROR -1

int resp_parse(const char *buffer, size_t length, resp_command_t *out, size_t *consumed);
void resp_command_free(resp_command_t *cmd);

int resp_send_simple_string(int fd, const char *msg);
int resp_send_error(int fd, const char *msg);
int resp_send_bulk_string(int fd, const char *msg);
int resp_send_null_bulk_string(int fd);
int resp_send_integer(int fd, long long value);
int resp_send_array(int fd, const char *const *items, size_t count);
int resp_send_null(int fd, int resp_version);
int resp_send_set(int fd, const char *const *items, size_t count);
int resp_send_bool(int fd, int resp_version, bool value);
int resp_send_string_map(int fd,
                         const char *const *keys,
                         const char *const *values,
                         size_t count,
                         int resp_version);

#endif // RESP_H
