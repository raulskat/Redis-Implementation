#ifndef RESP_H
#define RESP_H

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

#endif // RESP_H
