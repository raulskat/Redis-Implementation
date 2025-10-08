#ifndef CONFIG_H
#define CONFIG_H

#include <stdbool.h>
#include <stddef.h>

#define REDIS_PATH_MAX 512
#define REDIS_HOST_MAX 256
#define REDIS_PASSWORD_MAX 256
#define REDIS_USERNAME_MAX 128
#define REDIS_ACL_MAX_USERS 16

typedef struct {
    char username[REDIS_USERNAME_MAX];
    char password[REDIS_PASSWORD_MAX];
    unsigned int roles;
} redis_acl_user_t;

typedef struct {
    char dir[REDIS_PATH_MAX];
    char db_filename[REDIS_PATH_MAX];
    int port;
    bool is_slave;
    char master_host[REDIS_HOST_MAX];
    int master_port;
    char requireuser[REDIS_USERNAME_MAX];
    char requirepass[REDIS_PASSWORD_MAX];
    unsigned int require_roles;
    redis_acl_user_t acl_users[REDIS_ACL_MAX_USERS];
    size_t acl_user_count;
    size_t acl_default_user;
    bool persist_connection_names;
} redis_config_t;

void config_init(redis_config_t *config);
int config_parse_args(redis_config_t *config, int argc, char **argv);

#endif // CONFIG_H
