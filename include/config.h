#ifndef CONFIG_H
#define CONFIG_H

#include <stdbool.h>

#define REDIS_PATH_MAX 512
#define REDIS_HOST_MAX 256

typedef struct {
    char dir[REDIS_PATH_MAX];
    char db_filename[REDIS_PATH_MAX];
    int port;
    bool is_slave;
    char master_host[REDIS_HOST_MAX];
    int master_port;
} redis_config_t;

void config_init(redis_config_t *config);
int config_parse_args(redis_config_t *config, int argc, char **argv);

#endif // CONFIG_H
