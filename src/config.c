#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void set_string(char *dest, size_t dest_size, const char *src) {
    if (!dest || !dest_size) {
        return;
    }
    if (!src) {
        dest[0] = '\0';
        return;
    }
    snprintf(dest, dest_size, "%s", src);
}

void config_init(redis_config_t *config) {
    if (!config) {
        return;
    }
    set_string(config->dir, sizeof(config->dir), "/tmp/rdbfile");
    set_string(config->db_filename, sizeof(config->db_filename), "dump.rdb");
    config->port = 6379;
    config->is_slave = false;
    set_string(config->master_host, sizeof(config->master_host), "127.0.0.1");
    config->master_port = 6379;
}

int config_parse_args(redis_config_t *config, int argc, char **argv) {
    if (!config) {
        return -1;
    }

    for (int i = 1; i < argc; ++i) {
        const char *arg = argv[i];
        if (strcmp(arg, "--dir") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--dir requires a value\n");
                return -1;
            }
            set_string(config->dir, sizeof(config->dir), argv[++i]);
        } else if (strcmp(arg, "--dbfilename") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--dbfilename requires a value\n");
                return -1;
            }
            set_string(config->db_filename, sizeof(config->db_filename), argv[++i]);
        } else if (strcmp(arg, "--port") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--port requires a value\n");
                return -1;
            }
            config->port = atoi(argv[++i]);
        } else if (strcmp(arg, "--replicaof") == 0) {
            if (i + 2 >= argc) {
                fprintf(stderr, "--replicaof requires host and port\n");
                return -1;
            }
            set_string(config->master_host, sizeof(config->master_host), argv[++i]);
            config->master_port = atoi(argv[++i]);
            config->is_slave = true;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", arg);
            return -1;
        }
    }

    return 0;
}
