#include "config.h"

#include "acl.h"

#include <ctype.h>
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

static int str_icmp(const char *a, const char *b) {
    while (*a && *b) {
        int diff = tolower((unsigned char)*a) - tolower((unsigned char)*b);
        if (diff != 0) {
            return diff;
        }
        ++a;
        ++b;
    }
    return tolower((unsigned char)*a) - tolower((unsigned char)*b);
}

static unsigned int parse_role_token(const char *token, int *error) {
    if (!token) {
        if (error) {
            *error = 1;
        }
        return 0;
    }
    if (str_icmp(token, "read") == 0) {
        return ACL_ROLE_READ;
    }
    if (str_icmp(token, "write") == 0) {
        return ACL_ROLE_WRITE;
    }
    if (str_icmp(token, "admin") == 0) {
        return ACL_ROLE_ADMIN;
    }
    if (str_icmp(token, "all") == 0) {
        return ACL_ROLE_ALL;
    }
    if (error) {
        *error = 1;
    }
    return 0;
}

static unsigned int parse_role_list(const char *roles_str, int *error) {
    if (!roles_str || !roles_str[0]) {
        if (error) {
            *error = 1;
        }
        return ACL_ROLE_ALL;
    }
    unsigned int roles = 0;
    char buffer[256];
    snprintf(buffer, sizeof(buffer), "%s", roles_str);
    char *saveptr = NULL;
    char *token = strtok_r(buffer, ",", &saveptr);
    while (token) {
        while (*token && isspace((unsigned char)*token)) {
            ++token;
        }
        char *end = token + strlen(token);
        while (end > token && isspace((unsigned char)end[-1])) {
            *--end = '\0';
        }
        int parse_err = 0;
        unsigned int mask = parse_role_token(token, &parse_err);
        if (parse_err) {
            if (error) {
                *error = 1;
            }
            return 0;
        }
        if (mask == ACL_ROLE_ALL) {
            roles = ACL_ROLE_ALL;
            break;
        }
        roles |= mask;
        token = strtok_r(NULL, ",", &saveptr);
    }
    if (roles == 0) {
        roles = ACL_ROLE_ALL;
    }
    return roles;
}

static redis_acl_user_t *config_find_acl_user(redis_config_t *config, const char *username) {
    if (!config || !username) {
        return NULL;
    }
    for (size_t i = 0; i < config->acl_user_count; ++i) {
        if (strcmp(config->acl_users[i].username, username) == 0) {
            return &config->acl_users[i];
        }
    }
    return NULL;
}

static redis_acl_user_t *config_add_acl_user(redis_config_t *config, const char *username) {
    if (!config) {
        return NULL;
    }
    if (config->acl_user_count >= REDIS_ACL_MAX_USERS) {
        fprintf(stderr, "Maximum ACL users reached (%d)\n", REDIS_ACL_MAX_USERS);
        return NULL;
    }
    redis_acl_user_t *user = &config->acl_users[config->acl_user_count++];
    memset(user, 0, sizeof(*user));
    const char *name = (username && username[0] != '\0') ? username : ACL_DEFAULT_USERNAME;
    set_string(user->username, sizeof(user->username), name);
    set_string(user->password, sizeof(user->password), "");
    user->roles = ACL_ROLE_ALL;
    return user;
}

static redis_acl_user_t *config_ensure_acl_user(redis_config_t *config,
                                                const char *username,
                                                bool *created_out) {
    if (created_out) {
        *created_out = false;
    }
    redis_acl_user_t *user = config_find_acl_user(config, username);
    if (user) {
        return user;
    }
    user = config_add_acl_user(config, username);
    if (user && created_out) {
        *created_out = true;
    }
    return user;
}

static redis_acl_user_t *config_default_user(redis_config_t *config) {
    if (!config || config->acl_user_count == 0) {
        return NULL;
    }
    if (config->acl_default_user >= config->acl_user_count) {
        config->acl_default_user = 0;
    }
    return &config->acl_users[config->acl_default_user];
}

static void config_sync_default_user(redis_config_t *config) {
    if (!config) {
        return;
    }
    redis_acl_user_t *user = config_default_user(config);
    if (!user) {
        set_string(config->requireuser, sizeof(config->requireuser), ACL_DEFAULT_USERNAME);
        set_string(config->requirepass, sizeof(config->requirepass), "");
        config->require_roles = ACL_ROLE_ALL;
        return;
    }
    set_string(config->requireuser, sizeof(config->requireuser), user->username);
    set_string(config->requirepass, sizeof(config->requirepass), user->password);
    config->require_roles = (user->roles == 0) ? ACL_ROLE_ALL : user->roles;
}

static void config_set_default_user(redis_config_t *config, redis_acl_user_t *user) {
    if (!config || !user) {
        return;
    }
    size_t index = (size_t)(user - config->acl_users);
    config->acl_default_user = index;
    config_sync_default_user(config);
}

static redis_acl_user_t *config_ensure_default_user(redis_config_t *config) {
    if (!config) {
        return NULL;
    }
    redis_acl_user_t *user = config_default_user(config);
    if (user) {
        return user;
    }
    user = config_add_acl_user(config, ACL_DEFAULT_USERNAME);
    if (!user) {
        return NULL;
    }
    config->acl_default_user = config->acl_user_count - 1;
    config_sync_default_user(config);
    return user;
}

static int parse_acl_user_definition(const char *definition,
                                     char *username,
                                     size_t username_len,
                                     char *password,
                                     size_t password_len,
                                     unsigned int *roles_out) {
    if (!definition || !username || !password || !roles_out) {
        return -1;
    }
    username[0] = '\0';
    password[0] = '\0';
    *roles_out = ACL_ROLE_ALL;

    const char *eq = strchr(definition, '=');
    const char *colon = strchr(definition, ':');
    if (colon && eq && colon < eq) {
        return -1;
    }

    const char *name_end = eq ? eq : (colon ? colon : definition + strlen(definition));
    size_t name_len = (size_t)(name_end - definition);
    if (name_len == 0 || name_len >= username_len) {
        return -1;
    }
    memcpy(username, definition, name_len);
    username[name_len] = '\0';

    if (eq) {
        const char *pass_start = eq + 1;
        const char *pass_end = colon ? colon : definition + strlen(definition);
        size_t pass_len = (size_t)(pass_end - pass_start);
        if (pass_len >= password_len) {
            return -1;
        }
        memcpy(password, pass_start, pass_len);
        password[pass_len] = '\0';
    } else {
        password[0] = '\0';
    }

    if (colon) {
        const char *roles_str = colon + 1;
        int parse_err = 0;
        unsigned int parsed_roles = parse_role_list(roles_str, &parse_err);
        if (parse_err) {
            return -1;
        }
        *roles_out = parsed_roles;
    }

    return 0;
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
    config->persist_connection_names = false;
    config->acl_user_count = 0;
    config->acl_default_user = 0;
    config->require_roles = ACL_ROLE_ALL;
    redis_acl_user_t *default_user = config_add_acl_user(config, ACL_DEFAULT_USERNAME);
    if (default_user) {
        set_string(default_user->password, sizeof(default_user->password), "");
        default_user->roles = ACL_ROLE_ALL;
    }
    config_sync_default_user(config);
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
        } else if (strcmp(arg, "--requirepass") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--requirepass requires a value\n");
                return -1;
            }
            const char *password = argv[++i];
            redis_acl_user_t *user = config_ensure_default_user(config);
            if (!user) {
                fprintf(stderr, "Unable to allocate default ACL user\n");
                return -1;
            }
            set_string(user->password, sizeof(user->password), password);
            config_sync_default_user(config);
        } else if (strcmp(arg, "--requireuser") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--requireuser requires a value\n");
                return -1;
            }
            const char *username = argv[++i];
            bool created = false;
            redis_acl_user_t *user = config_ensure_acl_user(config, username, &created);
            if (!user) {
                return -1;
            }
            if (created) {
                set_string(user->password, sizeof(user->password), "");
                user->roles = ACL_ROLE_ALL;
            }
            config_set_default_user(config, user);
        } else if (strcmp(arg, "--userrole") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--userrole requires a value\n");
                return -1;
            }
            int parse_err = 0;
            unsigned int roles = parse_role_list(argv[++i], &parse_err);
            if (parse_err) {
                fprintf(stderr, "Invalid role list for --userrole\n");
                return -1;
            }
            redis_acl_user_t *user = config_ensure_default_user(config);
            if (!user) {
                fprintf(stderr, "Unable to allocate default ACL user\n");
                return -1;
            }
            user->roles = roles;
            config_sync_default_user(config);
        } else if (strcmp(arg, "--acluser") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--acluser requires a value\n");
                return -1;
            }
            const char *definition = argv[++i];
            char username[REDIS_USERNAME_MAX];
            char password[REDIS_PASSWORD_MAX];
            unsigned int roles = ACL_ROLE_ALL;
            if (parse_acl_user_definition(definition,
                                          username,
                                          sizeof(username),
                                          password,
                                          sizeof(password),
                                          &roles) != 0) {
                fprintf(stderr, "Invalid ACL user definition for --acluser\n");
                return -1;
            }
            bool created = false;
            redis_acl_user_t *user = config_ensure_acl_user(config, username, &created);
            if (!user) {
                return -1;
            }
            set_string(user->password, sizeof(user->password), password);
            user->roles = roles;
            if ((size_t)(user - config->acl_users) == config->acl_default_user) {
                config_sync_default_user(config);
            }
        } else if (strcmp(arg, "--persist-connection-names") == 0) {
            config->persist_connection_names = true;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", arg);
            return -1;
        }
    }

    return 0;
}


