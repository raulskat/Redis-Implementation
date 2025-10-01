#include "rdb.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SELECTDB 0xFE
#define RESIZEDB 0xFB
#define AUX 0xFA
#define EXPIRETIME 0xFD
#define EXPIRETIMEMS 0xFC
#define EOF_OP 0xFF
#define KEYVALUE 0x00

static int read_byte(FILE *file, unsigned char *out) {
    return (fread(out, sizeof(unsigned char), 1, file) == 1) ? 0 : -1;
}

static uint32_t read_le32(FILE *file, int *error) {
    unsigned char buf[4];
    if (fread(buf, 1, sizeof(buf), file) != sizeof(buf)) {
        if (error) {
            *error = -1;
        }
        return 0;
    }
    if (error) {
        *error = 0;
    }
    return (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) | ((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24);
}

static uint64_t read_le64(FILE *file, int *error) {
    unsigned char buf[8];
    if (fread(buf, 1, sizeof(buf), file) != sizeof(buf)) {
        if (error) {
            *error = -1;
        }
        return 0;
    }
    if (error) {
        *error = 0;
    }
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= ((uint64_t)buf[i]) << (8 * i);
    }
    return value;
}

static int read_length(FILE *file, uint64_t *len, int *is_special) {
    unsigned char first;
    if (read_byte(file, &first) != 0) {
        return -1;
    }
    unsigned char type = first >> 6;
    if (is_special) {
        *is_special = 0;
    }
    switch (type) {
    case 0:
        *len = first & 0x3F;
        return 0;
    case 1: {
        unsigned char second;
        if (read_byte(file, &second) != 0) {
            return -1;
        }
        *len = ((uint64_t)(first & 0x3F) << 8) | second;
        return 0;
    }
    case 2: {
        unsigned char buf[4];
        if (fread(buf, 1, sizeof(buf), file) != sizeof(buf)) {
            return -1;
        }
        *len = ((uint64_t)buf[0] << 24) | ((uint64_t)buf[1] << 16) |
               ((uint64_t)buf[2] << 8) | (uint64_t)buf[3];
        return 0;
    }
    case 3:
        if (is_special) {
            *is_special = 1;
        }
        *len = first & 0x3F;
        return 0;
    default:
        return -1;
    }
}

static char *read_string(FILE *file) {
    uint64_t length = 0;
    int is_special = 0;
    if (read_length(file, &length, &is_special) != 0) {
        return NULL;
    }
    if (is_special) {
        // Encoded strings are not yet supported
        return NULL;
    }
    char *str = malloc(length + 1);
    if (!str) {
        return NULL;
    }
    if (fread(str, 1, length, file) != length) {
        free(str);
        return NULL;
    }
    str[length] = '\0';
    return str;
}

int rdb_load(const redis_config_t *config, redis_store_t *store) {
    if (!config || !store) {
        return -1;
    }

    char path[REDIS_PATH_MAX * 2];
    snprintf(path, sizeof(path), "%s/%s", config->dir, config->db_filename);

    FILE *file = fopen(path, "rb");
    if (!file) {
        printf("RDB file not found at %s. Treating the database as empty.\n", path);
        return 0;
    }

    char magic[6] = {0};
    if (fread(magic, 1, 5, file) != 5) {
        fclose(file);
        return -1;
    }
    magic[5] = '\0';
    if (strcmp(magic, "REDIS") != 0) {
        printf("Invalid RDB magic header: %s\n", magic);
        fclose(file);
        return -1;
    }

    char version[5] = {0};
    if (fread(version, 1, 4, file) != 4) {
        fclose(file);
        return -1;
    }
    version[4] = '\0';
    printf("RDB version: %s\n", version);

    uint64_t pending_expiry_ms = 0;

    while (1) {
        int next = fgetc(file);
        if (next == EOF) {
            break;
        }
        unsigned char opcode = (unsigned char)next;

        switch (opcode) {
        case SELECTDB: {
            uint64_t db_number = 0;
            if (read_length(file, &db_number, NULL) != 0) {
                fclose(file);
                return -1;
            }
            printf("Switching to database %llu\n", (unsigned long long)db_number);
            break;
        }
        case RESIZEDB: {
            uint64_t hash_size = 0;
            uint64_t expire_size = 0;
            if (read_length(file, &hash_size, NULL) != 0 ||
                read_length(file, &expire_size, NULL) != 0) {
                fclose(file);
                return -1;
            }
            printf("RESIZEDB hash=%llu expire=%llu\n",
                   (unsigned long long)hash_size,
                   (unsigned long long)expire_size);
            break;
        }
        case AUX: {
            char *key = read_string(file);
            char *value = read_string(file);
            if (key && value) {
                printf("AUX %s = %s\n", key, value);
            }
            free(key);
            free(value);
            break;
        }
        case EXPIRETIME: {
            int err = 0;
            uint32_t seconds = read_le32(file, &err);
            if (err != 0) {
                fclose(file);
                return -1;
            }
            pending_expiry_ms = (uint64_t)seconds * 1000ULL;
            break;
        }
        case EXPIRETIMEMS: {
            int err = 0;
            uint64_t ms = read_le64(file, &err);
            if (err != 0) {
                fclose(file);
                return -1;
            }
            pending_expiry_ms = ms;
            break;
        }
        case EOF_OP:
            fclose(file);
            return 0;
        default:
            if (opcode == KEYVALUE) {
                char *key = read_string(file);
                char *value = read_string(file);
                if (!key || !value) {
                    free(key);
                    free(value);
                    fclose(file);
                    return -1;
                }
                if (datastore_set(store, key, value, pending_expiry_ms) != 0) {
                    printf("Failed to load key %s from RDB\n", key);
                }
                free(key);
                free(value);
                pending_expiry_ms = 0;
            } else {
                printf("Encountered unsupported opcode: 0x%02X\n", opcode);
                fclose(file);
                return -1;
            }
            break;
        }
    }

    fclose(file);
    return 0;
}
