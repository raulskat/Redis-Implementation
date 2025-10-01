#include "rdb.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

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


static int write_le64(FILE *file, uint64_t value) {
    unsigned char buf[8];
    for (int i = 0; i < 8; ++i) {
        buf[i] = (unsigned char)((value >> (8 * i)) & 0xFF);
    }
    return fwrite(buf, 1, sizeof(buf), file) == sizeof(buf) ? 0 : -1;
}

static int write_length(FILE *file, uint64_t len) {
    if (len < (1ULL << 6)) {
        unsigned char byte = (unsigned char)len;
        return fputc(byte, file) == EOF ? -1 : 0;
    }
    if (len < (1ULL << 14)) {
        unsigned char first = 0x40 | ((len >> 8) & 0x3F);
        unsigned char second = (unsigned char)(len & 0xFF);
        if (fputc(first, file) == EOF) {
            return -1;
        }
        if (fputc(second, file) == EOF) {
            return -1;
        }
        return 0;
    }
    if (len <= UINT32_MAX) {
        if (fputc(0x80, file) == EOF) {
            return -1;
        }
        unsigned char buf[4] = {
            (unsigned char)((len >> 24) & 0xFF),
            (unsigned char)((len >> 16) & 0xFF),
            (unsigned char)((len >> 8) & 0xFF),
            (unsigned char)(len & 0xFF),
        };
        if (fwrite(buf, 1, sizeof(buf), file) != sizeof(buf)) {
            return -1;
        }
        return 0;
    }
    return -1; // lengths beyond 32 bits not supported yet
}

static int ensure_directory(const char *dir) {
    if (!dir || !*dir) {
        errno = EINVAL;
        return -1;
    }

    char buffer[REDIS_PATH_MAX * 2];
    size_t len = strlen(dir);
    if (len >= sizeof(buffer)) {
        errno = ENAMETOOLONG;
        return -1;
    }

    memcpy(buffer, dir, len + 1);

    for (size_t i = 1; buffer[i] != '\0'; ++i) {
        if (buffer[i] == '/' || buffer[i] == '\\') {
            char saved = buffer[i];
            buffer[i] = '\0';
            if (buffer[0] != '\0' && mkdir(buffer, 0755) != 0 && errno != EEXIST) {
                buffer[i] = saved;
                return -1;
            }
            buffer[i] = saved;
        }
    }

    if (mkdir(buffer, 0755) != 0 && errno != EEXIST) {
        return -1;
    }

    return 0;
}

static int write_string(FILE *file, const char *str) {
    if (!str) {
        if (write_length(file, 0) != 0) {
            return -1;
        }
        return 0;
    }
    size_t len = strlen(str);
    if (write_length(file, (uint64_t)len) != 0) {
        return -1;
    }
    if (len == 0) {
        return 0;
    }
    return fwrite(str, 1, len, file) == len ? 0 : -1;
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


int rdb_save(const redis_config_t *config, const redis_snapshot_entry_t *entries, size_t count) {
    if (!config || (!entries && count > 0)) {
        return -1;
    }

    char target_path[REDIS_PATH_MAX * 2];
    int written = snprintf(target_path, sizeof(target_path), "%s/%s", config->dir, config->db_filename);
    if (written < 0 || (size_t)written >= sizeof(target_path)) {
        errno = ENAMETOOLONG;
        perror("Failed to build RDB target path");
        return -1;
    }

    char temp_path[REDIS_PATH_MAX * 2];
    written = snprintf(temp_path, sizeof(temp_path), "%s/%s.tmp-%ld", config->dir, config->db_filename, (long)getpid());
    if (written < 0 || (size_t)written >= sizeof(temp_path)) {
        errno = ENAMETOOLONG;
        perror("Failed to build temporary RDB path");
        return -1;
    }

    if (ensure_directory(config->dir) != 0) {
        perror("Failed to create RDB directory");
        return -1;
    }

    FILE *file = fopen(temp_path, "wb");
    if (!file) {
        perror("Failed to open temporary RDB file");
        return -1;
    }

    int rc = -1;

    if (fwrite("REDIS", 1, 5, file) != 5) {
        perror("Failed to write RDB header");
        goto done;
    }
    if (fwrite("0006", 1, 4, file) != 4) {
        perror("Failed to write RDB version");
        goto done;
    }

    if (fputc(SELECTDB, file) == EOF) {
        perror("Failed to write SELECTDB opcode");
        goto done;
    }
    if (write_length(file, 0) != 0) {
        perror("Failed to write database number");
        goto done;
    }

    size_t expire_count = 0;
    for (size_t i = 0; i < count; ++i) {
        if (entries[i].expiry_ms != 0) {
            ++expire_count;
        }
    }

    if (fputc(RESIZEDB, file) == EOF) {
        perror("Failed to write RESIZEDB opcode");
        goto done;
    }
    if (write_length(file, count) != 0 || write_length(file, expire_count) != 0) {
        perror("Failed to write RESIZEDB lengths");
        goto done;
    }

    for (size_t i = 0; i < count; ++i) {
        const redis_snapshot_entry_t *entry = &entries[i];
        if (entry->expiry_ms != 0) {
            if (fputc(EXPIRETIMEMS, file) == EOF) {
                perror("Failed to write EXPIRETIMEMS opcode");
                goto done;
            }
            if (write_le64(file, entry->expiry_ms) != 0) {
                perror("Failed to write EXPIRETIMEMS payload");
                goto done;
            }
        }

        if (fputc(KEYVALUE, file) == EOF) {
            perror("Failed to write KEYVALUE opcode");
            goto done;
        }
        if (write_string(file, entry->key) != 0 || write_string(file, entry->value) != 0) {
            perror("Failed to write key/value strings");
            goto done;
        }
    }

    if (fputc(EOF_OP, file) == EOF) {
        perror("Failed to write EOF opcode");
        goto done;
    }

    unsigned char checksum[8] = {0};
    if (fwrite(checksum, 1, sizeof(checksum), file) != sizeof(checksum)) {
        perror("Failed to write checksum");
        goto done;
    }

    if (fflush(file) != 0) {
        perror("Failed to flush RDB file");
        goto done;
    }

    int fd = fileno(file);
    if (fd != -1) {
        if (fsync(fd) != 0) {
            perror("fsync failed for RDB file");
            goto done;
        }
    }

    rc = 0;

done:
    if (fclose(file) != 0) {
        perror("Failed to close RDB file");
        rc = -1;
    }

    if (rc == 0) {
        if (rename(temp_path, target_path) != 0) {
            perror("Failed to atomically rename RDB file");
            rc = -1;
            unlink(temp_path);
        }
    } else {
        unlink(temp_path);
    }

    return rc;
}
