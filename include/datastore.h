#ifndef DATASTORE_H
#define DATASTORE_H

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
    char *key;
    char *value;
    uint64_t expiry_ms; // absolute time in milliseconds (0 means no expiration)
} redis_entry_t;

typedef struct {
    char *key;
    char *value;
    uint64_t expiry_ms;
} redis_snapshot_entry_t;

typedef struct {
    redis_entry_t *entries;
    size_t count;
    size_t capacity;
    pthread_mutex_t lock;
} redis_store_t;

void datastore_init(redis_store_t *store);
void datastore_free(redis_store_t *store);
int datastore_set(redis_store_t *store, const char *key, const char *value, uint64_t expiry_ms);
int datastore_get(redis_store_t *store, const char *key, char **value_out);
int datastore_keys(redis_store_t *store, char ***keys_out, size_t *count_out);
void datastore_flush(redis_store_t *store);
int datastore_snapshot(redis_store_t *store, redis_snapshot_entry_t **entries_out, size_t *count_out);
void datastore_snapshot_free(redis_snapshot_entry_t *entries, size_t count);
void datastore_delete(redis_store_t *store, const char *key);
uint64_t datastore_current_time_ms(void);

#endif // DATASTORE_H
