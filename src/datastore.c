#include "datastore.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define INITIAL_CAPACITY 16

static uint64_t current_time_ms(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

uint64_t datastore_current_time_ms(void) {
    return current_time_ms();
}

static void entry_free(redis_entry_t *entry) {
    if (!entry) {
        return;
    }
    free(entry->key);
    free(entry->value);
    entry->key = NULL;
    entry->value = NULL;
    entry->expiry_ms = 0;
}

static int ensure_capacity(redis_store_t *store) {
    if (store->count < store->capacity) {
        return 0;
    }
    size_t new_capacity = store->capacity == 0 ? INITIAL_CAPACITY : store->capacity * 2;
    redis_entry_t *new_entries = realloc(store->entries, new_capacity * sizeof(redis_entry_t));
    if (!new_entries) {
        return -1;
    }
    store->entries = new_entries;
    store->capacity = new_capacity;
    return 0;
}

static int find_entry_index(redis_store_t *store, const char *key) {
    for (size_t i = 0; i < store->count; ++i) {
        if (strcmp(store->entries[i].key, key) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static void delete_index(redis_store_t *store, size_t index) {
    if (index >= store->count) {
        return;
    }
    entry_free(&store->entries[index]);
    if (index != store->count - 1) {
        store->entries[index] = store->entries[store->count - 1];
    }
    --store->count;
}

static int prune_if_expired(redis_store_t *store, size_t index, uint64_t now_ms) {
    if (index >= store->count) {
        return 0;
    }
    redis_entry_t *entry = &store->entries[index];
    if (entry->expiry_ms != 0 && entry->expiry_ms <= now_ms) {
        delete_index(store, index);
        return 1; // expired and removed
    }
    return 0;
}

void datastore_init(redis_store_t *store) {
    if (!store) {
        return;
    }
    store->entries = NULL;
    store->count = 0;
    store->capacity = 0;
    pthread_mutex_init(&store->lock, NULL);
}

void datastore_flush(redis_store_t *store) {
    if (!store) {
        return;
    }
    pthread_mutex_lock(&store->lock);
    for (size_t i = 0; i < store->count; ++i) {
        entry_free(&store->entries[i]);
    }
    free(store->entries);
    store->entries = NULL;
    store->count = 0;
    store->capacity = 0;
    pthread_mutex_unlock(&store->lock);
}

void datastore_free(redis_store_t *store) {
    if (!store) {
        return;
    }
    datastore_flush(store);
    pthread_mutex_destroy(&store->lock);
}

int datastore_set(redis_store_t *store, const char *key, const char *value, uint64_t expiry_ms) {
    if (!store || !key || !value) {
        return -1;
    }
    int result = 0;
    pthread_mutex_lock(&store->lock);

    int index = find_entry_index(store, key);
    uint64_t now_ms = current_time_ms();

    if (index >= 0 && prune_if_expired(store, (size_t)index, now_ms)) {
        index = -1;
    }

    if (index >= 0) {
        // Update existing entry
        redis_entry_t *entry = &store->entries[index];
        char *new_value = strdup(value);
        if (!new_value) {
            result = -1;
            goto out;
        }
        free(entry->value);
        entry->value = new_value;
        entry->expiry_ms = expiry_ms;
    } else {
        if (ensure_capacity(store) != 0) {
            result = -1;
            goto out;
        }
        redis_entry_t *entry = &store->entries[store->count];
        entry->key = strdup(key);
        entry->value = strdup(value);
        entry->expiry_ms = expiry_ms;
        if (!entry->key || !entry->value) {
            free(entry->key);
            free(entry->value);
            entry->key = NULL;
            entry->value = NULL;
            result = -1;
            goto out;
        }
        ++store->count;
    }

out:
    pthread_mutex_unlock(&store->lock);
    return result;
}

int datastore_get(redis_store_t *store, const char *key, char **value_out) {
    if (!store || !key || !value_out) {
        return -1;
    }

    int result = -1;
    pthread_mutex_lock(&store->lock);
    int index = find_entry_index(store, key);
    uint64_t now_ms = current_time_ms();

    if (index >= 0) {
        if (prune_if_expired(store, (size_t)index, now_ms)) {
            result = -1;
        } else {
            redis_entry_t *entry = &store->entries[index];
            char *copy = strdup(entry->value);
            if (copy) {
                *value_out = copy;
                result = 0;
            }
        }
    }

    pthread_mutex_unlock(&store->lock);
    return result;
}

int datastore_keys(redis_store_t *store, char ***keys_out, size_t *count_out) {
    if (!store || !keys_out || !count_out) {
        return -1;
    }

    pthread_mutex_lock(&store->lock);
    uint64_t now_ms = current_time_ms();

    // First pass: prune expired and count valid entries
    size_t valid_count = 0;
    for (size_t i = 0; i < store->count;) {
        if (prune_if_expired(store, i, now_ms)) {
            continue; // current index now contains swapped entry
        }
        ++valid_count;
        ++i;
    }

    if (valid_count == 0) {
        *keys_out = NULL;
        *count_out = 0;
        pthread_mutex_unlock(&store->lock);
        return 0;
    }

    char **keys = calloc(valid_count, sizeof(char *));
    if (!keys) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }

    size_t out_index = 0;
    for (size_t i = 0; i < store->count && out_index < valid_count; ++i) {
        redis_entry_t *entry = &store->entries[i];
        if (!entry->key) {
            continue;
        }
        keys[out_index] = strdup(entry->key);
        if (!keys[out_index]) {
            for (size_t j = 0; j < out_index; ++j) {
                free(keys[j]);
            }
            free(keys);
            pthread_mutex_unlock(&store->lock);
            return -1;
        }
        ++out_index;
    }

    *keys_out = keys;
    *count_out = valid_count;
    pthread_mutex_unlock(&store->lock);
    return 0;
}


int datastore_snapshot(redis_store_t *store, redis_snapshot_entry_t **entries_out, size_t *count_out) {
    if (!store || !entries_out || !count_out) {
        return -1;
    }

    pthread_mutex_lock(&store->lock);
    uint64_t now_ms = current_time_ms();

    size_t valid_count = 0;
    for (size_t i = 0; i < store->count;) {
        if (prune_if_expired(store, i, now_ms)) {
            continue;
        }
        ++valid_count;
        ++i;
    }

    if (valid_count == 0) {
        *entries_out = NULL;
        *count_out = 0;
        pthread_mutex_unlock(&store->lock);
        return 0;
    }

    redis_snapshot_entry_t *entries = calloc(valid_count, sizeof(redis_snapshot_entry_t));
    if (!entries) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }

    size_t out_index = 0;
    for (size_t i = 0; i < store->count && out_index < valid_count; ++i) {
        redis_entry_t *entry = &store->entries[i];
        if (!entry->key) {
            continue;
        }
        entries[out_index].key = strdup(entry->key);
        entries[out_index].value = strdup(entry->value);
        entries[out_index].expiry_ms = entry->expiry_ms;
        if (!entries[out_index].key || !entries[out_index].value) {
            for (size_t j = 0; j <= out_index; ++j) {
                free(entries[j].key);
                free(entries[j].value);
            }
            free(entries);
            pthread_mutex_unlock(&store->lock);
            return -1;
        }
        ++out_index;
    }

    pthread_mutex_unlock(&store->lock);

    *entries_out = entries;
    *count_out = out_index;
    return 0;
}

void datastore_snapshot_free(redis_snapshot_entry_t *entries, size_t count) {
    if (!entries) {
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        free(entries[i].key);
        free(entries[i].value);
    }
    free(entries);
}

void datastore_delete(redis_store_t *store, const char *key) {
    if (!store || !key) {
        return;
    }
    pthread_mutex_lock(&store->lock);
    int index = find_entry_index(store, key);
    if (index >= 0) {
        delete_index(store, (size_t)index);
    }
    pthread_mutex_unlock(&store->lock);
}
