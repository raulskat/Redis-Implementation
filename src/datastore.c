#include "datastore.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DATASTORE_INITIAL_BUCKETS 16U
#define DATASTORE_MAX_LOAD_FACTOR 0.75

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

static uint64_t hash_key(const char *key) {
    // 64-bit FNV-1a
    const uint64_t fnv_offset = 1469598103934665603ULL;
    const uint64_t fnv_prime = 1099511628211ULL;
    uint64_t hash = fnv_offset;
    for (const unsigned char *p = (const unsigned char *)key; *p; ++p) {
        hash ^= (uint64_t)(*p);
        hash *= fnv_prime;
    }
    return hash;
}

static int ensure_buckets(redis_store_t *store) {
    if (store->bucket_count > 0 && store->buckets) {
        return 0;
    }
    size_t bucket_count = DATASTORE_INITIAL_BUCKETS;
    redis_entry_t **buckets = calloc(bucket_count, sizeof(*buckets));
    if (!buckets) {
        return -1;
    }
    store->buckets = buckets;
    store->bucket_count = bucket_count;
    store->size = 0;
    return 0;
}

static void entry_free(redis_entry_t *entry) {
    if (!entry) {
        return;
    }
    free(entry->key);
    free(entry->value);
    free(entry);
}

static int resize_if_needed(redis_store_t *store) {
    if (store->bucket_count == 0) {
        return ensure_buckets(store);
    }
    double load_factor =
        (store->bucket_count == 0) ? 0.0 : ((double)store->size / (double)store->bucket_count);
    if (load_factor <= DATASTORE_MAX_LOAD_FACTOR) {
        return 0;
    }

    size_t new_bucket_count = store->bucket_count * 2;
    if (new_bucket_count == 0) {
        new_bucket_count = DATASTORE_INITIAL_BUCKETS;
    }

    redis_entry_t **new_buckets = calloc(new_bucket_count, sizeof(*new_buckets));
    if (!new_buckets) {
        return -1;
    }

    for (size_t i = 0; i < store->bucket_count; ++i) {
        redis_entry_t *entry = store->buckets[i];
        while (entry) {
            redis_entry_t *next = entry->next;
            size_t index = hash_key(entry->key) % new_bucket_count;
            entry->next = new_buckets[index];
            new_buckets[index] = entry;
            entry = next;
        }
    }

    free(store->buckets);
    store->buckets = new_buckets;
    store->bucket_count = new_bucket_count;
    return 0;
}

static redis_entry_t **find_entry_slot(redis_store_t *store, const char *key) {
    if (store->bucket_count == 0 || !store->buckets) {
        return NULL;
    }
    size_t index = hash_key(key) % store->bucket_count;
    redis_entry_t **slot = &store->buckets[index];
    while (*slot) {
        if (strcmp((*slot)->key, key) == 0) {
            break;
        }
        slot = &(*slot)->next;
    }
    return slot;
}

static int remove_if_expired(redis_store_t *store,
                             redis_entry_t **slot,
                             uint64_t now_ms,
                             int *removed) {
    if (!slot || !*slot) {
        return 0;
    }
    redis_entry_t *entry = *slot;
    if (entry->expiry_ms != 0 && entry->expiry_ms <= now_ms) {
        *slot = entry->next;
        entry_free(entry);
        --store->size;
        if (removed) {
            *removed = 1;
        }
        return 1;
    }
    if (removed) {
        *removed = 0;
    }
    return 0;
}

void datastore_init(redis_store_t *store) {
    if (!store) {
        return;
    }
    store->buckets = NULL;
    store->bucket_count = 0;
    store->size = 0;
    pthread_mutex_init(&store->lock, NULL);
}

static void free_all_entries(redis_store_t *store) {
    if (!store || !store->buckets) {
        return;
    }
    for (size_t i = 0; i < store->bucket_count; ++i) {
        redis_entry_t *entry = store->buckets[i];
        while (entry) {
            redis_entry_t *next = entry->next;
            entry_free(entry);
            entry = next;
        }
        store->buckets[i] = NULL;
    }
    free(store->buckets);
    store->buckets = NULL;
    store->bucket_count = 0;
    store->size = 0;
}

void datastore_flush(redis_store_t *store) {
    if (!store) {
        return;
    }
    pthread_mutex_lock(&store->lock);
    free_all_entries(store);
    ensure_buckets(store);
    pthread_mutex_unlock(&store->lock);
}

void datastore_free(redis_store_t *store) {
    if (!store) {
        return;
    }
    pthread_mutex_lock(&store->lock);
    free_all_entries(store);
    pthread_mutex_unlock(&store->lock);
    pthread_mutex_destroy(&store->lock);
}

static redis_entry_t *entry_create(const char *key, const char *value, uint64_t expiry_ms) {
    redis_entry_t *entry = calloc(1, sizeof(*entry));
    if (!entry) {
        return NULL;
    }
    entry->key = strdup(key);
    entry->value = strdup(value);
    if (!entry->key || !entry->value) {
        entry_free(entry);
        return NULL;
    }
    entry->expiry_ms = expiry_ms;
    entry->next = NULL;
    return entry;
}

int datastore_set(redis_store_t *store, const char *key, const char *value, uint64_t expiry_ms) {
    if (!store || !key || !value) {
        return -1;
    }

    pthread_mutex_lock(&store->lock);
    if (ensure_buckets(store) != 0) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }
    if (resize_if_needed(store) != 0) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }

    redis_entry_t **slot = find_entry_slot(store, key);
    uint64_t now_ms = current_time_ms();
    if (slot && *slot) {
        int removed = 0;
        remove_if_expired(store, slot, now_ms, &removed);
        if (!removed && *slot) {
            redis_entry_t *entry = *slot;
            char *new_value = strdup(value);
            if (!new_value) {
                pthread_mutex_unlock(&store->lock);
                return -1;
            }
            free(entry->value);
            entry->value = new_value;
            entry->expiry_ms = expiry_ms;
            pthread_mutex_unlock(&store->lock);
            return 0;
        }
    }

    if (!slot) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }

    redis_entry_t *entry = entry_create(key, value, expiry_ms);
    if (!entry) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }
    entry->next = *slot;
    *slot = entry;
    ++store->size;

    pthread_mutex_unlock(&store->lock);
    return 0;
}

int datastore_get(redis_store_t *store, const char *key, char **value_out) {
    if (!store || !key || !value_out) {
        return -1;
    }
    *value_out = NULL;

    pthread_mutex_lock(&store->lock);
    if (!store->buckets || store->bucket_count == 0) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }

    redis_entry_t **slot = find_entry_slot(store, key);
    uint64_t now_ms = current_time_ms();
    if (!slot || !*slot) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }

    if (remove_if_expired(store, slot, now_ms, NULL)) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }

    char *copy = strdup((*slot)->value);
    if (!copy) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }
    *value_out = copy;
    pthread_mutex_unlock(&store->lock);
    return 0;
}

int datastore_keys(redis_store_t *store, char ***keys_out, size_t *count_out) {
    if (!store || !keys_out || !count_out) {
        return -1;
    }
    *keys_out = NULL;
    *count_out = 0;

    pthread_mutex_lock(&store->lock);
    size_t capacity = store->size;
    if (capacity == 0) {
        pthread_mutex_unlock(&store->lock);
        return 0;
    }
    char **keys = calloc(capacity, sizeof(*keys));
    if (!keys) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }

    size_t count = 0;
    uint64_t now_ms = current_time_ms();
    for (size_t i = 0; i < store->bucket_count; ++i) {
        redis_entry_t **slot = &store->buckets[i];
        while (*slot) {
            if (remove_if_expired(store, slot, now_ms, NULL)) {
                continue;
            }
            char *copy = strdup((*slot)->key);
            if (!copy) {
                for (size_t k = 0; k < count; ++k) {
                    free(keys[k]);
                }
                free(keys);
                pthread_mutex_unlock(&store->lock);
                return -1;
            }
            keys[count++] = copy;
            slot = &(*slot)->next;
        }
    }

    *keys_out = keys;
    *count_out = count;
    pthread_mutex_unlock(&store->lock);
    return 0;
}

int datastore_snapshot(redis_store_t *store,
                       redis_snapshot_entry_t **entries_out,
                       size_t *count_out) {
    if (!store || !entries_out || !count_out) {
        return -1;
    }
    *entries_out = NULL;
    *count_out = 0;

    pthread_mutex_lock(&store->lock);
    size_t capacity = store->size;
    if (capacity == 0) {
        pthread_mutex_unlock(&store->lock);
        return 0;
    }
    redis_snapshot_entry_t *entries = calloc(capacity, sizeof(*entries));
    if (!entries) {
        pthread_mutex_unlock(&store->lock);
        return -1;
    }

    size_t count = 0;
    uint64_t now_ms = current_time_ms();
    for (size_t i = 0; i < store->bucket_count; ++i) {
        redis_entry_t **slot = &store->buckets[i];
        while (*slot) {
            if (remove_if_expired(store, slot, now_ms, NULL)) {
                continue;
            }
            redis_entry_t *entry = *slot;
            entries[count].key = strdup(entry->key);
            entries[count].value = strdup(entry->value);
            entries[count].expiry_ms = entry->expiry_ms;
            if (!entries[count].key || !entries[count].value) {
                for (size_t j = 0; j <= count; ++j) {
                    free(entries[j].key);
                    free(entries[j].value);
                }
                free(entries);
                pthread_mutex_unlock(&store->lock);
                return -1;
            }
            ++count;
            slot = &entry->next;
        }
    }
    *entries_out = entries;
    *count_out = count;
    pthread_mutex_unlock(&store->lock);
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

int datastore_expire_at(redis_store_t *store, const char *key, uint64_t expiry_ms) {
    if (!store || !key) {
        return -1;
    }
    int result = 0;
    pthread_mutex_lock(&store->lock);

    redis_entry_t **slot = find_entry_slot(store, key);
    uint64_t now_ms = current_time_ms();
    if (slot && *slot) {
        if (remove_if_expired(store, slot, now_ms, NULL)) {
            result = 0;
        } else {
            if (expiry_ms <= now_ms) {
                redis_entry_t *entry = *slot;
                *slot = entry->next;
                entry_free(entry);
                --store->size;
                result = 0;
            } else {
                (*slot)->expiry_ms = expiry_ms;
                result = 1;
            }
        }
    }
    pthread_mutex_unlock(&store->lock);
    return result;
}

int datastore_expire_in(redis_store_t *store, const char *key, uint64_t ttl_ms) {
    if (!store || !key) {
        return -1;
    }
    uint64_t now_ms = current_time_ms();
    if (ttl_ms > UINT64_MAX - now_ms) {
        ttl_ms = UINT64_MAX - now_ms;
    }
    return datastore_expire_at(store, key, now_ms + ttl_ms);
}

int datastore_persist_key(redis_store_t *store, const char *key) {
    if (!store || !key) {
        return -1;
    }
    int result = 0;
    pthread_mutex_lock(&store->lock);
    redis_entry_t **slot = find_entry_slot(store, key);
    uint64_t now_ms = current_time_ms();
    if (slot && *slot) {
        if (remove_if_expired(store, slot, now_ms, NULL)) {
            result = 0;
        } else {
            (*slot)->expiry_ms = 0;
            result = 1;
        }
    }
    pthread_mutex_unlock(&store->lock);
    return result;
}

long long datastore_ttl_ms(redis_store_t *store, const char *key) {
    if (!store || !key) {
        return -2;
    }
    long long result = -2;
    pthread_mutex_lock(&store->lock);
    redis_entry_t **slot = find_entry_slot(store, key);
    uint64_t now_ms = current_time_ms();
    if (slot && *slot) {
        if (remove_if_expired(store, slot, now_ms, NULL)) {
            result = -2;
        } else if ((*slot)->expiry_ms == 0) {
            result = -1;
        } else if ((*slot)->expiry_ms <= now_ms) {
            redis_entry_t *entry = *slot;
            *slot = entry->next;
            entry_free(entry);
            --store->size;
            result = -2;
        } else {
            result = (long long)((*slot)->expiry_ms - now_ms);
        }
    }
    pthread_mutex_unlock(&store->lock);
    return result;
}

size_t datastore_prune_expired(redis_store_t *store, size_t limit) {
    if (!store) {
        return 0;
    }
    size_t removed = 0;
    pthread_mutex_lock(&store->lock);
    uint64_t now_ms = current_time_ms();
    for (size_t i = 0; i < store->bucket_count; ++i) {
        redis_entry_t **slot = &store->buckets[i];
        while (*slot) {
            int expired = 0;
            remove_if_expired(store, slot, now_ms, &expired);
            if (expired) {
                ++removed;
                if (limit > 0 && removed >= limit) {
                    pthread_mutex_unlock(&store->lock);
                    return removed;
                }
                continue;
            }
            slot = &(*slot)->next;
        }
    }
    pthread_mutex_unlock(&store->lock);
    return removed;
}

void datastore_delete(redis_store_t *store, const char *key) {
    if (!store || !key) {
        return;
    }
    pthread_mutex_lock(&store->lock);
    redis_entry_t **slot = find_entry_slot(store, key);
    if (slot && *slot) {
        redis_entry_t *entry = *slot;
        *slot = entry->next;
        entry_free(entry);
        --store->size;
    }
    pthread_mutex_unlock(&store->lock);
}
