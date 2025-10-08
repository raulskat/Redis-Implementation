#ifndef PERSISTENCE_H
#define PERSISTENCE_H

#include <stdbool.h>

#include "config.h"
#include "datastore.h"

typedef enum {
    PERSISTENCE_MODE_SYNC = 0,
    PERSISTENCE_MODE_ASYNC = 1,
} persistence_mode_t;

int persistence_save(redis_store_t *store, const redis_config_t *config, persistence_mode_t mode);
int persistence_save_sync(redis_store_t *store, const redis_config_t *config);
int persistence_save_async(redis_store_t *store, const redis_config_t *config);
bool persistence_is_async_in_progress(void);

#endif // PERSISTENCE_H
