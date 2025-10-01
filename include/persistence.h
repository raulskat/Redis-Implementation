#ifndef PERSISTENCE_H
#define PERSISTENCE_H

#include <stdbool.h>

#include "config.h"
#include "datastore.h"

int persistence_save_sync(redis_store_t *store, const redis_config_t *config);
int persistence_save_async(redis_store_t *store, const redis_config_t *config);
bool persistence_is_async_in_progress(void);

#endif // PERSISTENCE_H
