#ifndef RDB_H
#define RDB_H

#include "config.h"
#include "datastore.h"

int rdb_load(const redis_config_t *config, redis_store_t *store);
int rdb_save(const redis_config_t *config, const redis_snapshot_entry_t *entries, size_t count);

#endif // RDB_H
