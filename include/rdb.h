#ifndef RDB_H
#define RDB_H

#include "config.h"
#include "datastore.h"

int rdb_load(const redis_config_t *config, redis_store_t *store);

#endif // RDB_H
