#ifndef EXPIRY_H
#define EXPIRY_H

#include "datastore.h"

int expiry_start(redis_store_t *store);
void expiry_stop(void);

#endif // EXPIRY_H
