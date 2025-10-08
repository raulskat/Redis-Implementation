#include "persistence.h"

#include "rdb.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct {
    redis_store_t *store;
    const redis_config_t *config;
} persistence_job_t;

typedef struct {
    persistence_mode_t mode;
    int (*execute)(redis_store_t *store, const redis_config_t *config);
} persistence_strategy_t;

static pthread_mutex_t save_lock = PTHREAD_MUTEX_INITIALIZER;
static bool save_in_progress = false;

static void finish_save(void) {
    pthread_mutex_lock(&save_lock);
    save_in_progress = false;
    pthread_mutex_unlock(&save_lock);
}

static int begin_save(void) {
    pthread_mutex_lock(&save_lock);
    if (save_in_progress) {
        pthread_mutex_unlock(&save_lock);
        errno = EBUSY;
        return EBUSY;
    }
    save_in_progress = true;
    pthread_mutex_unlock(&save_lock);
    return 0;
}

static int perform_save(redis_store_t *store, const redis_config_t *config) {
    redis_snapshot_entry_t *entries = NULL;
    size_t count = 0;
    if (datastore_snapshot(store, &entries, &count) != 0) {
        return -1;
    }

    int rc = rdb_save(config, entries, count);
    datastore_snapshot_free(entries, count);
    return rc;
}

static void *background_save(void *arg) {
    persistence_job_t *job = (persistence_job_t *)arg;
    int rc = perform_save(job->store, job->config);
    if (rc == 0) {
        printf("Background saving terminated with success\n");
    } else {
        printf("Background saving terminated with error\n");
    }
    free(job);
    finish_save();
    return NULL;
}

static int persistence_execute_sync(redis_store_t *store, const redis_config_t *config) {
    int rc = perform_save(store, config);
    finish_save();
    return rc;
}

static int persistence_execute_async(redis_store_t *store, const redis_config_t *config) {
    persistence_job_t *job = malloc(sizeof(*job));
    if (!job) {
        finish_save();
        errno = ENOMEM;
        return -1;
    }
    job->store = store;
    job->config = config;

    pthread_t thread_id;
    int rc = pthread_create(&thread_id, NULL, background_save, job);
    if (rc != 0) {
        free(job);
        finish_save();
        errno = rc;
        return -1;
    }
    pthread_detach(thread_id);
    return 0;
}

static const persistence_strategy_t persistence_strategies[] = {
    {PERSISTENCE_MODE_SYNC, persistence_execute_sync},
    {PERSISTENCE_MODE_ASYNC, persistence_execute_async},
};

static const persistence_strategy_t *select_strategy(persistence_mode_t mode) {
    for (size_t i = 0; i < sizeof(persistence_strategies) / sizeof(persistence_strategies[0]); ++i) {
        if (persistence_strategies[i].mode == mode) {
            return &persistence_strategies[i];
        }
    }
    return NULL;
}

int persistence_save(redis_store_t *store, const redis_config_t *config, persistence_mode_t mode) {
    if (!store || !config) {
        errno = EINVAL;
        return -1;
    }

    int rc = begin_save();
    if (rc != 0) {
        return rc;
    }

    const persistence_strategy_t *strategy = select_strategy(mode);
    if (!strategy) {
        finish_save();
        errno = EINVAL;
        return -1;
    }

    return strategy->execute(store, config);
}

int persistence_save_sync(redis_store_t *store, const redis_config_t *config) {
    return persistence_save(store, config, PERSISTENCE_MODE_SYNC);
}

int persistence_save_async(redis_store_t *store, const redis_config_t *config) {
    return persistence_save(store, config, PERSISTENCE_MODE_ASYNC);
}

bool persistence_is_async_in_progress(void) {
    pthread_mutex_lock(&save_lock);
    bool in_progress = save_in_progress;
    pthread_mutex_unlock(&save_lock);
    return in_progress;
}
