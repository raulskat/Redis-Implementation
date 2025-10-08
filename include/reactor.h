#ifndef REACTOR_H
#define REACTOR_H

#include <poll.h>
#include <stddef.h>

typedef enum {
    REACTOR_EVENT_CONTINUE = 0,
    REACTOR_EVENT_REMOVE = -1,
    REACTOR_EVENT_SHUTDOWN = 1,
} reactor_event_result_t;

typedef reactor_event_result_t (*reactor_event_handler_fn)(int fd, short events, void *userdata);

typedef struct reactor {
    const struct reactor_backend_t *backend;
    void *backend_state;
    int should_stop;
} reactor_t;

typedef struct reactor_backend_t {
    const char *name;
    int (*init)(reactor_t *reactor);
    void (*deinit)(reactor_t *reactor);
    int (*add)(reactor_t *reactor, int fd, short events, reactor_event_handler_fn handler, void *userdata);
    int (*update)(reactor_t *reactor, int fd, short events);
    void (*remove)(reactor_t *reactor, int fd);
    int (*run)(reactor_t *reactor, int timeout_ms);
} reactor_backend_t;

void reactor_init(reactor_t *reactor);
void reactor_deinit(reactor_t *reactor);
int reactor_add(reactor_t *reactor, int fd, short events, reactor_event_handler_fn handler, void *userdata);
int reactor_update(reactor_t *reactor, int fd, short events);
void reactor_remove(reactor_t *reactor, int fd);
void reactor_stop(reactor_t *reactor);
int reactor_run(reactor_t *reactor);
int reactor_set_backend(reactor_t *reactor, const struct reactor_backend_t *backend);
const struct reactor_backend_t *reactor_backend_default(void);

#endif /* REACTOR_H */
