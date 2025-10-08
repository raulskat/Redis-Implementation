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

typedef struct {
    int fd;
    short events;
    reactor_event_handler_fn handler;
    void *userdata;
    int active;
    int poll_index;
} reactor_slot_t;

typedef struct {
    reactor_slot_t *slots;
    struct pollfd *pollfds;
    size_t count;
    size_t capacity;
    int should_stop;
} reactor_t;

void reactor_init(reactor_t *reactor);
void reactor_deinit(reactor_t *reactor);
int reactor_add(reactor_t *reactor, int fd, short events, reactor_event_handler_fn handler, void *userdata);
int reactor_update(reactor_t *reactor, int fd, short events);
void reactor_remove(reactor_t *reactor, int fd);
void reactor_stop(reactor_t *reactor);
int reactor_run(reactor_t *reactor);

#endif /* REACTOR_H */
