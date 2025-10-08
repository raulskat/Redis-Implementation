#include "command_events.h"

#include <stdlib.h>

#define COMMAND_EVENT_DISPATCHER_GROWTH 4

void command_event_dispatcher_init(command_event_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return;
    }
    dispatcher->listeners = NULL;
    dispatcher->count = 0;
    dispatcher->capacity = 0;
}

void command_event_dispatcher_deinit(command_event_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return;
    }
    free(dispatcher->listeners);
    dispatcher->listeners = NULL;
    dispatcher->count = 0;
    dispatcher->capacity = 0;
}

static int ensure_capacity(command_event_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return -1;
    }
    if (dispatcher->count < dispatcher->capacity) {
        return 0;
    }
    size_t new_capacity = dispatcher->capacity == 0 ? COMMAND_EVENT_DISPATCHER_GROWTH
                                                    : dispatcher->capacity * 2;
    command_event_listener_t *new_listeners =
        realloc(dispatcher->listeners, new_capacity * sizeof(*new_listeners));
    if (!new_listeners) {
        return -1;
    }
    dispatcher->listeners = new_listeners;
    dispatcher->capacity = new_capacity;
    return 0;
}

int command_event_dispatcher_add(command_event_dispatcher_t *dispatcher,
                                 command_event_listener_fn fn,
                                 void *userdata) {
    if (!dispatcher || !fn) {
        return -1;
    }
    if (ensure_capacity(dispatcher) != 0) {
        return -1;
    }
    dispatcher->listeners[dispatcher->count].fn = fn;
    dispatcher->listeners[dispatcher->count].userdata = userdata;
    ++dispatcher->count;
    return 0;
}

void command_event_dispatcher_remove_all(command_event_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return;
    }
    dispatcher->count = 0;
}

void command_event_dispatcher_dispatch(command_event_dispatcher_t *dispatcher,
                                       const command_event_t *event) {
    if (!dispatcher || !event) {
        return;
    }
    for (size_t i = 0; i < dispatcher->count; ++i) {
        command_event_listener_t *listener = &dispatcher->listeners[i];
        if (!listener->fn) {
            continue;
        }
        listener->fn(event, listener->userdata);
    }
}
