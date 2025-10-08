#include "reactor.h"

#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

typedef struct reactor_poll_slot {
    int fd;
    short events;
    reactor_event_handler_fn handler;
    void *userdata;
    int active;
    int poll_index;
} reactor_poll_slot_t;

typedef struct reactor_poll_state {
    reactor_poll_slot_t *slots;
    struct pollfd *pollfds;
    size_t count;
    size_t capacity;
} reactor_poll_state_t;

static reactor_poll_state_t *poll_state(reactor_t *reactor) {
    return (reactor_poll_state_t *)reactor->backend_state;
}

static ssize_t find_slot(reactor_poll_state_t *state, int fd) {
    if (!state) {
        return -1;
    }
    for (size_t i = 0; i < state->count; ++i) {
        if (state->slots[i].active && state->slots[i].fd == fd) {
            return (ssize_t)i;
        }
    }
    return -1;
}

static int reserve_slots(reactor_poll_state_t *state, size_t size) {
    if (!state) {
        return -1;
    }
    if (size <= state->capacity) {
        return 0;
    }
    size_t old_capacity = state->capacity;
    size_t new_capacity = old_capacity == 0 ? size : old_capacity;
    while (new_capacity < size) {
        new_capacity *= 2;
        if (new_capacity == 0) {
            new_capacity = size;
            break;
        }
    }

    reactor_poll_slot_t *new_slots =
        realloc(state->slots, new_capacity * sizeof(*new_slots));
    if (!new_slots) {
        return -1;
    }
    struct pollfd *new_pollfds =
        realloc(state->pollfds, new_capacity * sizeof(*new_pollfds));
    if (!new_pollfds) {
        free(new_slots);
        return -1;
    }

    state->slots = new_slots;
    state->pollfds = new_pollfds;
    for (size_t i = old_capacity; i < new_capacity; ++i) {
        state->slots[i].active = 0;
        state->slots[i].poll_index = -1;
        state->pollfds[i].fd = -1;
        state->pollfds[i].events = 0;
        state->pollfds[i].revents = 0;
    }
    state->capacity = new_capacity;
    return 0;
}

static int poll_init(reactor_t *reactor) {
    reactor_poll_state_t *state = calloc(1, sizeof(*state));
    if (!state) {
        return -1;
    }
    state->slots = NULL;
    state->pollfds = NULL;
    state->count = 0;
    state->capacity = 0;
    reactor->backend_state = state;
    return 0;
}

static void poll_deinit(reactor_t *reactor) {
    reactor_poll_state_t *state = poll_state(reactor);
    if (!state) {
        return;
    }
    free(state->slots);
    free(state->pollfds);
    free(state);
    reactor->backend_state = NULL;
}

static int poll_add(reactor_t *reactor,
                    int fd,
                    short events,
                    reactor_event_handler_fn handler,
                    void *userdata) {
    if (!reactor || fd < 0 || !handler) {
        return -1;
    }
    reactor_poll_state_t *state = poll_state(reactor);
    if (!state) {
        return -1;
    }

    if (find_slot(state, fd) >= 0) {
        return reactor->backend->update(reactor, fd, events);
    }

    size_t index = 0;
    for (; index < state->count; ++index) {
        if (!state->slots[index].active) {
            break;
        }
    }
    if (index == state->count) {
        if (index >= state->capacity) {
            size_t requested = state->capacity == 0 ? 4 : state->capacity * 2;
            if (reserve_slots(state, requested) != 0) {
                return -1;
            }
        }
        ++state->count;
    }

    state->slots[index].fd = fd;
    state->slots[index].events = events;
    state->slots[index].handler = handler;
    state->slots[index].userdata = userdata;
    state->slots[index].active = 1;
    state->slots[index].poll_index = -1;

    state->pollfds[index].fd = fd;
    state->pollfds[index].events = events;
    state->pollfds[index].revents = 0;
    return 0;
}

static int poll_update(reactor_t *reactor, int fd, short events) {
    reactor_poll_state_t *state = poll_state(reactor);
    if (!state) {
        return -1;
    }
    ssize_t index = find_slot(state, fd);
    if (index < 0) {
        return -1;
    }
    state->slots[index].events = events;
    state->pollfds[index].events = events;
    return 0;
}

static void poll_remove(reactor_t *reactor, int fd) {
    reactor_poll_state_t *state = poll_state(reactor);
    if (!state) {
        return;
    }
    ssize_t index = find_slot(state, fd);
    if (index < 0) {
        return;
    }
    state->slots[index].active = 0;
    state->slots[index].handler = NULL;
    state->slots[index].userdata = NULL;
    state->slots[index].poll_index = -1;
    state->pollfds[index].fd = -1;
    state->pollfds[index].events = 0;
    state->pollfds[index].revents = 0;
}

static int poll_run(reactor_t *reactor, int timeout_ms) {
    reactor_poll_state_t *state = poll_state(reactor);
    if (!state) {
        return -1;
    }
    while (!reactor->should_stop) {
        nfds_t nfds = 0;
        for (size_t i = 0; i < state->count; ++i) {
            reactor_poll_slot_t *slot = &state->slots[i];
            if (!slot->active) {
                state->pollfds[i].fd = -1;
                state->pollfds[i].events = 0;
                state->pollfds[i].revents = 0;
                slot->poll_index = -1;
                continue;
            }
            state->pollfds[nfds].fd = slot->fd;
            state->pollfds[nfds].events = slot->events;
            state->pollfds[nfds].revents = 0;
            slot->poll_index = (int)nfds;
            ++nfds;
        }

        if (nfds == 0) {
            break;
        }

        int ready = poll(state->pollfds, nfds, timeout_ms);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        for (size_t i = 0; i < state->count; ++i) {
            reactor_poll_slot_t *slot = &state->slots[i];
            if (!slot->active || slot->poll_index < 0) {
                continue;
            }
            struct pollfd *pfd = &state->pollfds[slot->poll_index];
            short revents = pfd->revents;
            pfd->revents = 0;
            slot->poll_index = -1;
            if (!revents) {
                continue;
            }
            if (!slot->handler) {
                continue;
            }
            reactor_event_result_t result = slot->handler(slot->fd, revents, slot->userdata);
            if (result == REACTOR_EVENT_REMOVE) {
                poll_remove(reactor, slot->fd);
            } else if (result == REACTOR_EVENT_SHUTDOWN) {
                reactor_stop(reactor);
                break;
            }
        }
    }
    return 0;
}

static const reactor_backend_t poll_backend = {
    .name = "poll",
    .init = poll_init,
    .deinit = poll_deinit,
    .add = poll_add,
    .update = poll_update,
    .remove = poll_remove,
    .run = poll_run,
};

const reactor_backend_t *reactor_backend_default(void) {
    return &poll_backend;
}
