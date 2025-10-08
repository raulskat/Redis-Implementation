#include "reactor.h"

#ifdef __linux__

#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <unistd.h>

typedef struct reactor_epoll_slot {
    int fd;
    short events;
    reactor_event_handler_fn handler;
    void *userdata;
    struct reactor_epoll_slot *next;
} reactor_epoll_slot_t;

typedef struct reactor_epoll_state {
    int epoll_fd;
    reactor_epoll_slot_t *slots;
} reactor_epoll_state_t;

static reactor_epoll_state_t *epoll_state(reactor_t *reactor) {
    return (reactor_epoll_state_t *)reactor->backend_state;
}

static reactor_epoll_slot_t *find_slot(reactor_epoll_state_t *state, int fd) {
    for (reactor_epoll_slot_t *slot = state->slots; slot; slot = slot->next) {
        if (slot->fd == fd) {
            return slot;
        }
    }
    return NULL;
}

static uint32_t poll_events_to_epoll(short events) {
    uint32_t mapped = 0;
    if (events & POLLIN) {
        mapped |= EPOLLIN;
    }
    if (events & POLLOUT) {
        mapped |= EPOLLOUT;
    }
    if (events & POLLERR) {
        mapped |= EPOLLERR;
    }
    if (events & POLLHUP) {
        mapped |= EPOLLHUP;
    }
    return mapped;
}

static short epoll_events_to_poll(uint32_t events) {
    short mapped = 0;
    if (events & (EPOLLIN | EPOLLPRI)) {
        mapped |= POLLIN;
    }
    if (events & EPOLLOUT) {
        mapped |= POLLOUT;
    }
    if (events & EPOLLERR) {
        mapped |= POLLERR;
    }
    if (events & EPOLLHUP) {
        mapped |= POLLHUP;
    }
    return mapped;
}

static int epoll_state_init(reactor_t *reactor) {
    reactor_epoll_state_t *state = calloc(1, sizeof(*state));
    if (!state) {
        return -1;
    }
    state->epoll_fd = epoll_create1(0);
    if (state->epoll_fd < 0) {
        free(state);
        return -1;
    }
    state->slots = NULL;
    reactor->backend_state = state;
    return 0;
}

static void epoll_state_deinit(reactor_t *reactor) {
    reactor_epoll_state_t *state = epoll_state(reactor);
    if (!state) {
        return;
    }
    reactor_epoll_slot_t *slot = state->slots;
    while (slot) {
        reactor_epoll_slot_t *next = slot->next;
        free(slot);
        slot = next;
    }
    if (state->epoll_fd >= 0) {
        close(state->epoll_fd);
    }
    free(state);
    reactor->backend_state = NULL;
}

static int epoll_add(reactor_t *reactor,
                     int fd,
                     short events,
                     reactor_event_handler_fn handler,
                     void *userdata) {
    if (!reactor || fd < 0 || !handler) {
        return -1;
    }
    reactor_epoll_state_t *state = epoll_state(reactor);
    if (!state) {
        return -1;
    }

    reactor_epoll_slot_t *existing = find_slot(state, fd);
    if (existing) {
        existing->handler = handler;
        existing->userdata = userdata;
        existing->events = events;
        return reactor->backend->update(reactor, fd, events);
    }

    reactor_epoll_slot_t *slot = calloc(1, sizeof(*slot));
    if (!slot) {
        return -1;
    }
    slot->fd = fd;
    slot->events = events;
    slot->handler = handler;
    slot->userdata = userdata;
    slot->next = state->slots;
    state->slots = slot;

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = poll_events_to_epoll(events);
    ev.data.ptr = slot;
    if (epoll_ctl(state->epoll_fd, EPOLL_CTL_ADD, fd, &ev) != 0) {
        state->slots = slot->next;
        free(slot);
        return -1;
    }
    return 0;
}

static int epoll_update(reactor_t *reactor, int fd, short events) {
    reactor_epoll_state_t *state = epoll_state(reactor);
    if (!state) {
        return -1;
    }
    reactor_epoll_slot_t *slot = find_slot(state, fd);
    if (!slot) {
        return -1;
    }
    slot->events = events;
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = poll_events_to_epoll(events);
    ev.data.ptr = slot;
    if (epoll_ctl(state->epoll_fd, EPOLL_CTL_MOD, fd, &ev) != 0) {
        return -1;
    }
    return 0;
}

static void epoll_remove(reactor_t *reactor, int fd) {
    reactor_epoll_state_t *state = epoll_state(reactor);
    if (!state) {
        return;
    }
    reactor_epoll_slot_t *prev = NULL;
    reactor_epoll_slot_t *slot = state->slots;
    while (slot && slot->fd != fd) {
        prev = slot;
        slot = slot->next;
    }
    if (!slot) {
        return;
    }
    epoll_ctl(state->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    if (prev) {
        prev->next = slot->next;
    } else {
        state->slots = slot->next;
    }
    free(slot);
}

static int epoll_run(reactor_t *reactor, int timeout_ms) {
    reactor_epoll_state_t *state = epoll_state(reactor);
    if (!state) {
        return -1;
    }
    const int max_events = 64;
    struct epoll_event events[max_events];

    while (!reactor->should_stop) {
        int ready = epoll_wait(state->epoll_fd, events, max_events, timeout_ms);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        for (int i = 0; i < ready; ++i) {
            reactor_epoll_slot_t *slot = (reactor_epoll_slot_t *)events[i].data.ptr;
            if (!slot || !slot->handler) {
                continue;
            }
            short mapped = epoll_events_to_poll(events[i].events);
            reactor_event_result_t result = slot->handler(slot->fd, mapped, slot->userdata);
            if (result == REACTOR_EVENT_REMOVE) {
                epoll_remove(reactor, slot->fd);
            } else if (result == REACTOR_EVENT_SHUTDOWN) {
                reactor_stop(reactor);
                break;
            }
        }
    }
    return 0;
}

static const reactor_backend_t epoll_backend = {
    .name = "epoll",
    .init = epoll_state_init,
    .deinit = epoll_state_deinit,
    .add = epoll_add,
    .update = epoll_update,
    .remove = epoll_remove,
    .run = epoll_run,
};

const reactor_backend_t *reactor_backend_epoll(void) {
    return &epoll_backend;
}

#endif /* __linux__ */
