#include "reactor.h"

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)

#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>

typedef struct reactor_kqueue_slot {
    int fd;
    short events;
    reactor_event_handler_fn handler;
    void *userdata;
    struct reactor_kqueue_slot *next;
} reactor_kqueue_slot_t;

typedef struct reactor_kqueue_state {
    int kq;
    reactor_kqueue_slot_t *slots;
} reactor_kqueue_state_t;

static reactor_kqueue_state_t *kqueue_state(reactor_t *reactor) {
    return (reactor_kqueue_state_t *)reactor->backend_state;
}

static reactor_kqueue_slot_t *find_slot(reactor_kqueue_state_t *state, int fd) {
    for (reactor_kqueue_slot_t *slot = state->slots; slot; slot = slot->next) {
        if (slot->fd == fd) {
            return slot;
        }
    }
    return NULL;
}

static short filter_to_poll(int filter) {
    switch (filter) {
    case EVFILT_READ:
        return POLLIN;
    case EVFILT_WRITE:
        return POLLOUT;
    default:
        return 0;
    }
}

static int kqueue_state_init(reactor_t *reactor) {
    reactor_kqueue_state_t *state = calloc(1, sizeof(*state));
    if (!state) {
        return -1;
    }
    state->kq = kqueue();
    if (state->kq < 0) {
        free(state);
        return -1;
    }
    state->slots = NULL;
    reactor->backend_state = state;
    return 0;
}

static void kqueue_state_deinit(reactor_t *reactor) {
    reactor_kqueue_state_t *state = kqueue_state(reactor);
    if (!state) {
        return;
    }
    reactor_kqueue_slot_t *slot = state->slots;
    while (slot) {
        reactor_kqueue_slot_t *next = slot->next;
        free(slot);
        slot = next;
    }
    if (state->kq >= 0) {
        close(state->kq);
    }
    free(state);
    reactor->backend_state = NULL;
}

static int register_events(reactor_kqueue_state_t *state, reactor_kqueue_slot_t *slot, int action) {
    if (!slot) {
        return 0;
    }
    struct kevent kev[2];
    int idx = 0;
    if (slot->events & POLLIN) {
        EV_SET(&kev[idx++], slot->fd, EVFILT_READ, action, 0, 0, slot);
    }
    if (slot->events & POLLOUT) {
        EV_SET(&kev[idx++], slot->fd, EVFILT_WRITE, action, 0, 0, slot);
    }
    if (idx == 0) {
        return 0;
    }
    return kevent(state->kq, kev, idx, NULL, 0, NULL);
}

static int kqueue_add(reactor_t *reactor,
                      int fd,
                      short events,
                      reactor_event_handler_fn handler,
                      void *userdata) {
    if (!reactor || fd < 0 || !handler) {
        return -1;
    }
    reactor_kqueue_state_t *state = kqueue_state(reactor);
    if (!state) {
        return -1;
    }

    reactor_kqueue_slot_t *slot = find_slot(state, fd);
    if (!slot) {
        slot = calloc(1, sizeof(*slot));
        if (!slot) {
            return -1;
        }
        slot->fd = fd;
        slot->next = state->slots;
        state->slots = slot;
    }
    slot->events = events;
    slot->handler = handler;
    slot->userdata = userdata;

    if (register_events(state, slot, EV_ADD | EV_ENABLE) != 0) {
        return -1;
    }
    return 0;
}

static int kqueue_update(reactor_t *reactor, int fd, short events) {
    reactor_kqueue_state_t *state = kqueue_state(reactor);
    if (!state) {
        return -1;
    }
    reactor_kqueue_slot_t *slot = find_slot(state, fd);
    if (!slot) {
        return -1;
    }
    slot->events = events;
    if (register_events(state, slot, EV_ADD | EV_ENABLE) != 0) {
        return -1;
    }
    return 0;
}

static void kqueue_remove(reactor_t *reactor, int fd) {
    reactor_kqueue_state_t *state = kqueue_state(reactor);
    if (!state) {
        return;
    }
    reactor_kqueue_slot_t *prev = NULL;
    reactor_kqueue_slot_t *slot = state->slots;
    while (slot && slot->fd != fd) {
        prev = slot;
        slot = slot->next;
    }
    if (!slot) {
        return;
    }
    register_events(state, slot, EV_DELETE);
    if (prev) {
        prev->next = slot->next;
    } else {
        state->slots = slot->next;
    }
    free(slot);
}

static int kqueue_run(reactor_t *reactor, int timeout_ms) {
    reactor_kqueue_state_t *state = kqueue_state(reactor);
    if (!state) {
        return -1;
    }

    const int max_events = 64;
    struct kevent events[max_events];
    struct timespec timeout;
    struct timespec *timeout_ptr = NULL;

    if (timeout_ms >= 0) {
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_nsec = (timeout_ms % 1000) * 1000000L;
        timeout_ptr = &timeout;
    }

    while (!reactor->should_stop) {
        int ready = kevent(state->kq, NULL, 0, events, max_events, timeout_ptr);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        for (int i = 0; i < ready; ++i) {
            struct kevent *ev = &events[i];
            reactor_kqueue_slot_t *slot = ev->udata ? (reactor_kqueue_slot_t *)ev->udata
                                                    : find_slot(state, (int)ev->ident);
            if (!slot || !slot->handler) {
                continue;
            }
            short mapped = filter_to_poll(ev->filter);
            if (ev->flags & EV_ERROR) {
                mapped |= POLLERR;
            }
            reactor_event_result_t result = slot->handler(slot->fd, mapped, slot->userdata);
            if (result == REACTOR_EVENT_REMOVE) {
                kqueue_remove(reactor, slot->fd);
            } else if (result == REACTOR_EVENT_SHUTDOWN) {
                reactor_stop(reactor);
                break;
            }
        }
    }
    return 0;
}

static const reactor_backend_t kqueue_backend = {
    .name = "kqueue",
    .init = kqueue_state_init,
    .deinit = kqueue_state_deinit,
    .add = kqueue_add,
    .update = kqueue_update,
    .remove = kqueue_remove,
    .run = kqueue_run,
};

const reactor_backend_t *reactor_backend_kqueue(void) {
    return &kqueue_backend;
}

#endif /* kqueue platforms */
