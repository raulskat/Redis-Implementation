#include "reactor.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

static ssize_t reactor_find_slot(const reactor_t *reactor, int fd) {
    if (!reactor) {
        return -1;
    }
    for (size_t i = 0; i < reactor->count; ++i) {
        if (reactor->slots[i].active && reactor->slots[i].fd == fd) {
            return (ssize_t)i;
        }
    }
    return -1;
}

static int reactor_reserve(reactor_t *reactor, size_t size) {
    if (!reactor) {
        return -1;
    }
    if (size <= reactor->capacity) {
        return 0;
    }
    size_t old_capacity = reactor->capacity;
    size_t new_capacity = old_capacity == 0 ? size : old_capacity;
    while (new_capacity < size) {
        new_capacity *= 2;
        if (new_capacity == 0) {
            new_capacity = size;
            break;
        }
    }
    reactor_slot_t *new_slots =
        realloc(reactor->slots, new_capacity * sizeof(*reactor->slots));
    if (!new_slots) {
        return -1;
    }
    struct pollfd *new_pollfds =
        realloc(reactor->pollfds, new_capacity * sizeof(*reactor->pollfds));
    if (!new_pollfds) {
        free(new_slots);
        return -1;
    }
    reactor->slots = new_slots;
    reactor->pollfds = new_pollfds;
    for (size_t i = old_capacity; i < new_capacity; ++i) {
        reactor->slots[i].active = 0;
        reactor->slots[i].poll_index = -1;
        reactor->pollfds[i].fd = -1;
        reactor->pollfds[i].events = 0;
        reactor->pollfds[i].revents = 0;
    }
    reactor->capacity = new_capacity;
    return 0;
}

void reactor_init(reactor_t *reactor) {
    if (!reactor) {
        return;
    }
    reactor->slots = NULL;
    reactor->pollfds = NULL;
    reactor->count = 0;
    reactor->capacity = 0;
    reactor->should_stop = 0;
}

void reactor_deinit(reactor_t *reactor) {
    if (!reactor) {
        return;
    }
    free(reactor->slots);
    free(reactor->pollfds);
    reactor->slots = NULL;
    reactor->pollfds = NULL;
    reactor->count = 0;
    reactor->capacity = 0;
    reactor->should_stop = 0;
}

int reactor_add(reactor_t *reactor, int fd, short events, reactor_event_handler_fn handler, void *userdata) {
    if (!reactor || fd < 0 || !handler) {
        return -1;
    }
    if (reactor_find_slot(reactor, fd) >= 0) {
        return reactor_update(reactor, fd, events);
    }

    size_t index = 0;
    for (; index < reactor->count; ++index) {
        if (!reactor->slots[index].active) {
            break;
        }
    }
    if (index == reactor->count) {
        if (index >= reactor->capacity) {
            size_t requested = reactor->capacity == 0 ? 4 : reactor->capacity * 2;
            if (reactor_reserve(reactor, requested) != 0) {
                return -1;
            }
        }
        ++reactor->count;
    }

    reactor->slots[index].fd = fd;
    reactor->slots[index].events = events;
    reactor->slots[index].handler = handler;
    reactor->slots[index].userdata = userdata;
    reactor->slots[index].active = 1;
    reactor->slots[index].poll_index = -1;

    reactor->pollfds[index].fd = fd;
    reactor->pollfds[index].events = events;
    reactor->pollfds[index].revents = 0;

    return 0;
}

int reactor_update(reactor_t *reactor, int fd, short events) {
    ssize_t index = reactor_find_slot(reactor, fd);
    if (index < 0) {
        return -1;
    }
    reactor->slots[index].events = events;
    reactor->pollfds[index].events = events;
    return 0;
}

void reactor_remove(reactor_t *reactor, int fd) {
    ssize_t index = reactor_find_slot(reactor, fd);
    if (index < 0) {
        return;
    }
    reactor->slots[index].active = 0;
    reactor->slots[index].handler = NULL;
    reactor->slots[index].userdata = NULL;
    reactor->slots[index].poll_index = -1;
    reactor->pollfds[index].fd = -1;
    reactor->pollfds[index].events = 0;
    reactor->pollfds[index].revents = 0;
}

void reactor_stop(reactor_t *reactor) {
    if (!reactor) {
        return;
    }
    reactor->should_stop = 1;
}

int reactor_run(reactor_t *reactor) {
    if (!reactor) {
        return -1;
    }
    while (!reactor->should_stop) {
        nfds_t nfds = 0;
        for (size_t i = 0; i < reactor->count; ++i) {
            reactor_slot_t *slot = &reactor->slots[i];
            if (!slot->active) {
                reactor->pollfds[i].fd = -1;
                reactor->pollfds[i].events = 0;
                reactor->pollfds[i].revents = 0;
                slot->poll_index = -1;
                continue;
            }
            reactor->pollfds[nfds].fd = slot->fd;
            reactor->pollfds[nfds].events = slot->events;
            reactor->pollfds[nfds].revents = 0;
            slot->poll_index = (int)nfds;
            ++nfds;
        }

        if (nfds == 0) {
            break;
        }

        int ready = poll(reactor->pollfds, nfds, -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        for (size_t i = 0; i < reactor->count; ++i) {
            reactor_slot_t *slot = &reactor->slots[i];
            if (!slot->active || slot->poll_index < 0) {
                continue;
            }
            struct pollfd *pfd = &reactor->pollfds[slot->poll_index];
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
                reactor_remove(reactor, slot->fd);
            } else if (result == REACTOR_EVENT_SHUTDOWN) {
                reactor_stop(reactor);
                break;
            }
        }
    }
    return 0;
}
