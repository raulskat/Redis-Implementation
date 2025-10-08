#include "reactor.h"

#include <stdlib.h>

#if defined(__linux__)
extern const reactor_backend_t *reactor_backend_epoll(void);
#endif
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
extern const reactor_backend_t *reactor_backend_kqueue(void);
#endif
extern const reactor_backend_t *reactor_backend_poll(void);

static int reactor_switch_backend(reactor_t *reactor, const reactor_backend_t *backend) {
    if (reactor->backend && reactor->backend->deinit) {
        reactor->backend->deinit(reactor);
    }
    reactor->backend = backend;
    reactor->backend_state = NULL;
    if (backend && backend->init) {
        return backend->init(reactor);
    }
    return 0;
}

void reactor_init(reactor_t *reactor) {
    if (!reactor) {
        return;
    }
    reactor->backend = NULL;
    reactor->backend_state = NULL;
    reactor->should_stop = 0;
    const reactor_backend_t *backend = reactor_backend_default();
    if (reactor_set_backend(reactor, backend) != 0) {
        const reactor_backend_t *fallback = reactor_backend_poll();
        reactor_set_backend(reactor, fallback);
    }
}

void reactor_deinit(reactor_t *reactor) {
    if (!reactor) {
        return;
    }
    reactor_switch_backend(reactor, NULL);
    reactor->should_stop = 0;
}

int reactor_set_backend(reactor_t *reactor, const reactor_backend_t *backend) {
    if (!reactor) {
        return -1;
    }
    if (!backend) {
        reactor_switch_backend(reactor, NULL);
        return 0;
    }
    return reactor_switch_backend(reactor, backend);
}

#if defined(__linux__)
static const reactor_backend_t *reactor_preferred_backend(void) {
    return reactor_backend_epoll();
}
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
static const reactor_backend_t *reactor_preferred_backend(void) {
    return reactor_backend_kqueue();
}
#else
static const reactor_backend_t *reactor_preferred_backend(void) {
    return reactor_backend_poll();
}
#endif

const reactor_backend_t *reactor_backend_default(void) {
    const reactor_backend_t *preferred = reactor_preferred_backend();
    if (!preferred) {
        return reactor_backend_poll();
    }
    return preferred;
}

int reactor_add(reactor_t *reactor,
                int fd,
                short events,
                reactor_event_handler_fn handler,
                void *userdata) {
    if (!reactor || !reactor->backend || !reactor->backend->add) {
        return -1;
    }
    return reactor->backend->add(reactor, fd, events, handler, userdata);
}

int reactor_update(reactor_t *reactor, int fd, short events) {
    if (!reactor || !reactor->backend || !reactor->backend->update) {
        return -1;
    }
    return reactor->backend->update(reactor, fd, events);
}

void reactor_remove(reactor_t *reactor, int fd) {
    if (!reactor || !reactor->backend || !reactor->backend->remove) {
        return;
    }
    reactor->backend->remove(reactor, fd);
}

void reactor_stop(reactor_t *reactor) {
    if (!reactor) {
        return;
    }
    reactor->should_stop = 1;
}

int reactor_run(reactor_t *reactor) {
    if (!reactor || !reactor->backend || !reactor->backend->run) {
        return -1;
    }
    return reactor->backend->run(reactor, -1);
}
