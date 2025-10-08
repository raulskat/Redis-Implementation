#ifndef COMMAND_EVENTS_H
#define COMMAND_EVENTS_H

#include <stddef.h>

#include "resp.h"

struct command_context_t;

typedef enum {
    COMMAND_EVENT_GENERIC = 0,
    COMMAND_EVENT_WRITE,
    COMMAND_EVENT_DELETE,
    COMMAND_EVENT_EXPIRY,
} command_event_type_t;

typedef struct {
    command_event_type_t type;
    const char *command_name;
    const resp_command_t *command;
    struct command_context_t *context;
    int handler_result;
} command_event_t;

typedef int (*command_event_listener_fn)(const command_event_t *event, void *userdata);

typedef struct {
    command_event_listener_fn fn;
    void *userdata;
} command_event_listener_t;

typedef struct {
    command_event_listener_t *listeners;
    size_t count;
    size_t capacity;
} command_event_dispatcher_t;

void command_event_dispatcher_init(command_event_dispatcher_t *dispatcher);
void command_event_dispatcher_deinit(command_event_dispatcher_t *dispatcher);
int command_event_dispatcher_add(command_event_dispatcher_t *dispatcher,
                                 command_event_listener_fn fn,
                                 void *userdata);
void command_event_dispatcher_remove_all(command_event_dispatcher_t *dispatcher);
void command_event_dispatcher_dispatch(command_event_dispatcher_t *dispatcher,
                                       const command_event_t *event);

#endif /* COMMAND_EVENTS_H */
