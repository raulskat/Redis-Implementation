# C Language Basics for Redis-Implementation

This guide covers the C features you will touch most often while working on Redis-Implementation. It bridges theoretical concepts with code that already lives in the tree, so reading through the linked files alongside the explanations is encouraged.

---

## Translation Units and Header Files

* **Translation unit:** Each `.c` file (for example, `src/command.c`) is compiled independently, then linked together. Keep function definitions private with `static` when they are used only inside one file.
* **Header interface:** Headers (`include/*.h`) expose data structures and function prototypes. They use include guards (e.g. `#ifndef ACL_H` in `include/acl.h`) to avoid duplicate definitions.
* **`extern` vs. definitions:** Declarations live in headers, implementations are in `.c` files. Never allocate storage in headers; instead provide prototypes (`void acl_reset_session(acl_state_t *acl);`) and define the function in a `.c` file.

---

## Core Data Types

| Type | Why it matters here | Example |
|------|---------------------|---------|
| `int`, `unsigned int` | Command counts, bitmasks, return codes. | ACL roles are defined as bitmasks (`include/acl.h`). |
| `size_t` | Represents sizes and indexes; always non-negative. | Loop bounds in `src/resp.c`. |
| `uint64_t` / `int64_t` | Millisecond timestamps and TTL math. | `datastore_current_time_ms()` in `src/datastore.c`. |
| `bool` | Readability for flags (C99’s `<stdbool.h>`). | `acl_state_t.require_auth` in `include/acl.h`. |
| `char *` | Strings are null-terminated arrays. Memory ownership matters—free if you allocate. |

> **Rule of thumb:** Use fixed-width integers for protocol-visible values (timestamps, replication offsets) so behavior is consistent on 32-bit vs. 64-bit targets.

---

## Structs and Typedefs

Structures capture related state. We prefer `typedef struct { ... } name_t;` to simplify signatures.

```c
typedef struct {
    command_context_t *ctx;
    acl_state_t acl;
    int resp_version;
    char connection_name[REDIS_CONNECTION_NAME_MAX];
    char persisted_connection_name[REDIS_CONNECTION_NAME_MAX];
    bool has_persisted_name;
} command_session_t; /* include/command.h */
```

* Access members with `.` (for values) or `->` (for pointers).
* Group booleans near each other so the struct is cache-friendly.
* Use `const` on pointer arguments if the function does not modify the data.

---

## Memory Management

* **Heap allocation:** `malloc`, `calloc`, `realloc`, and `free` live in `<stdlib.h>`. Always check for `NULL` before using the pointer.
* **Zero-initialization:** Use `calloc` or `memset(ptr, 0, size)` when you need a clean slate.
* **Ownership:** Whoever allocates a pointer is responsible for freeing it, unless explicitly documented otherwise. Functions like `resp_parse` allocate `cmd->argv`; callers must invoke `resp_command_free` after use.
* **Jemalloc:** The build can link against jemalloc (`make JEMALLOC=1`). Allocator semantics stay the same because jemalloc is a drop-in replacement for the glibc allocator.

---

## Bitmask Flags

Many subsystems (ACL, command dispatcher) use bitmasks. Define masks with `1u << n` and combine them with bitwise OR (`|`).

```c
#define ACL_ROLE_READ  (1u << 0)
#define ACL_ROLE_WRITE (1u << 1)
#define ACL_ROLE_ADMIN (1u << 2)

unsigned int required_roles = 0;
if (spec->flags & CMD_FLAG_WRITE) {
    required_roles |= ACL_ROLE_WRITE;
}
```

To test whether a role set includes another set, use `if ((roles & mask) == mask) { ... }` as shown in `acl_has_role` (`src/acl.c`).

---

## Error Handling Patterns

* Return `0` on success, `-1` or a positive errno-compatible value on failure. For example, `persistence_save` returns `EBUSY` when a background save is already running.
* Set `errno` when you propagate system-level errors. Prefer POSIX error codes (`EAGAIN`, `EINVAL`, etc.) for consistency.
* For public APIs, validate pointers (`if (!ctx) return -1;`) at the top of the function to prevent segmentation faults.
* Use descriptive error strings in RESP replies (e.g., `"ERR wrong number of arguments"`).

---

## Static Functions and Internal Linkage

Mark helper functions `static` to keep them file-local, reducing namespace pollution and allowing the compiler to inline aggressively. Examples include `send_all` in `src/handlers_basic.c` and `respond_with_mutation_flag` in `src/handlers_expiration.c`.

---

## Const-Correctness

* `const` protects data from accidental modification. Use it for configurations and command inputs (`const resp_command_t *cmd`).
* Passing `const` pointers lets the compiler place read-only data in `.rodata`, enabling optimizations.

---

## Threading and Synchronization

The project uses POSIX threads:

* `pthread_create` spawns background workers (e.g., `persistence_execute_async` in `src/persistence.c`).
* `pthread_mutex_t` guards shared structures (`save_lock`, `store->lock`). Always pair `pthread_mutex_lock` with `pthread_mutex_unlock`, even on early returns.
* Prefer `pthread_mutex_init` defaults; destroy the mutex when the owning object is torn down if the lifetime is dynamic.

> **Deadlock avoidance:** Acquire locks in a consistent order. For now, the datastore and persistence layers avoid nested locking; keep it that way unless you design a new locking strategy.

---

## Socket Programming Essentials

While deeper protocol details live in `network_protocols.md`, keep these C-specific points in mind:

* Sockets are integer file descriptors (`int`), wrapped by functions like `send` and `recv` from `<sys/socket.h>`.
* Network calls can return `-1` with `errno == EINTR`. Retry loops check for this and continue (`send_all` helper).
* For non-blocking I/O, be ready for `EAGAIN` / `EWOULDBLOCK` and exit the read loop gracefully (`connection_handle_event` in `src/connection.c`).

---

## Macros and Conditional Compilation

* Use macros for compile-time constants only. Avoid function-like macros when inline functions will do.
* Conditional compilation toggles features (`#ifdef HAVE_JEMALLOC`). Keep feature detection centralized in headers or top-level build files.

---

## Logging and Printing

* The codebase uses `printf` for simple logging. Wrap new logging points in helper functions if you need structured output so we can swap in a proper logging framework later.
* Use `%zu` for `size_t`, `%lld` for `long long`, `%llu` for `unsigned long long`, and `%s` for strings. Mixing specifiers leads to undefined behavior.

---

## Defensive Practices

1. **Initialize variables** when you declare them to avoid undefined values.
2. **Gracefully handle null pointers** from configuration or optional subsystems (`ctx->config` may be `NULL` in some handlers).
3. **Document ownership and lifetime** in comments when it is not obvious. This dictionary should supplement, not replace, inline comments.
4. **Prefer enums for finite states** (`command_event_type_t`) for type safety.

---

## Next Steps

If you are confident with these basics, move on to:

* [`build_and_tooling.md`](build_and_tooling.md) to understand how everything is compiled and linked.
* [`runtime_architecture.md`](runtime_architecture.md) for the big-picture flow from network events to command execution.
* [`glossary.md`](glossary.md) whenever you encounter an acronym or Redis-specific term.
