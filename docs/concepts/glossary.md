# Glossary

Use this alphabetical glossary to decode terminology, acronyms, and idioms used throughout Redis-Implementation. Each entry points to relevant concept guides or source files for deeper exploration.

---

### A

**ACL (Access Control List)**  
Role-based authorization system controlling command access. See [`security_acl.md`](security_acl.md).

**AOF (Append Only File)**  
Persistence mode that logs every write. Not implemented yet; planned in `docs/redis_parity_design.md`.

**Async**  
Operations running on background threads (e.g., `BGSAVE`). Implemented in `src/persistence.c` via `pthread_create`.

### B

**Backlog (Replication)**  
In-memory buffer of serialized write commands awaiting transmission to replicas. See `src/replication.c`.

**Bulk String**  
RESP type representing binary-safe strings. Format: `$<len>\r\n<data>\r\n`. Covered in [`network_protocols.md`](network_protocols.md).

### C

**Command Dispatcher**  
Registry mapping command names to handlers with metadata (flags, validators). See `src/command.c` and `src/command_dispatcher.c`.

**Connection Name**  
Label assigned via `HELLO SETNAME`. Persisted when `--persist-connection-names` is enabled. More in [`security_acl.md`](security_acl.md).

**Const Correctness**  
Practice of marking parameters as `const` when they should not be modified. Reviewed in [`c_language_basics.md`](c_language_basics.md).

### D

**Datastore**  
Hash table-based key/value storage with TTL support. Explained in [`data_storage.md`](data_storage.md).

**Dynamic String (SDS)**  
String representation with length metadata used for Redis compatibility. Implemented in `src/sds.c`; see [`data_storage.md`](data_storage.md).

### E

**Event Loop (Reactor)**  
Multiplexes socket readiness notifications. Handles incoming traffic without blocking. Covered in [`runtime_architecture.md`](runtime_architecture.md).

**Expiry (TTL)**  
Mechanism to remove keys after a timeout. Includes passive and active pruning. See [`data_storage.md`](data_storage.md).

### F

**FNV-1a Hash**  
Hash function used to distribute keys across buckets in the datastore (`hash_key` in `src/datastore.c`).

### G

**GDB**  
GNU Debugger, recommended for stepping through execution. Referenced in [`testing_debugging.md`](testing_debugging.md).

### H

**HELLO**  
Command for protocol negotiation, authentication, and connection naming. Implementation details in [`network_protocols.md`](network_protocols.md).

### J

**Jemalloc**  
High-performance memory allocator. Enable with `make JEMALLOC=1`. Notes in [`build_and_tooling.md`](build_and_tooling.md).

### L

**Linker Flags (`LDFLAGS`)**  
Options passed to the linker when building. Explained in [`build_and_tooling.md`](build_and_tooling.md).

### M

**Map (RESP3)**  
Key/value reply type introduced in RESP3. Used in `HELLO` and `CONFIG GET` responses. See [`network_protocols.md`](network_protocols.md).

### N

**Null Reply**  
Represents missing keys (`_` in RESP3, `$-1` in RESP2). Encoded via `resp_send_null`. Documented in [`network_protocols.md`](network_protocols.md).

### P

**Persistence**  
Durability features (SAVE/BGSAVE). Described in [`persistence_replication.md`](persistence_replication.md).

**PSYNC**  
Replication command for partial synchronization. Stubbed in `src/handlers_replication.c` pending full implementation.

### R

**RESP (Redis Serialization Protocol)**  
Wire protocol for client/server communication. See [`network_protocols.md`](network_protocols.md).

**Role Bitmask**  
Unsigned integer combining ACL capabilities (`read`, `write`, `admin`). Defined in `include/acl.h` and explained in [`security_acl.md`](security_acl.md).

### S

**Session**  
Per-connection state including ACL, RESP version, and connection name. See `include/command.h` and [`runtime_architecture.md`](runtime_architecture.md).

**Snapshot (RDB)**  
Serialized copy of the datastore saved to disk. Discussed in [`persistence_replication.md`](persistence_replication.md).

### T

**TLS**  
Transport Layer Security. Not yet implemented; slated for future roadmaps.

**TTL (Time-To-Live)**  
Expiration timer set on keys. Handling described in [`data_storage.md`](data_storage.md).

### U

**Undefined Behavior**  
Situations where C does not define program behavior (e.g., accessing freed memory). Use sanitizers to catch issues early. See [`testing_debugging.md`](testing_debugging.md).

### V

**Validators (Command)**  
Optional pre-handler checks enforcing command-specific rules (e.g., `CONFIG GET`). Implemented via `command_validator_t` in `src/command.c`.

### W

**Write Commands**  
Operations that mutate the datastore. They carry `CMD_FLAG_WRITE` and often trigger replication/backlog updates. See `include/command_dispatcher.h`.

### Z

**Zero-copy**  
Technique to avoid data duplication. Currently aspirational; track progress in `docs/redis_parity_design.md`.

---

Missing a term? Add it here and cross-link to the relevant concept file so the dictionary stays comprehensive.
