# Networking & Protocol Concepts

This guide explains the network stack that powers Redis-Implementation. It covers TCP fundamentals, how we parse and emit RESP2/RESP3 frames, and the semantics of connection-oriented commands such as `AUTH`, `HELLO`, and `REPLCONF`.

---

## TCP Essentials

* Redis is a long-lived TCP server. Each client connection is a stream socket using IPv4 or IPv6.
* Sockets are configured as non-blocking. We rely on the reactor to notify us when data is available (`POLLIN`) or when we can send more (`POLLOUT`).
* Common edge cases:
  * **Half-close:** `recv` returns `0` -> remote closed connection.
  * **Interrupted call:** `errno == EINTR` -> retry the system call.
  * **Backpressure:** `errno == EAGAIN` -> pause until the next reactor tick.

---

## RESP Protocol Primer

RESP (REdis Serialization Protocol) is the wire format for commands and replies. We support both RESP2 and RESP3, negotiated via `HELLO`.

### RESP2 Cheat Sheet

| Type | Prefix | Example |
|------|--------|---------|
| Simple String | `+` | `+OK\r\n` |
| Error | `-` | `-ERR invalid\r\n` |
| Integer | `:` | `:1000\r\n` |
| Bulk String | `$<len>\r\n<data>\r\n` | `$3\r\nfoo\r\n` |
| Null Bulk String | `$-1\r\n` | |
| Array | `*<len>\r\n...` | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

### RESP3 Extensions

| Type | Prefix | Usage in this project |
|------|--------|-----------------------|
| Null | `_` | Used for missing values (`GET`, `INFO`). |
| Boolean | `#t` / `#f` | Returned by `EXPIRE`, `PERSIST` when RESP3 is active. |
| Map | `%<len>\r\n` | `HELLO` and `CONFIG GET` replies. |
| Set | `~<len>\r\n` | `KEYS` response in RESP3 mode. |

The parser (`resp_parse`) only needs to understand RESP2-style command frames because clients send commands as arrays. Reply helpers decide whether to send RESP2 or RESP3 encodings based on `session->resp_version`.

---

## Command Parsing Flow

1. **Buffering:** `connection_handle_event` gathers bytes until a full RESP array is buffered.
2. **Parsing:** `resp_parse` validates frame structure, allocates `argv`, and surfaces `argc`.
3. **Dispatch:** `command_handle` routes the command to its handler.
4. **Cleanup:** `resp_command_free` releases allocated memory.

_Invariant:_ The parser assumes `\r\n` termination for each bulk string. Always write replies with the exact terminators to stay protocol-compliant.

---

## `HELLO` Negotiation

`HELLO` is the first command modern clients use. Key options:

* `HELLO 3` selects RESP3 (defaults to RESP2).
* `AUTH <username> <password>` authenticates using ACL data loaded from the configuration.
* `SETNAME <connection-name>` assigns a connection label mirrored in `INFO clients` (future command) and persistent if `--persist-connection-names` is set.

Handler reference: `handle_hello_command` in `src/handlers_basic.c`.

RESP3 reply example (`send_resp3_hello`):

```
%8
$5
proto
:3
$6
server
$5
redis
...
```

---

## Authentication and ACL Flow

* `AUTH <password>` (RESP2 clients) or `HELLO AUTH <username> <password>` (RESP3 aware clients) both call `acl_authenticate`.
* Failures return `-WRONGPASS invalid username-password pair` or `-ERR invalid username-password pair` to match Redis semantics.
* When authentication succeeds, handlers continue as usual; otherwise the dispatcher blocks commands unless they carry `CMD_FLAG_ALLOW_UNAUTH`.

See `docs/concepts/security_acl.md` for a deeper look at role enforcement.

---

## Networking Helpers Worth Knowing

* `send_all` in `src/handlers_basic.c` ensures full frames reach the wire, retrying on `EINTR`.
* `resp_send_*` functions centralize formatting, so handlers rarely touch low-level socket APIs directly.
* `connection_name` is stored in `command_session_t` and appended to the `HELLO` reply.

---

## Replication Negotiation (Planned)

While replication streaming is still under development, the current handshake scaffolding is in place:

1. **Replica connects** and sends `PING`, `REPLCONF capa psync2`, etc.
2. **Master responds** with placeholder replies (`+PONG`, `+OK`).
3. **`PSYNC`** always triggers a full resync for now (`FULLRESYNC` with static replication ID).
4. **Backlog** (`src/replication.c`) accumulates executed write commands for future incremental sync.

Future tasks will extend this area to stream write commands continuously and maintain offsets.

---

## Error Responses and Client Experience

* Use descriptive `ERR` messages so clients understand misconfigurations (missing arguments, unauthorized access).
* RESP3 errors still use the `-ERR` prefix for compatibility.
* When disconnecting a client due to protocol errors, send a final error message before closing the socket when possible (`"ERR protocol error"` from `resp_parse`).

---

## Testing Tools

* `redis-cli` works out of the box for interactive testing. RESP3 features require Redis 6+ client binaries (`redis-cli --resp 3`).
* `nc` (netcat) is handy for raw protocol debugging: `printf '*1\r\n$4\r\nPING\r\n' | nc -q1 127.0.0.1 6379`.
* Consider using `socat` to capture traffic or simulate slow clients for stress testing.

---

## Troubleshooting Network Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Clients disconnect immediately | Parser saw malformed RESP | Inspect logs for `"ERR protocol error"`, replay payload with `redis-cli --raw`. |
| `HELLO` returns RESP2 payloads | Session still in RESP2 mode | Ensure command included the protocol number (`HELLO 3`). |
| Authentication always fails | Username mismatch or missing ACL user | Confirm config via `--requireuser`/`--acluser` and check `docs/concepts/security_acl.md`. |
| Replica handshake stuck | Replication streaming incomplete | Feature pending; track progress in `docs/redis_parity_design.md`. |

---

## Further Reading

* [`runtime_architecture.md`](runtime_architecture.md) for the broader context.
* [`security_acl.md`](security_acl.md) for deeper ACL behavior and flags.
* Official Redis protocol docs: <https://redis.io/docs/reference/protocol-spec/>
