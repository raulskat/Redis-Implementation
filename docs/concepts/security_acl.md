# Security & ACL Concepts

Access control in Redis-Implementation is role-based and designed to mirror Redis ACL semantics while remaining lightweight. This guide explains configuration flags, authentication flow, role enforcement, and best practices for adding new protected commands.

---

## ACL Building Blocks

| Term | Definition |
|------|------------|
| **ACL user** | Named credential with password (optional) and role mask. |
| **Role mask** | Bitmask representing allowed capability flags (read/write/admin). |
| **Default user** | The account selected by `--requireuser` (falls back to `default`). |
| **Session** | Per-connection ACL context tracking authentication status and RESP version. |

The relevant structures live in `include/acl.h` and `src/acl.c`.

---

## Configuration Flags

| Flag | Purpose | Example |
|------|---------|---------|
| `--requireuser <name>` | Sets the default ACL username. | `--requireuser app` |
| `--requirepass <password>` | Assigns password to the default user. | `--requirepass secret` |
| `--userrole <roles>` | Comma-separated list (`read`, `write`, `admin`, `all`) for default roles. | `--userrole read,write` |
| `--acluser user=pass:roles` | Declares additional users. Password is optional; roles default to `all`. | `--acluser metrics=:read` |
| `--persist-connection-names` | Keeps `HELLO SETNAME` values across reconnects. | `--persist-connection-names` |

Roles map to command flags:

* `read`  → `CMD_FLAG_READONLY`
* `write` → `CMD_FLAG_WRITE`, `CMD_FLAG_DELETE`, `CMD_FLAG_EXPIRY`
* `admin` → `CMD_FLAG_ADMIN`

Commands can combine flags; the user must possess every required role bit to execute the command.

---

## Authentication Flow

1. **Session initialization:** `command_session_init` clones global ACL config and resets the session (`session->acl`). If the default user has no password, the session starts authenticated.
2. **Credentials provided:** Via `AUTH password` or `HELLO AUTH username password`.
3. **Lookup:** `acl_authenticate` matches username and password against configured users. Password comparison is constant-time per byte in practice (simple `strncmp` + length check).
4. **Result:** On success, `acl_state_t.authenticated` becomes `true` and the role mask is loaded. On failure, roles reset to zero and commands (except those flagged with `CMD_FLAG_ALLOW_UNAUTH`) are rejected.

---

## Role Enforcement

`command_spec_validate` orchestrates access control:

1. Permission flags are derived from the command spec (e.g., `CMD_FLAG_WRITE` + `CMD_FLAG_ADMIN`).
2. If the command is not marked `CMD_FLAG_ALLOW_UNAUTH`, unauthenticated clients receive `NOAUTH Authentication required.`
3. Role checks call `acl_has_role`, ensuring the session’s mask covers all required bits. Failures respond with `NOPERM this user has no access to the command`.

To add a new protected command:

```c
CMD_DEF("DEL", handle_del_command, CMD_FLAG_WRITE | CMD_FLAG_DELETE, 2, -1);
```

Make sure the spec’s flags reflect the access category, then update documentation under `docs/COMMANDS.md` as appropriate.

---

## ACL Data Structures

```c
typedef struct {
    bool require_auth;
    bool authenticated;
    unsigned int roles;
    const redis_acl_user_t *users;
    size_t user_count;
    size_t default_user_index;
    const redis_acl_user_t *default_user;
    char username[REDIS_USERNAME_MAX];
} acl_state_t;
```

`acl_state_t` stores only references to the global user array (`redis_acl_user_t`) configured at startup. No dynamic allocation occurs during authentication, keeping the critical path fast.

---

## Connection Names

* `HELLO SETNAME <name>` updates `session->connection_name` and optionally the persisted copy (`session->persisted_connection_name`).
* If `redis_config_t.persist_connection_names` is true, reconnecting clients inherit their last `SETNAME`, useful for monitoring dashboards.
* Names are echoed in RESP3 `HELLO` replies and will appear in future `CLIENT LIST` implementations.

---

## Security Best Practices

* **Use strong passwords.** Even though the server runs locally during development, treat credentials seriously in production deployments.
* **Least privilege:** Assign minimal roles to each user. For example, monitoring services often need only `read` access.
* **Avoid hard-coded secrets:** Load credentials from environment variables or config files instead of source control when possible.
* **Audit logs:** Add logging to `acl_authenticate` in future enhancements to trace failed attempts (ensure rate limiting to avoid log floods).

---

## Extending ACL Capabilities

* **Command categories:** Introduce fine-grained roles (e.g., `slowlog`) by adding new bits to `ACL_ROLE_*` and mapping them in `command_spec_validate`.
* **Pattern-based permissions:** Borrow from Redis ACL syntax (`~pattern`, `+command`). This requires a more expressive data model than the current bitmask-only approach.
* **Dynamic ACL edits:** Allow `ACL SETUSER`-style runtime modifications. Would require thread-safe updates to the global user list and a reader-writer lock or atomic swap.

Document new behaviors here and in `docs/redis_parity_design.md` to keep the parity roadmap accurate.

---

## Troubleshooting

| Symptom | Possible Cause | Diagnostic Steps |
|---------|----------------|------------------|
| `NOAUTH Authentication required.` despite calling `AUTH` | Wrong username, session reset, or missing `--requirepass`. | Confirm config with command-line flags, inspect `session->acl.username` in debugger. |
| `NOPERM` on write commands | User lacks `write` or `admin` role. | Check `--userrole` and `--acluser` definitions; roles combine via bitmasks. |
| HELLO returns empty username | Client skipped `AUTH` and default user requires a password. | Provide credentials or mark user passwordless intentionally. |
| Connection name lost after reconnect | `--persist-connection-names` not set. | Enable the flag in startup parameters. |

---

## Related Documents

* [`docs/COMMANDS.md`](../COMMANDS.md) — Behavior and syntax for ACL-related commands.
* [`network_protocols.md`](network_protocols.md) — Transport-level details for `AUTH` and `HELLO`.
* [`docs/project_guide.md`](../project_guide.md) — High-level summary of ACL subsystem responsibilities.
