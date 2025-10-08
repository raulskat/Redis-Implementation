# Testing & Debugging Guide

Reliable systems require disciplined testing and a toolkit for diagnosing issues quickly. This guide summarizes recommended practices, available helpers, and investigative techniques tailored to Redis-Implementation.

---

## Testing Philosophy

1. **Fast inner loop:** Prefer lightweight tests (unit or integration) that run locally before pushing code.
2. **Parity-driven:** Align tests with upstream Redis behavior, especially for protocol and ACL semantics.
3. **Regression-first:** Reproduce bugs with automated scripts, then lock them in with tests before fixing.
4. **Document gaps:** If a feature lacks automated coverage (e.g., replication streaming), note it in this guide and the roadmap.

---

## Types of Tests

| Test Type | Scope | Suggested Tools |
|-----------|-------|-----------------|
| Unit | Single function or module (e.g., `datastore_expire_in`). | `cmocka`, custom harness, or inline assertions in debug builds. |
| Integration | End-to-end command flows (RESP negotiation, ACL checks). | Python scripts with `redis` library, shell scripts using `redis-cli`. |
| Performance | Throughput/latency benchmarking. | `memtier_benchmark`, `redis-benchmark`, custom load generators. |
| Regression | Bug reproduction scripts. | Store under `tests/regression/` (recommended future structure). |

---

## Suggested Test Harness

While a dedicated test suite is in progress, you can bootstrap local checks with shell scripts:

```bash
#!/usr/bin/env bash
set -euo pipefail

PORT=6389
./redis-server --port "$PORT" --requirepass secret &
PID=$!
trap "kill $PID" EXIT

redis-cli -p "$PORT" ping
redis-cli -p "$PORT" AUTH secret
redis-cli -p "$PORT" SET foo bar
redis-cli -p "$PORT" GET foo | grep -q bar
```

Submit scripts alongside features so teammates can reproduce the scenario.

---

## Debugging Checklist

1. **Rebuild with symbols:** `make CFLAGS="-Wall -Wextra -std=c11 -g -O0"`
2. **Run under debugger:** `gdb --args ./redis-server --port 6380`
3. **Set breakpoints:** Focus on suspect handlers (`b handle_set_command`).
4. **Inspect session state:** `p session->acl` reveals authentication status and roles.
5. **Check logs:** Standard output shows persistence status, background thread results, and errors.
6. **Apply sanitizers:** AddressSanitizer catches out-of-bounds access; run tests post-build.
7. **Use Valgrind for leaks:** `valgrind --leak-check=full ./redis-server ...` (slower but thorough). *

_*Note:* Disable jemalloc when using Valgrind unless configured with appropriate suppressions._

---

## Common Pitfalls

| Symptom | Diagnosis | Solution |
|---------|-----------|----------|
| Segfault on command | Null session or ctx pointer | Ensure handlers validate inputs; add guard clauses. |
| Stuck background save | `save_in_progress` never resets | Confirm `finish_save()` executes on every code path. |
| Memory leak after tests | Missed `free` call | Verify new allocations have matching deallocation; run ASan/Valgrind. |
| Inconsistent RESP responses | Forgot to respect RESP version | Use `resp_send_bool`, `resp_send_string_map`, or `resp_send_set` based on `session->resp_version`. |
| ACL bypass | Command missing flags | Add `CMD_FLAG_*` to command spec and update tests. |

---

## Logging Enhancements

* Use `printf` sparingly; consider adding log levels (`DEBUG`, `INFO`, `ERROR`) behind macros if output becomes noisy.
* For long-term, plan integration with a structured logging library (e.g., `log.c`). Document new macros here when introduced.

---

## Instrumentation Ideas

| Feature | Benefit | Status |
|---------|---------|--------|
| Command latency histogram | Identify slow handlers | Planned |
| Memory usage metrics | Detect leaks or fragmentation | Planned |
| Replication lag gauge | Observe backlog health | Pending replication streaming |
| Trace hooks | Integrate with external monitoring | Future |

Track implementation progress in `docs/redis_parity_design.md`.

---

## When to Update This Guide

* You add a new testing framework or script.
* You integrate CI or build new targets (e.g., `make test`).
* You document recurring issues with established fixes.
* You introduce new debugging tools (e.g., `perf`, `bpftrace`) for performance analysis.

Always cross-link relevant documentation (e.g., update `build_and_tooling.md` when adding new instrumentation).
