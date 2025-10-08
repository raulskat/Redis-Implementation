# Build & Tooling Reference

This document explains how the Redis-Implementation project is built, which flags matter, and the supporting tools you should know. The goal is to make the build pipeline transparent so you can diagnose compilation failures, link errors, or runtime crashes quickly.

---

## Build System Overview

* **Makefile (root):** Defines compilation rules for every `.c` file under `src/`. Patterns expand automatically, so adding a new `.c` file is enough; no manual registration is required.
* **Compiler:** GCC or Clang (C11 standard). Customize via `make CC=clang` if desired.
* **Output:** The `redis-server` binary is linked from all object files (`.o`). Intermediate files are kept alongside the source files by default.

---

## Key Makefile Variables

| Variable | Purpose | Defaults / Notes |
|----------|---------|------------------|
| `CC` | C compiler | Usually `gcc`. |
| `CFLAGS` | Compiler flags | `-Wall -Wextra -Wpedantic -std=c11 -O2` (see `Makefile`). |
| `LDFLAGS` | Linker flags | Extended with `-ljemalloc` when `JEMALLOC=1`. |
| `JEMALLOC` | Optional allocator switch | `make JEMALLOC=1` enables jemalloc integration. |
| `OBJ` | Expanded list of object files | Generated automatically. |

**Common targets**

```bash
make             # build with default allocator and flags
make clean       # remove object files and the binary
make CC=clang    # compile with Clang
make JEMALLOC=1  # build with jemalloc linked in
```

> **Tip:** Use `make VERBOSE=1` to inspect the exact compiler command for a source file.

---

## Compiler Flags Explained

* `-Wall -Wextra -Wpedantic`: Catch most portability and correctness issues.
* `-std=c11`: Enables modern features like `_Static_assert` and `<stdbool.h>`.
* `-O2`: Optimizes for runtime speed without being overly aggressive.
* `-g`: (Add via `CFLAGS += -g`) includes debug symbols for GDB or LLDB.
* `-fsanitize=address` / `-fsanitize=undefined`: Optional sanitizers for catching memory leaks, buffer overflows, and UB. Use in debug builds only.

Example debug build:

```bash
make clean
make CFLAGS="-Wall -Wextra -Wpedantic -std=c11 -g -O0 -fsanitize=address"
```

---

## Linker Considerations

* The project currently has no external dependencies besides the standard C library and pthreads.
* When `JEMALLOC=1`, `-ljemalloc` is appended to `LDFLAGS`. Ensure jemalloc is installed on your platform; otherwise link errors will mention unresolved symbols like `malloc_conf`.
* For static builds, append `LDFLAGS="-static"` if your toolchain supports it (note that static linking of pthreads and jemalloc may require additional options).

---

## Directory Layout Recap

```
include/   # Public headers
src/       # Implementation files
docs/      # Documentation (high-level and concept dictionary)
build/     # Optional: create this yourself for out-of-tree builds
```

Creating an out-of-tree build directory keeps artifacts out of the source tree:

```bash
mkdir build && cd build
cmake ..   # if a future CMakeLists.txt is added
```

_Note: Currently we rely on Make. The CMake example illustrates how to keep builds separate if we adopt that tooling later._

---

## Recommended Toolchain for Development

| Tool | Why it matters |
|------|----------------|
| `gcc` / `clang` | Primary compilers; Clang has tighter warnings and better sanitizer support. |
| `gdb` / `lldb` | Step-through debugging, breakpoint inspection. |
| `valgrind` | Detect leaks and memory corruption (slower but thorough). |
| `clang-tidy` | Static analysis and modern C best practices. |
| `rg` (ripgrep) | Fast code search (preferred over `grep`). |
| `ctags` | Generate tag files for editor navigation. |

---

## Debugging Workflow

1. **Rebuild with debug info:** `make CFLAGS="-Wall -Wextra -std=c11 -g -O0"`
2. **Run under GDB:** `gdb --args ./redis-server --port 6380`
    * Set breakpoints (`b handle_ping_command`).
    * Inspect variables (`p session->resp_version`).
3. **Use sanitizers during development:** Build with AddressSanitizer for memory bugs.
4. **Capture logs:** The server uses `printf` statements. Redirect output for analysis: `./redis-server ... > server.log 2>&1`.

---

## Formatting & Style

* The project prefers four-space indentation with no tabs.
* Line length guideline: 100 characters max, but readability trumps strict limits.
* Use trailing commas in initializer lists where helpful.
* Header ordering: standard library headers, blank line, project headers (alphabetical within each block).

Consider adopting `clang-format` with a shared config if the team agrees. Until then, be consistent with existing files.

---

## Dependency Management

* Third-party headers are not bundled; install dependencies via your system package manager.
* When introducing new libraries, update the Makefile with include paths (`-I/path`) and library flags (`-L/path -lname`).
* Prefer optional features guarded by compile-time macros so the default build stays dependency-light.

---

## Continuous Integration Hooks (Future Work)

While CI is not configured yet, plan for:

* `make test` or similar target to run automated suites.
* Static analysis (`clang-tidy`, `cppcheck`) on every pull request.
* Sanitizer-enabled builds on the CI matrix (AddressSanitizer + UndefinedBehaviorSanitizer).

Document any new CI steps here to keep the reference up to date.

---

## Troubleshooting Cheatsheet

| Symptom | Likely Cause | Remedy |
|---------|--------------|--------|
| `undefined reference to pthread_*` | Missing `-pthread` in LDFLAGS | Add `LDFLAGS += -pthread` or compile with `-pthread`. |
| `ld: cannot find -ljemalloc` | Jemalloc not installed | Install via package manager or drop the `JEMALLOC=1` flag. |
| Warnings about implicit function declarations | Missing headers or prototypes | Include the correct header or add function prototypes in `include/`. |
| `multiple definition` linker error | Duplicate symbols across translation units | Make the helper `static` or move implementation into a single `.c` file. |
| `SIGSEGV` at startup | Uninitialized config pointer | Ensure `command_context_init` received a valid `redis_config_t`. |

---

## Next Steps

Once you are comfortable with the build pipeline, move to:

* [`runtime_architecture.md`](runtime_architecture.md) for subsystem interactions.
* [`testing_debugging.md`](testing_debugging.md) for end-to-end verification strategies.
