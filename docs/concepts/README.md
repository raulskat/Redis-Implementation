# Concept Dictionary Overview

Welcome to the Redis-Implementation concept dictionary. This collection is designed to flatten the learning curve for contributors who may be new to C, systems programming, or Redis internals. Each guide focuses on the practical knowledge you need to work in this codebase, with links back to source files and other docs when deeper dives help.

Use the table below to jump directly to the topic you need. Every document in this directory is meant to be searchable—try your editor's project search (`rg`, `Ctrl+P`, etc.) when you hit an unfamiliar term.

| Document | Scope |
|----------|-------|
| [c_language_basics.md](c_language_basics.md) | C syntax, data types, memory management, compilation model, common pitfalls. |
| [build_and_tooling.md](build_and_tooling.md) | Build system, compiler flags, debugging tools, formatting, dependency layout. |
| [runtime_architecture.md](runtime_architecture.md) | High-level subsystem map (reactor, dispatcher, datastore, ACL, persistence). |
| [network_protocols.md](network_protocols.md) | TCP fundamentals, sockets API, RESP2/RESP3 encoding, HELLO/AUTH handshake. |
| [data_storage.md](data_storage.md) | Hash table internals, SDS strings, TTL bookkeeping, snapshot representation. |
| [security_acl.md](security_acl.md) | Role-based ACL concepts, authentication flow, CLI flags, validator enforcement. |
| [persistence_replication.md](persistence_replication.md) | SAVE/BGSAVE strategies, RDB format notes, replication backlog and PSYNC scaffold. |
| [testing_debugging.md](testing_debugging.md) | Recommended testing strategies, logging, instrumentation, troubleshooting recipes. |
| [glossary.md](glossary.md) | Alphabetical reference for terminology, acronyms, and idioms used across the project. |

> **Tip:** If you are completely new to C, begin with `c_language_basics.md`, then skim the glossary before diving into specialized topics. The remaining docs are organized roughly in the order requests flow through the server.

All documentation is versioned with the code. When you add a subsystem or change a behavior, update both the relevant concept document _and_ the [project guide](../project_guide.md) so newcomers always have a consistent view.
