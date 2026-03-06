# Yggdrasil Documentation

**Automate anything — traceable plans, reproducible runs.**

Yggdrasil is an event-driven orchestration framework that watches different sources (CouchDB, file-system, etc.), routes change events to **realm** modules, and executes the resulting workflow plans via the **Engine**.

---

## By audience

| I want to… | Where to look |
|---|---|
| Install Yggdrasil and run it for the first time | [Getting Started → Quickstart](getting_started/quickstart.md) |
| Understand configuration files and environment variables | [Getting Started → Configuration](getting_started/configuration.md) |
| Learn the CLI flags and commands | [Getting Started → CLI reference](getting_started/cli.md) |
| Understand how Yggdrasil works end-to-end | [Architecture → Overview](architecture/overview.md) |
| Write a new realm | [Realm Authoring → Guide](realm_authoring/guide.md) |
| See common realm patterns | [Realm Authoring → Cookbook](realm_authoring/cookbook.md) |
| Use the Flow API to define steps and plans | [Flow API → Overview](flow_api/overview.md) |
| Look up terminology | [Reference → Glossary](reference/glossary.md) |
| Run test scenarios against the daemon | [Reference → Test Realm](reference/test_realm.md) |
| Debug a broken setup | [Reference → Troubleshooting](reference/troubleshooting.md) |

---

## Structure

```
docs/
  getting_started/    install, configure, run
  architecture/       how the pieces fit together
  realm_authoring/    write and register your own realm
  flow_api/           step decorator, planner, engine
  reference/          glossary, test scenarios, troubleshooting
  design/prds/        design documents (stable, implemented features)
```
