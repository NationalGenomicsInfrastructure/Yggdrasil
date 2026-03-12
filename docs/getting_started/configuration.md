# Configuration

Yggdrasil reads configuration from files under `yggdrasil_workspace/common/configurations/`.
That base path is set in `lib/core_utils/common.py` and can be overridden.

Two key files:

| File | Purpose |
|---|---|
| `main.json` | Global settings: logging, external systems, polling intervals |
| `dev_main.json` | Dev-mode overrides (merged on top of `main.json` when `--dev` is passed) |

---

## main.json fields

> **Note:** The top-level structure of `main.json` is still evolving. Fields are expected to be reorganised in a future release.

```json
{
    "yggdrasil": {
        "log_dir": "yggdrasil_workspace/logs",
        "job_monitor_poll_interval": 5
    },
    "report_transfer": {
        "server": "<server>",
        "user": "<username>",
        "destination": "<destination_path>",
        "ssh_key": "<ssh_key_path>"
    },
    "external_systems": { ... }
}
```

| Field | Description |
|---|---|
| `yggdrasil.log_dir` | Directory where Yggdrasil writes its log files |
| `yggdrasil.job_monitor_poll_interval` | Seconds between Slurm job status polls |
| `report_transfer.server` | SSH server for transferring reports |
| `report_transfer.user` | SSH user |
| `report_transfer.destination` | Remote destination path |
| `report_transfer.ssh_key` | Path to SSH key (optional) |

---

## external_systems — endpoints and connections

`external_systems` maps logical connection names to their backends. It has three sub-keys:

```json
"external_systems": {
    "endpoints": {
        "main_couchdb": {
            "backend": "couchdb",
            "url": "<host>:<port>",
            "auth": {
                "user_env": "YGG_COUCH_USER",
                "pass_env": "YGG_COUCH_PASS"
            }
        }
    },
    "connections": {
        "projects_db": {
            "endpoint": "main_couchdb",
            "resource": { "db": "projects" },
            "watch": {
                "poll_interval": 3,
                "include_docs": true,
                "limit": 100,
                "start_seq": "0"
            }
        },
        "yggdrasil_db": {
            "endpoint": "main_couchdb",
            "resource": { "db": "yggdrasil" },
            "data_access": {
                "realm_allowlist": ["my_realm"],
                "max_limit": 50
            }
        }
    },
    "defaults": {
        "start_seq": "0"
    }
}
```

**`endpoints`** define physical backend connections. Each entry specifies a backend type (`couchdb`), its URL, and auth credentials via environment variable names.

**`connections`** define named logical connections that realms reference. Each connection points to an `endpoint` and a `resource` (e.g. a database name), and configures one or both of:

| Key | Purpose |
|---|---|
| `watch` | Used by `WatcherManager` to poll the changes feed. Configures `poll_interval`, `include_docs`, `limit`, `start_seq`. |
| `data_access` | Used by `DataAccess` for realm data queries. Configures `realm_allowlist` (which realms may use this connection) and `max_limit` (max documents per query). |

**`defaults`** provides fallback values for connections that omit optional fields (e.g. `start_seq`).

`WatchSpec` entries in a realm's registration reference a connection by logical name (e.g. `connection="projects_db"`). The `WatcherManager` resolves the name to the concrete endpoint at startup.

---

## Environment variables

Sensitive credentials should be set as environment variables, not stored in config files.

| Variable | Purpose |
|---|---|
| `YGG_COUCH_USER` | CouchDB username |
| `YGG_COUCH_PASS` | CouchDB password |
| `YGG_WORK_ROOT` | Central workspace root for all plan and step working directories (default: `/tmp/ygg_work`). Set by the operator before starting the daemon. |
| `YGG_EVENT_SPOOL` | Root directory where `FileSpoolEmitter` writes structured event JSON files (default: `/tmp/ygg_events`). Set by the operator before starting the daemon. |
| `OPS_DB` | Operations database name for event consumers (default: `yggdrasil_ops`) |

`YGG_WORK_ROOT` and `YGG_EVENT_SPOOL` are resolved once at daemon startup and apply to all realms. Realm code does not read these variables — step functions receive the resolved paths via `ctx.workdir` and `ctx.scope_dir`, and emit events via `ctx.emitter`.

---

## Logging

- CLI `--dev` enables DEBUG logging and uses the dev config (if present).
- Default is INFO.
- Logs are written to the directory configured as `yggdrasil.log_dir` in `main.json` (one file per run), and optionally to console.
