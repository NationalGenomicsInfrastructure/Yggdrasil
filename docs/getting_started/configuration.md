# Configuration

Yggdrasil reads configuration files from the resolved workspace directory:

- If `YGG_HOME` is set, configuration files are loaded from `$YGG_HOME/common/configurations/`.
- If `YGG_HOME` is not set, Yggdrasil falls back to `yggdrasil_workspace/common/configurations/` beside the source tree.

`YGG_HOME` must point to the root of the Yggdrasil workspace, not to the source checkout or the conda environment.

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
                "limit": 100,
                "start_seq": "0"
            }
        },
        "yggdrasil_db": {
            "endpoint": "main_couchdb",
            "resource": { "db": "yggdrasil" },
            "data_access": {
                "realms": {
                    "my_realm": {
                        "planning":  { "permissions": ["read"] },
                        "execution": { "permissions": ["read", "write"] }
                    }
                },
                "options": { "max_limit": 50 }
            }
        }
    },
    "defaults": {
        "couchdb": {
            "start_seq": "0",
            "max_limit": 200
        }
    }
}
```

**`endpoints`** define physical backend connections. Each entry specifies a backend type (`couchdb`), its URL, and auth credentials via environment variable names.

**`connections`** define named logical connections that realms reference. Each connection points to an `endpoint` and a `resource` (e.g. a database name), and configures one or both of:

| Key | Purpose |
|---|---|
| `watch` | Used by `WatcherManager` to poll the changes feed. Configures `poll_interval`, `limit`, `start_seq`. A `limit` of at least 25 is recommended — very low values (< 5) will cause slow recovery after downtime. |
| `data_access` | Used by `DataAccess` for realm data queries. See the table below for sub-keys. |

**`data_access` sub-keys:**

| Key | Purpose |
|---|---|
| `data_access.realms` | Maps realm_id → phase → permission list. Unlisted realms are denied. Each phase (`planning`, `execution`) has its own `permissions` array (`"read"`, `"write"`). |
| `data_access.options` | Per-connection backend options (e.g. `max_limit`). Overrides the matching key in `defaults.<backend>`. |

**`defaults`** groups all backend defaults under `defaults.<backend>`. Both `WatcherManager` and `DataAccess` read from `defaults.<backend>` — watcher settings (e.g. `start_seq`, `poll_interval`) and data-access settings (e.g. `max_limit`) all live here. Connection-level overrides (`watch` keys and `data_access.options` respectively) take precedence over these defaults.

`WatchSpec` entries in a realm's registration reference a connection by logical name (e.g. `connection="projects_db"`). The `WatcherManager` resolves the name to the concrete endpoint at startup.

---

## Environment variables

### Configuration discovery

| Variable | Purpose |
|---|---|
| `YGG_HOME` | Root of the Yggdrasil workspace. When set, config files are resolved under `$YGG_HOME/common/configurations/`. Use an absolute path in production and HPC deployments. |

### Credentials and runtime paths

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
