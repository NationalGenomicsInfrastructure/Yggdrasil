# CLI Reference

## Invocation

```bash
yggdrasil [--dev] {daemon | run-doc} [OPTIONS]
```

You can also use the module form (useful when developing, since it bypasses the installed console-script):

```bash
python -m yggdrasil [--dev] {daemon | run-doc} [OPTIONS]
# or equivalently
python -m yggdrasil.cli [--dev] {daemon | run-doc} [OPTIONS]
```

---

## Global flags

| Flag    | Description |
|---------|-------------|
| `--dev` | Enable *development mode*: DEBUG-level logging, dev-mode configuration overrides (loads `dev_main.json` on top of `main.json`), enables the test realm |

`--dev` must come **before** the subcommand:

```bash
yggdrasil --dev daemon      # correct
yggdrasil daemon --dev      # incorrect (argument error)
```

---

## Subcommands

### `daemon`

Starts the long-running service:

- Instantiates all configured watchers (CouchDB, file-system, …)
- Auto-registers built-in and external realm handlers via `ygg.realm` entry-point discovery
- Processes events until stopped with **Ctrl-C**

```bash
# Production run
yggdrasil daemon

# Verbose local run with dev settings
yggdrasil --dev daemon
```

Logs are written to the directory set in `main.json` → `yggdrasil.log_dir`.

### `run-doc`

**Warning**: `run-doc` used to be a core tool of an earlier version, but the core has now moved on. We've attempted to keep it afloat, but it might not work as expected anymore.

Processes **exactly one** CouchDB project document and then exits. Useful for manual re-processing or debugging a specific project.

```bash
yggdrasil run-doc DOC_ID [--manual-submit]
```

| Option            | Description |
|-------------------|-------------|
| `DOC_ID`          | The `_id` of the CouchDB project document to process |
| `--manual-submit` | Force manual HPC submission: Yggdrasil writes a submit script but waits for you to run `sbatch` and insert the job ID back into the document |

#### Example: manual Slurm submission

Re-process project `a1b2c3d4e5f`, but hold before Slurm submission to edit the pipeline configuration manually:

```bash
yggdrasil run-doc a1b2c3d4e5f --manual-submit
```

After the script is written, edit project configuration as needed, then submit to Slurm. Copy the resulting `job_id` into the `external_job_id` field of the project's CouchDB document, then re-run:

```bash
yggdrasil run-doc a1b2c3d4e5f --manual-submit
```

Yggdrasil picks up the running Slurm job and waits for it to complete before continuing with post-processing.

---

## Quick-reference table

| Goal | Command |
|------|---------|
| Run as background service | `yggdrasil daemon` |
| Same, with dev logging and dev servers | `yggdrasil --dev daemon` |
| Re-process one document | `yggdrasil run-doc <DOC_ID>` |
| Re-process with manual Slurm submission | `yggdrasil run-doc <DOC_ID> --manual-submit` |
| Use module form (when developing) | `python -m yggdrasil ...` |

---

## See also

- [Configuration](configuration.md) — config files and environment variables
- [Quickstart](quickstart.md) — installation and first run
