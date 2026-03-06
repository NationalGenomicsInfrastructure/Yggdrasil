# Quickstart

Get Yggdrasil running locally in a few steps.

---

## Prerequisites

- Python 3.11+
- [Conda](https://docs.conda.io/) (recommended) or a virtualenv
- A running CouchDB instance

---

## Installation

### Developers / contributors

```bash
# Clone and create an isolated environment
git clone https://github.com/NationalGenomicsInfrastructure/Yggdrasil.git
cd Yggdrasil
conda create -n ygg-dev python=3.11 pip
conda activate ygg-dev

# Editable install with dev extras (ruff, black, mypy, pytest, pre-commit)
pip install -e .[dev]
```

### Production / CI runners

```bash
git clone https://github.com/NationalGenomicsInfrastructure/Yggdrasil.git
cd Yggdrasil
conda create -n ygg python=3.11 pip
conda activate ygg

# Install locked runtime stack, then Yggdrasil itself
pip install -r requirements/lock.txt
pip install -e .
```

`requirements/lock.txt` is generated with `pip-compile --strip-extras` and pins exact versions.

---

## Install an external realm (example: dataflow-dmx)

```bash
git clone https://github.com/NationalGenomicsInfrastructure/dmx.git
pip install -e ./dmx
```

Restart Yggdrasil so it re-scans entry-points. Startup log confirms:

```
[DEBUG message] Registered external handler 'dmx.yggdrasil_realm.handler.flowcell_dmx.FlowcellDemuxHandler' (id=flowcell_handler) for 'COUCHDB_DOC_CHANGED' from realm 'dmx'
✓  Realm 'dmx': 1 handler(s) registered
```

---

## First run

**Note:** For a successful run, the configuration file must be correctly filled in first.
See [Configuration](configuration.md) for required config files.

```bash
# Start the daemon (watches CouchDB and file-system sources)
yggdrasil daemon

# Or in dev mode (DEBUG logging, dev config overrides)
yggdrasil --dev daemon
```

See [CLI reference](cli.md) for all commands and flags.

---

## Development setup

### Pre-commit hooks

```bash
pre-commit install
```

This runs `ruff`, `black`, and `mypy` automatically on each commit.

### Everyday commands

| Task | Command |
|---|---|
| Format | `black .` |
| Lint | `ruff check .` |
| Type check | `mypy .` |
| Run all hooks | `pre-commit run --all-files` |
| Run tests | `pytest tests/` |

### VSCode (recommended extensions)

- Python (Microsoft)
- Ruff (Astral Software)
- Black Formatter (Microsoft)
- Mypy Type Checker (Microsoft)

Add to `settings.json`:

```json
{
    "editor.defaultFormatter": "ms-python.black-formatter",
    "editor.formatOnSave": true,
    "ruff.configuration": "pyproject.toml",
    "mypy-type-checker.args": ["--config-file=pyproject.toml"]
}
```

### Git blame hygiene (optional)

Ignore bulk-format commits so `git blame` stays useful:

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

Append the full commit hashes of large format-only commits to `.git-blame-ignore-revs`.

---

## CI

GitHub Actions runs `ruff`, `black`, `mypy`, and `pytest` on every push and pull request.
Workflow files: `.github/workflows/`.
