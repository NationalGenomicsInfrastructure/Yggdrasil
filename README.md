<div align="center">
  <picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/ygg_logo-dark_mode.png">
  <source media="(prefers-color-scheme: light)" srcset="docs/assets/ygg_logo-light_mode.png">
  <img alt="Yggdrasil Logo" src="docs/assets/ygg_logo-light_mode.png" width="15%" style="max-width: 100px;">
</picture>
</div>

# Yggdrasil

[![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/glrs/yggdrasil?sort=semver)](https://github.com/glrs/yggdrasil/releases)
&nbsp;
[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/2fa79bea21b142d9a75d0951ec2803dd)](https://app.codacy.com/gh/glrs/Yggdrasil/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_coverage)
&nbsp;
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/2fa79bea21b142d9a75d0951ec2803dd)](https://app.codacy.com/gh/glrs/Yggdrasil/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)


Yggdrasil is an in-house orchestration framework designed to automate well-defined workflows. It watches directories, CouchDB
changes, etc., then calls **realm modules** (external or internal packages) to do the heavy lifting. Example realms today:

* `tenx` (internal)         - 10x Genomics best practice analysis
* `smartseq3` (internal)    - Smart-seq3 best practice analysis
* `dataflow-dmx` (external) - [under developmennt] Demultiplexing pipeline for Illumina / Aviti / ONT
* *(more to come)*

External realms self-register through the entry-point group **`ygg.handler`**.

---

## Table of Contents


- [Installation](#installation)
  - [Developers / Contributors](#1-developers--contributors)
  - [Production / CI runners](#2-production--ci-runners)
- [Install External Realms](#install-an-external-realm-example-dataflow-dmx)
- [Project Structure](#project-structure)
- [Usage](#usage)
  - [Command-line interface](#command-line-interface)
  - [Daemon mode](#daemon-mode)
  - [One-off mode run-doc](#one-off-mode-run-doc)
- [Configuration](#configuration)
- [Development Guidelines](#development-guidelines)
  - [Setting Up the Development Environment](#1-setting-up-the-development-environment)
  - [Pre-Commit Hooks](#pre-commit-hooks)
  - [Everyday workflow](#2-everyday-workflow)
  - [VSCode Integration (recommended)](#3-vscode-integration-recommended)
  - [Git Blame hygiene (optional)](#4-git-blame-hygiene-optional)
  - [Continuous Integration](#5-continuous-integration)
- [Contributing](#contributing)
- [License](#license)


## Installation

### 1. Developers / Contributors

```bash
# Clone & create an isolated env
git clone https://github.com/NationalGenomicsInfrastructure/Yggdrasil.git
cd Yggdrasil
conda create -n ygg-dev python=3.11 pip
conda activate ygg-dev

# Editable install with dev extras (ruff, mypy, ...)
pip install -e .[dev]

# Run Yggdrasil
yggdrasil

# Or alternatively
python -m yggdrasil
```

* Runtime dependencies come from `[project] dependencies` in `pyproject.toml`.
* Dev tooling is pulled from `[project.optional-dependencies] dev`.

### 2. Production / CI runners

```bash
# 1. Clone & create an isolated env
git clone https://github.com/NationalGenomicsInfrastructure/Yggdrasil.git
cd Yggdrasil
conda create -n ygg python=3.11 pip
conda activate ygg

# 2. Install locked runtime stack
pip install -r requirements/lock.txt

# 3. Install Yggdrasil itself (no dev extras)
pip install -e .
```

`requirements/lock.txt` is generated from the dependency list with `pip-compile --strip-extras`


## Install an external realm (example: dataflow-dmx)
```bash
# Clone next to Yggdrasil or organize in a `realms` dir (any folder works)
git clone https://github.com/NationalGenomicsInfrastructure/dmx.git
pip install -e ./dmx
```

Restart Yggdrasil so it re-scans entry-points. Startup log shows the handler is active:
```
✓  registered external handler flowcell-dmx for FLOWCELL_READY
```

When a new event is detected, Yggdrasil schedules the appropriate handler as an
async background task in its event loop.


## Project Structure

Brief overview of the main components and directories:

```text
Yggdrasil/
├── lib/
│   ├── base/
│   ├── core_utils/
│   ├── couchdb/
│   ├── handlers/
│   ├── module_utils/
│   ├── realms/
│   │   ├── tenx/
│   │   └── smartseq3/
│   └── watchers/
├── tests/
├── .github/
│   └── workflows/
├── requirements/
├── yggdrasil.py
├── ygg_trunk.py (depr)
├── ygg-mule.py (depr)
├── pyproject.toml
├── LICENSE
└── README.md
```

*	**lib/**: Core library containing base classes and utilities.
    *	**base/**: Abstract base classes and interfaces.
    *   **core_utils/**: Utility modules for Yggdrasil core functionalities.
    *   **couchdb/**: Classes specific for Yggdrasil-CouchDB interactions and document management.
    *   **handlers/**: Base classes and built-in event/data handlers for processing and workflow orchestration.
    *	**module_utils/**: Utility modules for various Yggdrasil module functionalities.
    *	**realms/**: Internal modules specific to different sequencing technologies (e.g. TenX, SmartSeq3, etc.)
    *   **watchers/**: File system and CouchDB watchers for monitoring and triggering events.
*	**tests/**: Test cases for the application.
*	**.github/workflows/**: GitHub Actions workflows for CI/CD.
*   **requirements/**: Dependency lock files and requirements management for reproducible environments.


---

## Usage

### Command-line interface

Yggdrasil has a single entry-point for both daemon operation (background watchers + handlers) and one-off project processing.
After you installed Yggdrasil in an environment, call it in the following way:

```bash
yggdrasil [--dev] {daemon | run-doc} [OPTIONS]
```

| Global flag | Description                                                                                                       |
| ----------- | ----------------------------------------------------------------------------------------------------------------- |
| `--dev`     | Turns on *development mode*: <br>• DEBUG-level logging<br>• Dev-mode configuration overrides (useful on a laptop) |

You can also run the CLI via `python -m yggdrasil` or `python -m yggdrasil.cli` if you prefer.


### Daemon mode

Starts the long-running service:
* instantiates all configured watchers (file-system, CouchDB, ...);
* auto-registers built-in and external handlers;
* processes events until you stop it with **Ctrl-C**.

```bash
# production-style run
yggdrasil daemon

# verbose local run
yggdrasil --dev daemon
```

Logs are written to the directory set in `yggdrasil_workspace/common/configurations/config.json` → `yggdrasil_log_dir`.

### One-off mode: run-doc

Processes **exactly one** CouchDB project document and then exits. Useful for manual re-processing or debugging.

```bash
yggdrasil run-doc DOC_ID [--manual-submit]
```

| Option            | Meaning                                                                                                               |
| ----------------- | --------------------------------------------------------------------------------------------------------------------- |
| `--manual-submit` | Force **manual** HPC submission for this invocation (handlers check a session flag instead of auto‐calling `sbatch`). |

#### Example:
__Objective__: Rerun project N.Surname (CouchDB doc_id: a1b2c3d4e5f), but stop before Slurm submission because we need to manually edit the project's configurations.
```bash
# Initially run
yggdrasil run-doc a1b2c3d4e5f --manual-submit
```

After you run this, manually edit the project as needed and submit to Slurm. Copy the Slurm `job_id` to the respective field in the project's CouchDB doc, and re-run the same command:
```bash
yggdrasil run-doc a1b2c3d4e5f --manual-submit`
```

Yggdrasil will pick up the running Slurm job and wait for it until it finishes, to continue with post-processing.

### Invocation summary
| You want to…                                                   | Command                                      |
| -------------------------------------------------------------- | -------------------------------------------- |
| Run Yggdrasil as a background service                          | `yggdrasil daemon`                           |
| Same, but with dev logging & dev servers                       | `yggdrasil --dev daemon`                     |
| (re)Process one document                                       | `yggdrasil run-doc <DOC_ID>`                 |
| (re)Process with manual Slurm submission                       | `yggdrasil run-doc <DOC_ID> --manual-submit` |
| **When developing**, use module form instead of console-script | `python -m yggdrasil ...`                    |


---

## Configuration

Yggdrasil uses a configuration loader to manage settings. Configuration files should be placed in the `yggdrasil_workspace/common/configurations` directory. This directory path can be adjusted in the `lib/core_utils/common.py` script if needed.

### Configuration Files

**config.json**: This file contains global settings for Yggdrasil.

Fields:

    - yggdrasil_log_dir: Directory where logs will be stored.
    - couchdb_url: URL of the CouchDB server (host:port format).
    - couchdb_database: Name of the CouchDB project database.
    - couchdb_status_tracking: Name of the CouchDB yggdrasil database for project status tracking.
    - couchdb_poll_interval: Interval (in seconds) for polling CouchDB for changes.
    - job_monitor_poll_interval: Interval (in seconds) for polling the job monitor.
    - activate_ngi_cmd: Command to activate NGI environment (can be "None" if not used).
    - report_transfer: Settings for transferring reports (server, user, destination, ssh_key).

Example Configuration File (config.json)

```json
{
    "yggdrasil_log_dir": "yggdrasil_workspace/logs",
    "couchdb_url": "<host>:<port>",
    "couchdb_database": "my_projects",
    "couchdb_status_tracking": "my_yggdrasil_db",
    "couchdb_poll_interval": 3,
    "job_monitor_poll_interval": 5,
    "activate_ngi_cmd": "None",
    "report_transfer": {
        "server": "<server>",
        "user": "<username>",
        "destination": "<destination_path>",
        "ssh_key": "<ssh_key_path>"
    }
}
```

**module_registry.json**: This file maps different library construction methods to their respective internal processing modules. The modules specified here will be dynamically loaded and executed based on the entire name of a `library_prep_method` specified in the CouchDB document, or a designated prefix of them.

Example:

```json
{
    "SmartSeq 3": {
        "module": "lib.realms.smartseq3.smartseq3.SmartSeq3"
    },
    "10X": {
        "module": "lib.realms.tenx.tenx_project.TenXProject",
        "prefix": true
    }
}
```

- **SmartSeq 3**:
    - module: The path to the module handling SmartSeq 3 library data.
- **10X**:
    - module: The path to the module handling 10X-prefixed library data.

### Environment Variables

The following variables can also be set in the `config.json`, but for safety reasons, you are endorsed to set them as environment variables, like so:

    - COUCH_USER: Your CouchDB username.
    - COUCH_PASS: Your CouchDB password.

### Logging

Yggdrasil uses a custom logging utility to manage logs. Logs are stored in the directory specified by the `yggdrasil_log_dir` configuration.

**Debug Logging**: By setting the `--dev` flag when running Yggdrasil, the debug logging is enabled automatically.

## Development Guidelines

### 1. Setting Up the Development Environment

Ensure you have activated the Conda environment, and have installed runtime + dev tools. The latter can be done in one go with:
```bash
pip install -e .[dev]
```

`.[dev]` pulls:
* **ruff** (lint) · **black** (format) · **mypy** (type-check)
* **pip-tools** (`pip-compile`)
* **pre-commit** itself — no separate pip install needed.

### Pre-Commit Hooks

Use [pre-commit](https://pre-commit.com/) to automate code formatting and linting on each commit.

```bash
# Install Git hooks (runs ruff / black / mypy automatically)
pre-commit install
```

### 2. Everyday workflow

| Task              | Command                      |
| ----------------- | ---------------------------- |
| Format everything | `black .`                    |
| Lint              | `ruff check .`               |
| Static types      | `mypy .`                     |
| Run all hooks     | `pre-commit run --all-files` |

_(Hooks fire automatically on `git commit`; run manually only if you want a
full pass before staging.)_

### 3. VSCode Integration (recommended)

Install extensions:
* Python (Microsoft)
* Ruff (Astral Software)
* Black Formatter (Microsoft)
* Mypy Type Checker (Microsoft)

**VSCode Settings**

Add to `settings.json` (user or workspace):

```json
{
    "editor.defaultFormatter": "ms-python.black-formatter",
    "editor.formatOnSave": true,
    "ruff.configuration": "pyproject.toml",
    "mypy-type-checker.args": [ "--config-file=pyproject.toml" ]
}
```

### 4. Git Blame hygiene (optional)

Ignore bulk-format commits so git blame stays useful:

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

Append the commit (full) hashes of large "black-only" or "ruff-fix" commits to the `.git-blame-ignore-revs` file (one hash per line), e.g.:

```text
a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0
b1c2d3e4f5g6h7i8j9k0l1m2n3o4p5q6r7s8t9u0
```

### 5. Continuous Integration

GitHub Actions are set up to automatically run `ruff`, `black`, and `mypy` on pushes and pull requests.

* Workflow File: `.github/workflows/lint.yml`
* Jobs: `ruff-check`, `black-check`, `mypy-check`
* Each job installs exact runtime versions from `requirements/lock.txt`, **then** the tool it needs.

## Contributing

Contributions are very welcome! To have as smooth of an experience as possible, the following guidelines are recommended:

* **Forking**: Fork the main repository to your personal GitHub account.
* **Git workflow**: Open pull-requests **against the `dev` branch**.
* **Code Style**: Format with `black` and lint with `ruff`.
* **Type Annotations**: If you use type annotations make sure to set (and pass) `mypy` checks.
* **Pre-commit**: `black`, `ruff`, and `mypy` run automatically. Make sure `pre-commit install` is enabled and hooks pass before pushing.
* **Documentation**: Documented contributions are easier to understand and review.

**Suggested contributions**: Tests, Bug Fixes, Code Optimization, New Modules (reach out to Anastasios if you don't know where to start with developing a new module).

## License

Yggdrasil is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.