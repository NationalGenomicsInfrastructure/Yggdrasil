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
&nbsp;
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://nationalgenomicsinfrastructure.github.io/Yggdrasil/)

> **Automate anything — traceable plans, reproducible runs.**

Yggdrasil is an event-driven orchestration framework for automating well-defined workflows. It watches CouchDB databases and file-system paths for changes, routes events to **realm modules**, and executes the resulting workflow plans via the **Engine**.

**→ [Full documentation](docs/index.md)**

---

## Quickstart

```bash
# Clone and install (dev)
git clone https://github.com/NationalGenomicsInfrastructure/Yggdrasil.git
cd Yggdrasil
conda create -n ygg-dev python=3.11 pip && conda activate ygg-dev
pip install -e .[dev]

# Run the daemon
yggdrasil --dev daemon

# Or process a single document
yggdrasil run-doc <DOC_ID>
```

See [docs/getting_started/quickstart.md](docs/getting_started/quickstart.md) for full installation, configuration, and CLI reference.

---

## Project structure

| Path | Contents |
|------|---------|
| `yggdrasil/` | The package. Realm-facing API: `flow/` (execution framework), `watchers` (`EventType`, `WatchSpec`), `core/realm/` (`RealmDescriptor`) |
| `yggdrasil/daemon/`, `yggdrasil/cli.py` | Orchestrator (`YggdrasilCore`), execution coordinator, daemon lock; command line |
| `yggdrasil/watchers/`, `yggdrasil/storage/`, `yggdrasil/couchdb/`, `yggdrasil/ops/`, `yggdrasil/config/` | Internal implementation: watchers, internal storage (CouchDB and SQLite), CouchDB client, ops projection, configuration |
| `yggdrasil/realms/` | `test_realm`, the dev-only reference realm; production realms are external packages |
| `tests/` | Test suite (1700+ tests) |
| `docs/` | Documentation |

Realms are the extension point for domain-specific workflow logic. External realms self-register via the `ygg.realm` entry-point group. See [docs/realm_authoring/guide.md](docs/realm_authoring/guide.md).

---

## Contributing

Contributions are welcome. Open pull requests against the `dev` branch. Format with `black`, lint with `ruff`, type-check with `mypy`. Install both hook stages so checks fire automatically:

```bash
pre-commit install                        # commit-time: formatting, linting, mypy on staged files
pre-commit install --hook-type pre-push   # push-time: full-project mypy + test suite
```

## License

MIT — see [LICENSE](LICENSE).
