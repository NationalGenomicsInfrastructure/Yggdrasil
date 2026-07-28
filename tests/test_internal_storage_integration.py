"""Integration tests for the internal-storage factory and SQLite watcher path."""

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from lib.core_utils.event_types import EventType
from lib.storage import build_internal_storage
from lib.storage.config import CouchInternalStorageConfig
from lib.watchers.backends.base import Checkpoint
from lib.watchers.plan_watcher import PlanWatcher
from yggdrasil.flow.model import Plan, StepSpec


def _sqlite_config(path: Path) -> dict:
    return {
        "internal_storage": {
            "backend": "sqlite",
            "sqlite": {"path": str(path)},
        }
    }


class TestInternalStorageFactory(unittest.TestCase):
    """Backend selection through build_internal_storage()."""

    def test_sqlite_mode_constructs_no_couch_bundle(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("lib.storage.couch.build_couchdb_bundle") as mock_couch:
                bundle = build_internal_storage(
                    _sqlite_config(Path(tmp) / "ygg.sqlite3"), dev_mode=True
                )
        self.assertEqual(bundle.backend, "sqlite")
        mock_couch.assert_not_called()

    def test_absent_block_uses_legacy_couch_path(self):
        with patch("lib.storage.couch.build_couchdb_bundle") as mock_couch:
            mock_couch.return_value = MagicMock(backend="couchdb")
            bundle = build_internal_storage({})
        mock_couch.assert_called_once_with(None)
        self.assertEqual(bundle.backend, "couchdb")

    def test_legacy_bundle_constructs_coordination_manager_eagerly(self):
        """Legacy implicit path must not defer the coordination DB.

        A lazy checkpoint store would fail open (checkpoint load returns None,
        PlanWatcher starts at "now") if the 'yggdrasil' DB is unreachable,
        instead of failing fast at startup like pre-bundle Core did.
        """
        from lib.storage.couch import build_couchdb_bundle

        with (
            patch("lib.storage.couch.PlanDBManager"),
            patch("lib.storage.couch.OpsWriter"),
            patch("lib.storage.couch.ChangesFetcher"),
            patch("lib.storage.couch.YggdrasilDBManager") as mock_ydm,
            patch("lib.storage.couch.CouchDBCheckpointStore") as mock_cp,
        ):
            build_couchdb_bundle(None)

        mock_ydm.assert_called_once()  # coordination manager constructed eagerly
        _, kwargs = mock_cp.call_args
        self.assertIs(kwargs["db_manager"], mock_ydm.return_value)

    def test_explicit_couch_passes_resolved_config(self):
        config = {
            "internal_storage": {
                "backend": "couchdb",
                "couchdb": {
                    "connections": {
                        "coordination": "ygg_db",
                        "plans": "plans_db",
                        "operations": "ops_db",
                    }
                },
            },
            "external_systems": {
                "endpoints": {
                    "couchdb": {
                        "backend": "couchdb",
                        "url": "https://couch.example.org:5984",
                        "auth": {"user_env": "U", "pass_env": "P"},
                    }
                },
                "connections": {
                    "ygg_db": {"endpoint": "couchdb", "resource": {"db": "yggdrasil"}},
                    "plans_db": {
                        "endpoint": "couchdb",
                        "resource": {"db": "yggdrasil_plans"},
                    },
                    "ops_db": {
                        "endpoint": "couchdb",
                        "resource": {"db": "yggdrasil_ops"},
                    },
                },
            },
        }
        with patch("lib.storage.couch.build_couchdb_bundle") as mock_couch:
            mock_couch.return_value = MagicMock(backend="couchdb")
            build_internal_storage(config)
        (resolved,), _ = mock_couch.call_args
        self.assertIsInstance(resolved, CouchInternalStorageConfig)
        self.assertEqual(resolved.plans.db_name, "yggdrasil_plans")


class TestPlanWatcherWithSQLite(unittest.TestCase):
    """PlanWatcher runs end-to-end on the SQLite bundle (no CouchDB)."""

    def test_watcher_emits_and_checkpoints_on_sqlite(self):
        with tempfile.TemporaryDirectory() as tmp:
            bundle = build_internal_storage(
                _sqlite_config(Path(tmp) / "ygg.sqlite3"), dev_mode=True
            )

            # Deterministic start: seed the checkpoint at 0 so the watcher
            # replays every change instead of starting at "now".
            bundle.checkpoints.save(
                Checkpoint(backend_key=PlanWatcher.CHECKPOINT_KEY, value="0")
            )

            plan = Plan(
                plan_id="pln_it_P1_v1",
                realm="test_realm",
                scope={"kind": "project", "id": "P1"},
                steps=[
                    StepSpec(
                        step_id="s1",
                        name="echo",
                        fn_ref="tests.integration.mock_steps:echo_step",
                        params={},
                    )
                ],
            )
            bundle.plans.save_plan(
                plan,
                "test_realm",
                {"kind": "project", "id": "P1"},
                auto_run=True,
            )

            events = []
            got_event = asyncio.Event()

            def on_event(event):
                events.append(event)
                got_event.set()

            watcher = PlanWatcher(
                on_event=on_event,
                poll_interval_sec=0.01,
                plan_store=bundle.plans,
                change_source=bundle.plan_changes,
                checkpoint_store=bundle.checkpoints,
                execution_authority_filter="daemon",
            )

            async def _run():
                task = asyncio.create_task(watcher.start())
                # Generous timeout: the SQLite change source polls via
                # asyncio.to_thread, so event delivery is sensitive to
                # thread-pool scheduling under a loaded parallel test suite.
                await asyncio.wait_for(got_event.wait(), timeout=30.0)
                await watcher.stop()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

            asyncio.run(_run())

            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].event_type, EventType.PLAN_EXECUTION)
            self.assertEqual(events[0].payload["plan_doc_id"], "pln_it_P1_v1")
            self.assertEqual(
                events[0].payload["plan_doc"]["status"],
                "approved",
            )

            # Checkpoint advanced past the seeded value
            checkpoint = bundle.checkpoints.load(PlanWatcher.CHECKPOINT_KEY)
            self.assertNotEqual(checkpoint.value, "0")


if __name__ == "__main__":
    unittest.main()
