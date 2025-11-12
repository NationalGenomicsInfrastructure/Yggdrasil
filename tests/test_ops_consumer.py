"""
Comprehensive tests for lib/ops/consumer.py

Tests the FileSpoolConsumer and plan snapshot building logic.
"""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from lib.ops.consumer import (
    FileSpoolConsumer,
    _find_any_event,
    _read_scope_from_spool,
    _safe_load,
    build_plan_snapshot,
)


class TestSafeLoad(unittest.TestCase):
    """Tests for _safe_load helper function."""

    def test_safe_load_valid_json(self):
        """Test loading valid JSON file."""
        with TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.json"
            data = {"key": "value", "number": 42}
            file_path.write_text(json.dumps(data))

            result = _safe_load(file_path)
            self.assertEqual(result, data)

    def test_safe_load_invalid_json(self):
        """Test loading invalid JSON returns empty dict."""
        with TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "invalid.json"
            file_path.write_text("not json at all")

            result = _safe_load(file_path)
            self.assertEqual(result, {})

    def test_safe_load_missing_file(self):
        """Test loading missing file returns empty dict."""
        file_path = Path("/nonexistent/path/file.json")
        result = _safe_load(file_path)
        self.assertEqual(result, {})

    def test_safe_load_empty_file(self):
        """Test loading empty file returns empty dict."""
        with TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "empty.json"
            file_path.write_text("")

            result = _safe_load(file_path)
            self.assertEqual(result, {})


class TestFindAnyEvent(unittest.TestCase):
    """Tests for _find_any_event helper function."""

    def test_find_any_event_single_event(self):
        """Test finding a single event in spool."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            event_data = {
                "type": "step.started",
                "scope": {"kind": "project", "id": "P123"},
            }
            event_file = run_dir / "event1.json"
            event_file.write_text(json.dumps(event_data))

            result = _find_any_event(plan_dir)
            self.assertEqual(result, event_data)

    def test_find_any_event_multiple_events(self):
        """Test finding earliest event when multiple exist."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            event1_data = {
                "type": "step.started",
                "scope": {"kind": "project", "id": "P123"},
            }
            event2_data = {
                "type": "step.succeeded",
                "scope": {"kind": "project", "id": "P123"},
            }

            (run_dir / "aaa_event.json").write_text(json.dumps(event1_data))
            (run_dir / "zzz_event.json").write_text(json.dumps(event2_data))

            result = _find_any_event(plan_dir)
            # Should return first alphabetically
            self.assertEqual(result, event1_data)

    def test_find_any_event_ignores_tmp_files(self):
        """Test that .tmp files are ignored."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            event_data = {
                "type": "step.started",
                "scope": {"kind": "project", "id": "P123"},
            }
            tmp_data = {"type": "invalid", "scope": {}}

            (run_dir / "event.json").write_text(json.dumps(event_data))
            (run_dir / "temp.json.tmp").write_text(json.dumps(tmp_data))

            result = _find_any_event(plan_dir)
            self.assertEqual(result, event_data)

    def test_find_any_event_no_events(self):
        """Test returns None when no events found."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            result = _find_any_event(plan_dir)
            self.assertIsNone(result)

    def test_find_any_event_invalid_json(self):
        """Test handles invalid JSON gracefully."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            (run_dir / "bad.json").write_text("invalid json")

            result = _find_any_event(plan_dir)
            self.assertIsNone(result)

    def test_find_any_event_empty_directory(self):
        """Test with empty directory structure."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            result = _find_any_event(plan_dir)
            self.assertIsNone(result)


class TestReadScopeFromSpool(unittest.TestCase):
    """Tests for _read_scope_from_spool helper function."""

    def test_read_scope_from_event(self):
        """Test reading scope from event."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            scope = {"kind": "project", "id": "P12345"}
            event_data = {"type": "step.started", "scope": scope}
            (run_dir / "event.json").write_text(json.dumps(event_data))

            result = _read_scope_from_spool(plan_dir)
            self.assertEqual(result, scope)

    def test_read_scope_no_events(self):
        """Test returns empty dict when no events."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            result = _read_scope_from_spool(plan_dir)
            self.assertEqual(result, {})

    def test_read_scope_missing_scope_field(self):
        """Test handles missing scope field."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            event_data = {"type": "step.started"}
            (run_dir / "event.json").write_text(json.dumps(event_data))

            result = _read_scope_from_spool(plan_dir)
            self.assertEqual(result, {})


class TestBuildPlanSnapshot(unittest.TestCase):
    """Tests for build_plan_snapshot function."""

    def test_build_plan_snapshot_single_step(self):
        """Test building snapshot with single step."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            scope = {"kind": "project", "id": "P123"}
            event_data = {
                "type": "step.succeeded",
                "scope": scope,
                "step_name": "test_step",
                "fingerprint": "abc123",
                "artifacts": [{"role": "output", "path": "/out"}],
                "metrics": {"duration": 10},
                "job": {"id": "job1"},
                "ts": "2025-01-01T00:00:00Z",
            }
            (run_dir / "event.json").write_text(json.dumps(event_data))

            snapshot = build_plan_snapshot(plan_dir, "test_realm", "plan_001")

            self.assertEqual(snapshot["type"], "plan_status")
            self.assertEqual(snapshot["realm"], "test_realm")
            self.assertEqual(snapshot["plan_id"], "plan_001")
            self.assertEqual(snapshot["scope"], scope)
            self.assertIn("step1", snapshot["steps"])
            self.assertEqual(snapshot["steps"]["step1"]["step_name"], "test_step")
            self.assertEqual(snapshot["steps"]["step1"]["state"], "step.succeeded")
            self.assertEqual(snapshot["steps"]["step1"]["run_id"], "run001")
            self.assertEqual(snapshot["steps"]["step1"]["fingerprint"], "abc123")
            self.assertEqual(snapshot["steps"]["step1"]["progress"], 100)
            self.assertIn("updated_at", snapshot)

    def test_build_plan_snapshot_multiple_steps(self):
        """Test building snapshot with multiple steps."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)

            # Create multiple steps
            for i in range(3):
                step_dir = plan_dir / f"step{i}"
                run_dir = step_dir / "run001"
                run_dir.mkdir(parents=True)

                event_data = {
                    "type": "step.succeeded",
                    "scope": {"kind": "project", "id": "P123"},
                    "step_name": f"test_step_{i}",
                    "fingerprint": f"abc{i}",
                }
                (run_dir / "event.json").write_text(json.dumps(event_data))

            snapshot = build_plan_snapshot(plan_dir, "test_realm", "plan_001")

            self.assertEqual(len(snapshot["steps"]), 3)
            for i in range(3):
                self.assertIn(f"step{i}", snapshot["steps"])

    def test_build_plan_snapshot_latest_run(self):
        """Test uses latest run when multiple runs exist."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"

            # Create multiple runs
            run1_dir = step_dir / "run001"
            run2_dir = step_dir / "run002"
            run1_dir.mkdir(parents=True)
            run2_dir.mkdir(parents=True)

            event1 = {
                "type": "step.started",
                "scope": {"kind": "project", "id": "P123"},
                "step_name": "old",
            }
            event2 = {
                "type": "step.succeeded",
                "scope": {"kind": "project", "id": "P123"},
                "step_name": "new",
            }

            (run1_dir / "event.json").write_text(json.dumps(event1))
            (run2_dir / "event.json").write_text(json.dumps(event2))

            snapshot = build_plan_snapshot(plan_dir, "test_realm", "plan_001")

            # Should use run002 (latest)
            self.assertEqual(snapshot["steps"]["step1"]["run_id"], "run002")
            self.assertEqual(snapshot["steps"]["step1"]["step_name"], "new")

    def test_build_plan_snapshot_latest_event_in_run(self):
        """Test uses latest event in run."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            event1 = {
                "type": "step.started",
                "scope": {"kind": "project", "id": "P123"},
                "progress": 0,
            }
            event2 = {
                "type": "step.running",
                "scope": {"kind": "project", "id": "P123"},
                "progress": 50,
            }
            event3 = {
                "type": "step.succeeded",
                "scope": {"kind": "project", "id": "P123"},
                "progress": 100,
            }

            (run_dir / "a_event.json").write_text(json.dumps(event1))
            (run_dir / "b_event.json").write_text(json.dumps(event2))
            (run_dir / "c_event.json").write_text(json.dumps(event3))

            snapshot = build_plan_snapshot(plan_dir, "test_realm", "plan_001")

            # Should use last event
            self.assertEqual(snapshot["steps"]["step1"]["state"], "step.succeeded")
            self.assertEqual(snapshot["steps"]["step1"]["progress"], 100)

    def test_build_plan_snapshot_no_runs(self):
        """Test snapshot with step directory but no runs."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            step_dir.mkdir(parents=True)

            # Add scope via another step
            other_step = plan_dir / "step2"
            run_dir = other_step / "run001"
            run_dir.mkdir(parents=True)
            event = {"type": "step.started", "scope": {"kind": "project", "id": "P123"}}
            (run_dir / "event.json").write_text(json.dumps(event))

            snapshot = build_plan_snapshot(plan_dir, "test_realm", "plan_001")

            # step1 should not be in snapshot
            self.assertNotIn("step1", snapshot["steps"])
            # step2 should be
            self.assertIn("step2", snapshot["steps"])

    def test_build_plan_snapshot_progress_calculation(self):
        """Test progress calculation for non-succeeded states."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            event = {
                "type": "step.running",
                "scope": {"kind": "project", "id": "P123"},
                "progress": 75,
            }
            (run_dir / "event.json").write_text(json.dumps(event))

            snapshot = build_plan_snapshot(plan_dir, "test_realm", "plan_001")

            self.assertEqual(snapshot["steps"]["step1"]["progress"], 75)

    def test_build_plan_snapshot_default_progress(self):
        """Test default progress when not specified."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            event = {
                "type": "step.started",
                "scope": {"kind": "project", "id": "P123"},
            }
            (run_dir / "event.json").write_text(json.dumps(event))

            snapshot = build_plan_snapshot(plan_dir, "test_realm", "plan_001")

            # Should default to 0 for non-succeeded
            self.assertEqual(snapshot["steps"]["step1"]["progress"], 0)

    def test_build_plan_snapshot_succeeded_progress(self):
        """Test progress is 100 for succeeded events."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            event = {
                "type": "step.succeeded",
                "scope": {"kind": "project", "id": "P123"},
            }
            (run_dir / "event.json").write_text(json.dumps(event))

            snapshot = build_plan_snapshot(plan_dir, "test_realm", "plan_001")

            self.assertEqual(snapshot["steps"]["step1"]["progress"], 100)


class TestFileSpoolConsumer(unittest.TestCase):
    """Tests for FileSpoolConsumer class."""

    def test_consume_nonexistent_spool(self):
        """Test consume with non-existent spool directory."""
        mock_writer = Mock()
        consumer = FileSpoolConsumer(
            spool_root=Path("/nonexistent/spool"),
            writer=mock_writer,
        )

        # Should not raise, should not call writer
        consumer.consume()
        mock_writer.write.assert_not_called()

    def test_consume_empty_spool(self):
        """Test consume with empty spool directory."""
        with TemporaryDirectory() as tmpdir:
            mock_writer = Mock()
            consumer = FileSpoolConsumer(
                spool_root=Path(tmpdir),
                writer=mock_writer,
            )

            consumer.consume()
            mock_writer.write.assert_not_called()

    def test_consume_single_plan(self):
        """Test consuming single plan."""
        with TemporaryDirectory() as tmpdir:
            spool_root = Path(tmpdir)
            realm_dir = spool_root / "test_realm"
            plan_dir = realm_dir / "plan_001"
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            scope = {"kind": "project", "id": "P123"}
            event = {"type": "step.succeeded", "scope": scope}
            (run_dir / "event.json").write_text(json.dumps(event))

            mock_writer = Mock()
            consumer = FileSpoolConsumer(spool_root=spool_root, writer=mock_writer)

            consumer.consume()

            # Writer should be called once
            mock_writer.write.assert_called_once()
            call_args = mock_writer.write.call_args
            self.assertEqual(call_args[0][0], plan_dir)
            snapshot = call_args[0][1]
            self.assertEqual(snapshot["realm"], "test_realm")
            self.assertEqual(snapshot["plan_id"], "plan_001")

    def test_consume_multiple_plans(self):
        """Test consuming multiple plans."""
        with TemporaryDirectory() as tmpdir:
            spool_root = Path(tmpdir)

            # Create multiple realms and plans
            for realm in ["realm1", "realm2"]:
                for plan in ["plan_a", "plan_b"]:
                    plan_dir = spool_root / realm / plan
                    step_dir = plan_dir / "step1"
                    run_dir = step_dir / "run001"
                    run_dir.mkdir(parents=True)

                    scope = {"kind": "project", "id": f"{realm}_{plan}"}
                    event = {"type": "step.succeeded", "scope": scope}
                    (run_dir / "event.json").write_text(json.dumps(event))

            mock_writer = Mock()
            consumer = FileSpoolConsumer(spool_root=spool_root, writer=mock_writer)

            consumer.consume()

            # Should write 4 plans (2 realms × 2 plans)
            self.assertEqual(mock_writer.write.call_count, 4)

    def test_consume_with_filter(self):
        """Test consume with filter function."""
        with TemporaryDirectory() as tmpdir:
            spool_root = Path(tmpdir)

            # Create plans
            for plan_id in ["plan_a", "plan_b", "plan_c"]:
                plan_dir = spool_root / "realm" / plan_id
                step_dir = plan_dir / "step1"
                run_dir = step_dir / "run001"
                run_dir.mkdir(parents=True)

                scope = {"kind": "project", "id": plan_id}
                event = {"type": "step.succeeded", "scope": scope}
                (run_dir / "event.json").write_text(json.dumps(event))

            # Filter to only process plan_b
            def my_filter(realm: str, plan_id: str) -> bool:
                return plan_id == "plan_b"

            mock_writer = Mock()
            consumer = FileSpoolConsumer(
                spool_root=spool_root,
                writer=mock_writer,
                filt=my_filter,
            )

            consumer.consume()

            # Should only write plan_b
            self.assertEqual(mock_writer.write.call_count, 1)
            call_args = mock_writer.write.call_args
            snapshot = call_args[0][1]
            self.assertEqual(snapshot["plan_id"], "plan_b")

    def test_consume_skips_missing_scope(self):
        """Test that plans without scope are skipped."""
        with TemporaryDirectory() as tmpdir:
            spool_root = Path(tmpdir)
            plan_dir = spool_root / "realm" / "plan_001"
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            # Event without scope
            event = {"type": "step.succeeded"}
            (run_dir / "event.json").write_text(json.dumps(event))

            mock_writer = Mock()
            consumer = FileSpoolConsumer(spool_root=spool_root, writer=mock_writer)

            consumer.consume()

            # Should not call writer
            mock_writer.write.assert_not_called()

    def test_consume_ignores_files_in_realm_dir(self):
        """Test that files in realm directory are ignored."""
        with TemporaryDirectory() as tmpdir:
            spool_root = Path(tmpdir)
            realm_dir = spool_root / "realm"
            realm_dir.mkdir()

            # Create a file in realm dir (not a plan dir)
            (realm_dir / "somefile.txt").write_text("ignore me")

            # Create valid plan
            plan_dir = realm_dir / "plan_001"
            step_dir = plan_dir / "step1"
            run_dir = step_dir / "run001"
            run_dir.mkdir(parents=True)

            scope = {"kind": "project", "id": "P123"}
            event = {"type": "step.succeeded", "scope": scope}
            (run_dir / "event.json").write_text(json.dumps(event))

            mock_writer = Mock()
            consumer = FileSpoolConsumer(spool_root=spool_root, writer=mock_writer)

            consumer.consume()

            # Should only process the valid plan
            self.assertEqual(mock_writer.write.call_count, 1)

    def test_dataclass_fields(self):
        """Test FileSpoolConsumer dataclass fields."""
        mock_writer = Mock()
        filter_fn = lambda r, p: True

        consumer = FileSpoolConsumer(
            spool_root=Path("/tmp/spool"),
            writer=mock_writer,
            filt=filter_fn,
        )

        self.assertEqual(consumer.spool_root, Path("/tmp/spool"))
        self.assertEqual(consumer.writer, mock_writer)
        self.assertEqual(consumer.filt, filter_fn)

    def test_default_filter_is_none(self):
        """Test that default filter is None."""
        consumer = FileSpoolConsumer(
            spool_root=Path("/tmp"),
            writer=Mock(),
        )

        self.assertIsNone(consumer.filt)


if __name__ == "__main__":
    unittest.main()
