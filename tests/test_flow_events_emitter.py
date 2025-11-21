import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from yggdrasil.flow.events.emitter import (
    CouchEmitter,
    EventEmitter,
    FileSpoolEmitter,
    TeeEmitter,
)


class TestEventEmitterProtocol(unittest.TestCase):
    """
    Comprehensive tests for EventEmitter Protocol.

    Tests the protocol definition and compliance for event emitters.
    """

    # =====================================================
    # PROTOCOL COMPLIANCE TESTS
    # =====================================================

    def test_protocol_has_emit_method(self):
        """Test that EventEmitter protocol requires emit method."""
        # EventEmitter is a Protocol, so we can't instantiate it directly
        # but we can verify that classes implementing it have the right signature
        self.assertTrue(hasattr(EventEmitter, "emit"))

    def test_file_spool_emitter_implements_protocol(self):
        """Test that FileSpoolEmitter implements EventEmitter protocol."""
        temp_dir = TemporaryDirectory()
        try:
            emitter = FileSpoolEmitter(spool_dir=temp_dir.name)

            # Should have emit method
            self.assertTrue(hasattr(emitter, "emit"))
            self.assertTrue(callable(emitter.emit))
        finally:
            temp_dir.cleanup()

    def test_tee_emitter_implements_protocol(self):
        """Test that TeeEmitter implements EventEmitter protocol."""
        mock_emitter = Mock(spec=EventEmitter)
        emitter = TeeEmitter(mock_emitter)

        # Should have emit method
        self.assertTrue(hasattr(emitter, "emit"))
        self.assertTrue(callable(emitter.emit))

    def test_couch_emitter_implements_protocol(self):
        """Test that CouchEmitter implements EventEmitter protocol."""
        mock_couch = Mock()
        emitter = CouchEmitter(couch_client=mock_couch)

        # Should have emit method
        self.assertTrue(hasattr(emitter, "emit"))
        self.assertTrue(callable(emitter.emit))

    def test_custom_emitter_can_implement_protocol(self):
        """Test that custom classes can implement EventEmitter protocol."""

        class CustomEmitter:
            def emit(self, event: dict) -> None:
                pass

        emitter = CustomEmitter()

        # Should have emit method
        self.assertTrue(hasattr(emitter, "emit"))
        self.assertTrue(callable(emitter.emit))


class TestFileSpoolEmitter(unittest.TestCase):
    """
    Comprehensive tests for FileSpoolEmitter class.

    Tests file-based event spooling including directory structure,
    file creation, atomic writes, and path hints.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = TemporaryDirectory()
        self.spool_dir = Path(self.temp_dir.name) / "events"

    def tearDown(self):
        """Clean up temporary resources."""
        self.temp_dir.cleanup()

    # =====================================================
    # INITIALIZATION TESTS
    # =====================================================

    def test_initialization_with_explicit_spool_dir(self):
        """Test FileSpoolEmitter initialization with explicit spool_dir."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        self.assertEqual(emitter.root, self.spool_dir)
        self.assertTrue(self.spool_dir.exists())
        self.assertTrue(self.spool_dir.is_dir())

    def test_initialization_creates_spool_directory(self):
        """Test that initialization creates the spool directory."""
        spool_path = self.temp_dir.name + "/new_spool"
        self.assertFalse(Path(spool_path).exists())

        emitter = FileSpoolEmitter(spool_dir=spool_path)

        self.assertTrue(Path(spool_path).exists())
        self.assertTrue(Path(spool_path).is_dir())

    def test_initialization_with_env_var(self):
        """Test FileSpoolEmitter initialization from environment variable."""
        env_spool_dir = str(self.spool_dir)

        with patch.dict(os.environ, {"YGG_EVENT_SPOOL": env_spool_dir}):
            emitter = FileSpoolEmitter()

            self.assertEqual(emitter.root, self.spool_dir)

    def test_initialization_with_default_path(self):
        """Test FileSpoolEmitter initialization with default path."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove YGG_EVENT_SPOOL if it exists
            os.environ.pop("YGG_EVENT_SPOOL", None)

            emitter = FileSpoolEmitter()

            self.assertEqual(emitter.root, Path("/tmp/ygg_events"))

    def test_initialization_priority_explicit_over_env(self):
        """Test that explicit spool_dir takes priority over env var."""
        env_dir = str(self.spool_dir / "env")
        explicit_dir = str(self.spool_dir / "explicit")

        with patch.dict(os.environ, {"YGG_EVENT_SPOOL": env_dir}):
            emitter = FileSpoolEmitter(spool_dir=explicit_dir)

            self.assertEqual(emitter.root, Path(explicit_dir))

    # =====================================================
    # BASIC EVENT EMISSION TESTS
    # =====================================================

    def test_emit_simple_event(self):
        """Test emitting a simple event."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {"type": "test.event", "message": "Hello"}
        emitter.emit(event)

        # Should create file in default path
        event_files = list(self.spool_dir.rglob("*.json"))
        self.assertEqual(len(event_files), 1)

    def test_emit_adds_event_id(self):
        """Test that emit adds an event ID if not present."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {"type": "test.event"}
        emitter.emit(event)

        # Read the emitted event
        event_file = list(self.spool_dir.rglob("*.json"))[0]
        stored_event = json.loads(event_file.read_text())

        self.assertIn("eid", stored_event)
        self.assertIsInstance(stored_event["eid"], str)

    def test_emit_preserves_existing_event_id(self):
        """Test that emit preserves existing event ID."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {"type": "test.event", "eid": "custom-id-123"}
        emitter.emit(event)

        # Read the emitted event
        event_file = list(self.spool_dir.rglob("*.json"))[0]
        stored_event = json.loads(event_file.read_text())

        self.assertEqual(stored_event["eid"], "custom-id-123")

    def test_emit_adds_timestamp(self):
        """Test that emit adds a timestamp if not present."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {"type": "test.event"}
        emitter.emit(event)

        # Read the emitted event
        event_file = list(self.spool_dir.rglob("*.json"))[0]
        stored_event = json.loads(event_file.read_text())

        self.assertIn("ts", stored_event)
        self.assertIsInstance(stored_event["ts"], str)
        # Should be ISO format
        self.assertIn("T", stored_event["ts"])

    def test_emit_preserves_existing_timestamp(self):
        """Test that emit preserves existing timestamp."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        custom_ts = "2025-01-01T00:00:00.000000Z"
        event = {"type": "test.event", "ts": custom_ts}
        emitter.emit(event)

        # Read the emitted event
        event_file = list(self.spool_dir.rglob("*.json"))[0]
        stored_event = json.loads(event_file.read_text())

        self.assertEqual(stored_event["ts"], custom_ts)

    def test_emit_does_not_modify_original_event(self):
        """Test that emit does not modify the original event dict."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {"type": "test.event", "data": "value"}
        original_keys = set(event.keys())

        emitter.emit(event)

        # Original event should be unchanged
        self.assertEqual(set(event.keys()), original_keys)

    # =====================================================
    # PATH HINTS TESTS
    # =====================================================

    def test_emit_with_path_hints_creates_directory_structure(self):
        """Test that emit with path hints creates proper directory structure."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {
            "type": "step.started",
            "_spool_path": {
                "realm": "test_realm",
                "plan_id": "plan_123",
                "step_id": "step_456",
            },
        }
        emitter.emit(event)

        # Should create nested directory structure
        expected_dir = self.spool_dir / "test_realm" / "plan_123" / "step_456"
        self.assertTrue(expected_dir.exists())
        self.assertTrue(expected_dir.is_dir())

    def test_emit_removes_spool_path_from_stored_event(self):
        """Test that _spool_path is removed from stored event."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {
            "type": "test.event",
            "_spool_path": {"realm": "test", "plan_id": "p1", "step_id": "s1"},
        }
        emitter.emit(event)

        # Read the stored event
        event_file = list(self.spool_dir.rglob("*.json"))[0]
        stored_event = json.loads(event_file.read_text())

        # _spool_path should not be in stored event
        self.assertNotIn("_spool_path", stored_event)

    def test_emit_with_run_id_in_path(self):
        """Test that run_id is included in directory structure."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {
            "type": "step.started",
            "_spool_path": {
                "realm": "test",
                "plan_id": "plan1",
                "step_id": "step1",
                "run_id": "run_001",
            },
        }
        emitter.emit(event)

        # Should include run_id in path
        expected_dir = self.spool_dir / "test" / "plan1" / "step1" / "run_001"
        self.assertTrue(expected_dir.exists())

    def test_emit_without_run_id_in_path(self):
        """Test that run_id is not included when not provided."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {
            "type": "step.started",
            "_spool_path": {"realm": "test", "plan_id": "plan1", "step_id": "step1"},
        }
        emitter.emit(event)

        # Should not include run_id in path
        expected_dir = self.spool_dir / "test" / "plan1" / "step1"
        self.assertTrue(expected_dir.exists())

        # Should not have subdirectory
        subdirs = [d for d in expected_dir.iterdir() if d.is_dir()]
        self.assertEqual(len(subdirs), 0)

    def test_emit_plan_level_event_without_step_directory(self):
        """Test that plan-level events (type starts with 'plan.') are routed directly under realm/plan_id without step directory."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        # Plan-level event with 'plan.' prefix and no step_id
        event = {
            "type": "plan.draft",
            "_spool_path": {
                "realm": "test_realm",
                "plan_id": "plan_123",
                "filename": "plan_draft.json",
            },
        }
        emitter.emit(event)

        # Should create path: realm/plan_id (no step directory)
        expected_dir = self.spool_dir / "test_realm" / "plan_123"
        self.assertTrue(expected_dir.exists())

        # Verify file is directly under plan directory
        event_file = expected_dir / "plan_draft.json"
        self.assertTrue(event_file.exists())

        # Should NOT have unknown_step directory
        unknown_step_dir = expected_dir / "unknown_step"
        self.assertFalse(unknown_step_dir.exists())

    def test_emit_plan_level_event_with_run_id(self):
        """Test that plan-level events can include run_id for versioning."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {
            "type": "plan.submitted",
            "_spool_path": {
                "realm": "production",
                "plan_id": "plan_456",
                "run_id": "run_001",
                "filename": "submitted.json",
            },
        }
        emitter.emit(event)

        # Should create path: realm/plan_id/run_id
        expected_file = (
            self.spool_dir / "production" / "plan_456" / "run_001" / "submitted.json"
        )
        self.assertTrue(expected_file.exists())

    def test_emit_step_level_event_still_includes_step_directory(self):
        """Test that step-level events (not starting with 'plan.') still include step directory."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {
            "type": "step.succeeded",
            "_spool_path": {
                "realm": "test",
                "plan_id": "plan_789",
                "step_id": "step_001",
                "run_id": "run_002",
                "filename": "success.json",
            },
        }
        emitter.emit(event)

        # Should create full path including step directory
        expected_file = (
            self.spool_dir
            / "test"
            / "plan_789"
            / "step_001"
            / "run_002"
            / "success.json"
        )
        self.assertTrue(expected_file.exists())

    def test_emit_with_custom_filename(self):
        """Test that custom filename from hints is used."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {
            "type": "step.completed",
            "_spool_path": {
                "realm": "test",
                "plan_id": "p1",
                "step_id": "s1",
                "filename": "custom_event.json",
            },
        }
        emitter.emit(event)

        # Should create file with custom name
        event_file = self.spool_dir / "test" / "p1" / "s1" / "custom_event.json"
        self.assertTrue(event_file.exists())

    def test_emit_with_default_unknown_values(self):
        """Test that missing path hints use default 'unknown' values."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {"type": "test.event", "_spool_path": {}}
        emitter.emit(event)

        # Should use default unknown values
        expected_dir = self.spool_dir / "unknown" / "unknown_plan" / "unknown_step"
        self.assertTrue(expected_dir.exists())

    def test_emit_without_spool_path_hints(self):
        """Test emitting event without any _spool_path hints."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {"type": "test.event", "data": "value"}
        emitter.emit(event)

        # Should still emit to default path
        expected_dir = self.spool_dir / "unknown" / "unknown_plan" / "unknown_step"
        self.assertTrue(expected_dir.exists())

    # =====================================================
    # FILE WRITING TESTS
    # =====================================================

    def test_emit_creates_json_file(self):
        """Test that emit creates a JSON file."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {"type": "test.event", "data": {"key": "value"}}
        emitter.emit(event)

        event_files = list(self.spool_dir.rglob("*.json"))
        self.assertEqual(len(event_files), 1)
        self.assertTrue(event_files[0].suffix == ".json")

    def test_emit_writes_valid_json(self):
        """Test that emitted file contains valid JSON."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {"type": "test.event", "nested": {"data": [1, 2, 3]}}
        emitter.emit(event)

        event_file = list(self.spool_dir.rglob("*.json"))[0]

        # Should be valid JSON
        stored_event = json.loads(event_file.read_text())
        self.assertIsInstance(stored_event, dict)

    def test_emit_writes_sorted_json(self):
        """Test that JSON keys are sorted."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {"z_key": 1, "a_key": 2, "m_key": 3, "type": "test"}
        emitter.emit(event)

        event_file = list(self.spool_dir.rglob("*.json"))[0]
        content = event_file.read_text()

        # Keys should be sorted in the JSON string
        # "a_key" should appear before "m_key" and "z_key"
        a_pos = content.index('"a_key"')
        m_pos = content.index('"m_key"')
        z_pos = content.index('"z_key"')

        self.assertLess(a_pos, m_pos)
        self.assertLess(m_pos, z_pos)

    def test_emit_uses_atomic_write(self):
        """Test that emit uses atomic write (tmp then replace)."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {
            "type": "test.event",
            "_spool_path": {
                "realm": "test",
                "plan_id": "p1",
                "step_id": "s1",
                "filename": "atomic_test.json",
            },
        }

        # Patch Path.write_text to verify tmp file is created
        original_write_text = Path.write_text
        write_calls = []

        def track_write_text(self, *args, **kwargs):
            write_calls.append(str(self))
            return original_write_text(self, *args, **kwargs)

        with patch.object(Path, "write_text", track_write_text):
            emitter.emit(event)

        # Should have written to .tmp file
        self.assertTrue(any(".json.tmp" in call for call in write_calls))

        # Final file should exist
        final_file = self.spool_dir / "test" / "p1" / "s1" / "atomic_test.json"
        self.assertTrue(final_file.exists())

        # Tmp file should not exist
        tmp_file = self.spool_dir / "test" / "p1" / "s1" / "atomic_test.json.tmp"
        self.assertFalse(tmp_file.exists())

    def test_emit_preserves_event_data(self):
        """Test that all event data is preserved in the file."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {
            "type": "step.completed",
            "step_id": "s123",
            "metrics": {"duration": 42.5, "count": 100},
            "artifacts": ["file1.txt", "file2.txt"],
        }
        emitter.emit(event)

        event_file = list(self.spool_dir.rglob("*.json"))[0]
        stored_event = json.loads(event_file.read_text())

        # All original data should be present
        self.assertEqual(stored_event["type"], "step.completed")
        self.assertEqual(stored_event["step_id"], "s123")
        self.assertEqual(stored_event["metrics"]["duration"], 42.5)
        self.assertEqual(len(stored_event["artifacts"]), 2)

    def test_emit_multiple_events(self):
        """Test emitting multiple events creates multiple files."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        events = [
            {"type": "event1", "data": 1},
            {"type": "event2", "data": 2},
            {"type": "event3", "data": 3},
        ]

        for event in events:
            emitter.emit(event)

        event_files = list(self.spool_dir.rglob("*.json"))
        self.assertEqual(len(event_files), 3)

    def test_emit_to_different_paths(self):
        """Test emitting events to different paths."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event1 = {
            "type": "event1",
            "_spool_path": {"realm": "realm1", "plan_id": "p1", "step_id": "s1"},
        }
        event2 = {
            "type": "event2",
            "_spool_path": {"realm": "realm2", "plan_id": "p2", "step_id": "s2"},
        }

        emitter.emit(event1)
        emitter.emit(event2)

        # Should create separate directory structures
        self.assertTrue((self.spool_dir / "realm1" / "p1" / "s1").exists())
        self.assertTrue((self.spool_dir / "realm2" / "p2" / "s2").exists())

    # =====================================================
    # EDGE CASES AND ERROR HANDLING
    # =====================================================

    def test_emit_with_special_characters_in_event(self):
        """Test emitting event with special characters."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {
            "type": "test.event",
            "message": "Special chars: äöü ñ 中文 🎉",
            "unicode": "Testing: \u2603 \u2764",
        }
        emitter.emit(event)

        event_file = list(self.spool_dir.rglob("*.json"))[0]
        stored_event = json.loads(event_file.read_text())

        # Should preserve special characters
        self.assertEqual(stored_event["message"], event["message"])
        self.assertEqual(stored_event["unicode"], event["unicode"])

    def test_emit_with_nested_data_structures(self):
        """Test emitting event with deeply nested structures."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {
            "type": "complex.event",
            "nested": {
                "level1": {"level2": {"level3": {"data": [1, 2, 3], "value": "deep"}}}
            },
        }
        emitter.emit(event)

        event_file = list(self.spool_dir.rglob("*.json"))[0]
        stored_event = json.loads(event_file.read_text())

        # Should preserve nested structure
        self.assertEqual(
            stored_event["nested"]["level1"]["level2"]["level3"]["value"], "deep"
        )

    def test_emit_with_empty_event(self):
        """Test emitting an empty event."""
        emitter = FileSpoolEmitter(spool_dir=str(self.spool_dir))

        event = {}
        emitter.emit(event)

        # Should still create file with eid and ts
        event_file = list(self.spool_dir.rglob("*.json"))[0]
        stored_event = json.loads(event_file.read_text())

        self.assertIn("eid", stored_event)
        self.assertIn("ts", stored_event)


class TestTeeEmitter(unittest.TestCase):
    """
    Comprehensive tests for TeeEmitter class.

    Tests fan-out functionality to multiple emitters.
    """

    # =====================================================
    # INITIALIZATION TESTS
    # =====================================================

    def test_initialization_with_no_emitters(self):
        """Test TeeEmitter initialization with no emitters."""
        emitter = TeeEmitter()

        self.assertEqual(len(emitter.emitters), 0)

    def test_initialization_with_single_emitter(self):
        """Test TeeEmitter initialization with single emitter."""
        mock_emitter = Mock(spec=EventEmitter)
        emitter = TeeEmitter(mock_emitter)

        self.assertEqual(len(emitter.emitters), 1)
        self.assertIs(emitter.emitters[0], mock_emitter)

    def test_initialization_with_multiple_emitters(self):
        """Test TeeEmitter initialization with multiple emitters."""
        mock1 = Mock(spec=EventEmitter)
        mock2 = Mock(spec=EventEmitter)
        mock3 = Mock(spec=EventEmitter)

        emitter = TeeEmitter(mock1, mock2, mock3)

        self.assertEqual(len(emitter.emitters), 3)
        self.assertIs(emitter.emitters[0], mock1)
        self.assertIs(emitter.emitters[1], mock2)
        self.assertIs(emitter.emitters[2], mock3)

    # =====================================================
    # EMIT TESTS
    # =====================================================

    def test_emit_calls_all_emitters(self):
        """Test that emit calls all configured emitters."""
        mock1 = Mock(spec=EventEmitter)
        mock2 = Mock(spec=EventEmitter)
        mock3 = Mock(spec=EventEmitter)

        emitter = TeeEmitter(mock1, mock2, mock3)
        event = {"type": "test.event", "data": "value"}

        emitter.emit(event)

        # All emitters should have been called
        mock1.emit.assert_called_once_with(event)
        mock2.emit.assert_called_once_with(event)
        mock3.emit.assert_called_once_with(event)

    def test_emit_with_no_emitters(self):
        """Test that emit works with no emitters (no-op)."""
        emitter = TeeEmitter()
        event = {"type": "test.event"}

        # Should not raise
        emitter.emit(event)

    def test_emit_passes_same_event_to_all(self):
        """Test that the same event object is passed to all emitters."""
        received_events = []

        def capture_event(event):
            received_events.append(event)

        mock1 = Mock(spec=EventEmitter)
        mock1.emit = Mock(side_effect=capture_event)
        mock2 = Mock(spec=EventEmitter)
        mock2.emit = Mock(side_effect=capture_event)

        emitter = TeeEmitter(mock1, mock2)
        event = {"type": "test.event", "data": "value"}

        emitter.emit(event)

        # Both should receive the same event object
        self.assertEqual(len(received_events), 2)
        self.assertIs(received_events[0], event)
        self.assertIs(received_events[1], event)

    def test_emit_calls_emitters_in_order(self):
        """Test that emitters are called in the order they were added."""
        call_order = []

        def emitter1(event):
            call_order.append(1)

        def emitter2(event):
            call_order.append(2)

        def emitter3(event):
            call_order.append(3)

        mock1 = Mock(spec=EventEmitter)
        mock1.emit = Mock(side_effect=emitter1)
        mock2 = Mock(spec=EventEmitter)
        mock2.emit = Mock(side_effect=emitter2)
        mock3 = Mock(spec=EventEmitter)
        mock3.emit = Mock(side_effect=emitter3)

        emitter = TeeEmitter(mock1, mock2, mock3)
        emitter.emit({"type": "test"})

        self.assertEqual(call_order, [1, 2, 3])

    # =====================================================
    # INTEGRATION TESTS
    # =====================================================

    def test_tee_with_file_spool_emitters(self):
        """Test TeeEmitter with multiple FileSpoolEmitters."""
        temp_dir1 = TemporaryDirectory()
        temp_dir2 = TemporaryDirectory()

        try:
            spool1 = FileSpoolEmitter(spool_dir=temp_dir1.name)
            spool2 = FileSpoolEmitter(spool_dir=temp_dir2.name)

            emitter = TeeEmitter(spool1, spool2)
            event = {"type": "test.event", "data": "value"}

            emitter.emit(event)

            # Both spools should have the event
            files1 = list(Path(temp_dir1.name).rglob("*.json"))
            files2 = list(Path(temp_dir2.name).rglob("*.json"))

            self.assertEqual(len(files1), 1)
            self.assertEqual(len(files2), 1)

            # Events should be the same (except eid if generated)
            event1 = json.loads(files1[0].read_text())
            event2 = json.loads(files2[0].read_text())

            self.assertEqual(event1["type"], event2["type"])
            self.assertEqual(event1["data"], event2["data"])
        finally:
            temp_dir1.cleanup()
            temp_dir2.cleanup()

    def test_tee_with_mixed_emitter_types(self):
        """Test TeeEmitter with different emitter types."""
        temp_dir = TemporaryDirectory()

        try:
            file_emitter = FileSpoolEmitter(spool_dir=temp_dir.name)
            mock_emitter = Mock(spec=EventEmitter)

            emitter = TeeEmitter(file_emitter, mock_emitter)
            event = {"type": "test.event"}

            emitter.emit(event)

            # File emitter should create file
            files = list(Path(temp_dir.name).rglob("*.json"))
            self.assertEqual(len(files), 1)

            # Mock emitter should be called
            mock_emitter.emit.assert_called_once()
        finally:
            temp_dir.cleanup()

    def test_tee_emitter_error_propagation(self):
        """Test that errors from emitters are propagated."""
        mock1 = Mock(spec=EventEmitter)
        mock1.emit = Mock(side_effect=Exception("Emitter 1 failed"))
        mock2 = Mock(spec=EventEmitter)

        emitter = TeeEmitter(mock1, mock2)

        with self.assertRaises(Exception) as context:
            emitter.emit({"type": "test"})

        self.assertIn("Emitter 1 failed", str(context.exception))

        # Second emitter should not be called due to exception
        mock2.emit.assert_not_called()


class TestCouchEmitter(unittest.TestCase):
    """
    Comprehensive tests for CouchEmitter class.

    Tests CouchDB integration for event emission.
    """

    # =====================================================
    # INITIALIZATION TESTS
    # =====================================================

    def test_initialization_with_couch_client(self):
        """Test CouchEmitter initialization with couch client."""
        mock_couch = Mock()
        emitter = CouchEmitter(couch_client=mock_couch)

        self.assertIs(emitter.couch, mock_couch)

    # =====================================================
    # EMIT TESTS
    # =====================================================

    def test_emit_calls_couch_upsert_event(self):
        """Test that emit calls couch.upsert_event."""
        mock_couch = Mock()
        emitter = CouchEmitter(couch_client=mock_couch)

        event = {"type": "test.event", "data": "value"}
        emitter.emit(event)

        mock_couch.upsert_event.assert_called_once_with(event)

    def test_emit_passes_event_to_couch(self):
        """Test that the event is passed correctly to couch client."""
        mock_couch = Mock()
        emitter = CouchEmitter(couch_client=mock_couch)

        event = {
            "type": "step.completed",
            "step_id": "s123",
            "metrics": {"duration": 42},
        }
        emitter.emit(event)

        # Check the call arguments
        call_args = mock_couch.upsert_event.call_args[0][0]
        self.assertEqual(call_args["type"], "step.completed")
        self.assertEqual(call_args["step_id"], "s123")
        self.assertEqual(call_args["metrics"]["duration"], 42)

    def test_emit_multiple_events(self):
        """Test emitting multiple events to couch."""
        mock_couch = Mock()
        emitter = CouchEmitter(couch_client=mock_couch)

        events = [
            {"type": "event1", "data": 1},
            {"type": "event2", "data": 2},
            {"type": "event3", "data": 3},
        ]

        for event in events:
            emitter.emit(event)

        # Should be called once per event
        self.assertEqual(mock_couch.upsert_event.call_count, 3)

    def test_emit_with_complex_event(self):
        """Test emitting complex event structure to couch."""
        mock_couch = Mock()
        emitter = CouchEmitter(couch_client=mock_couch)

        event = {
            "type": "workflow.completed",
            "plan_id": "plan_123",
            "steps": [
                {"step_id": "s1", "status": "success"},
                {"step_id": "s2", "status": "success"},
            ],
            "metrics": {"total_duration": 100.5, "step_count": 2},
        }

        emitter.emit(event)

        # Should pass complete structure
        call_args = mock_couch.upsert_event.call_args[0][0]
        self.assertEqual(len(call_args["steps"]), 2)
        self.assertEqual(call_args["metrics"]["step_count"], 2)

    # =====================================================
    # ERROR HANDLING TESTS
    # =====================================================

    def test_emit_propagates_couch_errors(self):
        """Test that errors from couch client are propagated."""
        mock_couch = Mock()
        mock_couch.upsert_event = Mock(side_effect=Exception("Couch error"))

        emitter = CouchEmitter(couch_client=mock_couch)

        with self.assertRaises(Exception) as context:
            emitter.emit({"type": "test"})

        self.assertIn("Couch error", str(context.exception))


class TestEmitterIntegration(unittest.TestCase):
    """
    Integration tests for emitter combinations and real-world scenarios.
    """

    def test_file_and_couch_tee_integration(self):
        """Test TeeEmitter with FileSpoolEmitter and CouchEmitter."""
        temp_dir = TemporaryDirectory()

        try:
            file_emitter = FileSpoolEmitter(spool_dir=temp_dir.name)
            mock_couch = Mock()
            couch_emitter = CouchEmitter(couch_client=mock_couch)

            tee = TeeEmitter(file_emitter, couch_emitter)

            event = {
                "type": "workflow.started",
                "plan_id": "plan_001",
                "_spool_path": {
                    "realm": "production",
                    "plan_id": "plan_001",
                    "step_id": "init",
                },
            }

            tee.emit(event)

            # Should write to file spool
            files = list(Path(temp_dir.name).rglob("*.json"))
            self.assertEqual(len(files), 1)

            # Should call couch
            mock_couch.upsert_event.assert_called_once()
        finally:
            temp_dir.cleanup()

    def test_workflow_event_sequence(self):
        """Test emitting a sequence of workflow events."""
        temp_dir = TemporaryDirectory()

        try:
            emitter = FileSpoolEmitter(spool_dir=temp_dir.name)

            events = [
                {
                    "type": "workflow.started",
                    "_spool_path": {
                        "realm": "test",
                        "plan_id": "p1",
                        "step_id": "workflow",
                        "filename": "workflow_started.json",
                    },
                },
                {
                    "type": "step.started",
                    "step_id": "s1",
                    "_spool_path": {
                        "realm": "test",
                        "plan_id": "p1",
                        "step_id": "s1",
                        "filename": "started.json",
                    },
                },
                {
                    "type": "step.completed",
                    "step_id": "s1",
                    "_spool_path": {
                        "realm": "test",
                        "plan_id": "p1",
                        "step_id": "s1",
                        "filename": "completed.json",
                    },
                },
                {
                    "type": "workflow.completed",
                    "_spool_path": {
                        "realm": "test",
                        "plan_id": "p1",
                        "step_id": "workflow",
                        "filename": "workflow_completed.json",
                    },
                },
            ]

            for event in events:
                emitter.emit(event)

            # Should create all event files
            all_files = list(Path(temp_dir.name).rglob("*.json"))
            self.assertEqual(len(all_files), 4)

            # Check that workflow files exist
            workflow_dir = Path(temp_dir.name) / "test" / "p1" / "workflow"
            self.assertTrue((workflow_dir / "workflow_started.json").exists())
            self.assertTrue((workflow_dir / "workflow_completed.json").exists())

            # Check that step files exist
            step_dir = Path(temp_dir.name) / "test" / "p1" / "s1"
            self.assertTrue((step_dir / "started.json").exists())
            self.assertTrue((step_dir / "completed.json").exists())
        finally:
            temp_dir.cleanup()

    def test_multiple_plans_same_emitter(self):
        """Test using same emitter for multiple plans."""
        temp_dir = TemporaryDirectory()

        try:
            emitter = FileSpoolEmitter(spool_dir=temp_dir.name)

            plans = ["plan_A", "plan_B", "plan_C"]
            for plan_id in plans:
                event = {
                    "type": "plan.started",
                    "plan_id": plan_id,
                    "_spool_path": {
                        "realm": "test",
                        "plan_id": plan_id,
                        "step_id": "init",
                    },
                }
                emitter.emit(event)

            # Should create separate directories for each plan
            test_realm = Path(temp_dir.name) / "test"
            plan_dirs = [d.name for d in test_realm.iterdir() if d.is_dir()]

            self.assertEqual(set(plan_dirs), {"plan_A", "plan_B", "plan_C"})
        finally:
            temp_dir.cleanup()


if __name__ == "__main__":
    unittest.main()
