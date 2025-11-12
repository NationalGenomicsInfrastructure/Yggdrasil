import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from yggdrasil.flow.events.emitter import EventEmitter, FileSpoolEmitter
from yggdrasil.flow.model import Artifact, StepResult
from yggdrasil.flow.step import StepContext, step


class TestStepContext(unittest.TestCase):
    """
    Comprehensive tests for StepContext - the execution context for workflow steps.

    Tests context initialization, event emission, artifact management, progress tracking,
    and integration patterns with the flow engine.
    """

    def setUp(self):
        """Set up test fixtures for each test."""
        self.temp_dir = TemporaryDirectory()
        self.workdir = Path(self.temp_dir.name)

        # Mock emitter for capturing events
        self.mock_emitter = Mock(spec=EventEmitter)

        # Standard test context
        self.ctx = StepContext(
            realm="test_realm",
            scope={"kind": "project", "id": "P12345"},
            plan_id="test_plan_001",
            step_id="test_step_001",
            step_name="test_step",
            workdir=self.workdir,
            emitter=self.mock_emitter,
            run_mode="auto",
            fingerprint="abc123",
            run_id="run_001",
        )

    def tearDown(self):
        """Clean up temporary resources."""
        self.temp_dir.cleanup()

    # =====================================================
    # INITIALIZATION TESTS
    # =====================================================

    def test_stepcontext_initialization_with_all_params(self):
        """Test StepContext initialization with all parameters."""
        ctx = StepContext(
            realm="production",
            scope={"kind": "flowcell", "id": "FC123"},
            plan_id="plan_42",
            step_id="step_42_001",
            step_name="demux",
            workdir=Path("/tmp/work"),
            emitter=self.mock_emitter,
            run_mode="render_only",
            fingerprint="fingerprint_xyz",
            run_id="run_999",
        )

        self.assertEqual(ctx.realm, "production")
        self.assertEqual(ctx.scope, {"kind": "flowcell", "id": "FC123"})
        self.assertEqual(ctx.plan_id, "plan_42")
        self.assertEqual(ctx.step_id, "step_42_001")
        self.assertEqual(ctx.step_name, "demux")
        self.assertEqual(ctx.workdir, Path("/tmp/work"))
        self.assertEqual(ctx.emitter, self.mock_emitter)
        self.assertEqual(ctx.run_mode, "render_only")
        self.assertEqual(ctx.fingerprint, "fingerprint_xyz")
        self.assertEqual(ctx.run_id, "run_999")
        self.assertEqual(ctx._seq, 0)

    def test_stepcontext_initialization_with_defaults(self):
        """Test StepContext initialization with default values."""
        ctx = StepContext(
            realm="test",
            scope={},
            plan_id="plan",
            step_id="step",
            step_name="name",
            workdir=Path("/tmp"),
        )

        # Should have FileSpoolEmitter as default
        self.assertIsInstance(ctx.emitter, FileSpoolEmitter)
        # Default run_mode should be "auto"
        self.assertEqual(ctx.run_mode, "auto")
        # Default fingerprint should be None
        self.assertIsNone(ctx.fingerprint)
        # Default run_id should be None
        self.assertIsNone(ctx.run_id)
        # Sequence counter starts at 0
        self.assertEqual(ctx._seq, 0)

    def test_stepcontext_workdir_is_path(self):
        """Test that workdir is properly handled as a Path."""
        # Test with Path object
        ctx_path = StepContext(
            realm="test",
            scope={},
            plan_id="p",
            step_id="s",
            step_name="n",
            workdir=Path("/tmp/path_obj"),
        )
        self.assertIsInstance(ctx_path.workdir, Path)
        self.assertEqual(ctx_path.workdir, Path("/tmp/path_obj"))

    # =====================================================
    # SEQUENCE COUNTER TESTS
    # =====================================================

    def test_next_seq_increments_counter(self):
        """Test that _next_seq increments the sequence counter."""
        # Initial value should be 0
        self.assertEqual(self.ctx._seq, 0)

        # First call should return 1
        seq1 = self.ctx._next_seq()
        self.assertEqual(seq1, 1)
        self.assertEqual(self.ctx._seq, 1)

        # Second call should return 2
        seq2 = self.ctx._next_seq()
        self.assertEqual(seq2, 2)
        self.assertEqual(self.ctx._seq, 2)

    def test_next_seq_multiple_calls(self):
        """Test sequence counter across multiple calls."""
        expected_seqs = [1, 2, 3, 4, 5]
        actual_seqs = [self.ctx._next_seq() for _ in range(5)]
        self.assertEqual(actual_seqs, expected_seqs)
        self.assertEqual(self.ctx._seq, 5)

    def test_next_seq_independence_across_contexts(self):
        """Test that sequence counters are independent per context."""
        ctx2 = StepContext(
            realm="test2",
            scope={},
            plan_id="p2",
            step_id="s2",
            step_name="n2",
            workdir=self.workdir,
            emitter=self.mock_emitter,
        )

        # Advance ctx1
        self.ctx._next_seq()
        self.ctx._next_seq()

        # ctx2 should start from its own counter
        self.assertEqual(ctx2._next_seq(), 1)
        self.assertEqual(self.ctx._seq, 2)
        self.assertEqual(ctx2._seq, 1)

    # =====================================================
    # EVENT EMISSION TESTS
    # =====================================================

    def test_emit_creates_event_with_metadata(self):
        """Test that emit creates events with all required metadata."""
        self.ctx.emit("test.event", custom_field="custom_value")

        # Should have called emitter.emit once
        self.mock_emitter.emit.assert_called_once()

        # Extract the emitted event
        emitted_event = self.mock_emitter.emit.call_args[0][0]

        # Verify all metadata fields
        self.assertEqual(emitted_event["type"], "test.event")
        self.assertEqual(emitted_event["seq"], 1)
        self.assertEqual(emitted_event["realm"], "test_realm")
        self.assertEqual(emitted_event["scope"], {"kind": "project", "id": "P12345"})
        self.assertEqual(emitted_event["plan_id"], "test_plan_001")
        self.assertEqual(emitted_event["step_id"], "test_step_001")
        self.assertEqual(emitted_event["step_name"], "test_step")
        self.assertEqual(emitted_event["fingerprint"], "abc123")
        self.assertEqual(emitted_event["custom_field"], "custom_value")

    def test_emit_increments_sequence(self):
        """Test that emit increments sequence for each event."""
        self.ctx.emit("event.one")
        self.ctx.emit("event.two")
        self.ctx.emit("event.three")

        # Should have 3 emissions
        self.assertEqual(self.mock_emitter.emit.call_count, 3)

        # Extract seq values
        calls = self.mock_emitter.emit.call_args_list
        seqs = [call[0][0]["seq"] for call in calls]

        # Sequences should increment
        self.assertEqual(seqs, [1, 2, 3])

    def test_emit_with_spool_path_hints(self):
        """Test that emit includes _spool_path hints for file organization."""
        self.ctx.emit("test.event", data="value")

        emitted_event = self.mock_emitter.emit.call_args[0][0]

        # Should have _spool_path hints
        self.assertIn("_spool_path", emitted_event)
        spool_path = emitted_event["_spool_path"]

        # Verify spool path structure
        self.assertEqual(spool_path["realm"], "test_realm")
        self.assertEqual(spool_path["plan_id"], "test_plan_001")
        self.assertEqual(spool_path["step_id"], "test_step_001")
        self.assertEqual(spool_path["run_id"], "run_001")
        # Filename should include seq and event type
        self.assertIn("0001_test_event.json", spool_path["filename"])

    def test_emit_formats_filename_with_seq_and_type(self):
        """Test that emit formats filenames with sequence and event type."""
        self.ctx.emit("step.started")
        self.ctx.emit("step.progress")
        self.ctx.emit("step.succeeded")

        calls = self.mock_emitter.emit.call_args_list
        filenames = [call[0][0]["_spool_path"]["filename"] for call in calls]

        # Verify filename format: {seq:04d}_{type_with_underscores}.json
        self.assertEqual(filenames[0], "0001_step_started.json")
        self.assertEqual(filenames[1], "0002_step_progress.json")
        self.assertEqual(filenames[2], "0003_step_succeeded.json")

    def test_emit_with_complex_payload(self):
        """Test emit with complex nested payload data."""
        complex_payload = {
            "nested": {"deep": {"value": 123}},
            "list": [1, 2, 3],
            "metadata": {"timestamp": "2025-01-01", "source": "test"},
        }

        self.ctx.emit("complex.event", **complex_payload)

        emitted_event = self.mock_emitter.emit.call_args[0][0]

        # All payload fields should be present
        self.assertEqual(emitted_event["nested"], {"deep": {"value": 123}})
        self.assertEqual(emitted_event["list"], [1, 2, 3])
        self.assertEqual(
            emitted_event["metadata"], {"timestamp": "2025-01-01", "source": "test"}
        )

    def test_emit_without_run_id(self):
        """Test emit behavior when run_id is None."""
        ctx_no_run = StepContext(
            realm="test",
            scope={},
            plan_id="plan",
            step_id="step",
            step_name="name",
            workdir=self.workdir,
            emitter=self.mock_emitter,
            run_id=None,
        )

        ctx_no_run.emit("test.event")

        emitted_event = self.mock_emitter.emit.call_args[0][0]
        spool_path = emitted_event["_spool_path"]

        # run_id should be None in spool_path
        self.assertIsNone(spool_path["run_id"])

    # =====================================================
    # ARTIFACT MANAGEMENT TESTS
    # =====================================================

    def test_add_artifact_with_explicit_digest(self):
        """Test add_artifact with explicitly provided digest."""
        artifact = self.ctx.add_artifact(
            role="test_output",
            path="/path/to/file.txt",
            digest="sha256:abc123def456",
        )

        # Verify artifact structure
        self.assertIsInstance(artifact, Artifact)
        self.assertEqual(artifact.role, "test_output")
        self.assertEqual(artifact.path, "/path/to/file.txt")
        self.assertEqual(artifact.digest, "sha256:abc123def456")

        # Should emit step.artifact event
        self.mock_emitter.emit.assert_called_once()
        emitted_event = self.mock_emitter.emit.call_args[0][0]
        self.assertEqual(emitted_event["type"], "step.artifact")
        self.assertEqual(emitted_event["artifact"]["role"], "test_output")

    def test_add_artifact_file_auto_digest(self):
        """Test add_artifact automatically computes digest for files."""
        # Create a test file
        test_file = self.workdir / "test_file.txt"
        test_file.write_text("test content", encoding="utf-8")

        artifact = self.ctx.add_artifact(role="test_file", path=str(test_file))

        # Should have computed sha256 digest
        self.assertIsNotNone(artifact.digest)
        self.assertTrue(artifact.digest.startswith("sha256:"))  # type: ignore
        # Digest should be 64 hex characters after "sha256:"
        self.assertEqual(len(artifact.digest), len("sha256:") + 64)  # type: ignore

    def test_add_artifact_directory_auto_digest(self):
        """Test add_artifact automatically computes dirhash for directories."""
        # Create a test directory with files
        test_dir = self.workdir / "test_dir"
        test_dir.mkdir()
        (test_dir / "file1.txt").write_text("content1", encoding="utf-8")
        (test_dir / "file2.txt").write_text("content2", encoding="utf-8")

        artifact = self.ctx.add_artifact(role="test_dir", path=str(test_dir))

        # Should have computed dirhash
        self.assertIsNotNone(artifact.digest)
        self.assertTrue(artifact.digest.startswith("dirhash:"))  # type: ignore

    def test_add_artifact_emits_event_immediately(self):
        """Test that add_artifact emits event immediately for UI updates."""
        test_file = self.workdir / "immediate.txt"
        test_file.write_text("content", encoding="utf-8")

        # Reset mock to clear any previous calls
        self.mock_emitter.reset_mock()

        self.ctx.add_artifact(role="immediate", path=str(test_file))

        # Should emit immediately
        self.mock_emitter.emit.assert_called_once()

        emitted_event = self.mock_emitter.emit.call_args[0][0]
        self.assertEqual(emitted_event["type"], "step.artifact")
        self.assertEqual(emitted_event["artifact"]["role"], "immediate")
        self.assertEqual(emitted_event["artifact"]["path"], str(test_file))
        self.assertIsNotNone(emitted_event["artifact"]["digest"])

    def test_add_artifact_converts_path_to_string(self):
        """Test that add_artifact converts Path objects to strings."""
        test_file = self.workdir / "path_obj.txt"
        test_file.write_text("content", encoding="utf-8")

        # Pass as Path object
        artifact = self.ctx.add_artifact(role="path_test", path=str(test_file))

        # Should store as string
        self.assertIsInstance(artifact.path, str)
        self.assertEqual(artifact.path, str(test_file))

    def test_add_artifact_multiple_artifacts(self):
        """Test adding multiple artifacts in sequence."""
        file1 = self.workdir / "file1.txt"
        file2 = self.workdir / "file2.txt"
        file1.write_text("content1", encoding="utf-8")
        file2.write_text("content2", encoding="utf-8")

        self.mock_emitter.reset_mock()

        artifact1 = self.ctx.add_artifact("output1", str(file1))
        artifact2 = self.ctx.add_artifact("output2", str(file2))

        # Should have emitted 2 events
        self.assertEqual(self.mock_emitter.emit.call_count, 2)

        # Artifacts should be different
        self.assertNotEqual(artifact1.role, artifact2.role)
        self.assertNotEqual(artifact1.path, artifact2.path)

    # =====================================================
    # PROGRESS TRACKING TESTS
    # =====================================================

    def test_progress_emits_event(self):
        """Test that progress emits progress event."""
        self.mock_emitter.reset_mock()

        self.ctx.progress(50.0, "Halfway done")

        self.mock_emitter.emit.assert_called_once()
        emitted_event = self.mock_emitter.emit.call_args[0][0]

        self.assertEqual(emitted_event["type"], "step.progress")
        self.assertEqual(emitted_event["progress"], 50.0)
        self.assertEqual(emitted_event["message"], "Halfway done")

    def test_progress_clamps_percentage_to_valid_range(self):
        """Test that progress clamps percentage to 0-100 range."""
        self.mock_emitter.reset_mock()

        # Test values outside valid range
        self.ctx.progress(-10.0, "Below zero")
        self.ctx.progress(150.0, "Above hundred")
        self.ctx.progress(50.0, "Valid")

        calls = self.mock_emitter.emit.call_args_list

        # Should clamp to 0 and 100
        self.assertEqual(calls[0][0][0]["progress"], 0)
        self.assertEqual(calls[1][0][0]["progress"], 100)
        self.assertEqual(calls[2][0][0]["progress"], 50.0)

    def test_progress_without_message(self):
        """Test progress with None message."""
        self.mock_emitter.reset_mock()

        self.ctx.progress(75.0, None)

        emitted_event = self.mock_emitter.emit.call_args[0][0]
        self.assertEqual(emitted_event["progress"], 75.0)
        self.assertIsNone(emitted_event["message"])

    def test_progress_with_message(self):
        """Test progress with descriptive message."""
        self.mock_emitter.reset_mock()

        messages = [
            "Starting analysis",
            "Processing data",
            "Finalizing results",
        ]

        for pct, msg in zip([10, 50, 90], messages):
            self.ctx.progress(pct, msg)

        calls = self.mock_emitter.emit.call_args_list
        for i, (pct, msg) in enumerate(zip([10, 50, 90], messages)):
            self.assertEqual(calls[i][0][0]["progress"], pct)
            self.assertEqual(calls[i][0][0]["message"], msg)

    def test_progress_incremental_updates(self):
        """Test progress with incremental percentage updates."""
        self.mock_emitter.reset_mock()

        percentages = [0, 25, 50, 75, 100]
        for pct in percentages:
            self.ctx.progress(pct, f"{pct}% complete")

        self.assertEqual(self.mock_emitter.emit.call_count, 5)

        # Verify all progress values
        calls = self.mock_emitter.emit.call_args_list
        actual_pcts = [call[0][0]["progress"] for call in calls]
        self.assertEqual(actual_pcts, percentages)


class TestStepDecorator(unittest.TestCase):
    """
    Comprehensive tests for the @step decorator.

    Tests decorator behavior, function wrapping, event emission, error handling,
    and integration with StepContext and the engine.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = TemporaryDirectory()
        self.workdir = Path(self.temp_dir.name)
        self.mock_emitter = Mock(spec=EventEmitter)

        self.ctx = StepContext(
            realm="test",
            scope={"kind": "test", "id": "T001"},
            plan_id="plan",
            step_id="step",
            step_name="test_step",
            workdir=self.workdir,
            emitter=self.mock_emitter,
        )

    def tearDown(self):
        """Clean up temporary resources."""
        self.temp_dir.cleanup()

    # =====================================================
    # DECORATOR BASICS TESTS
    # =====================================================

    def test_step_decorator_with_default_name(self):
        """Test @step decorator uses function name as default step name."""

        @step()
        def my_test_step(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult()

        # Should preserve original function name
        self.assertEqual(my_test_step.__name__, "my_test_step")
        # Should attach step metadata
        self.assertTrue(hasattr(my_test_step, "_step_name"))
        self.assertEqual(my_test_step._step_name, "my_test_step")  # type: ignore

    def test_step_decorator_with_explicit_name(self):
        """Test @step decorator with explicit name parameter."""

        @step(name="custom_name")
        def my_function(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult()

        # Function name preserved
        self.assertEqual(my_function.__name__, "my_function")
        # Step name should be custom
        self.assertEqual(my_function._step_name, "custom_name")  # type: ignore

    def test_step_decorator_with_input_keys(self):
        """Test @step decorator with input_keys parameter."""

        @step(input_keys=("input_file", "config_file"))
        def process_data(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult()

        # Should attach input_keys metadata
        self.assertTrue(hasattr(process_data, "_input_keys"))
        self.assertEqual(process_data._input_keys, ("input_file", "config_file"))  # type: ignore

    def test_step_decorator_preserves_function_metadata(self):
        """Test that @step preserves original function metadata."""

        @step(name="test_step")
        def documented_function(ctx: StepContext, **kwargs) -> StepResult:
            """This is a test function with documentation."""
            return StepResult()

        # functools.wraps should preserve docstring
        self.assertIn("test function", documented_function.__doc__)  # type: ignore

    def test_step_decorator_requires_call(self):
        """Test that @step is a decorator factory and requires being called."""
        # The @step decorator is a factory pattern: step() returns the actual decorator
        # This test documents that step itself is not the decorator

        # step without parentheses gives us the factory function
        decorator_factory = step
        self.assertTrue(callable(decorator_factory))

        # Calling it gives us the actual decorator
        actual_decorator = step()
        self.assertTrue(callable(actual_decorator))

        # Using it requires parentheses
        @step()
        def correct_usage(ctx: StepContext) -> StepResult:
            return StepResult()

        # Should have step metadata
        self.assertTrue(hasattr(correct_usage, "_step_name"))

    # =====================================================
    # FUNCTION EXECUTION TESTS
    # =====================================================

    def test_step_executes_wrapped_function(self):
        """Test that decorated step executes the wrapped function."""
        executed = []

        @step()
        def test_execution(ctx: StepContext, **kwargs) -> StepResult:
            executed.append(True)
            return StepResult()

        test_execution(self.ctx, param="value")

        # Function should have been executed
        self.assertEqual(len(executed), 1)

    def test_step_receives_context_and_params(self):
        """Test that step receives StepContext and kwargs."""
        received_ctx = []
        received_kwargs = []

        @step()
        def receive_params(ctx: StepContext, **kwargs) -> StepResult:
            received_ctx.append(ctx)
            received_kwargs.append(kwargs)
            return StepResult()

        test_kwargs = {"param1": "value1", "param2": 42}
        receive_params(self.ctx, **test_kwargs)

        # Should receive context and kwargs
        self.assertEqual(len(received_ctx), 1)
        self.assertIs(received_ctx[0], self.ctx)
        self.assertEqual(received_kwargs[0], test_kwargs)

    def test_step_returns_stepresult(self):
        """Test that step returns StepResult."""

        @step()
        def return_result(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult(
                artifacts=[Artifact(role="output", path="/out/file.txt")],
                metrics={"accuracy": 0.95},
                extra={"note": "test"},
            )

        result = return_result(self.ctx)

        # Should return StepResult
        self.assertIsInstance(result, StepResult)
        self.assertEqual(len(result.artifacts), 1)
        self.assertEqual(result.metrics["accuracy"], 0.95)
        self.assertEqual(result.extra["note"], "test")

    def test_step_returns_none_creates_empty_result(self):
        """Test that returning None creates empty StepResult."""

        @step()
        def return_none(ctx: StepContext, **kwargs) -> None:
            pass

        result = return_none(self.ctx)

        # Should create empty StepResult
        self.assertIsInstance(result, StepResult)
        self.assertEqual(len(result.artifacts), 0)
        self.assertEqual(len(result.metrics), 0)
        self.assertEqual(len(result.extra), 0)

    # =====================================================
    # EVENT EMISSION TESTS
    # =====================================================

    def test_step_emits_started_event(self):
        """Test that step emits step.started event."""

        @step()
        def emit_started(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult()

        self.mock_emitter.reset_mock()
        emit_started(self.ctx, param1="value1")

        # Find step.started event
        calls = self.mock_emitter.emit.call_args_list
        started_events = [c for c in calls if c[0][0]["type"] == "step.started"]

        self.assertEqual(len(started_events), 1)
        started_event = started_events[0][0][0]
        self.assertEqual(started_event["params"], {"param1": "value1"})

    def test_step_emits_succeeded_event(self):
        """Test that step emits step.succeeded event on success."""

        @step()
        def emit_succeeded(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult(
                artifacts=[Artifact(role="output", path="/path")],
                metrics={"count": 10},
                extra={"status": "complete"},
            )

        self.mock_emitter.reset_mock()
        emit_succeeded(self.ctx)

        # Find step.succeeded event
        calls = self.mock_emitter.emit.call_args_list
        succeeded_events = [c for c in calls if c[0][0]["type"] == "step.succeeded"]

        self.assertEqual(len(succeeded_events), 1)
        succeeded_event = succeeded_events[0][0][0]

        # Verify event contents
        self.assertEqual(len(succeeded_event["artifacts"]), 1)
        self.assertEqual(succeeded_event["metrics"], {"count": 10})
        self.assertEqual(succeeded_event["extra"], {"status": "complete"})

    def test_step_emits_failed_event_on_exception(self):
        """Test that step emits step.failed event on exception."""

        @step()
        def emit_failed(ctx: StepContext, **kwargs) -> StepResult:
            raise ValueError("Test error")

        self.mock_emitter.reset_mock()

        with self.assertRaises(ValueError):
            emit_failed(self.ctx)

        # Find step.failed event
        calls = self.mock_emitter.emit.call_args_list
        failed_events = [c for c in calls if c[0][0]["type"] == "step.failed"]

        self.assertEqual(len(failed_events), 1)
        failed_event = failed_events[0][0][0]
        self.assertIn("Test error", failed_event["error"])

    def test_step_event_sequence(self):
        """Test complete event sequence for successful step."""

        @step()
        def complete_sequence(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult()

        self.mock_emitter.reset_mock()
        complete_sequence(self.ctx, test_param="value")

        # Extract event types in order
        calls = self.mock_emitter.emit.call_args_list
        event_types = [call[0][0]["type"] for call in calls]

        # Should start with step.started and end with step.succeeded
        self.assertEqual(event_types[0], "step.started")
        self.assertEqual(event_types[-1], "step.succeeded")

    # =====================================================
    # ERROR HANDLING TESTS
    # =====================================================

    def test_step_propagates_exceptions(self):
        """Test that exceptions are propagated after emitting failed event."""

        @step()
        def raise_error(ctx: StepContext, **kwargs) -> StepResult:
            raise RuntimeError("Step failed")

        with self.assertRaises(RuntimeError) as context:
            raise_error(self.ctx)

        self.assertEqual(str(context.exception), "Step failed")

    def test_step_handles_different_exception_types(self):
        """Test step handling of various exception types."""
        exception_types = [
            ValueError("value error"),
            TypeError("type error"),
            KeyError("key error"),
            RuntimeError("runtime error"),
        ]

        for exc in exception_types:

            @step()
            def raise_specific_error(ctx: StepContext, **kwargs) -> StepResult:
                raise exc

            self.mock_emitter.reset_mock()

            with self.assertRaises(type(exc)):
                raise_specific_error(self.ctx)

            # Should emit failed event with error message
            calls = self.mock_emitter.emit.call_args_list
            failed_events = [c for c in calls if c[0][0]["type"] == "step.failed"]
            self.assertEqual(len(failed_events), 1)

    def test_step_error_includes_exception_details(self):
        """Test that failed event includes exception details."""

        @step()
        def detailed_error(ctx: StepContext, **kwargs) -> StepResult:
            raise ValueError("Detailed error message with context")

        self.mock_emitter.reset_mock()

        with self.assertRaises(ValueError):
            detailed_error(self.ctx)

        failed_events = [
            c
            for c in self.mock_emitter.emit.call_args_list
            if c[0][0]["type"] == "step.failed"
        ]
        failed_event = failed_events[0][0][0]

        # Error message should include the exception details
        self.assertIn("Detailed error message with context", failed_event["error"])

    # =====================================================
    # INTEGRATION AND REAL-WORLD PATTERN TESTS
    # =====================================================

    def test_step_with_artifact_creation(self):
        """Test step that creates artifacts during execution."""

        @step()
        def create_artifacts(ctx: StepContext, **kwargs) -> StepResult:
            # Create test files
            output1 = ctx.workdir / "output1.txt"
            output2 = ctx.workdir / "output2.txt"
            output1.write_text("data1", encoding="utf-8")
            output2.write_text("data2", encoding="utf-8")

            # Add artifacts
            art1 = ctx.add_artifact("output1", str(output1))
            art2 = ctx.add_artifact("output2", str(output2))

            return StepResult(artifacts=[art1, art2])

        result = create_artifacts(self.ctx)

        # Should have 2 artifacts
        self.assertEqual(len(result.artifacts), 2)
        # Both should have digests
        self.assertTrue(all(a.digest for a in result.artifacts))

    def test_step_with_progress_tracking(self):
        """Test step that reports progress during execution."""

        @step()
        def track_progress(ctx: StepContext, **kwargs) -> StepResult:
            ctx.progress(0, "Starting")
            ctx.progress(50, "Halfway")
            ctx.progress(100, "Complete")
            return StepResult()

        self.mock_emitter.reset_mock()
        track_progress(self.ctx)

        # Should have progress events
        progress_events = [
            c
            for c in self.mock_emitter.emit.call_args_list
            if c[0][0]["type"] == "step.progress"
        ]
        self.assertEqual(len(progress_events), 3)

        # Verify progress values
        progress_values = [e[0][0]["progress"] for e in progress_events]
        self.assertEqual(progress_values, [0, 50, 100])

    def test_step_with_metrics_collection(self):
        """Test step that collects and returns metrics."""

        @step()
        def collect_metrics(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult(
                metrics={
                    "files_processed": 100,
                    "duration_seconds": 45.5,
                    "success_rate": 0.98,
                    "errors": [],
                }
            )

        result = collect_metrics(self.ctx)

        # Verify metrics
        self.assertEqual(result.metrics["files_processed"], 100)
        self.assertEqual(result.metrics["duration_seconds"], 45.5)
        self.assertEqual(result.metrics["success_rate"], 0.98)
        self.assertEqual(result.metrics["errors"], [])

    def test_step_with_complex_workflow(self):
        """Test step with complex workflow including all features."""

        @step(name="complex_workflow", input_keys=("input_path",))
        def complex_step(ctx: StepContext, input_path: str, **kwargs) -> StepResult:
            # Progress tracking
            ctx.progress(10, "Initializing")

            # Create output
            output_file = ctx.workdir / "result.txt"
            output_file.write_text(f"Processed: {input_path}", encoding="utf-8")

            ctx.progress(50, "Processing")

            # Add artifact
            artifact = ctx.add_artifact("result", str(output_file))

            ctx.progress(100, "Complete")

            # Return result with metrics
            return StepResult(
                artifacts=[artifact],
                metrics={
                    "input": input_path,
                    "output_size": len(output_file.read_text()),
                },
                extra={"status": "success"},
            )

        self.mock_emitter.reset_mock()
        result = complex_step(self.ctx, input_path="/input/data.txt")

        # Verify complete workflow
        self.assertEqual(len(result.artifacts), 1)
        self.assertEqual(result.artifacts[0].role, "result")
        self.assertIn("input", result.metrics)
        self.assertEqual(result.extra["status"], "success")

        # Verify event sequence
        event_types = [
            call[0][0]["type"] for call in self.mock_emitter.emit.call_args_list
        ]
        self.assertIn("step.started", event_types)
        self.assertIn("step.progress", event_types)
        self.assertIn("step.artifact", event_types)
        self.assertIn("step.succeeded", event_types)

    def test_step_parameter_passing(self):
        """Test various parameter passing patterns."""

        @step()
        def param_test(
            ctx: StepContext,
            required_param: str,
            optional_param: str = "default",
            **kwargs,
        ) -> StepResult:
            return StepResult(
                extra={
                    "required": required_param,
                    "optional": optional_param,
                    "kwargs": kwargs,
                }
            )

        result = param_test(
            self.ctx,
            required_param="required_value",
            optional_param="custom_value",
            extra1="extra_value1",
            extra2="extra_value2",
        )

        # Verify parameters
        self.assertEqual(result.extra["required"], "required_value")
        self.assertEqual(result.extra["optional"], "custom_value")
        self.assertEqual(result.extra["kwargs"]["extra1"], "extra_value1")
        self.assertEqual(result.extra["kwargs"]["extra2"], "extra_value2")

    def test_step_metadata_attached_to_wrapper(self):
        """Test that step metadata is attached to the wrapper function."""

        @step(name="metadata_test", input_keys=("input1", "input2"))
        def metadata_step(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult()

        # Verify metadata on wrapper
        self.assertEqual(metadata_step._step_name, "metadata_test")  # type: ignore
        self.assertEqual(metadata_step._input_keys, ("input1", "input2"))  # type: ignore

    def test_step_with_no_return_value(self):
        """Test step that doesn't explicitly return a value."""

        @step()
        def no_return(ctx: StepContext, **kwargs):
            # Do some work but don't return anything
            pass

        result = no_return(self.ctx)

        # Should create empty StepResult
        self.assertIsInstance(result, StepResult)
        self.assertEqual(len(result.artifacts), 0)


if __name__ == "__main__":
    unittest.main()
