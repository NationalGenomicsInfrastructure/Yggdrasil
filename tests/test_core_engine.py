import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from yggdrasil.core.engine import (
    Engine,
    _default_fingerprint,
    _json_sha256,
    _lint_missing_inputs,
    _looks_like_path,
    _new_run_id,
    _short,
)
from yggdrasil.flow.events.emitter import EventEmitter
from yggdrasil.flow.model import Artifact, Plan, StepResult, StepSpec
from yggdrasil.flow.step import StepContext


class TestUtilityFunctions(unittest.TestCase):
    """
    Comprehensive tests for engine utility functions.

    Tests helper functions used by the Engine for hashing, formatting,
    and validation.
    """

    # =====================================================
    # JSON HASHING TESTS
    # =====================================================

    def test_json_sha256_empty_dict(self):
        """Test hashing an empty dictionary."""
        result = _json_sha256({})

        self.assertIsInstance(result, str)
        self.assertEqual(len(result), 64)  # SHA-256 hex length

    def test_json_sha256_simple_dict(self):
        """Test hashing a simple dictionary."""
        data = {"key": "value", "number": 42}
        result = _json_sha256(data)

        self.assertEqual(len(result), 64)
        # Should be deterministic
        self.assertEqual(_json_sha256(data), result)

    def test_json_sha256_deterministic(self):
        """Test that same data produces same hash."""
        data = {"a": 1, "b": 2, "c": 3}

        hash1 = _json_sha256(data)
        hash2 = _json_sha256(data)

        self.assertEqual(hash1, hash2)

    def test_json_sha256_key_order_independent(self):
        """Test that key order doesn't affect hash (sorted)."""
        data1 = {"z": 1, "a": 2, "m": 3}
        data2 = {"a": 2, "m": 3, "z": 1}

        hash1 = _json_sha256(data1)
        hash2 = _json_sha256(data2)

        # Should be equal because keys are sorted
        self.assertEqual(hash1, hash2)

    def test_json_sha256_with_nested_structures(self):
        """Test hashing nested dictionaries and lists."""
        data = {"outer": {"inner": [1, 2, 3]}, "list": ["a", "b", "c"]}
        result = _json_sha256(data)

        self.assertEqual(len(result), 64)

    def test_json_sha256_with_special_types(self):
        """Test hashing with types requiring default serialization."""
        data = {"path": Path("/tmp/test"), "number": 42}
        result = _json_sha256(data)

        # Should handle Path via default=str
        self.assertEqual(len(result), 64)

    # =====================================================
    # SHORT HASH TESTS
    # =====================================================

    def test_short_default_length(self):
        """Test _short with default length of 4."""
        hash_value = "abcdef1234567890"
        result = _short(hash_value)

        self.assertEqual(result, "abcd")
        self.assertEqual(len(result), 4)

    def test_short_custom_length(self):
        """Test _short with custom length."""
        hash_value = "abcdef1234567890"

        self.assertEqual(_short(hash_value, 8), "abcdef12")
        self.assertEqual(_short(hash_value, 2), "ab")
        self.assertEqual(_short(hash_value, 16), "abcdef1234567890")

    def test_short_longer_than_input(self):
        """Test _short with length longer than input."""
        hash_value = "abc"
        result = _short(hash_value, 10)

        # Should return full string
        self.assertEqual(result, "abc")

    # =====================================================
    # RUN ID GENERATION TESTS
    # =====================================================

    def test_new_run_id_format(self):
        """Test that run ID has expected format."""
        run_id = _new_run_id()

        # Should start with 'run_'
        self.assertTrue(run_id.startswith("run_"))
        # Should contain underscores
        self.assertGreater(run_id.count("_"), 1)

    def test_new_run_id_uniqueness(self):
        """Test that consecutive run IDs are unique."""
        ids = [_new_run_id() for _ in range(10)]

        # All should be unique
        self.assertEqual(len(ids), len(set(ids)))

    def test_new_run_id_contains_timestamp(self):
        """Test that run ID contains timestamp component."""
        run_id = _new_run_id()

        # Should contain compact timestamp format (YYYYMMDDTHHMMSSfZ)
        parts = run_id.split("_")
        self.assertGreater(len(parts), 1)
        # Timestamp part should contain T and Z
        self.assertTrue(any("T" in part and "Z" in part for part in parts))

    # =====================================================
    # PATH DETECTION TESTS
    # =====================================================

    def test_looks_like_path_absolute_unix(self):
        """Test detecting absolute Unix paths."""
        self.assertTrue(_looks_like_path("/absolute/path/to/file"))
        self.assertTrue(_looks_like_path("/tmp/data"))
        self.assertTrue(_looks_like_path("/"))

    def test_looks_like_path_relative_unix(self):
        """Test detecting relative Unix paths."""
        self.assertTrue(_looks_like_path("relative/path/to/file"))
        self.assertTrue(_looks_like_path("./local/file"))
        self.assertTrue(_looks_like_path("../parent/file"))

    def test_looks_like_path_windows(self):
        """Test detecting Windows paths."""
        self.assertTrue(_looks_like_path("C:\\Windows\\path"))
        self.assertTrue(_looks_like_path("relative\\windows\\path"))

    def test_looks_like_path_not_path(self):
        """Test that non-path strings are not detected."""
        self.assertFalse(_looks_like_path("simple_string"))
        self.assertFalse(_looks_like_path("no-path-here"))
        self.assertFalse(_looks_like_path("key_value"))

    def test_looks_like_path_non_string(self):
        """Test that non-string values return False."""
        self.assertFalse(_looks_like_path(42))
        self.assertFalse(_looks_like_path(None))
        self.assertFalse(_looks_like_path(["/path/list"]))
        self.assertFalse(_looks_like_path({"path": "/value"}))

    # =====================================================
    # LINT MISSING INPUTS TESTS
    # =====================================================

    def test_lint_missing_inputs_with_declared_inputs(self):
        """Test that no warning when inputs are declared in spec."""
        spec = StepSpec(
            step_id="s1",
            name="step1",
            fn_ref="m:f",
            params={"input_file": "/path/to/input"},
            inputs={"input_file": "/path/to/input"},
        )
        fn = Mock()

        # Should not raise or log warning (we can't easily test logging, but no exception)
        _lint_missing_inputs(spec, fn)

    def test_lint_missing_inputs_with_decorator_keys(self):
        """Test that no warning when fn has _input_keys."""
        spec = StepSpec(
            step_id="s1",
            name="step1",
            fn_ref="m:f",
            params={"input_file": "/path/to/input"},
        )
        fn = Mock()
        fn._input_keys = ("input_file",)

        # Should not raise
        _lint_missing_inputs(spec, fn)

    def test_lint_missing_inputs_no_path_like_params(self):
        """Test that no warning when no path-like params."""
        spec = StepSpec(
            step_id="s1",
            name="step1",
            fn_ref="m:f",
            params={"count": 10, "name": "test"},
        )
        fn = Mock()

        # Should not raise
        _lint_missing_inputs(spec, fn)

    @patch("yggdrasil.core.engine.logger")
    def test_lint_missing_inputs_warns_on_suspicious_params(self, mock_logger):
        """Test that warning is logged for path-like params without declarations."""
        spec = StepSpec(
            step_id="s1",
            name="step1",
            fn_ref="m:f",
            params={"input_file": "/path/to/input", "output_dir": "/path/to/output"},
        )
        fn = Mock()
        fn._input_keys = ()

        _lint_missing_inputs(spec, fn)

        # Should have called logger.warning
        self.assertTrue(mock_logger.warning.called)

    # =====================================================
    # DEFAULT FINGERPRINT TESTS
    # =====================================================

    def test_default_fingerprint_params_only(self):
        """Test fingerprint with only params (no inputs)."""
        spec = StepSpec(
            step_id="s1",
            name="step1",
            fn_ref="m:f",
            params={"key": "value", "count": 42},
        )
        fn = Mock()
        fn._input_keys = ()

        fingerprint = _default_fingerprint(spec, fn)

        # Should start with 'sha256:'
        self.assertTrue(fingerprint.startswith("sha256:"))
        # Should be deterministic
        self.assertEqual(_default_fingerprint(spec, fn), fingerprint)

    def test_default_fingerprint_with_file_input(self):
        """Test fingerprint including file input digest."""
        temp_dir = TemporaryDirectory()
        try:
            test_file = Path(temp_dir.name) / "input.txt"
            test_file.write_text("test content", encoding="utf-8")

            spec = StepSpec(
                step_id="s1",
                name="step1",
                fn_ref="m:f",
                params={"input": str(test_file)},
                inputs={"input": str(test_file)},
            )
            fn = Mock()

            fingerprint = _default_fingerprint(spec, fn)

            self.assertTrue(fingerprint.startswith("sha256:"))
        finally:
            temp_dir.cleanup()

    def test_default_fingerprint_with_directory_input(self):
        """Test fingerprint including directory input digest."""
        temp_dir = TemporaryDirectory()
        try:
            test_dir = Path(temp_dir.name) / "input_dir"
            test_dir.mkdir()
            (test_dir / "file1.txt").write_text("content1", encoding="utf-8")

            spec = StepSpec(
                step_id="s1",
                name="step1",
                fn_ref="m:f",
                params={"input_dir": str(test_dir)},
                inputs={"input_dir": str(test_dir)},
            )
            fn = Mock()

            fingerprint = _default_fingerprint(spec, fn)

            self.assertTrue(fingerprint.startswith("sha256:"))
        finally:
            temp_dir.cleanup()

    def test_default_fingerprint_with_missing_input(self):
        """Test fingerprint handles missing input paths."""
        spec = StepSpec(
            step_id="s1",
            name="step1",
            fn_ref="m:f",
            params={"input": "/nonexistent/path"},
            inputs={"input": "/nonexistent/path"},
        )
        fn = Mock()

        fingerprint = _default_fingerprint(spec, fn)

        # Should still generate fingerprint
        self.assertTrue(fingerprint.startswith("sha256:"))

    def test_default_fingerprint_uses_decorator_input_keys(self):
        """Test fingerprint uses fn._input_keys when spec.inputs is empty."""
        temp_dir = TemporaryDirectory()
        try:
            test_file = Path(temp_dir.name) / "input.txt"
            test_file.write_text("test", encoding="utf-8")

            spec = StepSpec(
                step_id="s1",
                name="step1",
                fn_ref="m:f",
                params={"my_input": str(test_file)},
            )
            fn = Mock()
            fn._input_keys = ("my_input",)

            fingerprint = _default_fingerprint(spec, fn)

            self.assertTrue(fingerprint.startswith("sha256:"))
        finally:
            temp_dir.cleanup()


class TestEngine(unittest.TestCase):
    """
    Comprehensive tests for Engine class.

    Tests the workflow execution engine including plan management,
    step execution, caching, and event emission.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = TemporaryDirectory()
        self.work_root = Path(self.temp_dir.name)
        self.mock_emitter = Mock(spec=EventEmitter)
        self.engine = Engine(work_root=self.work_root, emitter=self.mock_emitter)

    def tearDown(self):
        """Clean up temporary resources."""
        self.temp_dir.cleanup()

    # =====================================================
    # ENGINE INITIALIZATION TESTS
    # =====================================================

    def test_engine_initialization_with_work_root(self):
        """Test Engine initialization with explicit work_root."""
        engine = Engine(work_root=self.work_root)

        self.assertEqual(engine.work_root, self.work_root)
        self.assertIsNotNone(engine.emitter)

    def test_engine_initialization_with_emitter(self):
        """Test Engine initialization with custom emitter."""
        custom_emitter = Mock(spec=EventEmitter)
        engine = Engine(work_root=self.work_root, emitter=custom_emitter)

        self.assertIs(engine.emitter, custom_emitter)

    def test_engine_initialization_defaults(self):
        """Test Engine initialization with defaults."""
        engine = Engine()

        # Should have default work_root
        self.assertIsInstance(engine.work_root, Path)
        # Should have FileSpoolEmitter
        self.assertIsNotNone(engine.emitter)

    def test_engine_initialization_from_env(self):
        """Test Engine initialization from environment variable."""
        with patch.dict(os.environ, {"YGG_WORK_ROOT": str(self.work_root)}):
            engine = Engine()

            self.assertEqual(engine.work_root, self.work_root)

    # =====================================================
    # PLAN DIRECTORY STRUCTURE TESTS
    # =====================================================

    def test_plan_dir_structure(self):
        """Test _plan_dir creates correct path."""
        plan = Plan(plan_id="test_plan_001", realm="test", scope={})

        plan_dir = self.engine._plan_dir(plan)

        expected = self.work_root / "test_plan_001"
        self.assertEqual(plan_dir, expected)

    def test_step_dir_structure(self):
        """Test _step_dir creates correct path."""
        plan = Plan(plan_id="test_plan_001", realm="test", scope={})
        spec = StepSpec(step_id="step_001", name="test_step", fn_ref="m:f", params={})
        plan_dir = self.engine._plan_dir(plan)

        step_dir = self.engine._step_dir(plan_dir, spec)

        expected = plan_dir / "step_001"
        self.assertEqual(step_dir, expected)

    # =====================================================
    # PLAN FILE WRITING TESTS
    # =====================================================

    def test_write_plan_file_creates_directory(self):
        """Test that _write_plan_file creates plan directory."""
        plan = Plan(plan_id="plan_001", realm="test", scope={})
        plan_dir = self.engine._plan_dir(plan)

        self.engine._write_plan_file(plan, plan_dir)

        self.assertTrue(plan_dir.exists())
        self.assertTrue(plan_dir.is_dir())

    def test_write_plan_file_creates_json(self):
        """Test that _write_plan_file creates plan.json."""
        plan = Plan(
            plan_id="plan_001",
            realm="test",
            scope={"kind": "project"},
            steps=[StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})],
        )
        plan_dir = self.engine._plan_dir(plan)

        self.engine._write_plan_file(plan, plan_dir)

        plan_file = plan_dir / "plan.json"
        self.assertTrue(plan_file.exists())

    def test_write_plan_file_content(self):
        """Test that plan.json contains correct data."""
        plan = Plan(
            plan_id="plan_001",
            realm="test_realm",
            scope={"kind": "project", "id": "P123"},
            steps=[
                StepSpec(
                    step_id="s1",
                    name="step1",
                    fn_ref="module:func",
                    params={"key": "value"},
                )
            ],
        )
        plan_dir = self.engine._plan_dir(plan)

        self.engine._write_plan_file(plan, plan_dir)

        plan_file = plan_dir / "plan.json"
        content = json.loads(plan_file.read_text())

        self.assertEqual(content["plan_id"], "plan_001")
        self.assertEqual(content["realm"], "test_realm")
        self.assertEqual(content["scope"]["kind"], "project")
        self.assertEqual(len(content["steps"]), 1)
        self.assertEqual(content["steps"][0]["step_id"], "s1")

    # =====================================================
    # TOPOLOGY VALIDATION TESTS
    # =====================================================

    def test_topo_validate_no_dependencies(self):
        """Test topology validation with no dependencies."""
        plan = Plan(
            plan_id="plan_001",
            realm="test",
            scope={},
            steps=[
                StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={}, deps=[]),
                StepSpec(step_id="s2", name="n2", fn_ref="m:f", params={}, deps=[]),
            ],
        )

        # Should not raise
        self.engine._topo_validate(plan)

    def test_topo_validate_valid_dependencies(self):
        """Test topology validation with valid dependencies."""
        plan = Plan(
            plan_id="plan_001",
            realm="test",
            scope={},
            steps=[
                StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={}, deps=[]),
                StepSpec(step_id="s2", name="n2", fn_ref="m:f", params={}, deps=["s1"]),
                StepSpec(
                    step_id="s3", name="n3", fn_ref="m:f", params={}, deps=["s1", "s2"]
                ),
            ],
        )

        # Should not raise
        self.engine._topo_validate(plan)

    def test_topo_validate_missing_dependency(self):
        """Test topology validation fails with missing dependency."""
        plan = Plan(
            plan_id="plan_001",
            realm="test",
            scope={},
            steps=[
                StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={}, deps=[]),
                StepSpec(
                    step_id="s2",
                    name="n2",
                    fn_ref="m:f",
                    params={},
                    deps=["nonexistent"],
                ),
            ],
        )

        with self.assertRaises(ValueError) as context:
            self.engine._topo_validate(plan)

        self.assertIn("Unknown deps", str(context.exception))

    def test_topo_validate_forward_reference(self):
        """Test topology validation allows forward references."""
        plan = Plan(
            plan_id="plan_001",
            realm="test",
            scope={},
            steps=[
                StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={}, deps=["s2"]),
                StepSpec(step_id="s2", name="n2", fn_ref="m:f", params={}, deps=[]),
            ],
        )

        # Should not raise (weak ordering allows forward refs)
        self.engine._topo_validate(plan)

    # =====================================================
    # STEP EXECUTION TESTS
    # =====================================================

    def test_run_single_step_plan(self):
        """Test running a plan with a single step."""

        # Create a test step function
        def test_step(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult()

        # Mock resolve_callable to return our test function
        with patch("yggdrasil.core.engine.resolve_callable", return_value=test_step):
            plan = Plan(
                plan_id="plan_001",
                realm="test",
                scope={},
                steps=[
                    StepSpec(
                        step_id="s1", name="step1", fn_ref="module:func", params={}
                    )
                ],
            )

            self.engine.run(plan)

            # Verify plan directory was created
            plan_dir = self.engine._plan_dir(plan)
            self.assertTrue(plan_dir.exists())

            # Verify plan.json was created
            self.assertTrue((plan_dir / "plan.json").exists())

            # Verify step directory was created
            step_dir = plan_dir / "s1"
            self.assertTrue(step_dir.exists())

    def test_run_multiple_step_plan(self):
        """Test running a plan with multiple steps."""

        def test_step(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult()

        with patch("yggdrasil.core.engine.resolve_callable", return_value=test_step):
            plan = Plan(
                plan_id="plan_002",
                realm="test",
                scope={},
                steps=[
                    StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={}),
                    StepSpec(step_id="s2", name="n2", fn_ref="m:f", params={}),
                    StepSpec(step_id="s3", name="n3", fn_ref="m:f", params={}),
                ],
            )

            self.engine.run(plan)

            plan_dir = self.engine._plan_dir(plan)

            # All step directories should exist
            self.assertTrue((plan_dir / "s1").exists())
            self.assertTrue((plan_dir / "s2").exists())
            self.assertTrue((plan_dir / "s3").exists())

    def test_run_step_with_params(self):
        """Test that step receives correct parameters."""
        received_params = {}

        def test_step(ctx: StepContext, **kwargs) -> StepResult:
            received_params.update(kwargs)
            return StepResult()

        with patch("yggdrasil.core.engine.resolve_callable", return_value=test_step):
            plan = Plan(
                plan_id="plan_003",
                realm="test",
                scope={},
                steps=[
                    StepSpec(
                        step_id="s1",
                        name="n1",
                        fn_ref="m:f",
                        params={"key1": "value1", "key2": 42},
                    )
                ],
            )

            self.engine.run(plan)

            self.assertEqual(received_params["key1"], "value1")
            self.assertEqual(received_params["key2"], 42)

    # =====================================================
    # CACHING TESTS
    # =====================================================

    def test_cache_skip_on_matching_fingerprint(self):
        """Test that step is skipped when fingerprint matches cache."""

        def test_step(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult()

        with patch("yggdrasil.core.engine.resolve_callable", return_value=test_step):
            plan = Plan(
                plan_id="plan_004",
                realm="test",
                scope={},
                steps=[
                    StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={"k": "v"})
                ],
            )

            # Run once
            self.engine.run(plan)

            # Reset emitter mock
            self.mock_emitter.reset_mock()

            # Run again with same params
            self.engine.run(plan)

            # Should emit step.skipped event
            calls = self.mock_emitter.emit.call_args_list
            skipped_events = [c for c in calls if c[0][0].get("type") == "step.skipped"]
            self.assertGreater(len(skipped_events), 0)

    def test_cache_invalidation_on_param_change(self):
        """Test that cache is invalidated when params change."""
        execution_count = [0]

        def test_step(ctx: StepContext, **kwargs) -> StepResult:
            execution_count[0] += 1
            return StepResult()

        with patch("yggdrasil.core.engine.resolve_callable", return_value=test_step):
            plan1 = Plan(
                plan_id="plan_005",
                realm="test",
                scope={},
                steps=[
                    StepSpec(
                        step_id="s1", name="n1", fn_ref="m:f", params={"key": "value1"}
                    )
                ],
            )

            # Run once
            self.engine.run(plan1)
            self.assertEqual(execution_count[0], 1)

            # Run again with different params
            plan2 = Plan(
                plan_id="plan_005",  # Same plan_id
                realm="test",
                scope={},
                steps=[
                    StepSpec(
                        step_id="s1", name="n1", fn_ref="m:f", params={"key": "value2"}
                    )
                ],
            )

            self.engine.run(plan2)

            # Should execute again (not cached)
            self.assertEqual(execution_count[0], 2)

    def test_fingerprint_file_creation(self):
        """Test that success.fingerprint file is created."""

        def test_step(ctx: StepContext, **kwargs) -> StepResult:
            return StepResult()

        with patch("yggdrasil.core.engine.resolve_callable", return_value=test_step):
            plan = Plan(
                plan_id="plan_006",
                realm="test",
                scope={},
                steps=[StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})],
            )

            self.engine.run(plan)

            plan_dir = self.engine._plan_dir(plan)
            step_dir = plan_dir / "s1"
            fp_file = step_dir / "success.fingerprint"

            self.assertTrue(fp_file.exists())
            # Should contain sha256: fingerprint
            content = fp_file.read_text().strip()
            self.assertTrue(content.startswith("sha256:"))

    # =====================================================
    # STEP CONTEXT TESTS
    # =====================================================

    def test_step_receives_correct_context(self):
        """Test that step receives properly configured StepContext."""
        received_context = {}

        def test_step(ctx: StepContext, **kwargs) -> StepResult:
            received_context["realm"] = ctx.realm
            received_context["plan_id"] = ctx.plan_id
            received_context["step_id"] = ctx.step_id
            received_context["step_name"] = ctx.step_name
            received_context["fingerprint"] = ctx.fingerprint
            received_context["workdir"] = ctx.workdir
            return StepResult()

        with patch("yggdrasil.core.engine.resolve_callable", return_value=test_step):
            plan = Plan(
                plan_id="plan_007",
                realm="test_realm",
                scope={"kind": "project"},
                steps=[
                    StepSpec(step_id="s1", name="step_name", fn_ref="m:f", params={})
                ],
            )

            self.engine.run(plan)

            self.assertEqual(received_context["realm"], "test_realm")
            self.assertEqual(received_context["plan_id"], "plan_007")
            self.assertEqual(received_context["step_id"], "s1")
            self.assertEqual(received_context["step_name"], "step_name")
            self.assertIsNotNone(received_context["fingerprint"])
            self.assertIsInstance(received_context["workdir"], Path)

    def test_step_context_emitter(self):
        """Test that step context uses engine's emitter."""
        received_emitter = {}

        def test_step(ctx: StepContext, **kwargs) -> StepResult:
            received_emitter["emitter"] = ctx.emitter
            return StepResult()

        with patch("yggdrasil.core.engine.resolve_callable", return_value=test_step):
            plan = Plan(
                plan_id="plan_008",
                realm="test",
                scope={},
                steps=[StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})],
            )

            self.engine.run(plan)

            self.assertIs(received_emitter["emitter"], self.mock_emitter)

    # =====================================================
    # ERROR HANDLING TESTS
    # =====================================================

    def test_run_handles_invalid_topology(self):
        """Test that run fails gracefully with invalid topology."""
        plan = Plan(
            plan_id="plan_009",
            realm="test",
            scope={},
            steps=[
                StepSpec(
                    step_id="s1",
                    name="n1",
                    fn_ref="m:f",
                    params={},
                    deps=["nonexistent"],
                )
            ],
        )

        with self.assertRaises(ValueError):
            self.engine.run(plan)

    def test_run_logs_warning_for_non_stepresult_return(self):
        """Test that engine logs warning for incorrect return type."""

        def bad_step(ctx: StepContext, **kwargs):
            return "not a StepResult"

        with patch("yggdrasil.core.engine.resolve_callable", return_value=bad_step):
            with patch.object(self.engine, "_logger") as mock_logger:
                plan = Plan(
                    plan_id="plan_010",
                    realm="test",
                    scope={},
                    steps=[StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})],
                )

                self.engine.run(plan)

                # Should log warning
                self.assertTrue(mock_logger.warning.called)

    # =====================================================
    # INTEGRATION TESTS
    # =====================================================

    def test_complete_workflow_execution(self):
        """Test complete workflow with artifacts and metrics."""

        def step1(ctx: StepContext, **kwargs) -> StepResult:
            output = ctx.workdir / "step1_output.txt"
            output.write_text("step1 result", encoding="utf-8")
            return StepResult(
                artifacts=[Artifact(key="output", path=str(output))],
                metrics={"processed": 100},
            )

        def step2(ctx: StepContext, input_path: str, **kwargs) -> StepResult:
            output = ctx.workdir / "step2_output.txt"
            output.write_text("step2 result", encoding="utf-8")
            return StepResult(
                artifacts=[Artifact(key="final_output", path=str(output))],
                metrics={"total": 200},
            )

        with patch("yggdrasil.core.engine.resolve_callable") as mock_resolve:
            # Configure mock to return different functions
            mock_resolve.side_effect = [step1, step2]

            plan_dir = self.work_root / "workflow_plan"
            step1_output = plan_dir / "s1" / "step1_output.txt"

            plan = Plan(
                plan_id="workflow_plan",
                realm="production",
                scope={"kind": "project", "id": "P001"},
                steps=[
                    StepSpec(step_id="s1", name="step1", fn_ref="m:f1", params={}),
                    StepSpec(
                        step_id="s2",
                        name="step2",
                        fn_ref="m:f2",
                        params={"input_path": str(step1_output)},
                        deps=["s1"],
                    ),
                ],
            )

            self.engine.run(plan)

            # Verify both steps created outputs
            self.assertTrue((plan_dir / "s1" / "step1_output.txt").exists())
            self.assertTrue((plan_dir / "s2" / "step2_output.txt").exists())

            # Verify fingerprint files
            self.assertTrue((plan_dir / "s1" / "success.fingerprint").exists())
            self.assertTrue((plan_dir / "s2" / "success.fingerprint").exists())


class TestEngineTypingCoercion(unittest.TestCase):
    """
    Tests for typing parameter coercion in Engine.

    The Engine now calls coerce_params_to_signature_types() before invoking
    step functions, converting string parameters to Path objects where
    the function signature expects Path type.
    """

    def setUp(self):
        """Set up test engine and temporary directory."""
        self.temp_dir = TemporaryDirectory()
        self.work_root = Path(self.temp_dir.name)
        self.mock_emitter = Mock(spec=EventEmitter)
        self.engine = Engine(work_root=self.work_root, emitter=self.mock_emitter)

    def tearDown(self):
        """Clean up temporary directory."""
        self.temp_dir.cleanup()

    def test_string_param_coerced_to_path(self):
        """Test that string parameters are coerced to Path objects."""
        received_params = {}

        def step_fn(ctx: StepContext, input_path: Path, **kwargs) -> StepResult:
            received_params["input_path"] = input_path
            return StepResult(artifacts=[])

        plan = Plan(
            plan_id="test-coerce-1",
            realm="test",
            scope={"kind": "test", "id": "test-1"},
            steps=[
                StepSpec(
                    step_id="coerce_step",
                    name="coerce_step",
                    fn_ref="m:coerce_step",
                    params={"input_path": "/tmp/some/path"},  # String param
                    scope=None,
                    deps=[],
                    inputs={},
                )
            ],
        )

        with patch("yggdrasil.core.engine.resolve_callable", return_value=step_fn):
            self.engine.run(plan)

        # Verify parameter was coerced to Path
        self.assertIsInstance(received_params["input_path"], Path)
        self.assertEqual(str(received_params["input_path"]), "/tmp/some/path")

    def test_non_path_params_unchanged(self):
        """Test that non-Path parameters are not modified."""
        received_params = {}

        def step_fn(ctx: StepContext, count: int, name: str, **kwargs) -> StepResult:
            received_params["count"] = count
            received_params["name"] = name
            return StepResult(artifacts=[])

        plan = Plan(
            plan_id="test-coerce-2",
            realm="test",
            scope={"kind": "test", "id": "test-2"},
            steps=[
                StepSpec(
                    step_id="type_step",
                    name="type_step",
                    fn_ref="m:type_step",
                    params={"count": 42, "name": "test"},
                    scope=None,
                    deps=[],
                    inputs={},
                )
            ],
        )

        with patch("yggdrasil.core.engine.resolve_callable", return_value=step_fn):
            self.engine.run(plan)

        # Verify non-Path params unchanged
        self.assertEqual(received_params["count"], 42)
        self.assertEqual(received_params["name"], "test")

    def test_mixed_params_with_path_coercion(self):
        """Test that mixed parameter types are handled correctly."""
        received_params = {}

        def step_fn(
            ctx: StepContext, input_file: Path, output_dir: Path, count: int, **kwargs
        ) -> StepResult:
            received_params["input_file"] = input_file
            received_params["output_dir"] = output_dir
            received_params["count"] = count
            return StepResult(artifacts=[])

        plan = Plan(
            plan_id="test-coerce-3",
            realm="test",
            scope={"kind": "test", "id": "test-3"},
            steps=[
                StepSpec(
                    step_id="mixed_step",
                    name="mixed_step",
                    fn_ref="m:mixed_step",
                    params={
                        "input_file": "/data/in.txt",
                        "output_dir": "/data/out",
                        "count": 10,
                    },
                    scope=None,
                    deps=[],
                    inputs={},
                )
            ],
        )

        with patch("yggdrasil.core.engine.resolve_callable", return_value=step_fn):
            self.engine.run(plan)

        # Verify Path coercion for paths, unchanged for int
        self.assertIsInstance(received_params["input_file"], Path)
        self.assertIsInstance(received_params["output_dir"], Path)
        self.assertEqual(received_params["count"], 10)

    def test_path_object_param_preserved(self):
        """Test that Path objects in params are preserved."""
        received_params = {}

        def step_fn(ctx: StepContext, file_path: Path, **kwargs) -> StepResult:
            received_params["file_path"] = file_path
            return StepResult(artifacts=[])

        # Create a real Path object
        test_path = Path("/tmp/test_file.txt")

        plan = Plan(
            plan_id="test-coerce-4",
            realm="test",
            scope={"kind": "test", "id": "test-4"},
            steps=[
                StepSpec(
                    step_id="path_obj_step",
                    name="path_obj_step",
                    fn_ref="m:path_obj_step",
                    params={"file_path": test_path},
                    scope=None,
                    deps=[],
                    inputs={},
                )
            ],
        )

        with patch("yggdrasil.core.engine.resolve_callable", return_value=step_fn):
            self.engine.run(plan)

        # Verify Path object preserved
        self.assertIsInstance(received_params["file_path"], Path)
        self.assertEqual(received_params["file_path"], test_path)

    def test_fingerprint_includes_path_params(self):
        """Test that fingerprints include path parameters correctly."""

        def step_fn(ctx: StepContext, input_path: Path, **kwargs) -> StepResult:
            return StepResult(artifacts=[])

        step_fn._input_keys = {"input_path"}

        # First execution
        plan1 = Plan(
            plan_id="test-fp-1",
            realm="test",
            scope={"kind": "test", "id": "test-fp-1"},
            steps=[
                StepSpec(
                    step_id="fp_step",
                    name="fp_step",
                    fn_ref="m:fp_step",
                    params={"input_path": "/data/file1.txt"},
                    scope=None,
                    deps=[],
                    inputs={},
                )
            ],
        )

        with patch("yggdrasil.core.engine.resolve_callable", return_value=step_fn):
            self.engine.run(plan1)

        # Get first fingerprint
        fp_file_1 = self.work_root / "test-fp-1" / "fp_step" / "success.fingerprint"
        fp_1 = fp_file_1.read_text().strip()

        # Second execution with different path
        plan2 = Plan(
            plan_id="test-fp-2",
            realm="test",
            scope={"kind": "test", "id": "test-fp-2"},
            steps=[
                StepSpec(
                    step_id="fp_step",
                    name="fp_step",
                    fn_ref="m:fp_step",
                    params={"input_path": "/data/file2.txt"},
                    scope=None,
                    deps=[],
                    inputs={},
                )
            ],
        )

        with patch("yggdrasil.core.engine.resolve_callable", return_value=step_fn):
            self.engine.run(plan2)

        # Get second fingerprint
        fp_file_2 = self.work_root / "test-fp-2" / "fp_step" / "success.fingerprint"
        fp_2 = fp_file_2.read_text().strip()

        # Different inputs should produce different fingerprints
        self.assertNotEqual(fp_1, fp_2)

    def test_signature_without_annotations_handled_gracefully(self):
        """Test that functions without type annotations work correctly."""
        received_params = {}

        def step_fn(ctx, **kwargs):  # No type annotations
            received_params["params"] = kwargs
            return StepResult(artifacts=[])

        plan = Plan(
            plan_id="test-coerce-5",
            realm="test",
            scope={"kind": "test", "id": "test-5"},
            steps=[
                StepSpec(
                    step_id="no_annot_step",
                    name="no_annot_step",
                    fn_ref="m:no_annot_step",
                    params={"param1": "/some/path", "param2": 42},
                    scope=None,
                    deps=[],
                    inputs={},
                )
            ],
        )

        with patch("yggdrasil.core.engine.resolve_callable", return_value=step_fn):
            self.engine.run(plan)

        # Verify params passed through (no coercion if no annotation)
        self.assertEqual(received_params["params"]["param1"], "/some/path")
        self.assertEqual(received_params["params"]["param2"], 42)

    def test_coercion_with_optional_path_param(self):
        """Test coercion with Optional[Path] parameters."""

        received_params = {}

        def step_fn(
            ctx: StepContext, config_path: Path | None = None, **kwargs
        ) -> StepResult:
            received_params["config_path"] = config_path
            return StepResult(artifacts=[])

        plan = Plan(
            plan_id="test-coerce-6",
            realm="test",
            scope={"kind": "test", "id": "test-6"},
            steps=[
                StepSpec(
                    step_id="opt_path_step",
                    name="opt_path_step",
                    fn_ref="m:opt_path_step",
                    params={"config_path": "/etc/config.yaml"},
                    scope=None,
                    deps=[],
                    inputs={},
                )
            ],
        )

        with patch("yggdrasil.core.engine.resolve_callable", return_value=step_fn):
            self.engine.run(plan)

        # Verify Optional[Path] was coerced
        self.assertIsInstance(received_params["config_path"], Path)


if __name__ == "__main__":
    unittest.main()
