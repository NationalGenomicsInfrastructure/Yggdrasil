import unittest
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory

from yggdrasil.flow.model import Plan
from yggdrasil.flow.planner.builder import PlanBuilder, _role_key, _validate_step_id


class TestUtilityFunctions(unittest.TestCase):
    """
    Comprehensive tests for builder utility functions.

    Tests helper functions for validation and key normalization.
    """

    # =====================================================
    # STEP ID VALIDATION TESTS
    # =====================================================

    def test_validate_step_id_valid_simple(self):
        """Test validation with simple valid step IDs."""
        valid_ids = ["step1", "Step2", "my_step", "step-name", "s", "Step_1-test"]

        for step_id in valid_ids:
            # Should not raise
            _validate_step_id(step_id)

    def test_validate_step_id_must_start_with_letter(self):
        """Test that step_id must start with a letter."""
        invalid_ids = ["1step", "_step", "-step", "123"]

        for step_id in invalid_ids:
            with self.assertRaises(ValueError) as context:
                _validate_step_id(step_id)
            self.assertIn("must match", str(context.exception))

    def test_validate_step_id_allows_letters_numbers_underscore_dash(self):
        """Test that step_id allows letters, numbers, underscore, dash."""
        valid_ids = [
            "a1",
            "Step_123",
            "my-step-name",
            "Step_1_2_3",
            "step-with-dashes",
            "MixedCase123",
        ]

        for step_id in valid_ids:
            _validate_step_id(step_id)

    def test_validate_step_id_rejects_empty_string(self):
        """Test that empty string is rejected."""
        with self.assertRaises(ValueError):
            _validate_step_id("")

    def test_validate_step_id_rejects_special_characters(self):
        """Test that special characters are rejected."""
        invalid_ids = [
            "step.name",
            "step name",
            "step@name",
            "step#1",
            "step!",
            "step$",
        ]

        for step_id in invalid_ids:
            with self.assertRaises(ValueError):
                _validate_step_id(step_id)

    # =====================================================
    # ROLE KEY NORMALIZATION TESTS
    # =====================================================

    def test_role_key_with_string(self):
        """Test _role_key with string input."""
        self.assertEqual(_role_key("input"), "input")
        self.assertEqual(_role_key("my_role"), "my_role")
        self.assertEqual(_role_key(""), "")

    def test_role_key_with_enum(self):
        """Test _role_key with Enum input."""

        class Role(Enum):
            INPUT = "input_data"
            OUTPUT = "output_data"

        self.assertEqual(_role_key(Role.INPUT), "input_data")
        self.assertEqual(_role_key(Role.OUTPUT), "output_data")

    def test_role_key_with_different_enum_types(self):
        """Test _role_key with various Enum types."""

        class FileRole(Enum):
            RAW = "raw_data"
            PROCESSED = "processed_data"

        class Status(Enum):
            READY = "ready"
            DONE = "done"

        self.assertEqual(_role_key(FileRole.RAW), "raw_data")
        self.assertEqual(_role_key(Status.READY), "ready")


class TestPlanBuilder(unittest.TestCase):
    """
    Comprehensive tests for PlanBuilder class.

    Tests the plan builder including path management, role wiring,
    step addition, and dependency resolution.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = TemporaryDirectory()
        self.base = Path(self.temp_dir.name) / "work"
        self.builder = PlanBuilder(
            plan_id="test_plan_001",
            realm="test",
            scope={"kind": "project", "id": "P123"},
            base=self.base,
        )

    def tearDown(self):
        """Clean up temporary resources."""
        self.temp_dir.cleanup()

    # =====================================================
    # INITIALIZATION TESTS
    # =====================================================

    def test_plan_builder_initialization(self):
        """Test PlanBuilder initialization."""
        builder = PlanBuilder(
            plan_id="plan_001",
            realm="production",
            scope={"kind": "project", "id": "P456"},
            base=Path("/tmp/work"),
        )

        self.assertEqual(builder.plan_id, "plan_001")
        self.assertEqual(builder.realm, "production")
        self.assertEqual(builder.scope["id"], "P456")
        self.assertEqual(builder.base, Path("/tmp/work"))
        self.assertEqual(builder.steps, [])
        self.assertEqual(builder._role_provider, {})
        self.assertEqual(builder._role_path, {})

    def test_plan_builder_is_dataclass(self):
        """Test that PlanBuilder is a dataclass."""
        self.assertTrue(hasattr(self.builder, "__dataclass_fields__"))

    # =====================================================
    # PATH HELPER TESTS
    # =====================================================

    def test_dir_for_creates_directory(self):
        """Test that dir_for creates directory."""
        role_dir = self.builder.dir_for("input")

        self.assertTrue(role_dir.exists())
        self.assertTrue(role_dir.is_dir())
        self.assertEqual(role_dir, self.base / "input")

    def test_dir_for_multiple_roles(self):
        """Test creating directories for multiple roles."""
        roles = ["input", "output", "temp", "logs"]

        for role in roles:
            role_dir = self.builder.dir_for(role)
            self.assertTrue(role_dir.exists())
            self.assertEqual(role_dir.parent, self.base)

    def test_dir_for_idempotent(self):
        """Test that dir_for is idempotent."""
        dir1 = self.builder.dir_for("test_role")
        dir2 = self.builder.dir_for("test_role")

        self.assertEqual(dir1, dir2)
        self.assertTrue(dir1.exists())

    def test_file_for_returns_path(self):
        """Test that file_for returns file path."""
        file_path = self.builder.file_for("data", "input.txt")

        expected = self.base / "data" / "input.txt"
        self.assertEqual(file_path, expected)
        # Directory should be created
        self.assertTrue(file_path.parent.exists())

    def test_file_for_multiple_files(self):
        """Test creating paths for multiple files in same role."""
        files = ["file1.txt", "file2.csv", "file3.json"]

        for filename in files:
            file_path = self.builder.file_for("output", filename)
            self.assertEqual(file_path.parent, self.base / "output")
            self.assertEqual(file_path.name, filename)

    def test_path_with_filename(self):
        """Test path helper with filename."""
        result = self.builder.path("data", "file.txt")

        self.assertEqual(result, self.base / "data" / "file.txt")
        self.assertTrue(result.parent.exists())

    def test_path_without_filename(self):
        """Test path helper without filename (directory)."""
        result = self.builder.path("logs")

        self.assertEqual(result, self.base / "logs")
        self.assertTrue(result.exists())
        self.assertTrue(result.is_dir())

    def test_path_with_none_filename(self):
        """Test path helper with explicit None filename."""
        result = self.builder.path("config", None)

        self.assertEqual(result, self.base / "config")
        self.assertTrue(result.is_dir())

    # =====================================================
    # ROLE REGISTRATION TESTS
    # =====================================================

    def test_provide_registers_role(self):
        """Test that provide registers role mapping."""
        self.builder.provide("input_data", "/path/to/input", by_step_id="step1")

        self.assertEqual(self.builder._role_provider["input_data"], "step1")
        self.assertEqual(self.builder._role_path["input_data"], "/path/to/input")

    def test_provide_multiple_roles(self):
        """Test registering multiple roles."""
        self.builder.provide("role1", "/path1", by_step_id="step1")
        self.builder.provide("role2", "/path2", by_step_id="step2")
        self.builder.provide("role3", "/path3", by_step_id="step1")

        self.assertEqual(self.builder._role_provider["role1"], "step1")
        self.assertEqual(self.builder._role_provider["role2"], "step2")
        self.assertEqual(self.builder._role_path["role2"], "/path2")

    def test_provide_with_enum_role(self):
        """Test provide with Enum role."""

        class DataRole(Enum):
            INPUT = "input_data"

        self.builder.provide(DataRole.INPUT, "/path", by_step_id="s1")  # type: ignore

        self.assertEqual(self.builder._role_provider["input_data"], "s1")

    def test_require_returns_registered_path(self):
        """Test that require returns registered path."""
        self.builder.provide("my_input", "/data/input.txt", by_step_id="s1")

        path = self.builder.require("my_input")

        self.assertEqual(path, "/data/input.txt")

    def test_require_raises_on_missing_role(self):
        """Test that require raises KeyError for unregistered role."""
        with self.assertRaises(KeyError) as context:
            self.builder.require("nonexistent_role")

        self.assertIn("not yet provided", str(context.exception))

    def test_require_with_enum_role(self):
        """Test require with Enum role."""

        class DataRole(Enum):
            OUTPUT = "output_data"

        self.builder.provide(DataRole.OUTPUT, "/out/data", by_step_id="s1")  # type: ignore

        path = self.builder.require(DataRole.OUTPUT)  # type: ignore

        self.assertEqual(path, "/out/data")

    # =====================================================
    # STEP ADDITION TESTS
    # =====================================================

    def test_add_step_fn_simple(self):
        """Test adding a step with a function."""

        def my_function():
            pass

        spec = self.builder.add_step_fn(
            my_function, step_id="step1", params={"key": "value"}
        )

        self.assertEqual(spec.step_id, "step1")
        self.assertEqual(spec.name, "my_function")
        self.assertEqual(spec.params["key"], "value")
        self.assertEqual(len(self.builder.steps), 1)

    def test_add_step_fn_generates_default_id(self):
        """Test that add_step_fn generates default step ID."""

        def process_data():
            pass

        spec = self.builder.add_step_fn(process_data, params={})

        # Default ID includes function name and project ID
        self.assertIn("process_data", spec.step_id)
        self.assertIn("P123", spec.step_id)
        self.assertIn("v1", spec.step_id)

    def test_add_step_fn_with_custom_version(self):
        """Test add_step_fn with custom version."""

        def my_func():
            pass

        spec = self.builder.add_step_fn(my_func, params={}, version="v2")

        self.assertIn("v2", spec.step_id)

    def test_add_step_fn_validates_step_id(self):
        """Test that invalid step IDs are rejected."""

        def test_func():
            pass

        with self.assertRaises(ValueError):
            self.builder.add_step_fn(test_func, step_id="123invalid", params={})

    def test_add_step_fn_with_inputs(self):
        """Test adding step with inputs."""

        def analyze():
            pass

        self.builder.provide("raw_data", "/data/raw.csv", by_step_id="prep")

        spec = self.builder.add_step_fn(
            analyze,
            step_id="analyze_step",
            params={},
            inputs={"raw_data": self.builder.require("raw_data")},
        )

        self.assertIn("raw_data", spec.inputs)
        self.assertEqual(spec.inputs["raw_data"], "/data/raw.csv")

    def test_add_step_fn_with_provides(self):
        """Test adding step that provides roles."""

        def generate_data():
            pass

        output_path = str(self.builder.file_for("output", "result.txt"))

        spec = self.builder.add_step_fn(
            generate_data,
            step_id="gen_step",
            params={},
            provides={"output_data": output_path},
        )

        # Role should be registered
        self.assertEqual(self.builder._role_provider["output_data"], "gen_step")
        self.assertEqual(self.builder._role_path["output_data"], output_path)

    def test_add_step_fn_with_requires_roles(self):
        """Test adding step with explicit role requirements."""

        def step1():
            pass

        def step2():
            pass

        # Step 1 provides a role
        self.builder.add_step_fn(
            step1,
            step_id="s1",
            params={},
            provides={"intermediate": "/tmp/inter"},
        )

        # Step 2 requires that role
        spec2 = self.builder.add_step_fn(
            step2,
            step_id="s2",
            params={},
            requires_roles=["intermediate"],
        )

        # Should have dependency on s1
        self.assertIn("s1", spec2.deps)

    def test_add_step_fn_automatic_dependency_from_inputs(self):
        """Test that dependencies are inferred from inputs."""

        def prep():
            pass

        def analyze():
            pass

        # Prep provides data
        self.builder.add_step_fn(
            prep,
            step_id="prep_step",
            params={},
            provides={"data": "/data/prep.csv"},
        )

        # Analyze uses that data
        spec = self.builder.add_step_fn(
            analyze,
            step_id="analyze_step",
            params={},
            inputs={"data": self.builder.require("data")},
        )

        # Should automatically depend on prep_step
        self.assertIn("prep_step", spec.deps)

    def test_add_step_fn_fails_on_missing_required_role(self):
        """Test that adding step fails if required role doesn't exist."""

        def my_step():
            pass

        with self.assertRaises(KeyError) as context:
            self.builder.add_step_fn(
                my_step,
                step_id="step1",
                params={},
                requires_roles=["nonexistent_role"],
            )

        self.assertIn("no provider", str(context.exception))

    def test_add_step_fn_multiple_dependencies(self):
        """Test step with multiple dependencies."""

        def step1():
            pass

        def step2():
            pass

        def step3():
            pass

        # Create two providers
        self.builder.add_step_fn(
            step1, step_id="s1", params={}, provides={"role1": "/path1"}
        )
        self.builder.add_step_fn(
            step2, step_id="s2", params={}, provides={"role2": "/path2"}
        )

        # Step 3 depends on both
        spec = self.builder.add_step_fn(
            step3,
            step_id="s3",
            params={},
            requires_roles=["role1", "role2"],
        )

        self.assertIn("s1", spec.deps)
        self.assertIn("s2", spec.deps)
        self.assertEqual(len(spec.deps), 2)

    def test_add_step_fn_no_self_dependency(self):
        """Test that step doesn't depend on itself even when providing and requiring same role."""

        def step1():
            pass

        def step2():
            pass

        # Step 1 provides a role
        self.builder.add_step_fn(
            step1,
            step_id="s1",
            params={},
            provides={"shared": "/path1"},
        )

        # Step 2 provides same role and requires it (from s1)
        spec = self.builder.add_step_fn(
            step2,
            step_id="s2",
            params={},
            requires_roles=["shared"],
            provides={"shared": "/path2"},  # Overrides the role
        )

        # Should depend on s1, not itself
        self.assertIn("s1", spec.deps)
        self.assertNotIn("s2", spec.deps)

    def test_add_step_fn_with_enum_roles(self):
        """Test adding steps with Enum-based roles."""

        class Role(Enum):
            RAW = "raw_data"
            PROCESSED = "processed_data"

        def generate():
            pass

        def process():
            pass

        # First step provides raw data
        self.builder.add_step_fn(
            generate,
            step_id="generate_step",
            params={},
            provides={Role.RAW.value: "/input"},
        )

        # Second step processes it
        spec = self.builder.add_step_fn(
            process,
            step_id="process_step",
            params={},
            inputs={Role.RAW.value: self.builder.require(Role.RAW.value)},
            provides={Role.PROCESSED.value: "/output"},
        )

        self.assertIn("raw_data", spec.inputs)
        self.assertEqual(self.builder._role_provider["processed_data"], "process_step")

    # =====================================================
    # PLAN FINALIZATION TESTS
    # =====================================================

    def test_to_plan_creates_plan(self):
        """Test that to_plan creates Plan object."""

        def step1():
            pass

        self.builder.add_step_fn(step1, step_id="s1", params={})

        plan = self.builder.to_plan()

        self.assertIsInstance(plan, Plan)
        self.assertEqual(plan.plan_id, "test_plan_001")
        self.assertEqual(plan.realm, "test")
        self.assertEqual(plan.scope["id"], "P123")
        self.assertEqual(len(plan.steps), 1)

    def test_to_plan_with_empty_steps(self):
        """Test to_plan with no steps."""
        plan = self.builder.to_plan()

        self.assertEqual(len(plan.steps), 0)
        self.assertEqual(plan.plan_id, "test_plan_001")

    def test_to_plan_preserves_step_order(self):
        """Test that to_plan preserves step order."""

        def step1():
            pass

        def step2():
            pass

        def step3():
            pass

        self.builder.add_step_fn(step1, step_id="s1", params={})
        self.builder.add_step_fn(step2, step_id="s2", params={})
        self.builder.add_step_fn(step3, step_id="s3", params={})

        plan = self.builder.to_plan()

        self.assertEqual(plan.steps[0].step_id, "s1")
        self.assertEqual(plan.steps[1].step_id, "s2")
        self.assertEqual(plan.steps[2].step_id, "s3")

    def test_to_plan_with_dependencies(self):
        """Test to_plan with dependent steps."""

        def prep():
            pass

        def analyze():
            pass

        self.builder.add_step_fn(
            prep, step_id="prep", params={}, provides={"data": "/data"}
        )
        self.builder.add_step_fn(
            analyze,
            step_id="analyze",
            params={},
            requires_roles=["data"],
        )

        plan = self.builder.to_plan()

        # Analyze step should depend on prep
        analyze_step = next(s for s in plan.steps if s.step_id == "analyze")
        self.assertIn("prep", analyze_step.deps)

    # =====================================================
    # INTEGRATION TESTS
    # =====================================================

    def test_complete_workflow_linear(self):
        """Test building a complete linear workflow."""

        def download_data():
            pass

        def preprocess():
            pass

        def analyze():
            pass

        # Build linear pipeline
        raw_path = str(self.builder.file_for("raw", "data.csv"))
        clean_path = str(self.builder.file_for("clean", "data.csv"))
        results_path = str(self.builder.file_for("results", "output.json"))

        self.builder.add_step_fn(
            download_data,
            step_id="download",
            params={"url": "http://example.com/data"},
            provides={"raw_data": raw_path},
        )

        self.builder.add_step_fn(
            preprocess,
            step_id="preprocess",
            params={},
            inputs={"raw_data": self.builder.require("raw_data")},
            provides={"clean_data": clean_path},
        )

        self.builder.add_step_fn(
            analyze,
            step_id="analyze",
            params={},
            inputs={"clean_data": self.builder.require("clean_data")},
            provides={"results": results_path},
        )

        plan = self.builder.to_plan()

        # Verify structure
        self.assertEqual(len(plan.steps), 3)

        # Verify dependencies
        preprocess_step = next(s for s in plan.steps if s.step_id == "preprocess")
        self.assertEqual(preprocess_step.deps, ["download"])

        analyze_step = next(s for s in plan.steps if s.step_id == "analyze")
        self.assertEqual(analyze_step.deps, ["preprocess"])

    def test_complete_workflow_branching(self):
        """Test building a workflow with branching."""

        def fetch():
            pass

        def process_a():
            pass

        def process_b():
            pass

        def merge():
            pass

        # Build branching pipeline
        data_path = str(self.builder.file_for("input", "data.csv"))
        result_a_path = str(self.builder.file_for("branch_a", "result.json"))
        result_b_path = str(self.builder.file_for("branch_b", "result.json"))
        merged_path = str(self.builder.file_for("output", "merged.json"))

        self.builder.add_step_fn(
            fetch,
            step_id="fetch",
            params={},
            provides={"data": data_path},
        )

        self.builder.add_step_fn(
            process_a,
            step_id="process_a",
            params={},
            requires_roles=["data"],
            provides={"result_a": result_a_path},
        )

        self.builder.add_step_fn(
            process_b,
            step_id="process_b",
            params={},
            requires_roles=["data"],
            provides={"result_b": result_b_path},
        )

        self.builder.add_step_fn(
            merge,
            step_id="merge",
            params={},
            requires_roles=["result_a", "result_b"],
            provides={"final": merged_path},
        )

        plan = self.builder.to_plan()

        # Both process steps depend on fetch
        process_a_step = next(s for s in plan.steps if s.step_id == "process_a")
        process_b_step = next(s for s in plan.steps if s.step_id == "process_b")
        self.assertIn("fetch", process_a_step.deps)
        self.assertIn("fetch", process_b_step.deps)

        # Merge depends on both process steps
        merge_step = next(s for s in plan.steps if s.step_id == "merge")
        self.assertIn("process_a", merge_step.deps)
        self.assertIn("process_b", merge_step.deps)

    def test_workflow_with_path_helpers(self):
        """Test using path helpers for file management."""

        def step1():
            pass

        def step2():
            pass

        # Use path helpers
        input_dir = self.builder.path("input")
        input_file = self.builder.path("input", "data.txt")
        output_file = self.builder.path("output", "result.txt")

        self.builder.add_step_fn(
            step1,
            step_id="s1",
            params={"input_dir": str(input_dir)},
            provides={"intermediate": str(input_file)},
        )

        self.builder.add_step_fn(
            step2,
            step_id="s2",
            params={"output": str(output_file)},
            requires_roles=["intermediate"],
        )

        plan = self.builder.to_plan()

        # Verify paths were created
        self.assertTrue(input_dir.exists())
        self.assertTrue(output_file.parent.exists())

        # Verify plan structure
        self.assertEqual(len(plan.steps), 2)
        self.assertIn("s1", plan.steps[1].deps)

    def test_builder_reusability(self):
        """Test that builder can generate multiple plans."""

        def step1():
            pass

        self.builder.add_step_fn(step1, step_id="s1", params={})

        plan1 = self.builder.to_plan()
        plan2 = self.builder.to_plan()

        # Both should be valid but separate objects
        self.assertEqual(plan1.plan_id, plan2.plan_id)
        self.assertEqual(len(plan1.steps), len(plan2.steps))
        self.assertIsNot(plan1, plan2)


class TestPlanBuilderEdgeCases(unittest.TestCase):
    """
    Tests for edge cases and error conditions in PlanBuilder.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = TemporaryDirectory()
        self.base = Path(self.temp_dir.name) / "work"
        self.builder = PlanBuilder(
            plan_id="edge_case_plan",
            realm="test",
            scope={},
            base=self.base,
        )

    def tearDown(self):
        """Clean up temporary resources."""
        self.temp_dir.cleanup()

    def test_circular_dependency_detection_implicit(self):
        """Test that circular dependencies can be detected."""

        def step_a():
            pass

        def step_b():
            pass

        # Create a situation that could lead to circular deps
        # (though the current implementation prevents self-deps)
        self.builder.add_step_fn(
            step_a, step_id="a", params={}, provides={"role_a": "/a"}
        )

        self.builder.add_step_fn(
            step_b,
            step_id="b",
            params={},
            requires_roles=["role_a"],
            provides={"role_b": "/b"},
        )

        # This should work (no actual cycle)
        plan = self.builder.to_plan()
        self.assertEqual(len(plan.steps), 2)

    def test_role_override(self):
        """Test that providing same role twice updates the provider."""

        def step1():
            pass

        def step2():
            pass

        self.builder.provide("shared_role", "/path1", by_step_id="s1")
        self.builder.provide("shared_role", "/path2", by_step_id="s2")

        # Latest provider wins
        self.assertEqual(self.builder._role_provider["shared_role"], "s2")
        self.assertEqual(self.builder._role_path["shared_role"], "/path2")

    def test_empty_params(self):
        """Test adding step with empty params."""

        def simple_step():
            pass

        spec = self.builder.add_step_fn(simple_step, step_id="s1", params={})

        self.assertEqual(spec.params, {})

    def test_complex_params(self):
        """Test adding step with complex params."""

        def complex_step():
            pass

        params = {
            "simple": "value",
            "number": 42,
            "list": [1, 2, 3],
            "dict": {"nested": "data"},
            "path": str(self.builder.path("data", "file.txt")),
        }

        spec = self.builder.add_step_fn(complex_step, step_id="s1", params=params)

        self.assertEqual(spec.params["simple"], "value")
        self.assertEqual(spec.params["number"], 42)
        self.assertEqual(len(spec.params["list"]), 3)
        self.assertIn("nested", spec.params["dict"])


if __name__ == "__main__":
    unittest.main()
