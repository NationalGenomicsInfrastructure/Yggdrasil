"""
Regression and functional tests for lib/realms/test_realm/steps.py
and lib/realms/test_realm/handler.py.

- Decorator tests: every step is a proper @step-decorated callable.
- Functional tests: new write/denial steps behave correctly with mocked DataAccess.
- Handler tests: _do_plan_time_fetch uses connection() (not couchdb()) and awaits get().
"""

import unittest
from unittest.mock import AsyncMock, MagicMock

from yggdrasil.flow.utils.callable_ref import resolve_callable

_FN_REF_PREFIX = "lib.realms.test_realm.steps"

# All fn_ref names referenced from test_realm recipes / custom step parsing.
_ALL_STEP_NAMES = [
    "step_echo",
    "step_sleep",
    "step_fail",
    "step_write_file",
    "step_random_fail",
    "step_fetch_from_db",
    "step_expect_denied",
    "step_write_to_db",
    "step_expect_read_denied",
    "step_exercise_all_fetch_methods",
    "step_verify_limit_clamping",
    "step_emit_metadata",
]


class TestTestRealmStepsAreDecorated(unittest.TestCase):
    """
    Ensure every test realm step resolved via fn_ref carries the _step_name
    attribute that the @step decorator sets. If a step loses its decorator,
    hasattr(fn, '_step_name') will be False and this test will catch it.
    """

    def _fn_ref(self, name: str) -> str:
        return f"{_FN_REF_PREFIX}.{name}"

    def test_all_steps_are_callable(self):
        """resolve_callable must return a callable for every registered step."""
        for name in _ALL_STEP_NAMES:
            with self.subTest(step=name):
                fn = resolve_callable(self._fn_ref(name))
                self.assertTrue(
                    callable(fn),
                    f"resolve_callable('{name}') did not return a callable",
                )

    def test_all_steps_have_step_name_attribute(self):
        """
        Every step must carry _step_name — the attribute set by @step.
        If this fails, the function is a plain def that will never emit
        step.started / step.succeeded / step.failed.
        """
        for name in _ALL_STEP_NAMES:
            with self.subTest(step=name):
                fn = resolve_callable(self._fn_ref(name))
                self.assertTrue(
                    hasattr(fn, "_step_name"),
                    f"Step '{name}' is missing _step_name — did you forget @step?",
                )

    def test_step_name_attribute_matches_function_name(self):
        """_step_name should match the bare function name (decorator default)."""
        for name in _ALL_STEP_NAMES:
            with self.subTest(step=name):
                fn = resolve_callable(self._fn_ref(name))
                if hasattr(fn, "_step_name"):
                    self.assertEqual(
                        fn._step_name,
                        name,
                        f"Step '{name}' has _step_name={fn._step_name!r}, expected {name!r}",
                    )


# ---------------------------------------------------------------------------
# Functional: step_write_to_db
# ---------------------------------------------------------------------------


class TestStepWriteToDb(unittest.TestCase):
    """Functional tests for step_write_to_db."""

    def setUp(self):
        from lib.realms.test_realm.steps import step_write_to_db
        from yggdrasil.flow.data_access.models import DataAccessWriteResult

        self.step_fn = step_write_to_db
        self.WriteResult = DataAccessWriteResult

    def _make_write_result(self, status="created", old_rev=None, new_rev="1-abc"):
        return self.WriteResult(
            backend="couchdb",
            connection_name="test_realm_write_db",
            resource="yggdrasil",
            operation="upsert",
            doc_id="data_access_test:write_result",
            status=status,
            old_rev=old_rev,
            new_rev=new_rev,
        )

    def _make_ctx(self, write_result):
        ctx = MagicMock()
        ctx.data.connection.return_value.put.return_value = write_result
        return ctx

    def test_put_called_with_clean_body(self):
        """Body passed to put() must not contain _id or _rev."""
        result = self._make_write_result()
        ctx = self._make_ctx(result)

        self.step_fn(ctx)

        put_call = ctx.data.connection.return_value.put.call_args
        body = put_call[0][1]  # second positional argument
        self.assertNotIn("_id", body)
        self.assertNotIn("_rev", body)

    def test_put_called_with_correct_doc_id_and_mode(self):
        result = self._make_write_result()
        ctx = self._make_ctx(result)

        self.step_fn(ctx, doc_id="custom:doc", mode="create")

        put_call = ctx.data.connection.return_value.put.call_args
        self.assertEqual(put_call[0][0], "custom:doc")
        self.assertEqual(put_call[1]["mode"], "create")

    def test_metrics_include_write_result_fields_on_create(self):
        result = self._make_write_result(
            status="created", old_rev=None, new_rev="1-abc"
        )
        ctx = self._make_ctx(result)

        step_result = self.step_fn(ctx)

        self.assertEqual(step_result.metrics["write_status"], "created")
        self.assertEqual(step_result.metrics["doc_id"], "data_access_test:write_result")
        self.assertIsNone(step_result.metrics["old_rev"])
        self.assertEqual(step_result.metrics["new_rev"], "1-abc")

    def test_metrics_include_write_result_fields_on_update(self):
        result = self._make_write_result(
            status="updated", old_rev="1-abc", new_rev="2-def"
        )
        ctx = self._make_ctx(result)

        step_result = self.step_fn(ctx)

        self.assertEqual(step_result.metrics["write_status"], "updated")
        self.assertEqual(step_result.metrics["old_rev"], "1-abc")
        self.assertEqual(step_result.metrics["new_rev"], "2-def")

    def test_raises_if_ctx_data_is_none(self):
        ctx = MagicMock()
        ctx.data = None
        with self.assertRaises(RuntimeError):
            self.step_fn(ctx)


# ---------------------------------------------------------------------------
# Functional: step_expect_read_denied
# ---------------------------------------------------------------------------


class TestStepExpectReadDenied(unittest.TestCase):
    """Functional tests for step_expect_read_denied."""

    def setUp(self):
        from lib.realms.test_realm.steps import step_expect_read_denied
        from yggdrasil.flow.data_access import DataAccessDeniedError

        self.step_fn = step_expect_read_denied
        self.DeniedError = DataAccessDeniedError

    def test_succeeds_when_get_is_denied(self):
        """Step must return StepResult when get() raises DataAccessDeniedError."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get.side_effect = self.DeniedError(
            "realm has no read permission"
        )

        result = self.step_fn(ctx)

        self.assertTrue(result.metrics["read_correctly_denied"])
        self.assertIn("denial_reason", result.metrics)

    def test_metrics_contain_connection_name(self):
        ctx = MagicMock()
        ctx.data.connection.return_value.get.side_effect = self.DeniedError("no read")

        result = self.step_fn(ctx, connection="test_realm_write_only_db")

        self.assertEqual(result.metrics["connection"], "test_realm_write_only_db")

    def test_fails_hard_if_read_succeeds(self):
        """Step must raise RuntimeError when get() unexpectedly returns a doc."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get.return_value = {"_id": "some_doc"}

        with self.assertRaises(RuntimeError) as cm:
            self.step_fn(ctx)

        self.assertIn("but read succeeded", str(cm.exception))

    def test_connection_is_called_before_get(self):
        """connection() must be called first (write permission allows it)."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get.side_effect = self.DeniedError("no read")

        self.step_fn(ctx, connection="test_realm_write_only_db")

        ctx.data.connection.assert_called_once_with("test_realm_write_only_db")

    def test_raises_if_ctx_data_is_none(self):
        ctx = MagicMock()
        ctx.data = None
        with self.assertRaises(RuntimeError):
            self.step_fn(ctx)


# ---------------------------------------------------------------------------
# Handler: _do_plan_time_fetch uses connection() and awaits get()
# ---------------------------------------------------------------------------


class TestHandlerPlanTimeFetch(unittest.IsolatedAsyncioTestCase):
    """Tests for TestRealmHandler._do_plan_time_fetch."""

    def setUp(self):
        from lib.realms.test_realm.handler import TestRealmHandler

        self.handler = TestRealmHandler()

    async def test_uses_connection_not_couchdb(self):
        """_do_plan_time_fetch must call ctx.data.connection(), not ctx.data.couchdb()."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get = AsyncMock(return_value=None)

        await self.handler._do_plan_time_fetch(ctx)

        ctx.data.connection.assert_called_once_with("yggdrasil_db")
        ctx.data.couchdb.assert_not_called()

    async def test_returns_missing_true_when_doc_absent(self):
        """get() returning None must produce {"doc_id": ..., "missing": True}."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get = AsyncMock(return_value=None)

        result = await self.handler._do_plan_time_fetch(ctx)

        self.assertTrue(result.get("missing"))
        self.assertNotIn("error", result)

    async def test_returns_doc_fields_when_doc_present(self):
        """get() returning a doc must produce structured dict with message and value."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get = AsyncMock(
            return_value={
                "_id": "data_access_test:reference_doc",
                "message": "hello",
                "value": 42,
            }
        )

        result = await self.handler._do_plan_time_fetch(ctx)

        self.assertEqual(result["message"], "hello")
        self.assertEqual(result["value"], 42)
        self.assertFalse(result.get("missing"))
        self.assertNotIn("error", result)

    async def test_get_is_awaited(self):
        """get() must be awaited (planning client returns a coroutine)."""
        ctx = MagicMock()
        async_get = AsyncMock(return_value={"_id": "x", "message": "m", "value": 1})
        ctx.data.connection.return_value.get = async_get

        await self.handler._do_plan_time_fetch(ctx)

        async_get.assert_awaited_once()

    async def test_returns_error_dict_on_data_access_error(self):
        """DataAccessError during get() must be caught and returned as an error dict."""
        from yggdrasil.flow.data_access import DataAccessDeniedError

        ctx = MagicMock()
        ctx.data.connection.return_value.get = AsyncMock(
            side_effect=DataAccessDeniedError("no read permission")
        )

        result = await self.handler._do_plan_time_fetch(ctx)

        self.assertIn("error", result)
        self.assertEqual(result["error_type"], "DataAccessDeniedError")
        self.assertNotIn("missing", result)


# ---------------------------------------------------------------------------
# data_fetch_plan_steps: internal helper used by handler Mode 1
# ---------------------------------------------------------------------------


class TestDataFetchPlanSteps(unittest.TestCase):
    """Unit tests for data_fetch_plan_steps (internal helper called by handler Mode 1).

    Covers the three ref_dict shapes the handler can produce:
    success, doc-absent (missing), and DataAccessError.
    """

    def setUp(self):
        from lib.realms.test_realm.recipes import data_fetch_plan_steps

        self.helper = data_fetch_plan_steps

    def test_returns_two_steps(self):
        steps = self.helper(
            {"doc_id": "x", "missing": False, "message": "hi", "value": 1}
        )
        self.assertEqual(len(steps), 2)
        self.assertEqual(steps[0].step_id, "echo_fetched")
        self.assertEqual(steps[1].step_id, "echo_confirm")

    def test_echo_fetched_uses_step_emit_metadata(self):
        steps = self.helper(
            {"doc_id": "x", "missing": False, "message": "hi", "value": 1}
        )
        self.assertIn("step_emit_metadata", steps[0].fn_ref)

    def test_success_case_bakes_ref_doc_into_params(self):
        ref_dict = {"doc_id": "x", "message": "hi", "value": 7, "missing": False}
        steps = self.helper(ref_dict)
        self.assertEqual(steps[0].params["ref_doc"], ref_dict)

    def test_missing_case_confirm_message_mentions_not_found(self):
        steps = self.helper({"doc_id": "x", "missing": True})
        self.assertIn("not found", steps[1].params["message"])

    def test_error_case_confirm_message_mentions_failed(self):
        steps = self.helper(
            {"doc_id": "x", "error": "denied", "error_type": "DataAccessDeniedError"}
        )
        self.assertIn("failed", steps[1].params["message"])

    def test_data_fetch_plan_not_in_recipes_registry(self):
        from lib.realms.test_realm.recipes import RECIPES

        self.assertNotIn("data_fetch_plan", RECIPES)
