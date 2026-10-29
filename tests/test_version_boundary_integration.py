"""The version boundary end to end, through the daemon's and run-once's callers.

A scenario document is planned by the test realm and the plan executed the
way the daemon and run-once execute it (see ``tests/integration_support.py``),
on both internal-storage backends. A family-1 plan runs. A plan whose stored
``schema_version`` is absent, invalid or names a newer family is not executed
and not finalized, its document is left exactly as it was, and the log names
the plan and what it records. Planning then replaces the unversioned plan with
a family-1 generation, which runs, and refuses to replace the other two.
"""

from __future__ import annotations

import logging
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from tests.execution_support import directory_tree, run_bounded
from tests.integration_support import (
    PLAN_ID,
    CoreTestCase,
    CouchStorage,
    SQLiteStorage,
    scenario,
)
from yggdrasil.daemon.plan_execution import ExecutionStatus

# Stands for a field removed from the stored document.
ABSENT = object()

# A scenario the test realm plans as a family-1 plan whose steps all succeed
# at once.
SUCCEEDING = scenario("happy_path", overrides={"brief_sleep": {"duration_sec": 0}})

# The run-once session that owns the plan in the run-once scenarios.
SESSION = "run_once:session"

# Stored markers that are not a supported family: how the executor's refusal
# shows each, and the version state it names.
UNVERSIONED = (ABSENT, "absent", "unversioned")
INVALID = ("1", "'1'", "invalid")
NEWER = (2, "2", "unsupported")


class BoundaryTestCase(CoreTestCase):
    """A core with ways to take its plan out of the contract and check the result.

    Subclasses choose the internal-storage backend with ``storage_type``.
    """

    def remark(self, marker: Any, **fields: Any) -> None:
        """Change the stored plan's schema_version, and fields, as a writer could.

        Args:
            marker: The new schema_version, or ABSENT to remove it.
            **fields: Other fields to set.
        """
        doc = self.stored()
        if marker is ABSENT:
            del doc["schema_version"]
        else:
            doc["schema_version"] = marker
        doc.update(fields)
        self.storage.conditional_put(doc)

    @contextmanager
    def unchanged(self) -> Iterator[None]:
        """Assert the block leaves the plan as stored and publishes nothing.

        Yields:
            None: For the block that must change nothing.
        """
        before, seq = self.stored(), self.storage.plan_seq()
        spool = directory_tree(self.spool)
        yield
        self.assertEqual(self.stored(), before)
        self.assertEqual(self.storage.plan_seq(), seq)
        self.assertEqual(directory_tree(self.spool), spool)

    def assert_named(self, lines: list[str], *texts: str) -> None:
        """Assert one logged line names the plan and each of texts."""
        named = [
            line
            for line in lines
            if f"'{PLAN_ID}'" in line and all(text in line for text in texts)
        ]
        self.assertTrue(named, lines)

    def assert_not_executed_as_daemon(self, shown: str, state: str) -> None:
        """Hand the daemon the plan's event; assert it is refused, unchanged.

        Args:
            shown: How the refusal shows the stored schema_version.
            state: The version state the refusal names.
        """
        with (
            self.unchanged(),
            self.assertLogs("yggdrasil", level=logging.WARNING) as logs,
        ):
            result = self.execute_as_daemon()

        self.assertEqual(
            result.status, ExecutionStatus.UNSUPPORTED_DOCUMENT, result.message
        )
        self.assertNotIn("last_finalized_execution", self.stored())
        self.assert_named(
            logs.output, f"its schema_version is {shown}, so it is {state}", "[1]"
        )

    def assert_planning_refuses(self, shown: str, reason: str) -> None:
        """Plan the scenario again; assert planning leaves the slot as it is.

        Args:
            shown: How the planning refusal shows the stored schema_version.
            reason: The reason it gives.
        """
        with (
            self.unchanged(),
            self.assertLogs("yggdrasil", level=logging.ERROR) as logs,
        ):
            self.plan(SUCCEEDING)

        self.assert_named(logs.output, f"its schema_version {shown} is {reason}")

    def run_once_session(self) -> int:
        """Run a run-once session for the stored plan, as ``SESSION``.

        Returns:
            int: The session's exit code.
        """
        return run_bounded(
            self.core._run_once_watcher_loop(
                pending_plan_ids=[PLAN_ID],
                execution_owner=SESSION,
                timeout_seconds=10,
            )
        )


class BoundaryScenarios:
    """The version boundary through the real callers, against one backend.

    Mixed into one BoundaryTestCase per internal-storage backend.
    """

    # ----- the daemon -----

    def test_a_family_one_plan_runs(self):
        doc = self.plan(SUCCEEDING)
        self.assertEqual(doc["schema_version"], 1)

        result = self.execute_as_daemon()

        self.assertEqual(result.status, ExecutionStatus.FINALIZED, result.message)
        self.assertTrue(result.succeeded)
        stored = self.stored()
        self.assertEqual(stored["executed_run_token"], 0)
        self.assertEqual(stored["schema_version"], 1)

    def test_an_unversioned_plan_runs_only_once_planning_replaces_it(self):
        original = self.plan(SUCCEEDING)
        self.remark(ABSENT)
        _, shown, state = UNVERSIONED

        self.assert_not_executed_as_daemon(shown, state)

        regenerated = self.plan(SUCCEEDING)
        self.assertEqual(regenerated["schema_version"], 1)
        self.assertNotEqual(regenerated["plan_generation"], original["plan_generation"])
        self.assertEqual(regenerated["created_at"], original["created_at"])
        result = self.execute_as_daemon()
        self.assertEqual(result.status, ExecutionStatus.FINALIZED, result.message)
        self.assertEqual(self.stored()["executed_run_token"], 0)

    def test_an_invalid_or_newer_plan_is_neither_executed_nor_replaced(self):
        self.plan(SUCCEEDING)
        for marker, shown, state in (INVALID, NEWER):
            with self.subTest(marker=shown):
                self.remark(marker)

                self.assert_not_executed_as_daemon(shown, state)
                # Planning refuses for the same reason the executor does.
                self.assert_planning_refuses(shown, state)

    # ----- run-once -----

    def test_run_once_runs_a_family_one_plan(self):
        exit_code = self.run_once(SUCCEEDING)

        self.assertEqual(exit_code, 0)
        stored = self.stored()
        self.assertEqual(stored["schema_version"], 1)
        self.assertEqual(stored["execution_authority"], "run_once")
        self.assertEqual(stored["executed_run_token"], 0)

    def test_run_once_does_not_execute_a_plan_of_no_supported_family(self):
        self.plan(SUCCEEDING)
        for marker, shown, state in (UNVERSIONED, INVALID, NEWER):
            with self.subTest(marker=shown):
                self.remark(
                    marker, execution_authority="run_once", execution_owner=SESSION
                )

                with (
                    self.unchanged(),
                    self.assertLogs("yggdrasil", level=logging.WARNING) as logs,
                ):
                    exit_code = self.run_once_session()

                self.assertEqual(exit_code, 1)
                self.assertNotIn("last_finalized_execution", self.stored())
                self.assert_named(
                    logs.output, f"its schema_version is {shown}, so it is {state}"
                )
                # The next marker is written over this one.
                if marker is ABSENT:
                    self.remark(1)

    def test_run_once_replaces_an_unversioned_plan_and_runs_it(self):
        original = self.plan(SUCCEEDING)
        self.remark(ABSENT)

        exit_code = self.run_once(SUCCEEDING, force_overwrite=True)

        self.assertEqual(exit_code, 0)
        stored = self.stored()
        self.assertEqual(stored["schema_version"], 1)
        self.assertNotEqual(stored["plan_generation"], original["plan_generation"])
        self.assertEqual(stored["execution_authority"], "run_once")
        self.assertEqual(stored["executed_run_token"], 0)

    def test_run_once_leaves_an_invalid_plan_as_it_is(self):
        self.plan(SUCCEEDING)
        marker, shown, _ = INVALID
        self.remark(marker)

        with (
            self.unchanged(),
            self.assertLogs("yggdrasil", level=logging.ERROR) as logs,
        ):
            exit_code = self.run_once(SUCCEEDING, force_overwrite=True)

        self.assertEqual(exit_code, 1)
        self.assert_named(logs.output, f"its schema_version {shown} is invalid")

    def test_run_once_leaves_a_newer_plan_as_it_is(self):
        self.plan(SUCCEEDING)
        marker, shown, _ = NEWER
        self.remark(marker)

        with (
            self.unchanged(),
            self.assertLogs("yggdrasil", level=logging.ERROR) as logs,
        ):
            exit_code = self.run_once(SUCCEEDING, force_overwrite=True)

        self.assertEqual(exit_code, 1)
        self.assert_named(logs.output, f"its schema_version {shown} is unsupported")


class TestBoundarySQLite(BoundaryScenarios, BoundaryTestCase):
    storage_type = SQLiteStorage


class TestBoundaryCouchDB(BoundaryScenarios, BoundaryTestCase):
    storage_type = CouchStorage


if __name__ == "__main__":
    unittest.main()
