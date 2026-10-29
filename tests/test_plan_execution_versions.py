"""The executor's version boundary, through the coordinator, on both backends.

The coordinator executes and finalizes only plan documents of a family this
release supports. It reads a plan with a plain fetch that writes nothing and
accepts only the plan's stored document: a body whose ``_id`` is not the
plan's, or whose ``_rev`` is not a non-empty string, is a storage failure
(ADMISSION_FAILED), never a missing or unversioned document, and leaves a
pending result pending.

The family is checked first, on every read a cycle decides from: before a
pending result is settled and before admission. A document whose
``schema_version`` is absent, invalid or names an unsupported family is not
executed (UNSUPPORTED_DOCUMENT), and a pending result for it is dropped with a
warning, never recorded or retried. The family is captured at admission, and
the finalizer writes only into a document still of that family. A supported
document without a generation is refused, and nothing gives it one. Every
refusal leaves the stored document as it was, and other plans in the same
store keep running.
"""

from __future__ import annotations

import json
import logging
import unittest
from collections.abc import Callable
from typing import Any

from tests.execution_support import (
    COORDINATOR_LOGGER,
    PLAN_ID,
    REALM,
    SCOPE,
    ControlTestCase,
    CouchBackend,
    SQLiteBackend,
    chain_plan,
)
from yggdrasil.daemon.plan_execution import (
    DAEMON_CLAIM,
    ExecutionClaim,
    ExecutionResult,
    ExecutionStatus,
)
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    attempt_dir,
    record_filename,
)
from yggdrasil.storage.errors import PlanStoreError
from yggdrasil.storage.plan_documents import build_plan_document
from yggdrasil.storage.plan_eligibility import is_plan_eligible
from yggdrasil.storage.plan_updates import SupersessionReason

# Stands for a field removed from the stored document.
ABSENT = object()

# Markers the executor refuses: each with how a refusal shows it and the
# version state it names.
REFUSED_MARKERS: dict[str, tuple[Any, str, str]] = {
    "absent": (ABSENT, "absent", "unversioned"),
    "a string": ("1", "'1'", "invalid"),
    "a float": (1.0, "1.0", "invalid"),
    "a boolean": (True, "True", "invalid"),
    "null": (None, "None", "invalid"),
    "zero": (0, "0", "invalid"),
    "negative": (-1, "-1", "invalid"),
    "a newer family": (2, "2", "unsupported"),
}

# How a refusal names the families this release executes.
SUPPORTED = "(supported plan-document families: [1])"

# How a read that is not the plan's stored document is reported.
NOT_THE_DOCUMENT = "not the plan's stored document"


def _marked(marker: Any) -> Callable[[dict[str, Any]], None]:
    """An edit that sets a plan document's schema_version, or removes it."""

    def edit(doc: dict[str, Any]) -> None:
        if marker is ABSENT:
            doc.pop("schema_version", None)
        else:
            doc["schema_version"] = marker

    return edit


class VersionTestCase(ControlTestCase):
    """A coordinator over a real spool, with ways to change what a read returns.

    Subclasses choose the backend with ``backend_type``.
    """

    def remark(self, plan_id: str, marker: Any) -> None:
        """Change the stored plan's schema_version, as another writer could."""
        doc = self.stored(plan_id)
        _marked(marker)(doc)
        self.backend.conditional_put(doc)

    def pending_result(self, plan_id: str) -> ExecutionResult:
        """Run a valid plan whose result cannot be recorded, keeping it pending.

        Args:
            plan_id: The plan to save and run.

        Returns:
            ExecutionResult: The FINALIZATION_PENDING result.
        """
        self.save(chain_plan(plan_id=plan_id))
        self.store.fail_finalize(PlanStoreError("permission denied"), plan_id=plan_id)
        result = self.execute(plan_id)
        self.assertEqual(
            result.status, ExecutionStatus.FINALIZATION_PENDING, result.message
        )
        pending = self.coordinator.pending_finalization(plan_id)
        assert pending is not None, "no pending finalization"
        # The family the request was admitted under travels with it.
        self.assertEqual(pending.schema_version, 1)
        return result

    def published_report(self, result: ExecutionResult) -> dict[str, Any]:
        """Return the report that result's attempt published to the spool."""
        report = result.report
        assert report is not None, "no attempt ran"
        directory = attempt_dir(
            self.sandbox / "spool", REALM, result.plan_doc_id, report.execution_id
        )
        record = directory / record_filename(ATTEMPT_REPORT_EVENT)
        return json.loads(record.read_text("utf-8"))["report"]

    # ----- reads that are not the plan's stored document -----

    def malformed_answers(self) -> dict[str, Callable[[str], None]]:
        """Ways to make a plan's next read return something else than its document.

        On SQLite the store supplies ``_rev`` itself, so what it can return is
        another plan's body stored under the plan's key, which stays until
        :meth:`repair`. CouchDB can answer any body with a success status; each
        is returned once.

        Returns:
            dict[str, Callable[[str], None]]: Each arrangement, taking the plan
            ID, by a description of what the read returns.
        """
        if isinstance(self.backend, SQLiteBackend):
            return {"a body stored under another _id": self.misfile}
        answer = self.backend.server.read_responses.append

        def stored(plan_id: str) -> dict[str, Any]:
            doc = self.raw(plan_id)
            assert doc is not None, f"plan {plan_id} is not stored"
            return doc

        return {
            "an object-shaped error": lambda plan_id: answer(
                {"error": "proxy failure"}
            ),
            "another document's body": lambda plan_id: answer(
                {**stored(plan_id), "_id": "pln_other"}
            ),
            "a body without a revision": lambda plan_id: answer(
                {k: v for k, v in stored(plan_id).items() if k != "_rev"}
            ),
        }

    def misfile(self, plan_id: str) -> None:
        """Store another plan's body under plan_id's key, on SQLite."""
        assert isinstance(self.backend, SQLiteBackend)
        doc = self.raw(plan_id)
        assert doc is not None, f"plan {plan_id} is not stored"
        doc["_id"] = "pln_other"
        self.backend.internal.put_document(
            "plans", plan_id, doc, bump_plan_seq=True, expected_rev=doc["_rev"]
        )

    def repair(self, plan_id: str) -> None:
        """Make plan_id's reads return its stored document again."""
        if isinstance(self.backend, CouchBackend):
            self.backend.server.read_responses.clear()
            return
        doc = self.raw(plan_id)
        assert doc is not None, f"plan {plan_id} is not stored"
        if doc["_id"] != plan_id:
            doc["_id"] = plan_id
            self.backend.conditional_put(doc)

    def after_next_read(self, action: Callable[[], None]) -> None:
        """Run action once, right after the backend's next document read."""
        if isinstance(self.backend, CouchBackend):
            self.backend.server.after_read.append(action)
            return
        internal = self.backend.internal
        real = internal.get_document

        def read_then_act(namespace: str, doc_id: str) -> dict[str, Any] | None:
            internal.get_document = real  # type: ignore[method-assign]
            doc = real(namespace, doc_id)
            action()
            return doc

        internal.get_document = read_then_act  # type: ignore[method-assign]


class VersionScenarios:
    """The version boundary, against one backend.

    Mixed into one VersionTestCase per plan-store backend.
    """

    # ----- what is executed -----

    def test_only_a_supported_family_is_executed(self):
        for index, (case, (marker, shown, state)) in enumerate(REFUSED_MARKERS.items()):
            plan_id = f"{PLAN_ID}_marker_{index}"
            with self.subTest(case=case):
                self.edited(plan_id, _marked(marker))

                self.assert_refused(
                    plan_id,
                    ExecutionStatus.UNSUPPORTED_DOCUMENT,
                    f"Plan '{plan_id}' is not executed",
                    f"its schema_version is {shown}, so it is {state}",
                    SUPPORTED,
                )
                # Eligibility does not look at the family: the boundary does.
                self.assertTrue(is_plan_eligible(self.stored(plan_id)))
        # A family-1 plan in the same store runs.
        self.assert_a_valid_plan_still_runs()

    def test_an_unversioned_plan_is_refused_with_a_warning(self):
        self.edited(PLAN_ID, _marked(ABSENT))

        with self.assertLogs(COORDINATOR_LOGGER, level=logging.WARNING) as logs:
            result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.UNSUPPORTED_DOCUMENT)
        (record,) = [r for r in logs.records if r.getMessage() == result.message]
        self.assertEqual(record.levelno, logging.WARNING)
        for text in (f"'{PLAN_ID}'", "absent", "unversioned", "[1]"):
            with self.subTest(text=text):
                self.assertIn(text, result.message)

    def test_an_unsupported_plan_without_a_generation_is_given_none(self):
        def edit(doc: dict[str, Any]) -> None:
            doc["schema_version"] = 2
            del doc["plan_generation"]

        self.edited(PLAN_ID, edit)

        self.assert_refused(
            PLAN_ID,
            ExecutionStatus.UNSUPPORTED_DOCUMENT,
            "its schema_version is 2, so it is unsupported",
        )
        self.assertNotIn("plan_generation", self.raw())
        self.assert_a_valid_plan_still_runs()

    def test_a_supported_plan_without_a_generation_is_refused_and_given_none(self):
        self.edited(PLAN_ID, lambda doc: doc.pop("plan_generation"))

        self.assert_refused(
            PLAN_ID, ExecutionStatus.INVALID_DOCUMENT, "has no plan_generation"
        )
        self.assertNotIn("plan_generation", self.raw())
        self.assert_a_valid_plan_still_runs()

    def test_the_family_is_checked_before_anything_else(self):
        session = ExecutionClaim(authority="run_once", owner="run_once:session")

        def unversioned_with_invalid_tokens(doc: dict[str, Any]) -> None:
            del doc["schema_version"]
            doc["run_token"] = "5"

        def newer_and_not_approved(doc: dict[str, Any]) -> None:
            doc.update(schema_version=2, status="draft")

        def newer_without_steps(doc: dict[str, Any]) -> None:
            doc["schema_version"] = 2
            del doc["plan"]["steps"]

        def invalid_under_another_authority(doc: dict[str, Any]) -> None:
            doc.update(schema_version="1", execution_authority="run_once")

        def newer_of_another_owner(doc: dict[str, Any]) -> None:
            doc.update(
                schema_version=2,
                execution_authority="run_once",
                execution_owner="run_once:other",
            )

        # Each edit, the claim it is executed under, and what the plan would
        # resolve to if its family were supported.
        cases: dict[str, tuple[Callable[[dict[str, Any]], None], ExecutionClaim]] = {
            "not eligible": (unversioned_with_invalid_tokens, DAEMON_CLAIM),
            "a draft": (newer_and_not_approved, DAEMON_CLAIM),
            "an invalid document": (newer_without_steps, DAEMON_CLAIM),
            "the daemon not authorized": (
                invalid_under_another_authority,
                DAEMON_CLAIM,
            ),
            "a session not authorized": (newer_of_another_owner, session),
        }
        for index, (case, (edit, claim)) in enumerate(cases.items()):
            plan_id = f"{PLAN_ID}_first_{index}"
            with self.subTest(otherwise=case):
                self.edited(plan_id, edit)

                self.assert_refused(
                    plan_id, ExecutionStatus.UNSUPPORTED_DOCUMENT, claim=claim
                )
        self.assert_a_valid_plan_still_runs()

    # ----- pending results -----

    def test_a_pending_result_is_dropped_when_its_plan_leaves_the_family(self):
        cases = {
            "a newer family": (2, "2", "unsupported"),
            "unversioned": (ABSENT, "absent", "unversioned"),
        }
        for index, (case, (marker, shown, state)) in enumerate(cases.items()):
            plan_id = f"{PLAN_ID}_pending_{index}"
            with self.subTest(case=case):
                pending = self.pending_result(plan_id)
                execution_id = pending.report.execution_id
                self.remark(plan_id, marker)
                del self.journal[:]

                with (
                    self.nothing_changes(plan_id),
                    self.assertLogs(COORDINATOR_LOGGER, level=logging.WARNING) as logs,
                ):
                    result = self.execute(plan_id)

                self.assertEqual(
                    result.status, ExecutionStatus.UNSUPPORTED_DOCUMENT, result.message
                )
                self.assertIsNone(self.coordinator.pending_finalization(plan_id))
                # Read once; the result was neither retried nor recorded.
                self.assertEqual(self.journal, ["read"])
                (dropped,) = [line for line in logs.output if "Dropping" in line]
                for text in (
                    f"execution '{execution_id}'",
                    f"plan '{plan_id}' is now {shown}, so it is {state}",
                    SUPPORTED,
                    "not recorded and is not retried",
                    "report remains in the event spool",
                ):
                    self.assertIn(text, dropped)
                self.assertEqual(
                    self.published_report(pending), pending.report.to_dict()
                )

                # Nothing is pending any more: a repeated request is refused
                # the same way, with no second warning.
                with self.assertLogs(
                    COORDINATOR_LOGGER, level=logging.WARNING
                ) as again:
                    repeated = self.execute(plan_id)
                self.assertEqual(repeated.status, ExecutionStatus.UNSUPPORTED_DOCUMENT)
                self.assertFalse(any("Dropping" in line for line in again.output))
        self.assert_a_valid_plan_still_runs()

    def test_nothing_is_admitted_when_the_plan_leaves_the_family_while_settling(
        self,
    ):
        pending = self.pending_result(PLAN_ID)
        self.request_rerun()
        left: dict[str, Any] = {}

        def leave_the_family(doc_id):
            self.remark(PLAN_ID, 2)
            left.update(doc=self.raw(), seq=self.plan_seq())

        # The first read finds family 1; the reread after settling, family 2.
        self.store.read_actions.extend([lambda doc_id: None, leave_the_family])
        del self.journal[:]

        result = self.execute()

        self.assertEqual(
            result.status, ExecutionStatus.UNSUPPORTED_DOCUMENT, result.message
        )
        # The pending result was recorded while the plan was family 1; the
        # newer request was not admitted.
        self.assertEqual(self.journal, ["read", "finalize:0", "read"])
        self.assertIsNone(self.coordinator.pending_finalization(PLAN_ID))
        self.assertEqual(len(self.engine.contexts), 1)
        stored = self.raw()
        self.assertEqual(stored, left["doc"])
        self.assertEqual(self.plan_seq(), left["seq"])
        self.assertEqual(
            stored["last_finalized_execution"]["execution_id"],
            pending.report.execution_id,
        )
        self.assertEqual((stored["run_token"], stored["executed_run_token"]), (1, 0))
        self.assert_a_valid_plan_still_runs()

    def test_a_result_is_not_recorded_into_a_plan_regenerated_as_another_family(
        self,
    ):
        self.save()
        regenerated: dict[str, Any] = {}

        def regenerate_as_family_2():
            newer = build_plan_document(chain_plan(), REALM, dict(SCOPE), auto_run=True)
            newer.update(schema_version=2, _rev=self.stored()["_rev"])
            self.backend.conditional_put(newer)
            regenerated.update(doc=self.raw(), seq=self.plan_seq())

        result = self.finish_after(regenerate_as_family_2)

        self.assertEqual(result.status, ExecutionStatus.SUPERSEDED, result.message)
        self.assertEqual(
            result.finalization.reason, SupersessionReason.UNSUPPORTED_DOCUMENT
        )
        self.assertIn("schema_version is now 2 (unsupported)", result.message)
        self.assertIsNone(self.coordinator.pending_finalization(PLAN_ID))
        self.assertEqual(self.raw(), regenerated["doc"])
        self.assertEqual(self.plan_seq(), regenerated["seq"])
        # The attempt's report was still published.
        self.assertEqual(self.published_report(result), result.report.to_dict())
        self.assert_a_valid_plan_still_runs()

    # ----- reads that are not the plan's stored document -----

    def test_a_malformed_read_admits_nothing_and_keeps_a_pending_result(self):
        pending = self.pending_result(PLAN_ID)
        request = self.coordinator.pending_finalization(PLAN_ID)
        idle = chain_plan(plan_id=f"{PLAN_ID}_idle")
        self.save(idle)

        for case, serve in self.malformed_answers().items():
            for plan_id in (PLAN_ID, idle.plan_id):
                with self.subTest(case=case, pending=plan_id == PLAN_ID):
                    serve(plan_id)
                    with (
                        self.nothing_changes(plan_id),
                        self.assertLogs(COORDINATOR_LOGGER, level=logging.ERROR),
                    ):
                        result = self.execute(plan_id)
                    self.repair(plan_id)

                    self.assertEqual(
                        result.status, ExecutionStatus.ADMISSION_FAILED, result.message
                    )
                    self.assertIn(NOT_THE_DOCUMENT, result.message)
                    self.assertEqual(
                        self.coordinator.pending_finalization(PLAN_ID), request
                    )
                    self.assertIsNone(
                        self.coordinator.pending_finalization(idle.plan_id)
                    )

        # The kept result is recorded once the reads are intact again, and a
        # newer request then runs.
        self.request_rerun()
        result = self.execute()
        self.assertEqual(result.status, ExecutionStatus.FINALIZED, result.message)
        recorded = [f for f in self.store.finalizations if f.recorded]
        self.assertIn(pending.report.execution_id, recorded[0].message)
        self.assert_recorded(result, run_token=1)
        self.assert_a_valid_plan_still_runs()

    def test_a_malformed_finalization_read_keeps_the_result_pending(self):
        def on_the_first_read(plan_id, serve):
            serve(plan_id)

        def on_the_conflict_reread(plan_id, serve):
            def rerun_then_serve():
                self.request_rerun(plan_id)
                serve(plan_id)

            self.after_next_read(rerun_then_serve)

        index = 0
        for read, arrange in (
            ("the first read", on_the_first_read),
            ("the conflict reread", on_the_conflict_reread),
        ):
            for case, serve in self.malformed_answers().items():
                index += 1
                plan_id = f"{PLAN_ID}_finalized_{index}"
                with self.subTest(read=read, case=case):
                    self.save(chain_plan(plan_id=plan_id))
                    finalizations = self.store.calls["finalize"]

                    result = self.finish_after(lambda: arrange(plan_id, serve), plan_id)
                    served = self.raw(plan_id)

                    self.assertEqual(
                        result.status,
                        ExecutionStatus.FINALIZATION_PENDING,
                        result.message,
                    )
                    self.assertIsNotNone(self.coordinator.pending_finalization(plan_id))
                    # One attempt: the failure is not worth retrying.
                    self.assertEqual(self.store.calls["finalize"], finalizations + 1)
                    self.assertEqual(self.delays, [])
                    self.assertNotIn("last_finalized_execution", served)
                    self.assertEqual(served["executed_run_token"], -1)
                    self.repair(plan_id)
        self.assert_a_valid_plan_still_runs()


class TestVersionsSQLite(VersionScenarios, VersionTestCase):
    """The version boundary against the SQLite plan store."""


class TestVersionsCouch(VersionScenarios, VersionTestCase):
    """The version boundary against the CouchDB plan store."""

    backend_type = CouchBackend


if __name__ == "__main__":
    unittest.main()
