"""Plan-store contract: generations, conditional writes, strict reads, finalization.

Every shared scenario runs unchanged against both backends: SQLite on a real
temporary file, and ``PlanDBManager`` on an in-memory client that enforces
CouchDB's revision rules. Concurrent writers are interleaved deterministically
by running them right after the store under test reads the document, which is
exactly the window a lost update needs. What a backend can answer instead of
the stored document differs (a body stored under another ID on SQLite, any
body CouchDB returns with a success status), so those scenarios are written
per backend, around one shared assertion.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from requests.exceptions import ConnectionError as RequestsConnectionError

from tests.contract.support import ConformingBuilders
from tests.plan_store_support import (
    DRAINED_FAILURE,
    PREFLIGHT_REJECTED,
    SCOPE,
    FakeApiException,
    FakeCouchServer,
    continuation_plan,
    finalization_for,
    make_plan,
    patch_api_exception,
    plan_db_manager_on,
)
from yggdrasil.contract.timestamps import is_canonical
from yggdrasil.storage.errors import PlanStoreError, RevisionConflictError
from yggdrasil.storage.plan_eligibility import is_plan_eligible
from yggdrasil.storage.plan_updates import FinalizationStatus, SupersessionReason
from yggdrasil.storage.sqlite import SQLiteInternalStore, SQLitePlanStore

PLAN_ID = "pln_test_P1_v1"

# Marks a field to remove from a document.
ABSENT = object()


class PlanStoreContract:
    """Scenarios every PlanStore backend must satisfy identically.

    Mixed into one ``unittest.TestCase`` per backend. Subclasses provide the
    store under test as ``self.plans`` and implement the backend hooks below.
    """

    plans: Any

    # ----- backend hooks -----

    def after_next_read(self, action: Callable[[], None]) -> None:
        """Run action once, right after the store's next document read."""
        raise NotImplementedError

    def fail_after_next_write(self) -> None:
        """Apply the next write, then make the backend call fail anyway."""
        raise NotImplementedError

    def fail_next_read(self, *, transient: bool = True) -> None:
        """Make the next document read fail in the backend.

        Args:
            transient: True for a failure another attempt could get past (a
                busy database, an overloaded server); False for one that would
                fail again identically (a refused permission, an unusable
                file).
        """
        raise NotImplementedError

    def raw_put(self, doc: dict[str, Any]) -> None:
        """Store doc as-is, bypassing the plan store (e.g. a legacy document)."""
        raise NotImplementedError

    def write_count(self, doc: dict[str, Any]) -> int:
        """Number of writes the document's revision reflects."""
        raise NotImplementedError

    def raw_stored(self) -> dict[str, Any] | None:
        """The plan document exactly as the backend holds it, ``_rev`` included."""
        raise NotImplementedError

    def plan_seq(self) -> int | None:
        """The head of the plan-change sequence; None where there is none."""
        raise NotImplementedError

    # ----- helpers -----

    def save(self, plan=None, **kwargs: Any) -> dict[str, Any]:
        """Save plan (approved by default) and return the stored document."""
        kwargs.setdefault("auto_run", True)
        plan = plan or make_plan(PLAN_ID)
        self.plans.save_plan(plan, "test_realm", dict(SCOPE), **kwargs)
        return self.plans.fetch_plan(plan.plan_id)

    def external_update(self, **fields: Any) -> None:
        """Change fields the way an approval actor does: conditionally."""
        doc = self.plans.fetch_plan(PLAN_ID)
        doc.update(fields)
        self.raw_conditional_put(doc)

    def raw_conditional_put(self, doc: dict[str, Any]) -> None:
        """Replace doc at the ``_rev`` it carries, bypassing the plan store."""
        raise NotImplementedError

    # ----- plan generation -----

    def test_every_save_assigns_a_fresh_generation(self):
        first = self.save()
        second = self.save()
        self.assertTrue(first["plan_generation"])
        self.assertTrue(second["plan_generation"])
        self.assertNotEqual(first["plan_generation"], second["plan_generation"])

    def test_approval_style_update_keeps_the_generation(self):
        doc = self.save(auto_run=False)
        self.external_update(status="approved", run_token=1)
        self.assertEqual(
            self.plans.fetch_plan(PLAN_ID)["plan_generation"], doc["plan_generation"]
        )

    def test_recreated_plan_does_not_reuse_the_deleted_generation(self):
        doc = self.save()
        self.assertTrue(self.plans.delete_plan(PLAN_ID))
        recreated = self.save()
        self.assertNotEqual(recreated["plan_generation"], doc["plan_generation"])

    # ----- version marker and timestamps -----

    def test_saved_plan_is_family_one_with_canonical_timestamps(self):
        doc = self.save()
        self.assertIs(type(doc["schema_version"]), int)
        self.assertEqual(doc["schema_version"], 1)
        for field in ("created_at", "updated_at"):
            with self.subTest(field=field):
                self.assertTrue(is_canonical(doc[field]), doc[field])

    def test_finalization_keeps_the_marker_and_writes_canonical_timestamps(self):
        doc = self.save()
        self.plans.finalize_execution(finalization_for(doc))

        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(stored["schema_version"], 1)
        written = {
            "updated_at": stored["updated_at"],
            "last_executed_at": stored["last_executed_at"],
            "finalized_at": stored["last_finalized_execution"]["finalized_at"],
        }
        for field, value in written.items():
            with self.subTest(field=field):
                self.assertTrue(is_canonical(value), value)

    # ----- competing save_plan -----

    def test_competing_creation_loser_raises_and_keeps_winner(self):
        self.after_next_read(lambda: self.save(make_plan(PLAN_ID, message="winner")))
        with self.assertRaises(RevisionConflictError):
            self.save(make_plan(PLAN_ID, message="loser"))

        doc = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(doc["plan"]["steps"][0]["params"]["message"], "winner")
        self.assertEqual(self.write_count(doc), 1)

    def test_competing_regeneration_loser_raises_without_replaying(self):
        self.save(make_plan(PLAN_ID, message="original"))
        self.after_next_read(lambda: self.save(make_plan(PLAN_ID, message="winner")))
        with self.assertRaises(RevisionConflictError):
            self.save(make_plan(PLAN_ID, message="loser"))

        doc = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(doc["plan"]["steps"][0]["params"]["message"], "winner")
        # original + winner only: the loser neither overwrote nor retried.
        self.assertEqual(self.write_count(doc), 2)

    def test_stale_regeneration_cannot_overwrite_a_finalized_result(self):
        doc = self.save()
        request = finalization_for(doc)
        finalized = []
        self.after_next_read(
            lambda: finalized.append(self.plans.finalize_execution(request))
        )

        with self.assertRaises(RevisionConflictError):
            self.save(make_plan(PLAN_ID, message="stale regeneration"))

        self.assertEqual(finalized[0].status, FinalizationStatus.COMMITTED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(stored["executed_run_token"], 0)
        self.assertEqual(
            stored["last_finalized_execution"]["execution_id"], request.execution_id
        )

    # ----- finalization: recording -----

    def test_success_records_outcome_and_token_in_one_write(self):
        doc = self.save()
        request = finalization_for(doc)

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        self.assertTrue(result.recorded)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(self.write_count(stored), self.write_count(doc) + 1)
        self.assertEqual(stored["executed_run_token"], 0)
        record = stored["last_finalized_execution"]
        self.assertEqual(
            {key: record[key] for key in request.identity()}, request.identity()
        )
        self.assertEqual(record["report"], request.report)
        self.assertEqual(stored["last_executed_at"], record["finalized_at"])
        self.assertFalse(is_plan_eligible(stored))
        self.assertEqual(
            self.plans.get_plan_summary(PLAN_ID)["last_finalized_outcome"], "succeeded"
        )

    def test_drained_continuation_failure_consumes_request_until_rerun(self):
        doc = self.save(continuation_plan(PLAN_ID))
        result = self.plans.finalize_execution(
            finalization_for(doc, ending=DRAINED_FAILURE)
        )

        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(stored["executed_run_token"], 0)
        self.assertEqual(stored["last_finalized_execution"]["outcome"], "failed")
        self.assertFalse(is_plan_eligible(stored))
        self.assertEqual(stored["status"], "approved")

        # Only an explicit new request makes it eligible again, same generation.
        self.external_update(run_token=1)
        rerun = self.plans.fetch_plan(PLAN_ID)
        self.assertTrue(is_plan_eligible(rerun))
        self.assertEqual(rerun["plan_generation"], doc["plan_generation"])

    def test_preflight_rejection_consumes_request_with_its_diagnostic(self):
        doc = self.save(continuation_plan(PLAN_ID))
        result = self.plans.finalize_execution(
            finalization_for(doc, ending=PREFLIGHT_REJECTED)
        )

        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertFalse(is_plan_eligible(stored))
        record = stored["last_finalized_execution"]
        self.assertEqual(record["termination_reason"], "preflight_rejected")
        self.assertEqual(
            record["report"]["diagnostic"]["message"],
            "dependency cycle: s1 -> s2 -> s1",
        )

    def test_newer_run_token_is_preserved_and_stays_pending(self):
        doc = self.save()
        request = finalization_for(doc)  # captured at run_token 0
        self.external_update(run_token=1)

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(stored["run_token"], 1)
        self.assertEqual(stored["executed_run_token"], 0)
        self.assertTrue(is_plan_eligible(stored))

    # ----- finalization: idempotency -----

    def test_replaying_a_committed_completion_writes_nothing(self):
        request = finalization_for(self.save())
        self.plans.finalize_execution(request)
        committed = self.plans.fetch_plan(PLAN_ID)

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.ALREADY_COMMITTED)
        self.assertTrue(result.recorded)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), committed)

    def test_uncertain_commit_is_resolved_by_calling_again(self):
        request = finalization_for(self.save())
        self.fail_after_next_write()

        with self.assertRaises(PlanStoreError):
            self.plans.finalize_execution(request)
        landed = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(landed["executed_run_token"], 0)

        result = self.plans.finalize_execution(request)
        self.assertEqual(result.status, FinalizationStatus.ALREADY_COMMITTED)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), landed)

    def test_other_attempt_with_equal_generation_token_and_outcome_is_not_mine(self):
        doc = self.save(continuation_plan(PLAN_ID))
        interrupted = finalization_for(doc, ending=DRAINED_FAILURE, execution_id="a")
        retry = finalization_for(doc, ending=DRAINED_FAILURE, execution_id="b")
        self.assertEqual(
            self.plans.finalize_execution(retry).status, FinalizationStatus.COMMITTED
        )
        committed = self.plans.fetch_plan(PLAN_ID)

        result = self.plans.finalize_execution(interrupted)

        self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
        self.assertEqual(result.reason, SupersessionReason.REQUEST_ALREADY_FINALIZED)
        self.assertFalse(result.recorded)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), committed)

    # ----- finalization: supersession -----

    def test_stale_worker_holding_a_fresh_revision_is_rejected_on_generation(self):
        request = finalization_for(self.save())
        regenerated = self.save()  # new generation; tokens reset to the same 0

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), regenerated)

    def test_old_worker_cannot_finalize_a_deleted_and_recreated_plan(self):
        request = finalization_for(self.save())
        self.plans.delete_plan(PLAN_ID)
        missing = self.plans.finalize_execution(request)
        self.assertEqual(missing.reason, SupersessionReason.PLAN_MISSING)

        recreated = self.save()
        result = self.plans.finalize_execution(request)
        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), recreated)

    def test_authority_or_owner_change_supersedes(self):
        request = finalization_for(
            self.save(execution_authority="run_once", execution_owner="run_once:a")
        )
        self.external_update(execution_owner="run_once:b")
        self.assertEqual(
            self.plans.finalize_execution(request).reason,
            SupersessionReason.AUTHORITY_CHANGED,
        )

        self.external_update(execution_authority="daemon", execution_owner=None)
        changed = self.plans.fetch_plan(PLAN_ID)
        result = self.plans.finalize_execution(request)
        self.assertEqual(result.reason, SupersessionReason.AUTHORITY_CHANGED)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), changed)

    def test_executed_token_is_never_lowered(self):
        doc = self.save()
        older = finalization_for(doc, execution_id="older")  # run_token 0
        self.external_update(run_token=1)
        newer = finalization_for(self.plans.fetch_plan(PLAN_ID), execution_id="newer")
        self.assertEqual(
            self.plans.finalize_execution(newer).status, FinalizationStatus.COMMITTED
        )
        committed = self.plans.fetch_plan(PLAN_ID)

        result = self.plans.finalize_execution(older)

        self.assertEqual(result.reason, SupersessionReason.REQUEST_ALREADY_FINALIZED)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), committed)
        self.assertEqual(committed["executed_run_token"], 1)

    # ----- finalization: invalid control state -----

    def assert_refused_without_writing(self, request):
        """Finalize request; assert it was refused and nothing was written.

        Returns:
            FinalizationResult: The refusal.
        """
        before, seq = self.raw_stored(), self.plan_seq()

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.INVALID_CONTROL_STATE)
        self.assertFalse(result.recorded)
        self.assertEqual(self.raw_stored(), before)
        self.assertEqual(self.plan_seq(), seq)
        return result

    def test_invalid_stored_tokens_are_refused_without_writing(self):
        request = finalization_for(self.save())
        self.external_update(executed_run_token="soon")

        result = self.assert_refused_without_writing(request)

        self.assertIn("executed_run_token 'soon' is a str", result.message)

    def test_a_write_that_would_leave_executed_above_run_is_not_made(self):
        self.save()
        self.external_update(run_token=5)
        request = finalization_for(self.plans.fetch_plan(PLAN_ID))  # run_token 5
        self.external_update(run_token=4)  # a valid pair, 4 / -1

        result = self.assert_refused_without_writing(request)

        self.assertIn("run_token 4 / executed_run_token 5", result.message)
        stored = self.raw_stored()
        self.assertEqual((stored["run_token"], stored["executed_run_token"]), (4, -1))
        self.assertNotIn("last_finalized_execution", stored)

    def test_tokens_lowered_between_read_and_write_are_refused_on_the_reread(self):
        self.save()
        self.external_update(run_token=5)
        request = finalization_for(self.plans.fetch_plan(PLAN_ID))
        self.after_next_read(lambda: self.external_update(run_token=4))

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.INVALID_CONTROL_STATE)
        stored = self.raw_stored()
        self.assertEqual((stored["run_token"], stored["executed_run_token"]), (4, -1))
        self.assertNotIn("last_finalized_execution", stored)
        # Nothing landed after the competing write: one save and two updates.
        self.assertEqual(self.write_count(stored), 3)

    def test_legacy_document_without_generation_cannot_be_finalized(self):
        doc = self.save()
        request = finalization_for(doc)
        stripped = {k: v for k, v in doc.items() if k != "plan_generation"}
        self.raw_conditional_put(stripped)

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)

    # ----- finalization: races between read and write -----

    def test_harmless_concurrent_write_returns_retryable_conflict(self):
        request = finalization_for(self.save())
        self.after_next_read(lambda: self.external_update(run_token=1))

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.CONFLICT)
        self.assertFalse(result.recorded)
        raced = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(raced["executed_run_token"], -1)
        self.assertNotIn("last_finalized_execution", raced)

        retried = self.plans.finalize_execution(request)
        self.assertEqual(retried.status, FinalizationStatus.COMMITTED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(stored["executed_run_token"], 0)
        self.assertEqual(stored["run_token"], 1)

    def test_regeneration_between_read_and_write_supersedes(self):
        request = finalization_for(self.save())
        self.after_next_read(lambda: self.save(make_plan(PLAN_ID, message="new")))

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertNotIn("last_finalized_execution", stored)
        self.assertEqual(stored["plan"]["steps"][0]["params"]["message"], "new")

    def test_own_completion_landing_between_read_and_write_is_already_committed(self):
        request = finalization_for(self.save())
        self.after_next_read(lambda: self.plans.finalize_execution(request))

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.ALREADY_COMMITTED)

    def test_deletion_between_read_and_write_supersedes(self):
        request = finalization_for(self.save())
        self.after_next_read(lambda: self.plans.delete_plan(PLAN_ID))

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.reason, SupersessionReason.PLAN_MISSING)
        self.assertIsNone(self.plans.fetch_plan(PLAN_ID))

    def test_backend_failure_says_whether_another_attempt_could_help(self):
        """The retry decision is available without catching backend errors.

        A caller bounded to a few finalization attempts has to tell "the
        database was busy" from "this will never work", and it cannot import
        sqlite3 or CouchDB exception types to do it.
        """
        request = finalization_for(self.save())
        for transient in (True, False):
            with self.subTest(transient=transient):
                self.fail_next_read(transient=transient)

                with self.assertRaises(PlanStoreError) as ctx:
                    self.plans.finalize_execution(request)

                self.assertIsNotNone(ctx.exception.__cause__)
                self.assertEqual(ctx.exception.retryable, transient)

        # The plan is untouched, so a later attempt still finalizes it.
        self.assertEqual(
            self.plans.finalize_execution(request).status,
            FinalizationStatus.COMMITTED,
        )

    # ----- finalization: the family -----

    def test_a_document_of_no_supported_family_is_not_finalized(self):
        request = finalization_for(self.save())
        for marker in (ABSENT, "1", 2):
            label = "absent" if marker is ABSENT else repr(marker)
            with self.subTest(marker=label):
                doc = self.plans.fetch_plan(PLAN_ID)
                if marker is ABSENT:
                    del doc["schema_version"]
                else:
                    doc["schema_version"] = marker
                self.raw_conditional_put(doc)
                before, seq = self.raw_stored(), self.plan_seq()

                result = self.plans.finalize_execution(request)

                self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
                self.assertEqual(result.reason, SupersessionReason.UNSUPPORTED_DOCUMENT)
                self.assertEqual(self.raw_stored(), before)
                self.assertEqual(self.plan_seq(), seq)

    def test_a_marker_changed_between_read_and_write_stops_the_write(self):
        request = finalization_for(self.save())
        self.after_next_read(lambda: self.external_update(schema_version=2))

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
        self.assertEqual(result.reason, SupersessionReason.UNSUPPORTED_DOCUMENT)
        stored = self.raw_stored()
        self.assertEqual(stored["schema_version"], 2)
        self.assertNotIn("last_finalized_execution", stored)
        # Nothing landed after the competing write: one save and one update.
        self.assertEqual(self.write_count(stored), 2)

    # ----- the strict read -----

    def test_the_strict_read_returns_the_document_as_stored(self):
        doc = self.save()
        seq = self.plan_seq()

        self.assertEqual(self.plans.read_plan_document(PLAN_ID), doc)
        self.assertIsNone(self.plans.read_plan_document("pln_missing"))
        self.assertEqual(self.raw_stored(), doc)
        self.assertEqual(self.plan_seq(), seq)

    def test_the_strict_read_gives_no_document_a_generation(self):
        self.save()
        for marker in (ABSENT, 1):
            label = "unversioned" if marker is ABSENT else "family 1"
            with self.subTest(document=label):
                legacy = self.plans.fetch_plan(PLAN_ID)
                legacy.pop("plan_generation", None)
                if marker is ABSENT:
                    del legacy["schema_version"]
                else:
                    legacy["schema_version"] = marker
                self.raw_conditional_put(legacy)
                stored, seq = self.raw_stored(), self.plan_seq()

                read = self.plans.read_plan_document(PLAN_ID)

                self.assertNotIn("plan_generation", read)
                self.assertEqual(read, stored)
                self.assertEqual(self.raw_stored(), stored)
                self.assertEqual(self.plan_seq(), seq)

    def test_a_read_failure_is_a_classified_plan_store_error(self):
        self.save()
        for transient in (True, False):
            with self.subTest(transient=transient):
                self.fail_next_read(transient=transient)

                with self.assertRaises(PlanStoreError) as ctx:
                    self.plans.read_plan_document(PLAN_ID)

                self.assertIsNotNone(ctx.exception.__cause__)
                self.assertEqual(ctx.exception.retryable, transient)

    def assert_not_the_stored_document(self, serve):
        """Assert that reads served by serve are failures, and write nothing.

        Both the strict read and finalization must raise a non-retryable
        PlanStoreError rather than conclude anything from what they read.

        Args:
            serve: Arranges for the store's next read of the plan to answer
                with something other than its stored document.
        """
        request = finalization_for(self.plans.fetch_plan(PLAN_ID))
        before, seq = self.raw_stored(), self.plan_seq()

        for call in (
            lambda: self.plans.read_plan_document(PLAN_ID),
            lambda: self.plans.finalize_execution(request),
        ):
            serve()
            with self.assertRaises(PlanStoreError) as ctx:
                call()
            self.assertFalse(ctx.exception.retryable)
            self.assertIn("not the plan's stored document", str(ctx.exception))

        self.assertEqual(self.raw_stored(), before)
        self.assertEqual(self.plan_seq(), seq)


class TestSQLiteConditionalUpdates(PlanStoreContract, ConformingBuilders):
    """The contract against SQLitePlanStore on a real temporary database."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = SQLiteInternalStore(Path(self._tmp.name) / "ygg.sqlite3")
        self.plans = SQLitePlanStore(self.store)

    def tearDown(self):
        self._tmp.cleanup()

    def after_next_read(self, action):
        real = self.store.get_document

        def read_then_act(namespace, doc_id):
            doc = real(namespace, doc_id)
            self.store.get_document = real  # one-shot
            action()
            return doc

        self.store.get_document = read_then_act

    def fail_after_next_write(self):
        real = self.store.put_document

        def write_then_fail(*args, **kwargs):
            self.store.put_document = real  # one-shot
            real(*args, **kwargs)
            raise sqlite3.OperationalError("disk I/O error")

        self.store.put_document = write_then_fail

    def fail_next_read(self, *, transient=True):
        real = self.store.get_document
        error = (
            sqlite3.OperationalError("database is locked")
            if transient
            else sqlite3.DatabaseError("file is not a database")
        )

        def fail(namespace, doc_id):
            self.store.get_document = real  # one-shot
            raise error

        self.store.get_document = fail

    def raw_put(self, doc):
        self.store.put_document("plans", doc["_id"], doc, bump_plan_seq=True)

    def raw_conditional_put(self, doc):
        self.store.put_document(
            "plans", doc["_id"], doc, bump_plan_seq=True, expected_rev=doc["_rev"]
        )

    def write_count(self, doc):
        return int(doc["_rev"])

    def raw_stored(self):
        return self.store.get_document("plans", PLAN_ID)

    def plan_seq(self):
        return self.store.current_plan_seq()

    # ----- what a read may return -----

    def misfile(self) -> None:
        """Store another plan's body under this plan's key, as given.

        The store supplies ``_rev`` from the row, but the body keeps its own
        ``_id``.
        """
        doc = self.plans.fetch_plan(PLAN_ID)
        doc["_id"] = "pln_other"
        self.store.put_document(
            "plans", PLAN_ID, doc, bump_plan_seq=True, expected_rev=doc["_rev"]
        )

    def test_a_body_stored_under_another_id_is_not_the_document(self):
        self.save()
        self.misfile()
        self.assertEqual(self.raw_stored()["_id"], "pln_other")

        self.assert_not_the_stored_document(lambda: None)

    def test_a_misfiled_body_on_the_conflict_reread_stops_the_write(self):
        request = finalization_for(self.save())
        self.after_next_read(self.misfile)

        with self.assertRaises(PlanStoreError) as ctx:
            self.plans.finalize_execution(request)

        self.assertFalse(ctx.exception.retryable)
        stored = self.raw_stored()
        self.assertEqual(stored["_id"], "pln_other")
        self.assertNotIn("last_finalized_execution", stored)
        # One save and the misfiling write; the finalizer wrote nothing.
        self.assertEqual(self.write_count(stored), 2)


class TestCouchConditionalUpdates(PlanStoreContract, ConformingBuilders):
    """The contract against PlanDBManager on a revision-enforcing fake client."""

    def setUp(self):
        patch_api_exception(self)
        self.server = FakeCouchServer()
        self.plans = plan_db_manager_on(self.server)

    def after_next_read(self, action):
        self.server.after_read.append(action)

    def fail_after_next_write(self):
        self.server.fail_after_next_write = RequestsConnectionError("reset by peer")

    def fail_next_read(self, *, transient=True):
        self.server.fail_next_read = (
            FakeApiException(503, "unavailable")
            if transient
            else FakeApiException(403, "forbidden")
        )

    def raw_put(self, doc):
        body = dict(doc)
        body.pop("_rev", None)
        self.server.put_document(db="yggdrasil_plans", doc_id=doc["_id"], document=body)

    def raw_conditional_put(self, doc):
        self.server.put_document(db="yggdrasil_plans", doc_id=doc["_id"], document=doc)

    def write_count(self, doc):
        return int(doc["_rev"].split("-")[0])

    def raw_stored(self):
        return self.server.raw(PLAN_ID)

    def plan_seq(self):
        return None

    # ----- what a read may return -----

    def answers(self) -> dict[str, dict[str, Any]]:
        """Bodies CouchDB might return with a success status for this plan."""
        stored = self.plans.fetch_plan(PLAN_ID)
        other = {**stored, "_id": "pln_other"}
        unrevised = {k: v for k, v in stored.items() if k != "_rev"}
        return {
            "an object-shaped error": {"error": "proxy failure"},
            "another document's body": other,
            "a body without a revision": unrevised,
        }

    def test_a_success_answer_that_is_not_the_document_is_a_failure(self):
        self.save()
        for case, body in self.answers().items():
            with self.subTest(case=case):
                self.assert_not_the_stored_document(
                    lambda body=body: self.server.read_responses.append(body)
                )

    def test_a_malformed_conflict_reread_stops_the_write(self):
        request = finalization_for(self.save())
        for case, body in self.answers().items():
            with self.subTest(case=case):

                def rerun_then_answer(body: dict[str, Any] = body) -> None:
                    self.external_update(run_token=self.raw_stored()["run_token"] + 1)
                    self.server.read_responses.append(body)

                self.after_next_read(rerun_then_answer)
                updated_writes = self.write_count(self.raw_stored()) + 1

                with self.assertRaises(PlanStoreError) as ctx:
                    self.plans.finalize_execution(request)

                self.assertFalse(ctx.exception.retryable)
                stored = self.raw_stored()
                self.assertNotIn("last_finalized_execution", stored)
                # Only the competing write landed; the finalizer wrote nothing.
                self.assertEqual(self.write_count(stored), updated_writes)


if __name__ == "__main__":
    unittest.main()
