"""Planning replaces only the plan documents it may.

Planning replaces the whole document in a plan's slot. It may replace an
empty slot, an unversioned (pre-contract) document or a document of a
supported family. It refuses an invalid ``schema_version``, an unsupported
family and a ``created_at`` the replacement cannot keep, and a refusal writes
nothing: the stored document, its revision and the plan-change sequence stay
as they were. Covered here:

- the rule on documents built by ``build_plan_document``;
- ``save_plan`` on both backends: SQLite on a real temporary file, and
  ``PlanDBManager`` on an in-memory client that enforces CouchDB's revision
  rules;
- the planning callers of ``YggdrasilCore`` (daemon planning,
  ``--plan-only`` and run-once) on both backends: each logs a refusal once
  and goes on with its other drafts;
- the check ``--plan-only`` and run-once make before they write, on both
  backends: any document already in the slot, readable or not, is shown
  and kept unless ``--force`` is given, and with it the rule above decides.
  On CouchDB, a malformed answer to the read is shown like an unreadable
  document, never taken for an empty slot, and a failed read writes
  nothing.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import unittest
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

from requests.exceptions import ConnectionError as RequestsConnectionError

from tests.contract.support import ConformingBuilders
from tests.execution_support import CouchBackend, SQLiteBackend
from tests.plan_store_support import SCOPE, FakeApiException, make_plan
from yggdrasil.daemon.core import YggdrasilCore
from yggdrasil.daemon.singleton import SingletonMeta
from yggdrasil.flow.model import Plan
from yggdrasil.flow.planner.api import PlanDraft
from yggdrasil.storage import (
    PlanStoreError,
    RevisionConflictError,
    UnsupportedDocumentError,
)
from yggdrasil.storage.plan_documents import build_plan_document
from yggdrasil.storage.protocols import InternalStorageBundle
from yggdrasil.watchers.events import EventType

PLAN_ID = "pln_test_P1_v1"
REALM = "test_realm"

# A canonical creation time no build in these tests can produce, and the same
# instant as earlier releases spelled it.
CREATED = "2026-01-01T00:00:00Z"
LEGACY_CREATED = "2026-01-01T00:00:00+00:00"

# Marks a field to remove from a builder-made document.
ABSENT = object()

# An integer Python refuses to convert to text: it has more digits than
# sys.get_int_max_str_digits() allows by default.
HUGE = 10**5000

# Markers planning refuses, with the reason it gives.
REFUSED_MARKERS = (
    (2, "unsupported"),
    ("1", "invalid"),
    (1.0, "invalid"),
    (True, "invalid"),
    (None, "invalid"),
    (0, "invalid"),
    (-1, "invalid"),
)

# created_at values no replacement can keep, whatever the slot's family.
UNUSABLE_CREATED_AT = (
    (ABSENT, "missing"),
    (None, "malformed"),
    (20260101, "malformed"),
    ("yesterday", "malformed"),
    ("2026-02-30T00:00:00Z", "malformed"),
    ("2026-01-01T00:00:00Z\n", "malformed"),
    ("2026-01-01T02:00:00+02:00", "unsupported"),
    ("2026-01-01T00:00:00-00:00", "unsupported"),
    ("2026-01-01T00:00:00.000+00:00", "unsupported"),
    ("2026-01-01T00:00:00.5+00:00", "unsupported"),
    ("2026-01-01T00:00:00.250Z", "unsupported"),
    ("2026-01-01T00:00:00.1234567Z", "unsupported"),
    ("2026-01-01t00:00:00z", "unsupported"),
)

# Canonical creation times with a fraction of a second. A supported document
# keeps one verbatim. An unversioned document cannot carry one: earlier
# releases wrote whole seconds only.
FRACTIONAL_CREATED = (
    "2026-01-01T00:00:00.5Z",
    "2026-01-01T00:00:00.25Z",
    "2026-01-01T00:00:00.000001Z",
    "2026-01-01T00:00:00.123456Z",
)


def slot_document(plan_id: str = PLAN_ID, **changes: Any) -> dict[str, Any]:
    """Return a builder-made family-1 document created at CREATED, changed.

    Args:
        plan_id: The plan ID.
        **changes: Fields to set; a field set to ABSENT is removed.

    Returns:
        dict[str, Any]: The document, without a revision.
    """
    doc = build_plan_document(make_plan(plan_id), REALM, dict(SCOPE), now=CREATED)
    for field, value in changes.items():
        if value is ABSENT:
            doc.pop(field, None)
        else:
            doc[field] = value
    return doc


def recorded_value(value: Any) -> Any:
    """Return what a refusal records for a stored value: None when absent."""
    return None if value is ABSENT else value


def label(value: Any) -> str:
    """Return a subTest label for a stored value, ABSENT included."""
    return "absent" if value is ABSENT else repr(value)


class TestReplaceRule(ConformingBuilders):
    """What ``build_plan_document`` may build over the document in a slot."""

    def rebuild(self, existing: dict[str, Any]) -> dict[str, Any]:
        """Build a new generation of the plan over existing."""
        return build_plan_document(
            make_plan(PLAN_ID), REALM, dict(SCOPE), existing=existing
        )

    def assert_refused(
        self, existing: dict[str, Any], *, field: str, reason: str, recorded: Any
    ) -> UnsupportedDocumentError:
        """Assert that building over existing is refused, and how."""
        with self.assertRaises(UnsupportedDocumentError) as ctx:
            self.rebuild(existing)
        error = ctx.exception
        self.assertEqual(
            (error.doc_id, error.field, error.reason), (PLAN_ID, field, reason)
        )
        self.assertIs(type(error.recorded), type(recorded))
        self.assertEqual(error.recorded, recorded)
        self.assertEqual(error.supported, frozenset({1}))
        return error

    def test_an_invalid_or_unsupported_marker_is_refused(self):
        for marker, reason in REFUSED_MARKERS:
            with self.subTest(marker=marker):
                self.assert_refused(
                    slot_document(schema_version=marker),
                    field="schema_version",
                    reason=reason,
                    recorded=marker,
                )

    def test_the_marker_is_judged_before_created_at(self):
        for created_at, _ in UNUSABLE_CREATED_AT:
            with self.subTest(created_at=label(created_at)):
                self.assert_refused(
                    slot_document(schema_version=2, created_at=created_at),
                    field="schema_version",
                    reason="unsupported",
                    recorded=2,
                )

    def test_an_unversioned_document_keeps_its_instant_spelled_canonically(self):
        for created_at in (LEGACY_CREATED, CREATED):
            with self.subTest(created_at=created_at):
                existing = slot_document(schema_version=ABSENT, created_at=created_at)

                regenerated = self.rebuild(existing)

                self.assertEqual(regenerated["created_at"], CREATED)
                self.assertEqual(regenerated["schema_version"], 1)

    def test_an_unversioned_document_with_an_unusable_created_at_is_refused(self):
        for created_at, reason in UNUSABLE_CREATED_AT:
            with self.subTest(created_at=label(created_at)):
                self.assert_refused(
                    slot_document(schema_version=ABSENT, created_at=created_at),
                    field="created_at",
                    reason=reason,
                    recorded=recorded_value(created_at),
                )

    def test_a_supported_document_keeps_its_canonical_created_at_verbatim(self):
        regenerated = self.rebuild(slot_document())

        self.assertEqual(regenerated["created_at"], CREATED)
        self.assertNotEqual(regenerated["updated_at"], CREATED)

    def test_a_supported_document_keeps_a_canonical_fraction_verbatim(self):
        for created_at in FRACTIONAL_CREATED:
            with self.subTest(created_at=created_at):
                regenerated = self.rebuild(slot_document(created_at=created_at))

                self.assertEqual(regenerated["created_at"], created_at)
                self.assertNotEqual(regenerated["updated_at"], created_at)

    def test_an_unversioned_document_with_a_fraction_is_refused(self):
        for created_at in FRACTIONAL_CREATED:
            with self.subTest(created_at=created_at):
                self.assert_refused(
                    slot_document(schema_version=ABSENT, created_at=created_at),
                    field="created_at",
                    reason="unsupported",
                    recorded=created_at,
                )

    def test_a_supported_document_needs_a_canonical_created_at(self):
        for created_at, reason in (
            (LEGACY_CREATED, "unsupported"),
            *UNUSABLE_CREATED_AT,
        ):
            with self.subTest(created_at=label(created_at)):
                self.assert_refused(
                    slot_document(created_at=created_at),
                    field="created_at",
                    reason=reason,
                    recorded=recorded_value(created_at),
                )

    def test_the_refusal_names_the_plan_the_value_and_the_supported_families(self):
        error = self.assert_refused(
            slot_document(schema_version="1"),
            field="schema_version",
            reason="invalid",
            recorded="1",
        )
        self.assertEqual(
            str(error),
            "Planning refused to replace plan 'pln_test_P1_v1': its "
            "schema_version '1' is invalid (supported plan-document families: [1])",
        )

        missing = self.assert_refused(
            slot_document(created_at=ABSENT),
            field="created_at",
            reason="missing",
            recorded=None,
        )
        self.assertIn("its created_at is missing", str(missing))

    def test_a_value_that_cannot_be_shown_is_still_refused(self):
        cases = {
            "huge marker": ("schema_version", HUGE, "unsupported"),
            "nested huge marker": ("schema_version", {"family": [HUGE]}, "invalid"),
            "huge created_at": ("created_at", HUGE, "malformed"),
            "nested huge created_at": ("created_at", {"at": [HUGE]}, "malformed"),
        }
        for case, (field, value, reason) in cases.items():
            with self.subTest(case=case):
                error = self.assert_refused(
                    slot_document(**{field: value}),
                    field=field,
                    reason=reason,
                    recorded=value,
                )

                self.assertIs(error.recorded, value)
                shown = f"<unrepresentable {type(value).__name__}>"
                self.assertIn(f"its {field} {shown} is {reason}", str(error))
                self.assertIn(shown, repr(error))

    def test_the_message_shows_a_bounded_representation(self):
        cases = {
            "long string": ("created_at", "x" * 100_000, "malformed"),
            "long list": ("schema_version", list(range(100_000)), "invalid"),
        }
        for case, (field, value, reason) in cases.items():
            with self.subTest(case=case):
                error = self.assert_refused(
                    slot_document(**{field: value}),
                    field=field,
                    reason=reason,
                    recorded=value,
                )

                self.assertIs(error.recorded, value)
                self.assertLess(len(str(error)), 400)

    def test_the_refusal_is_neither_a_backend_failure_nor_a_lost_race(self):
        error = self.assert_refused(
            slot_document(schema_version=2),
            field="schema_version",
            reason="unsupported",
            recorded=2,
        )
        self.assertNotIsInstance(error, PlanStoreError)
        self.assertNotIsInstance(error, RevisionConflictError)


class SQLiteSlots(SQLiteBackend):
    """The SQLite plan store, with reads of its stored state."""

    def stored(self, doc_id: str) -> dict[str, Any] | None:
        """Return the document as stored, ``_rev`` included."""
        return self.internal.get_document("plans", doc_id)

    def plan_seq(self) -> int | None:
        """Return the head of the plan-change sequence."""
        return self.internal.current_plan_seq()

    def after_next_read(self, action: Callable[[], None]) -> None:
        """Run action once, right after the store's next document read."""
        real = self.internal.get_document

        def read_then_act(namespace: str, doc_id: str) -> dict[str, Any] | None:
            self.internal.get_document = real  # type: ignore[method-assign]
            doc = real(namespace, doc_id)
            action()
            return doc

        self.internal.get_document = read_then_act  # type: ignore[method-assign]


class CouchSlots(CouchBackend):
    """The CouchDB plan store on its fake client, with reads of its state."""

    def stored(self, doc_id: str) -> dict[str, Any] | None:
        """Return the document as stored, ``_rev`` included."""
        return self.server.raw(doc_id)

    def plan_seq(self) -> int | None:
        """Return None: CouchDB keeps no plan-change sequence of its own."""
        return None

    def after_next_read(self, action: Callable[[], None]) -> None:
        """Run action once, right after the store's next document read."""
        self.server.after_read.append(action)


class SlotTestCase(ConformingBuilders):
    """A plan store whose slots tests fill directly.

    Subclasses choose the backend with ``backend_type``.
    """

    backend_type: type[SQLiteSlots] | type[CouchSlots] = SQLiteSlots

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.backend = self.backend_type(self, Path(temp_dir.name))

    def put(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Store doc as given, bypassing the plan store; return it as stored."""
        self.backend.raw_put(doc)
        stored = self.backend.stored(doc["_id"])
        assert stored is not None
        return stored

    def save(self, plan_id: str = PLAN_ID) -> dict[str, Any]:
        """Plan plan_id through the store and return the stored document."""
        self.backend.plans.save_plan(
            make_plan(plan_id), REALM, dict(SCOPE), auto_run=True
        )
        stored = self.backend.stored(plan_id)
        assert stored is not None
        return stored

    def assert_refused_without_writing(
        self, plan_id: str, *, field: str, reason: str, recorded: Any
    ) -> None:
        """Assert that planning plan_id is refused, and that nothing was written.

        The stored document is compared whole, ``_rev`` included, and the
        plan-change sequence must not move.
        """
        before = self.backend.stored(plan_id)
        sequence = self.backend.plan_seq()

        with self.assertRaises(UnsupportedDocumentError) as ctx:
            self.save(plan_id)

        error = ctx.exception
        self.assertEqual(
            (error.doc_id, error.field, error.reason), (plan_id, field, reason)
        )
        self.assertIs(type(error.recorded), type(recorded))
        self.assertEqual(error.recorded, recorded)
        self.assertEqual(self.backend.stored(plan_id), before)
        self.assertEqual(self.backend.plan_seq(), sequence)


class SaveScenarios:
    """``save_plan`` over each kind of slot, against one backend.

    Mixed into one test case per plan-store backend.
    """

    def test_an_invalid_or_unsupported_marker_is_left_as_it_is(self):
        for index, (marker, reason) in enumerate(REFUSED_MARKERS):
            plan_id = f"pln_marker_{index}"
            with self.subTest(marker=marker):
                self.put(slot_document(plan_id, schema_version=marker))
                self.assert_refused_without_writing(
                    plan_id, field="schema_version", reason=reason, recorded=marker
                )

    def test_an_unversioned_document_becomes_a_new_family_one_generation(self):
        for index, created_at in enumerate((LEGACY_CREATED, CREATED)):
            plan_id = f"pln_unversioned_{index}"
            with self.subTest(created_at=created_at):
                legacy = self.put(
                    slot_document(
                        plan_id,
                        schema_version=ABSENT,
                        created_at=created_at,
                        plan_generation="g-legacy",
                        run_token=3,
                        executed_run_token=3,
                    )
                )

                stored = self.save(plan_id)

                self.assertEqual(stored["schema_version"], 1)
                self.assertNotIn(stored["plan_generation"], ("g-legacy", None, ""))
                self.assertEqual(
                    (stored["run_token"], stored["executed_run_token"]), (0, -1)
                )
                self.assertEqual(stored["created_at"], CREATED)
                self.assertNotEqual(stored["_rev"], legacy["_rev"])

    def test_an_unversioned_document_with_an_unusable_created_at_is_left(self):
        for index, (created_at, reason) in enumerate(UNUSABLE_CREATED_AT):
            plan_id = f"pln_unversioned_{index}"
            with self.subTest(created_at=label(created_at)):
                self.put(
                    slot_document(plan_id, schema_version=ABSENT, created_at=created_at)
                )
                self.assert_refused_without_writing(
                    plan_id,
                    field="created_at",
                    reason=reason,
                    recorded=recorded_value(created_at),
                )

    def test_a_supported_document_keeps_its_canonical_created_at(self):
        self.put(slot_document())

        stored = self.save()

        self.assertEqual(stored["created_at"], CREATED)
        self.assertNotEqual(stored["updated_at"], CREATED)

    def test_a_supported_document_keeps_a_canonical_fraction_verbatim(self):
        for index, created_at in enumerate(FRACTIONAL_CREATED):
            plan_id = f"pln_fraction_{index}"
            with self.subTest(created_at=created_at):
                before = self.put(slot_document(plan_id, created_at=created_at))

                stored = self.save(plan_id)

                self.assertEqual(stored["created_at"], created_at)
                self.assertNotEqual(stored["updated_at"], created_at)
                self.assertNotEqual(
                    stored["plan_generation"], before["plan_generation"]
                )
                self.assertNotEqual(stored["_rev"], before["_rev"])

    def test_an_unversioned_document_with_a_fraction_is_left(self):
        for index, created_at in enumerate(FRACTIONAL_CREATED):
            plan_id = f"pln_unversioned_fraction_{index}"
            with self.subTest(created_at=created_at):
                self.put(
                    slot_document(plan_id, schema_version=ABSENT, created_at=created_at)
                )
                self.assert_refused_without_writing(
                    plan_id,
                    field="created_at",
                    reason="unsupported",
                    recorded=created_at,
                )

    def test_a_supported_document_with_a_noncanonical_created_at_is_left(self):
        cases = (
            (LEGACY_CREATED, "unsupported"),
            (ABSENT, "missing"),
            ("yesterday", "malformed"),
            ("2026-01-01T00:00:00.250Z", "unsupported"),
            ("2026-01-01T00:00:00.1234567Z", "unsupported"),
        )
        for index, (created_at, reason) in enumerate(cases):
            plan_id = f"pln_supported_{index}"
            with self.subTest(created_at=label(created_at)):
                self.put(slot_document(plan_id, created_at=created_at))
                self.assert_refused_without_writing(
                    plan_id,
                    field="created_at",
                    reason=reason,
                    recorded=recorded_value(created_at),
                )

    def test_regeneration_keeps_only_the_creation_time(self):
        self.put(
            slot_document(
                plan_generation="g-reviewed",
                status="approved",
                approved_at="2026-01-02T00:00:00Z",
                approved_by="alice",
                run_token=2,
                run_requested_at="2026-01-03T00:00:00Z",
                run_requested_by="bob",
                executed_run_token=1,
                last_finalized_execution={"execution_id": "e1", "outcome": "failed"},
                last_executed_at="2026-01-03T01:00:00Z",
                ticket="OPS-1",
                x_ops_ticket="OPS-1",
            )
        )

        stored = self.save()

        fresh = build_plan_document(make_plan(PLAN_ID), REALM, dict(SCOPE))
        self.assertEqual(set(stored), set(fresh) | {"_rev"})
        self.assertEqual(stored["created_at"], CREATED)
        self.assertEqual(stored["schema_version"], 1)
        self.assertNotEqual(stored["plan_generation"], "g-reviewed")
        self.assertEqual((stored["run_token"], stored["executed_run_token"]), (0, -1))

    def test_a_slot_that_changes_family_after_the_read_is_not_replaced(self):
        read = self.put(slot_document())
        newer = dict(read, schema_version=2)
        self.backend.after_next_read(lambda: self.backend.conditional_put(newer))

        with self.assertRaises(RevisionConflictError):
            self.save()

        stored = self.backend.stored(PLAN_ID)
        self.assertEqual(stored["schema_version"], 2)
        self.assertEqual(stored["plan_generation"], read["plan_generation"])


class TestSQLiteSave(SaveScenarios, SlotTestCase):
    """``save_plan`` against SQLitePlanStore on a real temporary database."""


class TestCouchSave(SaveScenarios, SlotTestCase):
    """``save_plan`` against PlanDBManager on a revision-enforcing fake client."""

    backend_type = CouchSlots


REFUSED_ID = "pln_test_P1_refused"
ACCEPTED_ID = "pln_test_P1_accepted"

# The words with which the stores and the callers report a plan they wrote.
SAVE_REPORT = re.compile(r"\b(saved|persisted|created)\b", re.IGNORECASE)


class DraftingHandler:
    """Stands in for a realm handler: drafts the same plans for any document."""

    realm_id = REALM

    def __init__(self, *plans: Plan) -> None:
        self.plans = plans

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        return dict(SCOPE)

    def build_planning_context(self, **kwargs: Any) -> None:
        return None

    def drafts(self) -> list[PlanDraft]:
        return [PlanDraft(plan=plan, auto_run=True) for plan in self.plans]

    def run_now(self, payload: dict[str, Any]) -> list[PlanDraft]:
        return self.drafts()

    async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
        return self.drafts()

    def class_qualified_name(self) -> str:
        return f"{__name__}.DraftingHandler"


class PlanningTestCase(ConformingBuilders):
    """A real ``YggdrasilCore`` whose project handler drafts the plans a test sets.

    The handler drafts no plan until a test or subclass sets
    ``self.handler.plans``. Subclasses choose the backend with
    ``backend_type``.
    """

    backend_type: type[SQLiteSlots] | type[CouchSlots] = SQLiteSlots

    def setUp(self) -> None:
        SingletonMeta._instances.clear()
        self.addCleanup(SingletonMeta._instances.clear)
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        directory = Path(temp_dir.name)
        for patcher in (
            patch.dict(os.environ, {"YGG_EVENT_SPOOL": str(directory / "spool")}),
            patch("yggdrasil.daemon.core.Engine"),
            patch("yggdrasil.daemon.core.OpsConsumerService"),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

        self.backend = self.backend_type(self, directory)
        bundle = InternalStorageBundle(
            backend=self.backend.name,
            plans=self.backend.plans,
            plan_changes=Mock(),
            checkpoints=Mock(),
            ops_snapshots=Mock(),
        )
        self.core = YggdrasilCore(
            {"work_root": str(directory / "work")}, storage=bundle
        )
        self.core.pdm = Mock()
        self.core.pdm.fetch_document_by_id.return_value = {"project_id": "P1"}

        handler: Any = DraftingHandler()
        self.handler = handler
        self.core.subscriptions[EventType.PROJECT_CHANGE] = [handler]


class CallerTestCase(PlanningTestCase):
    """A real ``YggdrasilCore`` planning two drafts, one into a refused slot.

    The first draft's slot holds a family-2 document, which this release must
    not replace; the second draft's slot is empty. Subclasses choose the
    backend with ``backend_type``.
    """

    def setUp(self) -> None:
        super().setUp()
        self.backend.raw_put(slot_document(REFUSED_ID, schema_version=2))
        self.newer = self.backend.stored(REFUSED_ID)
        self.handler.plans = (make_plan(REFUSED_ID), make_plan(ACCEPTED_ID))

    def assert_only_the_refused_draft_was_skipped(
        self, records: list[logging.LogRecord]
    ) -> dict[str, Any]:
        """Assert the refused slot is unchanged and logged once; the other is saved.

        Args:
            records: Everything the yggdrasil loggers logged meanwhile.

        Returns:
            dict[str, Any]: The accepted plan's stored document.
        """
        self.assertEqual(self.backend.stored(REFUSED_ID), self.newer)
        accepted = self.backend.stored(ACCEPTED_ID)
        assert accepted is not None
        self.assertEqual(accepted["schema_version"], 1)

        errors = [r for r in records if r.levelno >= logging.ERROR]
        self.assertEqual(len(errors), 1, [r.getMessage() for r in errors])
        message = errors[0].getMessage()
        self.assertIn(f"'{REFUSED_ID}'", message)
        self.assertIn("its schema_version 2 is unsupported", message)
        self.assertIsNone(errors[0].exc_info)

        # Only the accepted draft is reported as saved, persisted or created.
        reports = [
            r.getMessage()
            for r in records
            if r.levelno == logging.INFO and SAVE_REPORT.search(r.getMessage())
        ]
        self.assertTrue(reports)
        for report in reports:
            with self.subTest(report=report):
                self.assertIn(ACCEPTED_ID, report)
        return accepted


class CallerScenarios:
    """Each planning caller skips a refused draft and persists the others.

    Mixed into one test case per plan-store backend.
    """

    def test_daemon_planning_persists_the_other_drafts(self):
        with self.assertLogs("yggdrasil", level=logging.INFO) as logs:
            asyncio.run(self.core._generate_and_persist_plan(self.handler, {}))

        self.assert_only_the_refused_draft_was_skipped(logs.records)

    def test_plan_only_returns_only_the_persisted_drafts(self):
        with self.assertLogs("yggdrasil", level=logging.INFO) as logs:
            persisted = self.core.create_plan_from_doc("P1", force_overwrite=True)

        self.assertEqual(persisted, [ACCEPTED_ID])
        accepted = self.assert_only_the_refused_draft_was_skipped(logs.records)
        self.assertEqual(accepted["status"], "draft")

    def test_run_once_watches_only_the_persisted_drafts(self):
        watch = AsyncMock(return_value=0)
        with (
            patch.object(self.core, "_run_once_watcher_loop", watch),
            patch("yggdrasil.ops.consumer.FileSpoolConsumer"),
            self.assertLogs("yggdrasil", level=logging.INFO) as logs,
        ):
            exit_code = self.core.run_once_with_watcher("P1", force_overwrite=True)

        self.assertEqual(exit_code, 0)
        session = watch.await_args.kwargs
        self.assertEqual(session["pending_plan_ids"], [ACCEPTED_ID])
        accepted = self.assert_only_the_refused_draft_was_skipped(logs.records)
        self.assertEqual(accepted["execution_authority"], "run_once")
        self.assertEqual(accepted["execution_owner"], session["execution_owner"])

    def test_run_once_without_any_persisted_plan_exits_nonzero(self):
        self.handler.plans = (make_plan(REFUSED_ID),)
        watch = AsyncMock(return_value=0)
        with (
            patch.object(self.core, "_run_once_watcher_loop", watch),
            self.assertLogs("yggdrasil", level=logging.ERROR) as logs,
        ):
            exit_code = self.core.run_once_with_watcher("P1", force_overwrite=True)

        self.assertEqual(exit_code, 1)
        watch.assert_not_called()
        self.assertEqual(self.backend.stored(REFUSED_ID), self.newer)
        messages = [r.getMessage() for r in logs.records]
        self.assertEqual(len(messages), 2, messages)
        self.assertIn("its schema_version 2 is unsupported", messages[0])
        self.assertIn("Failed to create any plans", messages[1])


class TestSQLiteCallers(CallerScenarios, CallerTestCase):
    """The planning callers over SQLitePlanStore."""


class TestCouchCallers(CallerScenarios, CallerTestCase):
    """The planning callers over PlanDBManager on a revision-enforcing fake client."""

    backend_type = CouchSlots


# The planning callers that check a plan's slot before they write.
PLAN_ONLY = "plan_only"
RUN_ONCE = "run_once"
CHECKING_CALLERS = (PLAN_ONLY, RUN_ONCE)

# Slots holding a document the shared reader cannot use: how a builder-made
# document is changed into one, and the version state its problem names.
UNREADABLE_SLOTS = (
    ("unversioned", {"schema_version": ABSENT}, "unversioned"),
    ("invalid", {"schema_version": "1"}, "invalid"),
    ("unsupported", {"schema_version": 2}, "unsupported"),
)

# What the overwrite check shows of a document it cannot read.
UNKNOWN_LINES = (
    "│ Status:     unknown",
    "│ Origin:     unknown",
    "│ Updated:    unknown",
    "│ Run Token:  unknown (executed: unknown)",
)

# The title of the box in which planning shows an existing document.
BOX_TITLE = "Existing Plan Found"


class OverwriteTestCase(PlanningTestCase):
    """Planning through plan-only and run-once, into slots tests fill directly."""

    def put(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Store doc as given, bypassing the plan store; return it as stored."""
        self.backend.raw_put(doc)
        stored = self.backend.stored(doc["_id"])
        assert stored is not None
        return stored

    def plan(
        self, caller: str, *plan_ids: str, force: bool
    ) -> tuple[list[str], list[logging.LogRecord]]:
        """Have caller plan plan_ids; return the IDs it persisted and what it logged.

        Args:
            caller: PLAN_ONLY or RUN_ONCE.
            *plan_ids: The plans the handler drafts, in order.
            force: Whether ``--force`` is given.

        Returns:
            tuple[list[str], list[logging.LogRecord]]: The plans persisted,
            which run-once watches, and everything the yggdrasil loggers
            logged at INFO or above.
        """
        self.handler.plans = tuple(make_plan(plan_id) for plan_id in plan_ids)
        if caller == PLAN_ONLY:
            with self.assertLogs("yggdrasil", level=logging.INFO) as logs:
                persisted = self.core.create_plan_from_doc("P1", force_overwrite=force)
            return persisted, logs.records
        watch = AsyncMock(return_value=0)
        with (
            patch.object(self.core, "_run_once_watcher_loop", watch),
            patch("yggdrasil.ops.consumer.FileSpoolConsumer"),
            self.assertLogs("yggdrasil", level=logging.INFO) as logs,
        ):
            exit_code = self.core.run_once_with_watcher("P1", force_overwrite=force)
        if watch.await_args is None:
            self.assertEqual(exit_code, 1)
            return [], logs.records
        self.assertEqual(exit_code, 0)
        return list(watch.await_args.kwargs["pending_plan_ids"]), logs.records

    def boxes(self, records: list[logging.LogRecord]) -> list[list[str]]:
        """Return each "Existing Plan Found" box logged, as its lines."""
        found = [record for record in records if BOX_TITLE in record.getMessage()]
        for record in found:
            self.assertEqual(record.levelno, logging.WARNING)
        return [record.getMessage().splitlines() for record in found]

    def problems_line(self, box: list[str]) -> str:
        """Return a box's one Problems line."""
        (line,) = [line for line in box if line.startswith("│ Problems:")]
        return line


class OverwriteScenarios:
    """Plan-only and run-once over a slot that already holds a document.

    The check shows any stored document, readable or not, and stops planning
    without ``--force``; with it, the replace rule decides. Mixed into one
    test case per plan-store backend.
    """

    def test_an_unreadable_document_is_shown_and_kept_without_force(self):
        stored = {
            name: self.put(slot_document(f"pln_test_P1_{name}", **changes))
            for name, changes, _ in UNREADABLE_SLOTS
        }
        sequence = self.backend.plan_seq()
        for caller in CHECKING_CALLERS:
            for name, _, state in UNREADABLE_SLOTS:
                slot_id = f"pln_test_P1_{name}"
                with self.subTest(caller=caller, slot=name):
                    persisted, records = self.plan(caller, slot_id, force=False)

                    self.assertEqual(persisted, [])
                    self.assertEqual(self.backend.stored(slot_id), stored[name])
                    self.assertEqual(self.backend.plan_seq(), sequence)
                    (box,) = self.boxes(records)
                    for line in UNKNOWN_LINES:
                        self.assertIn(line, box)
                    self.assertIn(f"so it is {state}", self.problems_line(box))
                    self.assertIn(
                        f"Plan '{slot_id}' already exists. Use --force to overwrite.",
                        [record.getMessage() for record in records],
                    )

    def test_a_readable_document_is_shown_as_stored_and_kept_without_force(self):
        stored = self.put(slot_document(PLAN_ID))
        sequence = self.backend.plan_seq()
        for caller in CHECKING_CALLERS:
            with self.subTest(caller=caller):
                persisted, records = self.plan(caller, PLAN_ID, force=False)

                self.assertEqual(persisted, [])
                self.assertEqual(self.backend.stored(PLAN_ID), stored)
                self.assertEqual(self.backend.plan_seq(), sequence)
                (box,) = self.boxes(records)
                for line in (
                    "│ Status:     draft",
                    "│ Origin:     daemon",
                    f"│ Updated:    {CREATED}",
                    "│ Run Token:  0 (executed: -1)",
                ):
                    self.assertIn(line, box)
                self.assertFalse([line for line in box if "Problems" in line])

    def test_with_force_the_replace_rule_decides(self):
        for caller in CHECKING_CALLERS:
            with self.subTest(caller=caller):
                unversioned_id = f"pln_test_{caller}_unversioned"
                unsupported_id = f"pln_test_{caller}_unsupported"
                unversioned = self.put(
                    slot_document(unversioned_id, schema_version=ABSENT)
                )
                unsupported = self.put(slot_document(unsupported_id, schema_version=2))

                persisted, records = self.plan(
                    caller, unversioned_id, unsupported_id, force=True
                )

                # The unversioned slot is replaced by a new family-1 generation.
                self.assertEqual(persisted, [unversioned_id])
                replaced = self.backend.stored(unversioned_id)
                assert replaced is not None
                self.assertEqual(replaced["schema_version"], 1)
                self.assertNotEqual(replaced["_rev"], unversioned["_rev"])
                # The unsupported one is shown, then refused as before.
                self.assertEqual(self.backend.stored(unsupported_id), unsupported)
                boxes = self.boxes(records)
                self.assertEqual(len(boxes), 2)
                for box in boxes:
                    self.assertIn("so it is", self.problems_line(box))
                errors = [
                    record.getMessage()
                    for record in records
                    if record.levelno >= logging.ERROR
                ]
                self.assertEqual(len(errors), 1, errors)
                self.assertIn("its schema_version 2 is unsupported", errors[0])


class TestSQLiteOverwriteCheck(OverwriteScenarios, OverwriteTestCase):
    """Plan-only and run-once over SQLitePlanStore."""


class TestCouchOverwriteCheck(OverwriteScenarios, OverwriteTestCase):
    """Plan-only and run-once over PlanDBManager on a revision-enforcing fake client."""

    backend_type = CouchSlots

    def test_a_malformed_response_is_never_taken_for_an_empty_slot(self):
        stored = self.put(slot_document(PLAN_ID))
        empty_id = "pln_test_P1_empty"
        for caller in CHECKING_CALLERS:
            for slot_id in (empty_id, PLAN_ID):
                for name, body in (
                    ("JSON null", None),
                    ("a list", []),
                    ("text", "not found"),
                ):
                    with self.subTest(caller=caller, slot=slot_id, body=name):
                        # The read of the slot is answered with body, as a
                        # proxy or a misrouted request might answer it.
                        self.backend.server.read_responses.append(body)

                        persisted, records = self.plan(caller, slot_id, force=False)

                        self.assertEqual(persisted, [])
                        (box,) = self.boxes(records)
                        for line in UNKNOWN_LINES:
                            self.assertIn(line, box)
                        self.assertIn("not an object", self.problems_line(box))
        self.assertIsNone(self.backend.stored(empty_id))
        self.assertEqual(self.backend.stored(PLAN_ID), stored)

    def test_a_failed_read_writes_nothing_and_shows_nothing(self):
        stored = self.put(slot_document(PLAN_ID))
        for caller in CHECKING_CALLERS:
            for force in (False, True):
                for name, error in (
                    ("server error", FakeApiException(500, "Internal Server Error")),
                    ("connection refused", RequestsConnectionError("refused")),
                ):
                    with self.subTest(caller=caller, force=force, error=name):
                        self.backend.server.fail_next_read = error

                        persisted, records = self.plan(caller, PLAN_ID, force=force)

                        self.assertEqual(persisted, [])
                        self.assertEqual(self.backend.stored(PLAN_ID), stored)
                        self.assertEqual(self.boxes(records), [])
                        # The caller reports the failure itself, unchanged.
                        self.assertTrue(
                            [
                                record
                                for record in records
                                if record.exc_info and record.exc_info[1] is error
                            ]
                        )


if __name__ == "__main__":
    unittest.main()
