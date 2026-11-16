"""How the callers that judge stored plans report the plans they refuse, on both backends.

Eligibility is decided by the shared reader, which logs nothing; each caller
that refuses a plan on its verdict reports the refusal itself:

- PlanWatcher, on its change path and in scoped recovery, reports a plan
  whose ``schema_version`` is absent, invalid or unsupported at WARNING, and
  so are malformed control state (``invalid_tokens``, ``status_missing``),
  but only for plans within its execution authority and owner scope. The
  scope filters run first on both paths. A valid draft and an executed
  request stay DEBUG, and nothing is emitted for a refused plan.
- Both stores' recovery listings leave an unsupported plan out with one
  WARNING each, and every other refusal silently.

Warnings come from evaluating a change or a recovery document, never from
polling: a refused change advances the cursor like any other, an idle poll
reports nothing, and only an actual change to the document reports it again.
No refusal changes a stored document.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import Mock, patch

from tests.execution_support import (
    WAIT,
    CouchBackend,
    SQLiteBackend,
    run_bounded,
)
from tests.plan_store_support import ABSENT, supported_plan_document
from yggdrasil.couchdb.changes_fetcher import ChangesFetcher
from yggdrasil.couchdb.models import ChangesBatch, ChangesRow
from yggdrasil.storage.couchdb.bundle import CouchPlanChangeSource
from yggdrasil.storage.couchdb.plan_store import PlanDBManager
from yggdrasil.storage.protocols import PlanChangeSource, PlanStore
from yggdrasil.storage.sqlite import SQLitePlanChangeSource
from yggdrasil.watchers.backends.base import Checkpoint, RawWatchEvent
from yggdrasil.watchers.backends.checkpoint_store import InMemoryCheckpointStore
from yggdrasil.watchers.events import YggdrasilEvent
from yggdrasil.watchers.plan_watcher import PlanWatcher

WATCHER_LOGGER = "yggdrasil.watchers.plan_watcher.PlanWatcher"
SQLITE_STORE_LOGGER = "yggdrasil.storage.sqlite.SQLitePlanStore"
COUCH_STORE_LOGGER = "yggdrasil.storage.couchdb.plan_store.PlanDBManager"

PLAN_ID = "pln_refused_P1_v1"
OWNER = "run_once:this-session"

# Markers no reader accepts: each with how a refusal shows it and the version
# state it names.
UNSUPPORTED_MARKERS: dict[str, tuple[Any, str, str]] = {
    "unversioned": (ABSENT, "absent", "unversioned"),
    "invalid": ("1", "'1'", "invalid"),
    "unsupported": (2, "2", "unsupported"),
}

# Malformed control state of a supported plan: the fields that make it so,
# the reason code and the diagnostic its refusal shows. Tokens are judged
# before the approval status, so a malformed draft is among them.
MALFORMED: dict[str, tuple[dict[str, Any], str, str]] = {
    "a token that is not an integer": (
        {"run_token": "5"},
        "invalid_tokens",
        "Not eligible: invalid tokens (run_token '5' is a str, not a JSON integer)",
    ),
    "a draft without an executed token": (
        {"status": "draft", "executed_run_token": ABSENT},
        "invalid_tokens",
        "Not eligible: invalid tokens (executed_run_token is missing)",
    ),
    "no status": (
        {"status": ABSENT},
        "status_missing",
        "Not eligible: status is missing (must be 'approved')",
    ),
}

# Plans that are simply not to be executed now.
ORDINARY_SKIPS: dict[str, dict[str, Any]] = {
    "a valid draft": {"status": "draft"},
    "an executed request": {"executed_run_token": 0},
}

SUPPORTED = "(supported plan-document families: [1])"
NOT_EMITTED = "no execution event is emitted for it"
LEFT_OUT = "it is left out of the recovery listing"

# Idle polls to wait for after a change, before judging what was reported.
IDLE_POLLS = 3


def unsupported_line(plan_id: str, shown: str, state: str, consequence: str) -> str:
    """The whole line that reports an unsupported plan.

    Args:
        plan_id: The plan.
        shown: Its schema_version, as a diagnostic shows it.
        state: The version state.
        consequence: What the caller does about it.

    Returns:
        str: The line.
    """
    return (
        f"Plan '{plan_id}' is not executed: its schema_version is {shown}, so it "
        f"is {state} {SUPPORTED}; {consequence}"
    )


def malformed_line(plan_id: str, code: str, reason: str) -> str:
    """The whole line that reports a plan with malformed control state.

    Args:
        plan_id: The plan.
        code: The eligibility reason code.
        reason: The eligibility decision's reason.

    Returns:
        str: The line.
    """
    return (
        f"Plan '{plan_id}' is not executed: its control state is malformed "
        f"({code}). {reason}; {NOT_EMITTED}"
    )


def warnings(records: list[logging.LogRecord]) -> list[str]:
    """Return the messages of the records at WARNING or above."""
    return [r.getMessage() for r in records if r.levelno >= logging.WARNING]


class StoreTestCase(unittest.TestCase):
    """A plan store on one backend, with a PlanWatcher and no coordinator.

    Subclasses choose the backend with ``backend_type``.

    Attributes:
        events: Every event a watcher made here emitted.
    """

    backend_type: type[SQLiteBackend] | type[CouchBackend] = SQLiteBackend

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.backend = self.backend_type(self, Path(temp_dir.name))
        self.events: list[YggdrasilEvent] = []

    @property
    def store_logger(self) -> str:
        """The name of the plan store's logger."""
        if isinstance(self.backend, SQLiteBackend):
            return SQLITE_STORE_LOGGER
        return COUCH_STORE_LOGGER

    def put(self, document: dict[str, Any]) -> None:
        """Store document exactly as given, as another writer could."""
        self.backend.raw_put(document)

    def raw(self, plan_id: str) -> dict[str, Any] | None:
        """The plan document exactly as the backend holds it, ``_rev`` included."""
        if isinstance(self.backend, SQLiteBackend):
            return self.backend.internal.get_document("plans", plan_id)
        return self.backend.server.raw(plan_id)

    def plan_seq(self) -> int | None:
        """The SQLite store's plan-change sequence; None on CouchDB."""
        if isinstance(self.backend, SQLiteBackend):
            return self.backend.internal.current_plan_seq()
        return None

    def watcher(
        self, *, authority: str = "daemon", owner: str | None = None
    ) -> PlanWatcher:
        """A PlanWatcher over this store, with the daemon's or a session's scope.

        Args:
            authority: The execution authority it watches for.
            owner: The execution owner it watches for, if any.

        Returns:
            PlanWatcher: A watcher that appends what it emits to events.
        """
        return PlanWatcher(
            on_event=self.events.append,
            poll_interval_sec=0,
            plan_store=self.backend.plans,
            change_source=Mock(spec=PlanChangeSource),
            checkpoint_store=InMemoryCheckpointStore(),
            execution_authority_filter=authority,
            execution_owner_filter=owner,
        )

    def evaluate(self, watcher: PlanWatcher, plan_id: str) -> None:
        """Have watcher evaluate the plan's stored document as a change."""
        change = RawWatchEvent(id=plan_id, doc=self.raw(plan_id), seq=None)
        run_bounded(watcher._evaluate_change(change))

    def recover(self, watcher: PlanWatcher, *plan_ids: str) -> list[dict[str, Any]]:
        """Run watcher's scoped recovery for plan_ids, and return what it emitted."""
        return run_bounded(watcher.recover_pending_plans(list(plan_ids)))

    @contextmanager
    def nothing_changes(self, *plan_ids: str, emitted: int = 0) -> Iterator[None]:
        """Assert that the block leaves the store as it was, emitting no more than said.

        Args:
            *plan_ids: The plans whose stored documents, revision included,
                must not change.
            emitted: How many events the block emits.

        Yields:
            None: For the block that must change nothing.
        """
        before = {plan_id: self.raw(plan_id) for plan_id in plan_ids}
        seq = self.plan_seq()
        events = len(self.events)
        yield
        for plan_id in plan_ids:
            self.assertEqual(self.raw(plan_id), before[plan_id], plan_id)
        self.assertEqual(self.plan_seq(), seq)
        self.assertEqual(len(self.events), events + emitted)


class ChangePathScenarios:
    """What PlanWatcher's change path reports, against one backend.

    Mixed into one StoreTestCase per plan-store backend.
    """

    def test_a_plan_without_schema_version_is_reported_and_not_emitted(self):
        self.put(supported_plan_document(PLAN_ID, schema_version=ABSENT))

        with (
            self.nothing_changes(PLAN_ID),
            self.assertLogs(WATCHER_LOGGER, level=logging.DEBUG) as logs,
        ):
            self.evaluate(self.watcher(), PLAN_ID)

        self.assertEqual(
            warnings(logs.records),
            [unsupported_line(PLAN_ID, "absent", "unversioned", NOT_EMITTED)],
        )

    def test_every_unsupported_version_state_is_reported(self):
        for index, (case, (marker, shown, state)) in enumerate(
            UNSUPPORTED_MARKERS.items()
        ):
            plan_id = f"{PLAN_ID}_{index}"
            with self.subTest(case=case):
                self.put(supported_plan_document(plan_id, schema_version=marker))

                with (
                    self.nothing_changes(plan_id),
                    self.assertLogs(WATCHER_LOGGER, level=logging.WARNING) as logs,
                ):
                    self.evaluate(self.watcher(), plan_id)

                self.assertEqual(
                    warnings(logs.records),
                    [unsupported_line(plan_id, shown, state, NOT_EMITTED)],
                )

    def test_malformed_control_state_in_scope_is_reported(self):
        for index, (case, (fields, code, reason)) in enumerate(MALFORMED.items()):
            plan_id = f"{PLAN_ID}_{index}"
            with self.subTest(case=case):
                self.put(supported_plan_document(plan_id, **fields))

                with (
                    self.nothing_changes(plan_id),
                    self.assertLogs(WATCHER_LOGGER, level=logging.WARNING) as logs,
                ):
                    self.evaluate(self.watcher(), plan_id)

                self.assertEqual(
                    warnings(logs.records), [malformed_line(plan_id, code, reason)]
                )

    def test_a_session_watcher_reports_its_own_malformed_plans(self):
        fields, code, reason = MALFORMED["no status"]
        self.put(
            supported_plan_document(
                PLAN_ID, execution_authority="run_once", execution_owner=OWNER, **fields
            )
        )

        with (
            self.nothing_changes(PLAN_ID),
            self.assertLogs(WATCHER_LOGGER, level=logging.WARNING) as logs,
        ):
            self.evaluate(self.watcher(authority="run_once", owner=OWNER), PLAN_ID)

        self.assertEqual(
            warnings(logs.records), [malformed_line(PLAN_ID, code, reason)]
        )

    def test_a_refused_plan_out_of_scope_stays_below_warning(self):
        refused = {
            **{case: fields for case, (fields, _, _) in MALFORMED.items()},
            "unversioned": {"schema_version": ABSENT},
            "unsupported": {"schema_version": 2},
        }
        scopes = {
            "another authority": (
                {"execution_authority": "run_once"},
                {"authority": "daemon"},
                "doesn't match filter",
            ),
            "another owner": (
                {
                    "execution_authority": "run_once",
                    "execution_owner": "run_once:other",
                },
                {"authority": "run_once", "owner": OWNER},
                "owner='run_once:other' doesn't match filter",
            ),
        }
        index = 0
        for case, fields in refused.items():
            for scope, (stored_scope, watching, skipped) in scopes.items():
                index += 1
                plan_id = f"{PLAN_ID}_{index}"
                with self.subTest(case=case, scope=scope):
                    self.put(supported_plan_document(plan_id, **fields, **stored_scope))
                    watcher = self.watcher(**watching)

                    with (
                        self.nothing_changes(plan_id),
                        self.assertLogs(WATCHER_LOGGER, level=logging.DEBUG) as logs,
                    ):
                        self.evaluate(watcher, plan_id)

                    self.assertEqual(warnings(logs.records), [])
                    (line,) = [r.getMessage() for r in logs.records]
                    self.assertIn(f"Skipping plan {plan_id}", line)
                    self.assertIn(skipped, line)

    def test_ordinary_skips_stay_debug(self):
        for index, (case, fields) in enumerate(ORDINARY_SKIPS.items()):
            plan_id = f"{PLAN_ID}_{index}"
            with self.subTest(case=case):
                self.put(supported_plan_document(plan_id, **fields))
                watcher = self.watcher()

                with (
                    self.nothing_changes(plan_id),
                    self.assertLogs(WATCHER_LOGGER, level=logging.DEBUG) as logs,
                ):
                    self.evaluate(watcher, plan_id)

                self.assertEqual(warnings(logs.records), [])
                (line,) = [r.getMessage() for r in logs.records]
                self.assertTrue(
                    line.startswith(
                        f"Skipping ineligible plan: {plan_id} (Not eligible:"
                    ),
                    line,
                )

    def test_a_plan_without_authority_gets_only_the_authority_warning(self):
        self.put(
            supported_plan_document(
                PLAN_ID, schema_version=ABSENT, execution_authority=ABSENT
            )
        )

        with (
            self.nothing_changes(PLAN_ID),
            self.assertLogs(WATCHER_LOGGER, level=logging.WARNING) as logs,
        ):
            self.evaluate(self.watcher(), PLAN_ID)

        self.assertEqual(
            warnings(logs.records),
            [f"Skipping plan with missing execution_authority: {PLAN_ID}"],
        )

    def test_an_eligible_plan_is_still_emitted(self):
        self.put(supported_plan_document(PLAN_ID, run_token=3, executed_run_token=2))

        with self.assertLogs(WATCHER_LOGGER, level=logging.DEBUG) as logs:
            self.evaluate(self.watcher(), PLAN_ID)

        (event,) = self.events
        self.assertEqual(event.payload["plan_doc_id"], PLAN_ID)
        self.assertEqual(event.payload["plan_doc"], self.raw(PLAN_ID))
        self.assertEqual(warnings(logs.records), [])
        self.assertIn(
            f"Eligible plan detected: {PLAN_ID} (authority=daemon, run_token=3, "
            "executed_run_token=2)",
            [r.getMessage() for r in logs.records],
        )

    def test_a_refusal_shows_recorded_values_bounded(self):
        huge = "x" * 100_000
        cases = {
            "a huge schema_version": {"schema_version": huge},
            "a huge token": {"run_token": huge},
        }
        for index, (case, fields) in enumerate(cases.items()):
            plan_id = f"{PLAN_ID}_{index}"
            with self.subTest(case=case):
                self.put(supported_plan_document(plan_id, **fields))

                with self.assertLogs(WATCHER_LOGGER, level=logging.WARNING) as logs:
                    self.evaluate(self.watcher(), plan_id)

                (line,) = warnings(logs.records)
                self.assertTrue(line.startswith(f"Plan '{plan_id}' is not executed:"))
                self.assertIn("...", line)
                self.assertLess(len(line), 500)


class ScopedRecoveryScenarios:
    """What PlanWatcher's scoped recovery reports, against one backend.

    Mixed into one StoreTestCase per plan-store backend.
    """

    def test_a_requested_plan_without_schema_version_is_reported(self):
        self.put(supported_plan_document(PLAN_ID, schema_version=ABSENT))

        with (
            self.nothing_changes(PLAN_ID),
            self.assertLogs(WATCHER_LOGGER, level=logging.DEBUG) as logs,
        ):
            recovered = self.recover(self.watcher(), PLAN_ID)

        self.assertEqual(recovered, [])
        self.assertEqual(
            warnings(logs.records),
            [
                "Recovery: "
                + unsupported_line(PLAN_ID, "absent", "unversioned", NOT_EMITTED)
            ],
        )

    def test_every_unsupported_version_state_of_a_requested_plan_is_reported(self):
        for index, (case, (marker, shown, state)) in enumerate(
            UNSUPPORTED_MARKERS.items()
        ):
            plan_id = f"{PLAN_ID}_{index}"
            with self.subTest(case=case):
                self.put(supported_plan_document(plan_id, schema_version=marker))

                with (
                    self.nothing_changes(plan_id),
                    self.assertLogs(WATCHER_LOGGER, level=logging.WARNING) as logs,
                ):
                    recovered = self.recover(self.watcher(), plan_id)

                self.assertEqual(recovered, [])
                self.assertEqual(
                    warnings(logs.records),
                    [
                        "Recovery: "
                        + unsupported_line(plan_id, shown, state, NOT_EMITTED)
                    ],
                )

    def test_malformed_control_state_of_a_requested_plan_is_reported(self):
        for index, (case, (fields, code, reason)) in enumerate(MALFORMED.items()):
            plan_id = f"{PLAN_ID}_{index}"
            with self.subTest(case=case):
                self.put(supported_plan_document(plan_id, **fields))

                with (
                    self.nothing_changes(plan_id),
                    self.assertLogs(WATCHER_LOGGER, level=logging.WARNING) as logs,
                ):
                    recovered = self.recover(self.watcher(), plan_id)

                self.assertEqual(recovered, [])
                self.assertEqual(
                    warnings(logs.records),
                    ["Recovery: " + malformed_line(plan_id, code, reason)],
                )

    def test_scope_comes_before_any_refusal_report(self):
        """Judged silently, then scoped; only in-scope refusals are reported."""
        documents = {
            "mine": supported_plan_document(
                "mine", execution_authority="run_once", execution_owner=OWNER
            ),
            "mine, unsupported": supported_plan_document(
                "mine-unsupported",
                execution_authority="run_once",
                execution_owner=OWNER,
                schema_version=2,
            ),
            "mine, a draft": supported_plan_document(
                "mine-draft",
                execution_authority="run_once",
                execution_owner=OWNER,
                status="draft",
            ),
            "another owner's": supported_plan_document(
                "theirs",
                execution_authority="run_once",
                execution_owner="run_once:other",
            ),
            "another owner's, unsupported": supported_plan_document(
                "theirs-unsupported",
                execution_authority="run_once",
                execution_owner="run_once:other",
                schema_version=ABSENT,
            ),
            "the daemon's, malformed": supported_plan_document(
                "daemons-malformed", run_token="5"
            ),
        }
        for document in documents.values():
            self.put(document)
        plan_ids = [document["_id"] for document in documents.values()]

        with (
            self.nothing_changes(*plan_ids, emitted=1),
            self.assertLogs(WATCHER_LOGGER, level=logging.DEBUG) as logs,
        ):
            recovered = self.recover(
                self.watcher(authority="run_once", owner=OWNER), *plan_ids
            )

        self.assertEqual(recovered, [self.raw("mine")])
        self.assertEqual(self.events[0].payload["plan_doc_id"], "mine")
        self.assertEqual(
            warnings(logs.records),
            [
                "Recovery: "
                + unsupported_line("mine-unsupported", "2", "unsupported", NOT_EMITTED)
            ],
        )
        lines = [r.getMessage() for r in logs.records]
        # Eligible documents are counted before the scope filters, as before.
        self.assertIn(
            "Found 1 eligible plans for recovery (of 2 total eligible)", lines
        )
        self.assertIn(
            "Recovery: requested plan mine-draft is not eligible "
            "(Not eligible: status 'draft' (must be 'approved'))",
            lines,
        )
        for plan_id in ("theirs", "theirs-unsupported"):
            self.assertIn(
                f"Recovery: skipping {plan_id} (owner='run_once:other' != "
                f"filter='{OWNER}')",
                lines,
            )
        self.assertIn(
            "Recovery: skipping daemons-malformed (authority=daemon, filter=run_once)",
            lines,
        )

    def test_ordinary_skips_of_requested_plans_stay_debug(self):
        for index, (case, fields) in enumerate(ORDINARY_SKIPS.items()):
            plan_id = f"{PLAN_ID}_{index}"
            with self.subTest(case=case):
                self.put(supported_plan_document(plan_id, **fields))

                with (
                    self.nothing_changes(plan_id),
                    self.assertLogs(WATCHER_LOGGER, level=logging.DEBUG) as logs,
                ):
                    recovered = self.recover(self.watcher(), plan_id)

                self.assertEqual(recovered, [])
                self.assertEqual(warnings(logs.records), [])
                self.assertTrue(
                    any(
                        r.getMessage().startswith(
                            f"Recovery: requested plan {plan_id} is not eligible "
                            "(Not eligible:"
                        )
                        for r in logs.records
                    )
                )

    def test_a_requested_plan_without_authority_is_warned_about_even_if_ineligible(
        self,
    ):
        self.put(
            supported_plan_document(PLAN_ID, status="draft", execution_authority=ABSENT)
        )

        with (
            self.nothing_changes(PLAN_ID),
            self.assertLogs(WATCHER_LOGGER, level=logging.WARNING) as logs,
        ):
            recovered = self.recover(self.watcher(), PLAN_ID)

        self.assertEqual(recovered, [])
        self.assertEqual(
            warnings(logs.records),
            [f"Recovery: skipping {PLAN_ID} (missing execution_authority)"],
        )

    def test_an_eligible_requested_plan_is_still_emitted(self):
        self.put(supported_plan_document(PLAN_ID))

        with self.assertLogs(WATCHER_LOGGER, level=logging.DEBUG) as logs:
            recovered = self.recover(self.watcher(), PLAN_ID)

        self.assertEqual(recovered, [self.raw(PLAN_ID)])
        (event,) = self.events
        self.assertEqual(event.payload["plan_doc_id"], PLAN_ID)
        self.assertEqual(warnings(logs.records), [])
        self.assertIn(
            "Found 1 eligible plans for recovery (of 1 total eligible)",
            [r.getMessage() for r in logs.records],
        )


class ListingScenarios:
    """What a store's recovery listing reports, against one backend.

    Mixed into one StoreTestCase per plan-store backend.
    """

    def test_a_plan_without_schema_version_is_left_out_with_a_warning(self):
        self.put(supported_plan_document(PLAN_ID, schema_version=ABSENT))

        with (
            self.nothing_changes(PLAN_ID),
            self.assertLogs(self.store_logger, level=logging.INFO) as logs,
        ):
            listed = self.backend.plans.query_approved_pending()

        self.assertEqual(listed, [])
        self.assertEqual(
            warnings(logs.records),
            [unsupported_line(PLAN_ID, "absent", "unversioned", LEFT_OUT)],
        )
        self.assertIn(
            "Found 0 eligible plans (of 1 total) for recovery",
            [r.getMessage() for r in logs.records],
        )

    def test_every_unsupported_plan_is_left_out_with_one_warning(self):
        plan_ids = []
        expected = []
        for index, (marker, shown, state) in enumerate(UNSUPPORTED_MARKERS.values()):
            plan_id = f"{PLAN_ID}_{index}"
            self.put(supported_plan_document(plan_id, schema_version=marker))
            plan_ids.append(plan_id)
            expected.append(unsupported_line(plan_id, shown, state, LEFT_OUT))
        self.put(supported_plan_document("pln_valid"))

        with (
            self.nothing_changes(*plan_ids, "pln_valid"),
            self.assertLogs(self.store_logger, level=logging.INFO) as logs,
        ):
            listed = self.backend.plans.query_approved_pending()

        self.assertEqual(listed, [self.raw("pln_valid")])
        self.assertCountEqual(warnings(logs.records), expected)
        self.assertIn(
            "Found 1 eligible plans (of 4 total) for recovery",
            [r.getMessage() for r in logs.records],
        )

    def test_every_other_refusal_is_silent(self):
        refused = {
            **{case: fields for case, (fields, _, _) in MALFORMED.items()},
            **ORDINARY_SKIPS,
        }
        plan_ids = []
        for index, fields in enumerate(refused.values()):
            plan_id = f"{PLAN_ID}_{index}"
            self.put(supported_plan_document(plan_id, **fields))
            plan_ids.append(plan_id)
        self.put(supported_plan_document("pln_valid"))

        with (
            self.nothing_changes(*plan_ids, "pln_valid"),
            self.assertLogs(level=logging.DEBUG) as logs,
        ):
            listed = self.backend.plans.query_approved_pending()

        self.assertEqual(listed, [self.raw("pln_valid")])
        self.assertEqual(warnings(logs.records), [])
        self.assertEqual(
            [r.getMessage() for r in logs.records if r.name == self.store_logger],
            [f"Found 1 eligible plans (of {len(refused) + 1} total) for recovery"],
        )


class TestRefusalReportingSQLite(
    ChangePathScenarios, ScopedRecoveryScenarios, ListingScenarios, StoreTestCase
):
    """Refusal reporting against the SQLite plan store."""

    def test_the_listing_names_a_plan_by_its_stored_id(self):
        """The listing has only the bodies, so it shows each body's ``_id``."""
        assert isinstance(self.backend, SQLiteBackend)
        for index, (stored_id, shown) in enumerate(
            (("pln_elsewhere", "'pln_elsewhere'"), (5, "5"))
        ):
            plan_id = f"{PLAN_ID}_{index}"
            with self.subTest(stored_id=shown):
                document = supported_plan_document(plan_id, schema_version=ABSENT)
                document["_id"] = stored_id
                self.backend.internal.put_document(
                    "plans", plan_id, document, bump_plan_seq=True
                )

                with self.assertLogs(self.store_logger, level=logging.WARNING) as logs:
                    self.backend.plans.query_approved_pending()

                self.assertIn(
                    f"Plan {shown} is not executed: its schema_version is absent",
                    warnings(logs.records)[-1],
                )


class TestRefusalReportingCouch(
    ChangePathScenarios, ScopedRecoveryScenarios, ListingScenarios, StoreTestCase
):
    """Refusal reporting against the CouchDB plan store."""

    backend_type = CouchBackend

    def test_design_documents_are_skipped_silently(self):
        assert isinstance(self.backend, CouchBackend)
        self.backend.server.put_document(
            db="yggdrasil_plans", doc_id="_design/views", document={"views": {}}
        )
        self.put(supported_plan_document("pln_valid"))

        with (
            self.nothing_changes("_design/views", "pln_valid"),
            self.assertLogs(level=logging.DEBUG) as logs,
        ):
            listed = self.backend.plans.query_approved_pending()

        self.assertEqual(listed, [self.raw("pln_valid")])
        self.assertEqual(warnings(logs.records), [])
        self.assertIn(
            "Found 1 eligible plans (of 2 total) for recovery",
            [r.getMessage() for r in logs.records],
        )

    def test_the_listing_names_a_plan_by_its_row_id(self):
        """A body whose ``_id`` is not its key is reported under its key."""
        assert isinstance(self.backend, CouchBackend)
        self.put(supported_plan_document(PLAN_ID, schema_version=ABSENT))
        rows = self.backend.server.post_all_docs(
            db="yggdrasil_plans", include_docs=True
        ).get_result()["rows"]
        rows[0]["doc"]["_id"] = "pln_elsewhere"

        with (
            patch.object(
                self.backend.server,
                "post_all_docs",
                return_value=Mock(get_result=Mock(return_value={"rows": rows})),
            ),
            self.assertLogs(self.store_logger, level=logging.WARNING) as logs,
        ):
            self.backend.plans.query_approved_pending()

        self.assertEqual(
            warnings(logs.records),
            [unsupported_line(PLAN_ID, "absent", "unversioned", LEFT_OUT)],
        )


# ----- idle polls -----


class PollRecorder:
    """Records each poll of a change source, from whichever thread makes it.

    Attributes:
        polls: The cursor each poll asked after, as text, and how many changes
            it returned, in order.
    """

    def __init__(self) -> None:
        self.polls: list[tuple[str, int]] = []
        self._condition = threading.Condition()

    def polled(self, cursor: object, changes: int) -> None:
        """Record a poll that asked after cursor and returned changes."""
        with self._condition:
            self.polls.append((str(cursor), changes))
            self._condition.notify_all()

    def count(self) -> int:
        """Return how many polls there have been so far."""
        with self._condition:
            return len(self.polls)

    def wait_until_idle(self, since: int) -> list[str]:
        """Wait for a poll after since that returns a change, then idle polls.

        Args:
            since: The number of polls already made before the change.

        Returns:
            list[str]: The cursors the idle polls asked after.

        Raises:
            AssertionError: If that does not happen within WAIT.
        """

        def idle_cursors() -> list[str] | None:
            later = self.polls[since:]
            changed = [i for i, (_, changes) in enumerate(later) if changes]
            if not changed:
                return None
            idle = later[changed[-1] + 1 :]
            if len(idle) < IDLE_POLLS or any(changes for _, changes in idle):
                return None
            return [cursor for cursor, _ in idle]

        with self._condition:
            if not self._condition.wait_for(lambda: idle_cursors() is not None, WAIT):
                raise AssertionError(f"no idle polls after the change: {self.polls}")
            cursors = idle_cursors()
            assert cursors is not None
            return cursors


class IdlePollTestCase(unittest.TestCase):
    """A real change source under PlanWatcher.start(), with every poll recorded.

    Subclasses provide the backend: the plan store, the change source, and
    how a document is stored as a change.

    Attributes:
        polls: The change source's polls.
    """

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.directory = Path(temp_dir.name)
        self.polls = PollRecorder()

    def plan_store(self) -> PlanStore:
        """Return the plan store the watcher uses."""
        raise NotImplementedError

    def change_source(self) -> PlanChangeSource:
        """Return the change source the watcher streams from."""
        raise NotImplementedError

    def head(self) -> str:
        """Return the cursor of the latest change, before any is made here."""
        raise NotImplementedError

    def change(self, document: dict[str, Any]) -> str:
        """Store document as an actual change to it, and return its sequence."""
        raise NotImplementedError

    def stored(self, plan_id: str) -> dict[str, Any]:
        """Return the plan document as stored."""
        doc = self.plan_store().fetch_plan(plan_id)
        assert doc is not None, f"plan {plan_id} is not stored"
        return doc

    def assert_reported_once_per_change(
        self, document: dict[str, Any], mention: str
    ) -> None:
        """Assert a refused plan is reported once per change, never per poll.

        The plan is stored, the watcher starts from the cursor before it, and
        idle polls follow. Then an unrelated edit changes the document, and
        idle polls follow again.

        Args:
            document: The refused plan document.
            mention: Text its refusal's warning contains.
        """
        plan_id = document["_id"]
        checkpoints = InMemoryCheckpointStore()
        checkpoints.save(
            Checkpoint(backend_key=PlanWatcher.CHECKPOINT_KEY, value=self.head())
        )
        events: list[YggdrasilEvent] = []
        watcher = PlanWatcher(
            on_event=events.append,
            poll_interval_sec=0,
            plan_store=self.plan_store(),
            change_source=self.change_source(),
            checkpoint_store=checkpoints,
            execution_authority_filter="daemon",
        )

        def refusals() -> list[str]:
            return [
                line
                for line in warnings(logs.records)
                if plan_id in line and mention in line
            ]

        def saved() -> object:
            checkpoint = checkpoints.load(PlanWatcher.CHECKPOINT_KEY)
            return None if checkpoint is None else checkpoint.value

        async def scenario() -> None:
            first = self.change(document)
            task = asyncio.create_task(watcher.start())
            try:
                idle = await asyncio.to_thread(self.polls.wait_until_idle, 0)
                # The cursor moved past the refused change, which was saved
                # and reported once; the idle polls after it reported nothing.
                self.assertEqual(set(idle), {first})
                self.assertEqual(saved(), first)
                self.assertEqual(len(refusals()), 1)

                since = self.polls.count()
                edited = {**self.stored(plan_id), "notes": "an unrelated edit"}
                second = self.change(edited)
                idle = await asyncio.to_thread(self.polls.wait_until_idle, since)
                self.assertEqual(set(idle), {second})
                self.assertEqual(saved(), second)
                self.assertEqual(len(refusals()), 2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        with self.assertLogs(WATCHER_LOGGER, level=logging.WARNING) as logs:
            run_bounded(scenario())

        self.assertEqual(events, [])
        self.assertEqual(len(refusals()), 2)
        self.assertEqual(len(warnings(logs.records)), 2)


class IdlePollScenarios:
    """No refusal is repeated on an idle poll, against one change source.

    Mixed into one IdlePollTestCase per backend.
    """

    def test_an_unsupported_plan_is_reported_once_per_change(self):
        self.assert_reported_once_per_change(
            supported_plan_document(PLAN_ID, schema_version=ABSENT),
            "its schema_version is absent, so it is unversioned",
        )

    def test_malformed_control_state_is_reported_once_per_change(self):
        self.assert_reported_once_per_change(
            supported_plan_document(PLAN_ID, run_token="5"),
            "its control state is malformed (invalid_tokens)",
        )


class TestIdlePollsSQLite(IdlePollScenarios, IdlePollTestCase):
    """Idle polls of the SQLite change source."""

    def setUp(self) -> None:
        super().setUp()
        self.backend = SQLiteBackend(self, self.directory)
        internal = self.backend.internal
        real = internal.plan_changes_after

        def recorded(
            cursor: int, limit: int = 500
        ) -> list[tuple[int, str, dict[str, Any] | None, bool]]:
            rows = real(cursor, limit)
            self.polls.polled(cursor, len(rows))
            return rows

        patcher = patch.object(internal, "plan_changes_after", side_effect=recorded)
        patcher.start()
        self.addCleanup(patcher.stop)

    def plan_store(self) -> PlanStore:
        return self.backend.plans

    def change_source(self) -> PlanChangeSource:
        return SQLitePlanChangeSource(self.backend.internal)

    def head(self) -> str:
        return str(self.backend.internal.current_plan_seq())

    def change(self, document: dict[str, Any]) -> str:
        self.backend.raw_put(document)
        return str(self.backend.internal.current_plan_seq())


class ScriptedChangesHandler:
    """A CouchDB handler whose ``_changes`` feed is scripted.

    Documents are read from the real plan store. Each poll returns the next
    published batch, or else an empty batch carrying the last sequence, the
    way CouchDB answers when nothing changed.

    Attributes:
        plans: The plan store documents are read from.
        polls: Where each poll is recorded.
    """

    def __init__(self, plans: PlanDBManager, polls: PollRecorder) -> None:
        self.plans = plans
        self.polls = polls
        self.db_name = plans.db_name
        self._batches: list[ChangesBatch] = []
        self._published = 0
        self._last_seq = "0"
        self._lock = threading.Lock()

    def publish(self, doc_id: str) -> str:
        """Make the next poll return a change to doc_id; return its sequence."""
        with self._lock:
            self._published += 1
            seq = f"{self._published}-g1"
            self._last_seq = seq
            self._batches.append(
                ChangesBatch(
                    rows=[ChangesRow(id=doc_id, seq=seq)], last_seq=seq, pending=0
                )
            )
            return seq

    def fetch_changes_raw(
        self,
        *,
        since: str | int | None = None,
        feed: str = "normal",
        limit: int | None = None,
        timeout_ms: int = 30_000,
    ) -> ChangesBatch:
        """Return the next published batch, or an empty one."""
        with self._lock:
            batch = (
                self._batches.pop(0)
                if self._batches
                else ChangesBatch(rows=[], last_seq=self._last_seq, pending=0)
            )
        self.polls.polled(since, len(batch.rows))
        return batch

    def fetch_document_by_id(self, doc_id: str) -> dict[str, Any] | None:
        """Read the document from the plan store."""
        return self.plans.fetch_document_by_id(doc_id)


class TestIdlePollsCouch(IdlePollScenarios, IdlePollTestCase):
    """Idle polls of the CouchDB change source, with a scripted ``_changes`` feed."""

    def setUp(self) -> None:
        super().setUp()
        self.backend = CouchBackend(self, self.directory)
        assert isinstance(self.backend.plans, PlanDBManager)
        self.handler = ScriptedChangesHandler(self.backend.plans, self.polls)

    def plan_store(self) -> PlanStore:
        return self.backend.plans

    def change_source(self) -> PlanChangeSource:
        return CouchPlanChangeSource(
            ChangesFetcher(db_handler=self.handler, include_docs=True)
        )

    def head(self) -> str:
        return "0"

    def change(self, document: dict[str, Any]) -> str:
        if "_rev" in document:
            self.backend.conditional_put(document)
        else:
            self.backend.raw_put(document)
        return self.handler.publish(document["_id"])


if __name__ == "__main__":
    unittest.main()
