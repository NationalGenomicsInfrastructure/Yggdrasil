"""The plan summary: what planning shows of a plan document it would replace.

``plan_summary_from_document`` is a projection of the shared reader's
``read_plan``, which reads the document under the ID it was requested by.
Nothing is filled in: a value the reader does not recognise is None, never a
display default, and ``problems`` carries the reader's problems. The stores
return a summary for every stored document, readable or not, and None only
when they hold no document under the ID, because planning takes None for an
empty slot and fills it without asking for ``--force``. Covered here:

- the projection over regression-corpus documents, and over such documents
  with one field changed;
- documents the reader cannot use: of no supported family, stored under
  another ID, or not an object;
- both plan stores: SQLite on a temporary file, and ``PlanDBManager`` on an
  in-memory client that enforces CouchDB's revision rules, including
  CouchDB's malformed success responses and its failed reads;
- the "Existing Plan Found" box of ``YggdrasilCore._check_plan_overwrite``.

Planning over an existing document through the real planning callers is
covered in ``tests/test_planning_replace_rule.py``.
"""

from __future__ import annotations

import copy
import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import MagicMock, patch

from requests.exceptions import ConnectionError as RequestsConnectionError

from tests.contract.read_support import plan_document
from tests.contract.support import changed
from tests.execution_support import CouchBackend, SQLiteBackend
from tests.plan_store_support import (
    ABSENT,
    SCOPE,
    FakeApiException,
    make_plan,
)
from yggdrasil.contract import read
from yggdrasil.daemon.core import YggdrasilCore
from yggdrasil.daemon.singleton import SingletonMeta
from yggdrasil.storage.plan_documents import plan_summary_from_document

PLAN_ID = "pln_test_P1_v1"
REALM = "test_realm"

# The ID the corpus's plan documents are stored under.
CORPUS_ID = "pln_lifecycle"

# Every key of a summary, whatever the document.
SUMMARY_KEYS = {
    "status",
    "execution_authority",
    "execution_owner",
    "updated_at",
    "realm",
    "run_token",
    "executed_run_token",
    "plan_generation",
    "last_finalized_outcome",
    "problems",
}

# The values of a summary that nothing was derived for.
NOTHING_DERIVED = dict.fromkeys(SUMMARY_KEYS - {"problems"})

# Sound corpus documents, each with the outcome its summary reports: that of
# its last finalized execution, which may belong to an earlier request.
SOUND = (
    ("draft", None),
    ("approved_finalized", "succeeded"),
    ("run_once_finalized", "succeeded"),
    ("rerun_requested", "succeeded"),
)

# Corpus documents of no supported family, each with its problem's code.
NO_SUPPORTED_FAMILY = (
    ("pre_contract", "unversioned"),
    ("schema_version_string", "invalid_version"),
    ("schema_version_2", "unsupported_version"),
)

# Bodies that are not JSON objects, by name.
NOT_OBJECTS: tuple[tuple[str, Any], ...] = (
    ("JSON null", None),
    ("an empty list", []),
    ("a list holding the document", [plan_document("draft")]),
    ("text", "not found"),
    ("a number", 404),
    ("a boolean", True),
)

# The required top-level fields of a plan document that a summary shows.
CONTROL_FIELDS = (
    "status",
    "execution_authority",
    "execution_owner",
    "updated_at",
    "realm",
    "run_token",
    "executed_run_token",
)


def summarise(document: object, plan_id: str = CORPUS_ID) -> dict[str, Any]:
    """Return the summary of document, requested under plan_id."""
    return plan_summary_from_document(document, plan_id=plan_id)


def problem_sites(summary: dict[str, Any]) -> list[tuple[str, str]]:
    """Return each of a summary's problems as (path, code), in reported order."""
    return [(problem.path, problem.code) for problem in summary["problems"]]


class SummaryAssertions(unittest.TestCase):
    """Assertions shared by the projection and store tests."""

    def assert_nothing_derived(
        self, summary: dict[str, Any] | None, *, path: str, code: str
    ) -> None:
        """Assert a summary exists, every value is None, and one problem says why.

        Args:
            summary: The summary.
            path: Where the problem is, ``""`` for the document itself.
            code: The problem's code.
        """
        assert summary is not None, "a stored document has no summary"
        self.assertEqual(
            {**summary, "problems": None}, {**NOTHING_DERIVED, "problems": None}
        )
        self.assertEqual(problem_sites(summary), [(path, code)])


class TestProjection(SummaryAssertions):
    """The summary of a sound document shows what it records."""

    def test_a_sound_document_is_shown_as_stored(self):
        for name, outcome in SOUND:
            with self.subTest(document=name):
                doc = plan_document(name)

                summary = summarise(doc, doc["_id"])

                self.assertEqual(
                    summary,
                    {
                        "status": doc["status"],
                        "execution_authority": doc["execution_authority"],
                        "execution_owner": doc["execution_owner"],
                        "updated_at": doc["updated_at"],
                        "realm": doc["realm"],
                        "run_token": doc["run_token"],
                        "executed_run_token": doc["executed_run_token"],
                        "plan_generation": doc["plan_generation"],
                        "last_finalized_outcome": outcome,
                        "problems": (),
                    },
                )

    def test_a_run_once_plan_shows_its_owner(self):
        summary = summarise(plan_document("run_once_finalized"), "pln_run_once")

        self.assertEqual(summary["execution_authority"], "run_once")
        self.assertTrue(summary["execution_owner"].startswith("run_once:"))

    def test_an_earlier_requests_outcome_is_shown_beside_the_pending_request(self):
        doc = plan_document("rerun_requested")

        summary = summarise(doc)

        # The usable record belongs to request 0; request 1 is pending.
        request = read.read_plan(doc, plan_id=CORPUS_ID).request
        assert request is not None
        self.assertIs(request.finalized_is_current, False)
        self.assertEqual((summary["run_token"], summary["executed_run_token"]), (1, 0))
        self.assertEqual(summary["last_finalized_outcome"], "succeeded")

    def test_every_summary_has_the_same_keys(self):
        documents = {
            **{name: plan_document(name) for name, _ in SOUND},
            **{name: plan_document(name) for name, _ in NO_SUPPORTED_FAMILY},
            **dict(NOT_OBJECTS),
        }
        for name, document in documents.items():
            with self.subTest(document=name):
                self.assertEqual(set(summarise(document)), SUMMARY_KEYS)
                self.assertEqual(
                    set(summarise(document, "pln_requested")), SUMMARY_KEYS
                )


class TestNoDefaults(SummaryAssertions):
    """A value the reader does not recognise is None, never a display default."""

    def setUp(self) -> None:
        self.draft = plan_document("draft")
        self.shown = summarise(self.draft)

    def assert_only_unknown(
        self, summary: dict[str, Any], field: str, problems: list[tuple[str, str]]
    ) -> None:
        """Assert only field differs from the draft's summary, as None, with problems."""
        self.assertEqual(
            {**summary, "problems": ()}, {**self.shown, field: None, "problems": ()}
        )
        self.assertEqual(problem_sites(summary), problems)

    def test_an_absent_field_is_none_and_named_by_a_problem(self):
        codes = {"run_token": "invalid_tokens", "executed_run_token": "invalid_tokens"}
        for field in CONTROL_FIELDS:
            with self.subTest(field=field):
                summary = summarise(changed(self.draft, field, ABSENT))

                self.assert_only_unknown(
                    summary, field, [(field, codes.get(field, "missing_field"))]
                )

    def test_a_value_the_reader_cannot_use_is_none_and_named_by_a_problem(self):
        cases = (
            ("run token as text", "run_token", "5", "invalid_tokens"),
            ("run token as a boolean", "run_token", True, "invalid_tokens"),
            ("executed token as a float", "executed_run_token", -1.0, "invalid_tokens"),
            ("status as a number", "status", 1, "wrong_type"),
            ("authority as a list", "execution_authority", ["daemon"], "wrong_type"),
            ("owner as a number", "execution_owner", 7, "wrong_type"),
            ("empty realm", "realm", "", "wrong_type"),
            ("updated_at as a number", "updated_at", 20261022, "bad_timestamp"),
            (
                "updated_at naming no instant",
                "updated_at",
                "yesterday",
                "bad_timestamp",
            ),
        )
        for case, field, value, code in cases:
            with self.subTest(case=case):
                summary = summarise(changed(self.draft, field, value))

                self.assert_only_unknown(summary, field, [(field, code)])

    def test_each_token_is_shown_when_valid_on_its_own(self):
        # A run token below the executed one makes the pair invalid, but
        # each token is still a valid token.
        summary = summarise(changed(self.draft, "executed_run_token", 3))

        self.assertEqual((summary["run_token"], summary["executed_run_token"]), (0, 3))
        self.assertEqual(
            problem_sites(summary), [("executed_run_token", "invalid_tokens")]
        )

    def test_a_word_the_contract_does_not_know_is_none_without_a_problem(self):
        for field, word in (("status", "pending"), ("execution_authority", "cron")):
            with self.subTest(field=field):
                summary = summarise(changed(self.draft, field, word))

                self.assert_only_unknown(summary, field, [])

    def test_no_owner_is_none_without_a_problem(self):
        for case, value in (("null", None), ("empty text", "")):
            with self.subTest(owner=case):
                summary = summarise(changed(self.draft, "execution_owner", value))

                self.assert_only_unknown(summary, "execution_owner", [])

    def test_updated_at_is_shown_as_recorded_when_it_names_an_instant(self):
        for case, value in (
            ("an earlier release's spelling", "2026-10-22T09:00:00+00:00"),
            ("a canonical fraction", "2026-10-22T09:00:00.5Z"),
        ):
            with self.subTest(updated_at=case):
                summary = summarise(changed(self.draft, "updated_at", value))

                self.assertEqual(summary["updated_at"], value)

    def test_an_outcome_is_shown_only_from_a_record_that_agrees_with_its_report(self):
        finalized = plan_document("approved_finalized")
        disagreeing = changed(
            finalized, "last_finalized_execution.plan_generation", "elsewhere"
        )

        summary = summarise(disagreeing)

        self.assertIsNone(summary["last_finalized_outcome"])
        self.assertEqual(
            problem_sites(summary),
            [("last_finalized_execution", "record_report_disagree")],
        )

    def test_an_outcome_the_contract_does_not_know_is_none(self):
        finalized = plan_document("approved_finalized")
        for path in ("last_finalized_execution", "last_finalized_execution.report"):
            finalized = changed(finalized, f"{path}.outcome", "exploded")

        summary = summarise(finalized)

        self.assertIsNone(summary["last_finalized_outcome"])
        self.assertEqual(summary["problems"], ())


class TestUnusableDocuments(SummaryAssertions):
    """A document the reader cannot use has a summary that says why."""

    def test_a_document_of_no_supported_family_has_nothing_derived(self):
        for name, code in NO_SUPPORTED_FAMILY:
            with self.subTest(document=name):
                doc = plan_document(name)

                summary = summarise(doc)

                self.assert_nothing_derived(summary, path="schema_version", code=code)
                self.assertEqual(
                    summary["problems"], read.read_plan(doc, plan_id=CORPUS_ID).problems
                )

    def test_a_body_stored_under_another_id_has_nothing_derived(self):
        summary = summarise(plan_document("draft"), "pln_requested")

        self.assert_nothing_derived(summary, path="_id", code="identity_mismatch")
        self.assertIn("pln_requested", summary["problems"][0].message)

    def test_a_body_that_is_not_an_object_has_nothing_derived(self):
        for name, body in NOT_OBJECTS:
            with self.subTest(body=name):
                summary = summarise(body)

                self.assert_nothing_derived(summary, path="", code="wrong_type")

    def test_an_empty_object_is_unversioned(self):
        self.assert_nothing_derived(
            summarise({}), path="schema_version", code="unversioned"
        )


class SQLiteSummaries(SQLiteBackend):
    """The SQLite plan store, with control over what its next read returns."""

    def serve(self, body: Any) -> None:
        """Have the store's next document read return body, exactly as given.

        SQLite returns every stored body with ``_rev`` added, so it never
        returns an empty object; this checks that the store does not rely on
        that.
        """
        real = self.internal.get_document

        def serve_once(namespace: str, doc_id: str) -> Any:
            self.internal.get_document = real  # type: ignore[method-assign]
            return copy.deepcopy(body)

        self.internal.get_document = serve_once  # type: ignore[method-assign]

    def put_under(self, doc_id: str, body: dict[str, Any]) -> None:
        """Store body under doc_id, keeping the ``_id`` it carries."""
        self.internal.put_document("plans", doc_id, body, bump_plan_seq=True)


class CouchSummaries(CouchBackend):
    """The CouchDB plan store, with control over what its next read returns."""

    def serve(self, body: Any) -> None:
        """Have the store's next document read answer body, with a success status."""
        self.server.read_responses.append(body)

    def put_under(self, doc_id: str, body: dict[str, Any]) -> None:
        """Answer the next read of doc_id with body, keeping the ``_id`` it carries.

        CouchDB stores every document under its own ``_id``, so another
        document's body reaches a read only as a response, such as one to a
        misrouted request.
        """
        self.serve(body)


class StoreTestCase(SummaryAssertions):
    """A plan store whose slots tests fill directly.

    Subclasses choose the backend with ``backend_type``.
    """

    backend_type: type[SQLiteSummaries] | type[CouchSummaries] = SQLiteSummaries

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.backend = self.backend_type(self, Path(temp_dir.name))
        self.plans = self.backend.plans


class StoreScenarios:
    """``get_plan_summary`` over each kind of slot, against one backend.

    Mixed into one test case per plan-store backend.
    """

    def test_an_empty_slot_has_no_summary(self):
        self.assertIsNone(self.plans.get_plan_summary(PLAN_ID))

    def test_a_deleted_document_has_no_summary(self):
        self.plans.save_plan(make_plan(PLAN_ID), REALM, dict(SCOPE))
        self.assertTrue(self.plans.delete_plan(PLAN_ID))

        self.assertIsNone(self.plans.get_plan_summary(PLAN_ID))

    def test_a_planned_document_is_summarised(self):
        self.plans.save_plan(make_plan(PLAN_ID), REALM, dict(SCOPE), auto_run=True)

        summary = self.plans.get_plan_summary(PLAN_ID)

        stored = self.plans.fetch_plan(PLAN_ID)
        assert stored is not None
        self.assertEqual(
            summary,
            {
                "status": "approved",
                "execution_authority": "daemon",
                "execution_owner": None,
                "updated_at": stored["updated_at"],
                "realm": REALM,
                "run_token": 0,
                "executed_run_token": -1,
                "plan_generation": stored["plan_generation"],
                "last_finalized_outcome": None,
                "problems": (),
            },
        )

    def test_a_stored_document_of_no_supported_family_has_a_summary(self):
        for name, code in NO_SUPPORTED_FAMILY:
            with self.subTest(document=name):
                slot_id = f"pln_{name}"
                self.backend.raw_put(changed(plan_document(name), "_id", slot_id))

                summary = self.plans.get_plan_summary(slot_id)

                self.assert_nothing_derived(summary, path="schema_version", code=code)

    def test_an_empty_body_has_a_summary(self):
        self.backend.serve({})

        summary = self.plans.get_plan_summary(PLAN_ID)

        self.assert_nothing_derived(summary, path="schema_version", code="unversioned")

    def test_a_body_under_another_id_has_a_summary_that_says_so(self):
        self.backend.put_under(PLAN_ID, plan_document("draft"))

        summary = self.plans.get_plan_summary(PLAN_ID)

        self.assert_nothing_derived(summary, path="_id", code="identity_mismatch")


class TestSQLiteSummaries(StoreScenarios, StoreTestCase):
    """``get_plan_summary`` on SQLitePlanStore."""


class TestCouchSummaries(StoreScenarios, StoreTestCase):
    """``get_plan_summary`` on PlanDBManager on a revision-enforcing fake client."""

    backend_type = CouchSummaries

    def test_a_success_response_that_is_not_an_object_has_a_summary(self):
        self.plans.save_plan(make_plan(PLAN_ID), REALM, dict(SCOPE))
        for name, body in NOT_OBJECTS:
            with self.subTest(body=name):
                self.backend.serve(body)

                summary = self.plans.get_plan_summary(PLAN_ID)

                self.assert_nothing_derived(summary, path="", code="wrong_type")

    def test_a_failed_read_reaches_the_caller_unchanged(self):
        self.plans.save_plan(make_plan(PLAN_ID), REALM, dict(SCOPE))
        for name, error in (
            ("server error", FakeApiException(500, "Internal Server Error")),
            ("unauthorized", FakeApiException(401, "unauthorized")),
            ("connection refused", RequestsConnectionError("refused")),
        ):
            with self.subTest(error=name):
                self.backend.server.fail_next_read = error

                with (
                    self.assertLogs(self.plans._logger, level=logging.ERROR),
                    self.assertRaises(type(error)) as raised,
                ):
                    self.plans.get_plan_summary(PLAN_ID)

                self.assertIs(raised.exception, error)


# The lines of the box that show the values of an unusable document.
UNKNOWN_LINES = (
    "│ Status:     unknown",
    "│ Origin:     unknown",
    "│ Updated:    unknown",
    "│ Run Token:  unknown (executed: unknown)",
)


class TestOverwriteBox(unittest.TestCase):
    """The "Existing Plan Found" box shows a summary, and planning stops without force."""

    def setUp(self) -> None:
        SingletonMeta._instances.clear()
        self.addCleanup(SingletonMeta._instances.clear)
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        for patcher in (
            patch("yggdrasil.daemon.core.build_internal_storage"),
            patch("yggdrasil.daemon.core.OpsConsumerService"),
            patch("yggdrasil.daemon.core.FileSpoolEmitter"),
            patch("yggdrasil.daemon.core.Engine"),
            patch("yggdrasil.daemon.core.YggdrasilCore._init_db_managers"),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.core = YggdrasilCore({"work_root": temp_dir.name})
        self.core.plan_dbm = MagicMock()

    def check(
        self, summary: dict[str, Any], *, force: bool = False
    ) -> tuple[bool, list[str], list[logging.LogRecord]]:
        """Run the overwrite check over summary; return its decision and the box.

        Args:
            summary: What the store returns for the plan's slot.
            force: Whether ``--force`` was given.

        Returns:
            tuple[bool, list[str], list[logging.LogRecord]]: Whether planning
            may go on, the lines of the one box logged, and every record the
            check logged at INFO or above.
        """
        self.core.plan_dbm.get_plan_summary.return_value = summary
        with self.assertLogs(self.core._logger, level=logging.INFO) as logs:
            plan_id, proceed = self.core._check_plan_overwrite(PLAN_ID, force=force)

        self.assertEqual(plan_id, PLAN_ID)
        boxes = [
            record
            for record in logs.records
            if "Existing Plan Found" in record.getMessage()
        ]
        self.assertEqual(len(boxes), 1)
        self.assertEqual(boxes[0].levelno, logging.WARNING)
        return proceed, boxes[0].getMessage().splitlines(), logs.records

    def test_an_unusable_document_is_shown_as_unknown_with_its_problem(self):
        summary = summarise(plan_document("pre_contract"))

        proceed, box, _ = self.check(summary)

        self.assertFalse(proceed)
        self.assertIn(f"│ Plan ID:    {PLAN_ID}", box)
        for line in UNKNOWN_LINES:
            self.assertIn(line, box)
        (problem,) = summary["problems"]
        self.assertIn(f"│ Problems:   {problem.message}", box)

    def test_a_sound_document_is_shown_without_a_problems_line(self):
        proceed, box, _ = self.check(summarise(plan_document("draft")))

        self.assertFalse(proceed)
        for line in (
            "│ Status:     draft",
            "│ Origin:     daemon",
            "│ Updated:    2026-10-22T09:00:00Z",
            "│ Run Token:  0 (executed: -1)",
        ):
            self.assertIn(line, box)
        self.assertFalse([line for line in box if "Problems" in line])

    def test_only_the_values_not_recognised_are_unknown(self):
        summary = summarise(changed(plan_document("draft"), "run_token", "5"))

        _, box, _ = self.check(summary)

        self.assertIn("│ Status:     draft", box)
        self.assertIn("│ Run Token:  unknown (executed: -1)", box)
        (problem,) = summary["problems"]
        self.assertIn(f"│ Problems:   {problem.message}", box)

    def test_at_most_three_problems_are_listed_and_the_rest_counted(self):
        draft = plan_document("draft")
        for count, more in (
            (1, ""),
            (3, ""),
            (4, " (and 1 more)"),
            (7, " (and 4 more)"),
        ):
            with self.subTest(problems=count):
                doc = draft
                for field in CONTROL_FIELDS[:count]:
                    doc = changed(doc, field, ABSENT)
                summary = summarise(doc)
                messages = [problem.message for problem in summary["problems"]]
                self.assertEqual(len(messages), count)

                _, box, _ = self.check(summary)

                problem_lines = [line for line in box if "Problems" in line]
                self.assertEqual(
                    problem_lines,
                    [f"│ Problems:   {'; '.join(messages[:3])}{more}"],
                )

    def test_a_summary_without_a_problems_key_is_shown(self):
        _, box, _ = self.check(
            {
                "status": "approved",
                "execution_authority": "run_once",
                "updated_at": "2026-10-26T12:00:00Z",
                "run_token": 1,
                "executed_run_token": None,
            }
        )

        self.assertIn("│ Run Token:  1 (executed: unknown)", box)
        self.assertFalse([line for line in box if "Problems" in line])

    def test_without_force_planning_stops_and_with_force_it_goes_on(self):
        summary = summarise(plan_document("schema_version_2"))

        proceed, _, records = self.check(summary)
        self.assertFalse(proceed)
        self.assertEqual(records[-1].levelno, logging.ERROR)
        self.assertEqual(
            records[-1].getMessage(),
            f"Plan '{PLAN_ID}' already exists. Use --force to overwrite.",
        )

        proceed, box, records = self.check(summary, force=True)
        self.assertTrue(proceed)
        self.assertTrue([line for line in box if "Problems" in line])
        self.assertEqual(records[-1].levelno, logging.INFO)
        self.assertIn("--force specified", records[-1].getMessage())
        self.assertFalse([r for r in records if r.levelno >= logging.ERROR])

    def test_an_empty_slot_shows_no_box(self):
        self.core.plan_dbm.get_plan_summary.return_value = None

        with self.assertNoLogs(self.core._logger, level=logging.WARNING):
            plan_id, proceed = self.core._check_plan_overwrite(PLAN_ID, force=False)

        self.assertEqual((plan_id, proceed), (PLAN_ID, True))


if __name__ == "__main__":
    unittest.main()
