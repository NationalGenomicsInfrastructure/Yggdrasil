"""Tests for where a file spool keeps each attempt, and reading and reserving it.

The history the execution-ID allocator reads is the names of a plan's attempt
directories, never their contents; the reserver creates an attempt's
directory exclusively. Both work only in the spool they were given.
"""

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
    STEP_BLOCKED_EVENT,
    STEP_PART_ESCAPE,
    SpoolAttemptDirectories,
    SpoolAttemptHistory,
    attempt_dir,
    attempt_spool_path,
    attempts_dir,
    is_record_key,
    record_filename,
    step_event_filename,
    step_events_dir,
    step_id_of_dir,
)

REALM = "test_realm"
PLAN_ID = "pln_records"
FIRST = "20260921T120500000000Z_aaaa"
SECOND = "20260921T120600000000Z_bbbb"


class TestNames(unittest.TestCase):
    """Record names are fixed; step events are numbered per step."""

    def test_plan_records_have_fixed_names(self):
        self.assertEqual(
            record_filename(ATTEMPT_STARTED_EVENT), "plan_attempt_started.json"
        )
        self.assertEqual(
            record_filename(ATTEMPT_REPORT_EVENT), "plan_attempt_report.json"
        )

    def test_step_event_names_are_numbered_and_dots_become_underscores(self):
        self.assertEqual(
            step_event_filename(1, "step.started"), "0001_step_started.json"
        )
        self.assertEqual(
            step_event_filename(12, STEP_BLOCKED_EVENT), "0012_step_blocked.json"
        )
        self.assertEqual(
            step_event_filename(3, "data_access.write.succeeded"),
            "0003_data_access_write_succeeded.json",
        )

    def test_numbers_past_four_digits_take_more_digits(self):
        self.assertEqual(
            step_event_filename(10000, "step.progress"), "10000_step_progress.json"
        )

    def test_only_ids_usable_as_a_directory_name_are_record_keys(self):
        self.assertTrue(is_record_key(FIRST))
        self.assertTrue(is_record_key("exec_sched_plan"))
        for key in ("", ".", "..", "a/b", "../escape", "a\\b", None, 7):
            with self.subTest(key=key):
                self.assertFalse(is_record_key(key))


class TestPaths(unittest.TestCase):
    """Every attempt has its own directory; every step its own stream in it."""

    def test_attempt_and_step_directories(self):
        root = Path("/spool")
        attempt = attempt_dir(root, REALM, PLAN_ID, FIRST)

        self.assertEqual(
            attempts_dir(root, REALM, PLAN_ID), root / REALM / PLAN_ID / "attempts"
        )
        self.assertEqual(attempt, root / REALM / PLAN_ID / "attempts" / FIRST)
        self.assertEqual(
            step_events_dir(attempt, "lane_1__process"),
            attempt / "steps" / "lane_1__process",
        )

    def test_ordinary_step_ids_keep_their_names_and_nesting(self):
        # Manually built steps need not match PlanBuilder's ID pattern; each
        # "/"-separated part of an ID is one directory level.
        attempt = attempt_dir(Path("/spool"), REALM, PLAN_ID, FIRST)

        for step_id in ("lane_1/process", "1.lane", "a%b", "lane.jsonl", "x y"):
            with self.subTest(step_id=step_id):
                self.assertEqual(
                    step_events_dir(attempt, step_id),
                    attempt.joinpath("steps", *step_id.split("/")),
                )

    def test_ambiguous_parts_are_escaped(self):
        attempt = attempt_dir(Path("/spool"), REALM, PLAN_ID, FIRST)
        steps = attempt / "steps"

        for step_id, expected in (
            # Parts that could be an event file of the step above.
            ("lane/0001_step_started.json", "lane/%0001_step_started.json"),
            ("lane/0001_step_started.json.tmp", "lane/%0001_step_started.json.tmp"),
            ("lane.json", "%lane.json"),
            # Parts that path normalization would drop or resolve.
            ("lane/./process", "lane/%./process"),
            ("lane//process", "lane/%/process"),
            ("lane/", "lane/%"),
            ("/lane", "%/lane"),
            ("lane/../other", "lane/%../other"),
            ("..", "%.."),
            # The escape itself.
            ("%lane", "%%lane"),
            ("lane/%.", "lane/%%."),
        ):
            with self.subTest(step_id=step_id):
                self.assertEqual(
                    step_events_dir(attempt, step_id),
                    steps.joinpath(*expected.split("/")),
                )

    def test_every_step_id_gets_a_directory_of_its_own_inside_steps(self):
        attempt = attempt_dir(Path("/spool"), REALM, PLAN_ID, FIRST)
        steps = attempt / "steps"
        step_ids = [
            "lane",
            "lane/",
            "/lane",
            "lane/process",
            "lane/./process",
            "lane//process",
            "lane/../lane/process",
            "lane/0001_step_started.json",
            "lane/0001_step_started.json.tmp",
            "lane/%0001_step_started.json",
            "lane/%",
            "%",
            "",
            ".",
            "..",
            "../escape",
        ]
        event_files = [
            step_event_filename(1, "step.started"),
            step_event_filename(1, "step.started").replace(".json", ".json.tmp"),
            step_event_filename(10000, "data_access.write.succeeded"),
        ]
        directories = {
            step_id: step_events_dir(attempt, step_id) for step_id in step_ids
        }

        self.assertEqual(len(set(directories.values())), len(step_ids))
        for step_id, directory in directories.items():
            with self.subTest(step_id=step_id):
                # Nothing the filesystem would normalize or resolve elsewhere.
                self.assertEqual(os.path.normpath(directory), str(directory))
                self.assertTrue(directory.is_relative_to(steps))
                self.assertNotEqual(directory, steps)
                # Never the path of another step's event file.
                for other in directories.values():
                    for name in event_files:
                        self.assertNotEqual(directory, other / name)

    def test_event_file_names_never_start_with_the_escape(self):
        for name in (
            step_event_filename(1, "step.started"),
            step_event_filename(12345, STEP_BLOCKED_EVENT),
            record_filename(ATTEMPT_STARTED_EVENT),
        ):
            with self.subTest(name=name):
                self.assertFalse(name.startswith(STEP_PART_ESCAPE))
                self.assertTrue(name.endswith(".json"))

    def test_single_level_directory_names_map_back_to_step_ids(self):
        attempt = attempt_dir(Path("/spool"), REALM, PLAN_ID, FIRST)

        for step_id in ("lane", "lane.json", "%lane", "", ".", ".."):
            with self.subTest(step_id=step_id):
                name = step_events_dir(attempt, step_id).name
                self.assertEqual(step_id_of_dir(name), step_id)

    def test_an_id_that_cannot_name_a_directory_is_refused(self):
        for execution_id in ("", "..", "a/b"):
            with self.subTest(execution_id=execution_id):
                with self.assertRaises(ValueError):
                    attempt_dir(Path("/spool"), REALM, PLAN_ID, execution_id)

    def test_spool_path_hints(self):
        self.assertEqual(
            attempt_spool_path(REALM, PLAN_ID, FIRST, "plan_attempt_started.json"),
            {
                "realm": REALM,
                "plan_id": PLAN_ID,
                "execution_id": FIRST,
                "filename": "plan_attempt_started.json",
            },
        )
        self.assertEqual(
            attempt_spool_path(REALM, PLAN_ID, FIRST, "0001_x.json", step_id="a")[
                "step_id"
            ],
            "a",
        )


class SpoolTestCase(unittest.TestCase):
    """A temporary spool."""

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.spool = Path(temp_dir.name) / "spool"
        self.attempts = attempts_dir(self.spool, REALM, PLAN_ID)


class TestSpoolAttemptHistory(SpoolTestCase):
    """The history is the names of a plan's attempt directories, nothing else."""

    def setUp(self) -> None:
        super().setUp()
        self.history = SpoolAttemptHistory(self.spool)

    def test_plan_without_attempts_has_no_history(self):
        self.assertEqual(self.history.recorded_execution_ids(REALM, PLAN_ID), [])
        (self.spool / REALM / PLAN_ID).mkdir(parents=True)
        self.assertEqual(self.history.recorded_execution_ids(REALM, PLAN_ID), [])

    def test_every_attempt_directory_counts_whatever_it_holds(self):
        (self.attempts / FIRST).mkdir(parents=True)  # an empty reservation
        (self.attempts / SECOND).mkdir()
        (self.attempts / SECOND / record_filename(ATTEMPT_STARTED_EVENT)).write_text(
            "{not json"
        )

        self.assertEqual(
            sorted(self.history.recorded_execution_ids(REALM, PLAN_ID)),
            [FIRST, SECOND],
        )

    def test_files_are_not_attempts(self):
        self.attempts.mkdir(parents=True)
        (self.attempts / FIRST).write_text("stray")

        self.assertEqual(self.history.recorded_execution_ids(REALM, PLAN_ID), [])

    def test_the_old_layout_is_not_read(self):
        plan_dir = self.spool / REALM / PLAN_ID
        (plan_dir / "a" / "run_1").mkdir(parents=True)
        (plan_dir / f"exec_{FIRST}_plan_attempt_started.json").write_text("{}")

        self.assertEqual(self.history.recorded_execution_ids(REALM, PLAN_ID), [])

    def test_unlistable_attempts_are_raised(self):
        (self.attempts / FIRST).mkdir(parents=True)

        with patch(
            "yggdrasil.flow.events.attempt_records.os.scandir",
            side_effect=PermissionError("denied"),
        ):
            with self.assertRaises(PermissionError):
                self.history.recorded_execution_ids(REALM, PLAN_ID)

    def test_a_file_in_place_of_the_attempts_directory_is_raised(self):
        self.attempts.parent.mkdir(parents=True)
        self.attempts.write_text("not a directory")

        with self.assertRaises(NotADirectoryError):
            self.history.recorded_execution_ids(REALM, PLAN_ID)

    def test_reads_only_the_spool_it_was_given(self):
        elsewhere = attempts_dir(self.spool.parent / "default_spool", REALM, PLAN_ID)
        (elsewhere / FIRST).mkdir(parents=True)

        with patch.dict(os.environ, {"YGG_EVENT_SPOOL": str(elsewhere.parents[2])}):
            self.assertEqual(self.history.recorded_execution_ids(REALM, PLAN_ID), [])


class TestSpoolAttemptDirectories(SpoolTestCase):
    """Reserving creates an attempt's directory, exclusively."""

    def setUp(self) -> None:
        super().setUp()
        self.directories = SpoolAttemptDirectories(self.spool)

    def test_reserving_creates_the_directory_and_its_parents(self):
        self.assertTrue(self.directories.reserve(REALM, PLAN_ID, FIRST))

        self.assertTrue((self.attempts / FIRST).is_dir())
        self.assertEqual(list((self.attempts / FIRST).iterdir()), [])

    def test_a_taken_directory_is_reported_and_left_as_it_was(self):
        (self.attempts / FIRST).mkdir(parents=True)
        record = self.attempts / FIRST / record_filename(ATTEMPT_REPORT_EVENT)
        record.write_text("the other attempt's report")

        self.assertFalse(self.directories.reserve(REALM, PLAN_ID, FIRST))

        self.assertEqual(record.read_text(), "the other attempt's report")
        self.assertEqual(list((self.attempts / FIRST).iterdir()), [record])

    def test_a_taken_name_held_by_a_file_is_taken_too(self):
        self.attempts.mkdir(parents=True)
        (self.attempts / FIRST).write_text("stray")

        self.assertFalse(self.directories.reserve(REALM, PLAN_ID, FIRST))

    def test_failures_creating_parents_are_errors_not_collisions(self):
        self.attempts.parent.mkdir(parents=True)
        self.attempts.write_text("not a directory")

        with self.assertRaises(OSError):
            self.directories.reserve(REALM, PLAN_ID, FIRST)

        self.assertEqual(self.attempts.read_text(), "not a directory")

    def test_an_id_that_cannot_name_a_directory_is_refused(self):
        with self.assertRaises(ValueError):
            self.directories.reserve(REALM, PLAN_ID, "../escape")
        self.assertFalse(self.spool.exists())

    def test_reserves_only_in_the_spool_it_was_given(self):
        elsewhere = self.spool.parent / "default_spool"

        with patch.dict(os.environ, {"YGG_EVENT_SPOOL": str(elsewhere)}):
            self.directories.reserve(REALM, PLAN_ID, FIRST)

        self.assertFalse(elsewhere.exists())
        self.assertTrue((self.attempts / FIRST).is_dir())


if __name__ == "__main__":
    unittest.main()
