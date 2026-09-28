"""Tests for execution-ID allocation and reservation.

Each test pins one way an ordering key or a reservation can fail. The clock
tests are the ones that motivated allocating timestamps rather than reading
them: a clock that stands still, one that moves backwards within a process,
and a restart after which the clock is behind the last recorded attempt. The
last one cannot pass without reading the plan's attempt directories back.

The reservation tests force complete-ID collisions and a race between two
allocators, and show that a collision is never resolved by overwriting, nor by
a different suffix at the same timestamp.

Clocks and suffixes are injected; cross-thread tests coordinate with barriers.
Nothing here sleeps.
"""

import re
import threading
import unittest
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from tests.execution_support import Clock
from yggdrasil.core.execution_ids import (
    RESERVATION_CANDIDATES,
    ExecutionIdAllocator,
    ReservationExhaustedError,
    execution_order_key,
    execution_timestamp,
    format_execution_id,
    reserve_execution_id,
)
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_STARTED_EVENT,
    SpoolAttemptDirectories,
    SpoolAttemptHistory,
    attempts_dir,
)

REALM = "test_realm"
PLAN_ID = "pln_ids"
T0 = datetime(2026, 9, 21, 12, 5, tzinfo=UTC)
ONE_MICROSECOND = timedelta(microseconds=1)
ID_SHAPE = re.compile(r"^[0-9]{8}T[0-9]{12}Z_[0-9a-f]{4}$")

# Upper bound for cross-thread handshakes. Never slept on.
WAIT = 5.0

# IDs a caller could build that look allocated but are not canonical.
SUFFIX = "c68e"
NEAR_MISSES = (
    "2026111T000000000000Z",  # unpadded date, no suffix
    "2026111T0000000000000Z_" + SUFFIX,  # unpadded date, padded time
    "20261101T000000000000Z",  # no suffix
    "20261101T000000000000Z_",  # empty suffix
    "20261101T000000000000Z_c68",  # three hex characters
    "20261101T000000000000Z_c68e0",  # five hex characters
    "20261101T000000000000Z_C68E",  # not lowercase
    "20261101T000000000000Z_g68e",  # not hex
    "20261101T00000000000Z_" + SUFFIX,  # short time
    "20261301T000000000000Z_" + SUFFIX,  # month 13
    "20260230T000000000000Z_" + SUFFIX,  # 30 February
    "20261101T240000000000Z_" + SUFFIX,  # hour 24
    "20261101T000000000000Z_" + SUFFIX + "\n",  # trailing newline
    " 20261101T000000000000Z_" + SUFFIX,  # leading space
    "exec_20261101T000000000000Z_" + SUFFIX,  # the old prefix
    "exec_20261101T000000000000Z_" + "a" * 32,  # the old shape entirely
    "٢٠٢٦١١٠١T000000000000Z_" + SUFFIX,  # non-ASCII digits in the timestamp
    "20261101T000000000000Z_١٢٣٤",  # non-ASCII suffix
)


def at(timestamp: datetime, suffix: str = SUFFIX) -> str:
    """The canonical ID of a timestamp and suffix."""
    return format_execution_id(timestamp, suffix)


class HistoryStub:
    """A history returning fixed IDs, or raising once given an error."""

    def __init__(self, ids: list[str] | None = None) -> None:
        self.ids = list(ids or [])
        self.error: Exception | None = None

    def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
        if self.error is not None:
            raise self.error
        return list(self.ids)


class ScriptedReserver:
    """A reserver answering from a script, recording what it was asked."""

    def __init__(self, *answers: bool | Exception) -> None:
        self.answers = list(answers)
        self.asked: list[str] = []

    def reserve(self, realm: str, plan_id: str, execution_id: str) -> bool:
        self.asked.append(execution_id)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


class SpoolTestCase(unittest.TestCase):
    """Provides a temporary spool directory and ways to fill it."""

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.spool = Path(temp_dir.name) / "spool"

    def attempt_dir(self, execution_id: str, plan_id: str = PLAN_ID) -> Path:
        """Create an attempt directory, empty, as a reservation leaves it."""
        directory = attempts_dir(self.spool, REALM, plan_id) / execution_id
        directory.mkdir(parents=True)
        return directory

    def history_allocator(self, clock: Clock, **kwargs) -> ExecutionIdAllocator:
        """An allocator ordering against this test's spool."""
        return ExecutionIdAllocator(
            SpoolAttemptHistory(self.spool), clock=clock, **kwargs
        )


class TestExecutionIdShape(unittest.TestCase):
    """IDs are a timestamp and four hex characters; name order is time order."""

    def test_id_is_its_timestamp_and_four_hex_characters(self):
        execution_id = ExecutionIdAllocator(clock=Clock(T0)).allocate(REALM, PLAN_ID)

        self.assertRegex(execution_id, ID_SHAPE)
        self.assertEqual(execution_timestamp(execution_id), T0)
        self.assertFalse(execution_id.startswith("exec_"))

    def test_the_documented_example_is_canonical(self):
        self.assertEqual(
            execution_timestamp("20260924T120000000001Z_c68e"),
            datetime(2026, 9, 24, 12, 0, 0, 1, tzinfo=UTC),
        )

    def test_every_four_hex_suffix_is_valid(self):
        for suffix in ("0000", "ffff", "09af"):
            with self.subTest(suffix=suffix):
                self.assertEqual(execution_timestamp(at(T0, suffix)), T0)

    def test_default_suffixes_are_four_lowercase_hex_characters(self):
        allocator = ExecutionIdAllocator(clock=Clock(T0))

        for _ in range(20):
            self.assertRegex(allocator.allocate(REALM, PLAN_ID), ID_SHAPE)

    def test_name_order_is_timestamp_order(self):
        stamps = [
            T0 + timedelta(days=400),
            T0 + ONE_MICROSECOND,
            T0,
            T0 + timedelta(seconds=1),
            T0 - timedelta(hours=10),
        ]
        # Suffixes chosen to disagree with the timestamps' order.
        ids = [
            at(stamp, suffix * 4) for stamp, suffix in zip(stamps, "0f5a9", strict=True)
        ]

        self.assertEqual(
            sorted(ids), [i for _, i in sorted(zip(stamps, ids, strict=True))]
        )

    def test_suffix_breaks_ties_between_equal_timestamps_only(self):
        self.assertEqual(
            sorted([at(T0, "ffff"), at(T0 + ONE_MICROSECOND, "0000"), at(T0, "0000")]),
            [at(T0, "0000"), at(T0, "ffff"), at(T0 + ONE_MICROSECOND, "0000")],
        )

    def test_ids_without_the_allocated_shape_have_no_timestamp(self):
        for execution_id in (
            "",
            "exec_",
            "exec_sched_plan",
            "2026",
            "run_20260921T120500000000Z_abcdef",
            "20260921T120500000000Z-c68e",
        ):
            with self.subTest(execution_id=execution_id):
                self.assertIsNone(execution_timestamp(execution_id))

    def test_ids_that_merely_resemble_the_canonical_shape_are_not_canonical(self):
        for execution_id in NEAR_MISSES:
            with self.subTest(execution_id=execution_id):
                self.assertIsNone(execution_timestamp(execution_id))

    def test_order_key_puts_every_allocated_id_above_any_other(self):
        allocated = [at(T0 + timedelta(seconds=s)) for s in (2, 0, 1)]
        others = ["exec_sched_plan", "zzz", "exec_2099", *NEAR_MISSES]

        ordered = sorted(allocated + others, key=execution_order_key)

        self.assertEqual(ordered[: len(others)], sorted(others))
        self.assertEqual(ordered[len(others) :], sorted(allocated))

    def test_format_builds_only_canonical_ids(self):
        for suffix in ("x", "", "a" * 3, "a" * 5, "A" * 4, "g" * 4, "a/a/", "a" * 32):
            with self.subTest(suffix=suffix):
                with self.assertRaises(ValueError):
                    format_execution_id(T0, suffix)

    def test_timestamp_is_converted_to_utc(self):
        plus_two = datetime(2026, 9, 21, 16, 5, tzinfo=timezone(timedelta(hours=2)))

        self.assertEqual(
            execution_timestamp(at(plus_two)),
            datetime(2026, 9, 21, 14, 5, tzinfo=UTC),
        )


class TestClockSafety(SpoolTestCase):
    """Allocation keeps its order whatever the clock does."""

    def test_frozen_clock_still_gives_distinct_ordered_ids(self):
        allocator = ExecutionIdAllocator(clock=Clock(T0), new_suffix=lambda: SUFFIX)

        first = allocator.allocate(REALM, PLAN_ID)
        second = allocator.allocate(REALM, PLAN_ID)

        self.assertLess(first, second)
        self.assertEqual(execution_timestamp(second), T0 + ONE_MICROSECOND)

    def test_clock_moving_back_within_a_process_is_clamped(self):
        clock = Clock(T0)
        allocator = ExecutionIdAllocator(clock=clock)
        first = allocator.allocate(REALM, PLAN_ID)
        clock.now = T0 - timedelta(hours=1)

        second = allocator.allocate(REALM, PLAN_ID)

        self.assertLess(first, second)
        self.assertEqual(execution_timestamp(second), T0 + ONE_MICROSECOND)

    def test_clock_moving_forward_again_is_followed(self):
        clock = Clock(T0)
        allocator = ExecutionIdAllocator(clock=clock)
        allocator.allocate(REALM, PLAN_ID)
        clock.now = T0 + timedelta(minutes=3)

        later = allocator.allocate(REALM, PLAN_ID)

        self.assertEqual(execution_timestamp(later), T0 + timedelta(minutes=3))

    def test_order_holds_across_plans_within_a_process(self):
        clock = Clock(T0)
        allocator = ExecutionIdAllocator(clock=clock)
        first = allocator.allocate(REALM, "plan_a")
        clock.now = T0 - timedelta(hours=1)

        second = allocator.allocate(REALM, "plan_b")

        self.assertLess(first, second)

    def test_restart_with_the_clock_behind_a_retained_attempt(self):
        before = self.history_allocator(Clock(T0))
        recorded, _ = reserve_execution_id(
            before, SpoolAttemptDirectories(self.spool), REALM, PLAN_ID
        )
        behind = Clock(T0 - timedelta(hours=1))

        # A new allocator is a restarted process: it remembers nothing.
        next_id = self.history_allocator(behind).allocate(REALM, PLAN_ID)

        self.assertGreater(next_id, recorded)
        self.assertEqual(execution_timestamp(next_id), T0 + ONE_MICROSECOND)
        # Without the read-back, the same restart orders the attempt below.
        forgetful = ExecutionIdAllocator(clock=behind)
        self.assertLess(forgetful.allocate(REALM, PLAN_ID), recorded)

    def test_empty_reservation_sets_the_floor(self):
        # Left by an attempt cancelled or killed before it recorded anything.
        empty = self.attempt_dir(at(T0))

        next_id = self.history_allocator(Clock(T0 - timedelta(hours=1))).allocate(
            REALM, PLAN_ID
        )

        self.assertEqual(list(empty.iterdir()), [])
        self.assertEqual(execution_timestamp(next_id), T0 + ONE_MICROSECOND)

    def test_unreadable_record_does_not_remove_its_directorys_floor(self):
        corrupt = self.attempt_dir(at(T0))
        (corrupt / "plan_attempt_started.json").write_text("{not json")

        next_id = self.history_allocator(Clock(T0)).allocate(REALM, PLAN_ID)

        self.assertEqual(execution_timestamp(next_id), T0 + ONE_MICROSECOND)

    def test_an_existing_timestamp_with_any_suffix_forces_a_later_timestamp(self):
        # Whatever suffix the retained attempt has, and whatever suffix the new
        # one draws, a new suffix never stands in for a later timestamp.
        for existing, drawn in (("0000", "ffff"), ("ffff", "0000"), (SUFFIX, SUFFIX)):
            with self.subTest(existing=existing, drawn=drawn):
                plan_id = f"{PLAN_ID}_{existing}_{drawn}"
                self.attempt_dir(at(T0, existing), plan_id)
                allocator = self.history_allocator(
                    Clock(T0), new_suffix=lambda drawn=drawn: drawn
                )

                next_id = allocator.allocate(REALM, plan_id)

                self.assertEqual(next_id, at(T0 + ONE_MICROSECOND, drawn))

    def test_latest_directory_sets_the_floor(self):
        for offset in (timedelta(minutes=5), timedelta(0), timedelta(hours=2)):
            self.attempt_dir(at(T0 + offset))

        next_id = self.history_allocator(Clock(T0)).allocate(REALM, PLAN_ID)

        self.assertEqual(
            execution_timestamp(next_id), T0 + timedelta(hours=2) + ONE_MICROSECOND
        )

    def test_history_is_read_on_every_allocation(self):
        # Another allocator on the same spool reserves a later attempt after
        # this one last allocated for the plan.
        allocator = self.history_allocator(Clock(T0))
        allocator.allocate(REALM, PLAN_ID)
        elsewhere = at(T0 + timedelta(hours=2))
        self.attempt_dir(elsewhere)

        next_id = allocator.allocate(REALM, PLAN_ID)

        self.assertGreater(next_id, elsewhere)

    def test_attempts_of_other_plans_do_not_move_a_plan_forward(self):
        self.attempt_dir(at(T0 + timedelta(days=1)), "other")

        next_id = self.history_allocator(Clock(T0)).allocate(REALM, PLAN_ID)

        self.assertEqual(execution_timestamp(next_id), T0)

    def test_separate_histories_stay_isolated(self):
        other = attempts_dir(self.spool.parent / "other", REALM, PLAN_ID)
        (other / at(T0 + timedelta(days=1))).mkdir(parents=True)

        next_id = self.history_allocator(Clock(T0)).allocate(REALM, PLAN_ID)

        self.assertEqual(execution_timestamp(next_id), T0)

    def test_names_that_are_not_canonical_set_no_floor(self):
        # Several resemble IDs allocated after T0.
        for name in ("exec_sched_plan", "zzz", "exec_2099", *NEAR_MISSES):
            self.attempt_dir(name)
        # A file with a canonical name is not an attempt directory.
        (attempts_dir(self.spool, REALM, PLAN_ID) / at(T0 + timedelta(days=1))).touch()

        next_id = self.history_allocator(Clock(T0)).allocate(REALM, PLAN_ID)

        self.assertEqual(execution_timestamp(next_id), T0)

    def test_leftover_records_of_the_old_layout_set_no_floor(self):
        # A spool from before attempt directories: records beside the steps.
        plan_dir = self.spool / REALM / PLAN_ID
        (plan_dir / "step_a" / "run_1").mkdir(parents=True)
        old_id = "exec_20990101T000000000000Z_" + "f" * 32
        (plan_dir / f"{old_id}_plan_attempt_started.json").write_text(
            f'{{"type": "{ATTEMPT_STARTED_EVENT}", "execution_id": "{old_id}"}}'
        )

        next_id = self.history_allocator(Clock(T0)).allocate(REALM, PLAN_ID)

        self.assertEqual(execution_timestamp(next_id), T0)

    def test_deleted_history_with_a_clock_behind_can_order_below_it(self):
        # The accepted limit: ordering survives restarts only while attempt
        # directories do. The suffix does not repair it.
        before = self.history_allocator(Clock(T0))
        erased, _ = reserve_execution_id(
            before, SpoolAttemptDirectories(self.spool), REALM, PLAN_ID
        )
        attempts_dir(self.spool, REALM, PLAN_ID).joinpath(erased).rmdir()

        after = self.history_allocator(Clock(T0 - timedelta(hours=1)))

        self.assertLess(after.allocate(REALM, PLAN_ID), erased)

    def test_pruning_attempts_does_not_erase_a_living_allocators_floor(self):
        allocator = self.history_allocator(Clock(T0 - timedelta(hours=1)))
        pruned = at(T0)
        self.attempt_dir(pruned)
        allocator.allocate(REALM, PLAN_ID)
        attempts_dir(self.spool, REALM, PLAN_ID).joinpath(pruned).rmdir()

        self.assertGreater(allocator.allocate(REALM, PLAN_ID), pruned)

    def test_id_resembling_an_allocated_one_sets_no_floor_and_orders_below(self):
        # Parsed on its own, the unpadded timestamp reads as 1 November.
        malformed = "2026111T000000000000Z"
        november = datetime(2026, 11, 1, tzinfo=UTC)
        allocator = ExecutionIdAllocator(
            HistoryStub([malformed]), clock=Clock(november)
        )

        allocated = allocator.allocate(REALM, PLAN_ID)

        self.assertEqual(execution_timestamp(allocated), november)
        self.assertEqual(
            max([malformed, allocated], key=execution_order_key), allocated
        )

    def test_suffix_source_that_breaks_the_shape_is_refused(self):
        for suffix in ("x", "c68", "c68e0", "C68E"):
            with self.subTest(suffix=suffix):
                allocator = ExecutionIdAllocator(
                    clock=Clock(T0), new_suffix=lambda suffix=suffix: suffix
                )
                with self.assertRaises(ValueError):
                    allocator.allocate(REALM, PLAN_ID)

    def test_failed_history_read_allocates_nothing(self):
        history = HistoryStub()
        history.error = PermissionError("spool unreadable")
        allocator = ExecutionIdAllocator(history, clock=Clock(T0))

        with self.assertRaises(PermissionError):
            allocator.allocate(REALM, PLAN_ID)

        history.error = None
        self.assertEqual(execution_timestamp(allocator.allocate(REALM, PLAN_ID)), T0)

    def test_naive_clock_is_rejected(self):
        allocator = ExecutionIdAllocator(clock=lambda: datetime(2026, 9, 21, 12, 5))

        with self.assertRaises(ValueError):
            allocator.allocate(REALM, PLAN_ID)


class TestReservation(SpoolTestCase):
    """A candidate is claimed exclusively; a taken one is never overwritten."""

    def test_reservation_creates_an_empty_attempt_directory(self):
        allocator = self.history_allocator(Clock(T0))

        execution_id, reserved = reserve_execution_id(
            allocator, SpoolAttemptDirectories(self.spool), REALM, PLAN_ID
        )

        self.assertTrue(reserved)
        directory = attempts_dir(self.spool, REALM, PLAN_ID) / execution_id
        self.assertTrue(directory.is_dir())
        self.assertEqual(list(directory.iterdir()), [])

    def test_without_a_reserver_nothing_is_claimed(self):
        execution_id, reserved = reserve_execution_id(
            ExecutionIdAllocator(clock=Clock(T0)), None, REALM, PLAN_ID
        )

        self.assertFalse(reserved)
        self.assertEqual(execution_timestamp(execution_id), T0)
        self.assertFalse(self.spool.exists())

    def test_complete_id_collision_leaves_the_existing_attempt_and_advances(self):
        # No history, so the allocator cannot see the existing attempt and
        # proposes its exact ID: only the exclusive creation stops it.
        taken = self.attempt_dir(at(T0))
        (taken / "plan_attempt_started.json").write_text("existing record")
        allocator = ExecutionIdAllocator(clock=Clock(T0), new_suffix=lambda: SUFFIX)

        execution_id, reserved = reserve_execution_id(
            allocator, SpoolAttemptDirectories(self.spool), REALM, PLAN_ID
        )

        self.assertTrue(reserved)
        self.assertEqual(execution_id, at(T0 + ONE_MICROSECOND))
        self.assertEqual(
            (taken / "plan_attempt_started.json").read_text(), "existing record"
        )
        self.assertEqual(
            [p.name for p in taken.iterdir()], ["plan_attempt_started.json"]
        )

    def test_retries_advance_with_a_frozen_or_backwards_clock(self):
        for name, times in (
            ("frozen", [T0, T0, T0]),
            ("backwards", [T0, T0 - timedelta(hours=1), T0 - timedelta(hours=2)]),
        ):
            with self.subTest(clock=name):
                ticks = iter(times)
                allocator = ExecutionIdAllocator(
                    clock=lambda ticks=ticks: next(ticks), new_suffix=lambda: SUFFIX
                )
                reserver = ScriptedReserver(False, False, True)

                execution_id, reserved = reserve_execution_id(
                    allocator, reserver, REALM, PLAN_ID
                )

                self.assertTrue(reserved)
                self.assertEqual(
                    reserver.asked,
                    [at(T0 + n * ONE_MICROSECOND) for n in range(3)],
                )
                self.assertEqual(execution_id, reserver.asked[-1])

    def test_three_taken_candidates_exhaust_the_reservation(self):
        reserver = ScriptedReserver(False, False, False)

        with self.assertRaises(ReservationExhaustedError) as caught:
            reserve_execution_id(
                ExecutionIdAllocator(clock=Clock(T0)), reserver, REALM, PLAN_ID
            )

        self.assertEqual(RESERVATION_CANDIDATES, 3)
        self.assertEqual(len(reserver.asked), 3)
        self.assertEqual(reserver.asked, sorted(set(reserver.asked)))
        for candidate in reserver.asked:
            self.assertIn(candidate, str(caught.exception))

    def test_other_failures_are_not_retried_as_collisions(self):
        reserver = ScriptedReserver(PermissionError("spool read-only"), True)

        with self.assertRaises(PermissionError):
            reserve_execution_id(
                ExecutionIdAllocator(clock=Clock(T0)), reserver, REALM, PLAN_ID
            )

        self.assertEqual(len(reserver.asked), 1)

    def test_a_file_in_place_of_a_parent_is_not_a_collision(self):
        # FileExistsError from a parent directory must not read as "taken".
        plan_dir = self.spool / REALM / PLAN_ID
        plan_dir.mkdir(parents=True)
        (plan_dir / "attempts").write_text("not a directory")
        reserver = SpoolAttemptDirectories(self.spool)
        asked: list[str] = []

        class Counting:
            def reserve(self, realm: str, plan_id: str, execution_id: str) -> bool:
                asked.append(execution_id)
                return reserver.reserve(realm, plan_id, execution_id)

        with self.assertRaises(OSError):
            reserve_execution_id(
                ExecutionIdAllocator(clock=Clock(T0)), Counting(), REALM, PLAN_ID
            )

        self.assertEqual(len(asked), 1)
        self.assertEqual((plan_dir / "attempts").read_text(), "not a directory")

    def test_racing_allocators_with_identical_candidates_never_share_one(self):
        # Two allocators (two engines, say) share one spool, one frozen clock
        # and one suffix, so both propose the very same full ID. Both read the
        # floor before either reserves; the exclusive creation decides.
        both_read = threading.Barrier(2)
        directories = SpoolAttemptDirectories(self.spool)

        class FirstReadWaits:
            def __init__(self) -> None:
                self.inner = SpoolAttemptHistory(directories.root)
                self.first = True

            def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
                ids = self.inner.recorded_execution_ids(realm, plan_id)
                if self.first:
                    self.first = False
                    both_read.wait(WAIT)
                return ids

        class Recording:
            def __init__(self) -> None:
                self.answers: list[tuple[str, bool]] = []

            def reserve(self, realm: str, plan_id: str, execution_id: str) -> bool:
                answer = directories.reserve(realm, plan_id, execution_id)
                self.answers.append((execution_id, answer))
                return answer

        reservers = [Recording(), Recording()]
        results: list[tuple[str, bool]] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def reserve(reserver: Recording) -> None:
            allocator = ExecutionIdAllocator(
                FirstReadWaits(), clock=Clock(T0), new_suffix=lambda: SUFFIX
            )
            try:
                result = reserve_execution_id(allocator, reserver, REALM, PLAN_ID)
            except BaseException as exc:  # surfaced by the assertions below
                with lock:
                    errors.append(exc)
                return
            with lock:
                results.append(result)

        threads = [threading.Thread(target=reserve, args=(r,)) for r in reservers]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(WAIT)

        self.assertEqual(errors, [])
        first_answers = sorted(r.answers[0] for r in reservers)
        # Both proposed the identical ID; exactly one created it.
        self.assertEqual(first_answers, [(at(T0), False), (at(T0), True)])
        (loser,) = [r for r in reservers if r.answers[0][1] is False]
        self.assertEqual(loser.answers[1], (at(T0 + ONE_MICROSECOND), True))
        self.assertEqual(
            sorted(execution_id for execution_id, _ in results),
            [at(T0), at(T0 + ONE_MICROSECOND)],
        )
        self.assertEqual(
            sorted(p.name for p in attempts_dir(self.spool, REALM, PLAN_ID).iterdir()),
            [at(T0), at(T0 + ONE_MICROSECOND)],
        )


class TestConcurrentAllocation(unittest.TestCase):
    """Allocation is synchronized; a slow history read holds up nobody else."""

    def test_concurrent_allocations_are_distinct_and_ordered(self):
        allocator = ExecutionIdAllocator(clock=Clock(T0), new_suffix=lambda: SUFFIX)
        ids: list[str] = []
        lock = threading.Lock()
        start = threading.Barrier(8)

        def allocate_many() -> None:
            start.wait(WAIT)
            for _ in range(50):
                allocated = allocator.allocate(REALM, PLAN_ID)
                with lock:
                    ids.append(allocated)

        threads = [threading.Thread(target=allocate_many) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(WAIT)

        self.assertEqual(len(ids), 400)
        stamps = [execution_timestamp(i) for i in ids]
        # One allocator never repeats a timestamp, so even a fixed suffix
        # leaves every ID distinct.
        self.assertEqual(len(set(stamps)), 400, "two allocations shared a timestamp")
        self.assertEqual(len(set(ids)), 400)
        self.assertEqual(
            sorted(ids), [i for _, i in sorted(zip(stamps, ids, strict=True))]
        )

    def test_slow_history_read_for_one_plan_does_not_hold_up_another(self):
        reading, finish = threading.Event(), threading.Event()

        class SlowForOnePlan:
            def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
                if plan_id == "slow":
                    reading.set()
                    if not finish.wait(WAIT):
                        raise TimeoutError("never released")
                return []

        allocator = ExecutionIdAllocator(SlowForOnePlan(), clock=Clock(T0))
        slow = threading.Thread(target=allocator.allocate, args=(REALM, "slow"))
        slow.start()
        self.addCleanup(slow.join, WAIT)
        self.addCleanup(finish.set)
        self.assertTrue(reading.wait(WAIT))

        # Returns while the other plan's read is still in progress.
        self.assertRegex(allocator.allocate(REALM, "fast"), ID_SHAPE)


if __name__ == "__main__":
    unittest.main()
