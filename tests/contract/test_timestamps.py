"""The canonical timestamp spelling, RFC 3339 date-times, and legacy re-spelling."""

import unittest
from datetime import UTC, datetime

from yggdrasil.contract.timestamps import (
    CANONICAL_FORMAT,
    CANONICAL_PATTERN,
    canonical_from_legacy,
    is_canonical,
    is_date_time,
)
from yggdrasil.flow.utils.ygg_time import utcnow_iso

CANONICAL = "2026-10-22T10:00:00Z"

# Arabic-Indic and fullwidth digits: digits to Python's \d, never to [0-9].
ARABIC_INDIC = "٢٠٢٦-١٠-٢٢T١٠:٠٠:٠٠Z"
FULLWIDTH = "２０２６-１０-２２T１０:００:００Z"

NOT_STRINGS = (None, 20261022, 1.5, True, b"2026-10-22T10:00:00Z", [CANONICAL])

# Not a real date or time, though every field has the right width.
IMPOSSIBLE = (
    "2026-02-30T00:00:00Z",
    "2025-02-29T00:00:00Z",  # not a leap year
    "2026-13-01T00:00:00Z",
    "2026-00-10T00:00:00Z",
    "2026-10-00T00:00:00Z",
    "2026-10-22T24:00:00Z",
    "2026-10-22T10:60:00Z",
    "2026-10-22T10:00:60Z",  # a leap second
    "0000-01-01T00:00:00Z",
)

TRAILING_NEWLINE = ("2026-10-22T10:00:00Z\n", "2026-10-22T10:00:00+00:00\n")


class TestCanonical(unittest.TestCase):
    """Whole seconds, UTC, ``Z``: one spelling per instant."""

    def test_pattern_is_published_as_written(self):
        self.assertEqual(
            CANONICAL_PATTERN,
            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
        )
        self.assertNotIn("\\d", CANONICAL_PATTERN)

    def test_format_writes_the_canonical_spelling(self):
        written = datetime(2026, 10, 22, 10, 0, 0).strftime(CANONICAL_FORMAT)
        self.assertEqual(written, CANONICAL)

    def test_the_shared_formatter_writes_canonical_timestamps(self):
        self.assertTrue(is_canonical(utcnow_iso()))

    def test_accepts_real_whole_second_utc_times(self):
        for value in (
            CANONICAL,
            "2024-02-29T23:59:59Z",
            "0001-01-01T00:00:00Z",
            "9999-12-31T23:59:59Z",
        ):
            with self.subTest(value=value):
                self.assertTrue(is_canonical(value))

    def test_rejects_every_other_spelling(self):
        for value in (
            "2026-10-22T10:00:00+00:00",
            "2026-10-22T10:00:00-00:00",
            "2026-10-22T12:00:00+02:00",
            "2026-10-22T10:00:00.5Z",
            "2026-10-22T10:00:00.000Z",
            "2026-10-22t10:00:00Z",
            "2026-10-22T10:00:00z",
            "2026-10-22 10:00:00Z",
            "2026-10-22T10:00Z",
            "2026-10-22T10:00:00",
            "2026-10-22",
            "20261022T100000Z",
            "2026-1-22T10:00:00Z",
            "26-10-22T10:00:00Z",
            "2026-10-22T1:00:00Z",
            "+2026-10-22T10:00:00Z",
            " 2026-10-22T10:00:00Z",
            "2026-10-22T10:00:00Z ",
            "2026-10-22T10:00:00Zx",
            "2026-10-22T10:00:00ZZ",
            "",
            ARABIC_INDIC,
            FULLWIDTH,
        ):
            with self.subTest(value=value):
                self.assertFalse(is_canonical(value))

    def test_rejects_impossible_dates_and_times(self):
        for value in IMPOSSIBLE:
            with self.subTest(value=value):
                self.assertFalse(is_canonical(value))

    def test_rejects_values_that_are_not_strings(self):
        for value in NOT_STRINGS:
            with self.subTest(value=value):
                self.assertFalse(is_canonical(value))


class TestDateTime(unittest.TestCase):
    """Any valid RFC 3339 spelling, naming a date and time that exist."""

    def test_accepts_every_rfc3339_spelling(self):
        for value in (
            CANONICAL,
            "2026-10-22T10:00:00+00:00",
            "2026-10-22T10:00:00-00:00",
            "2026-10-22T12:00:00+02:00",
            "2026-10-22T04:30:00-05:30",
            "2026-10-22T10:00:00+23:59",
            "2026-10-22T10:00:00.5Z",
            "2026-10-22T10:00:00.123456789+01:00",
            "2026-10-22t10:00:00z",
            "2024-02-29T23:59:59Z",
        ):
            with self.subTest(value=value):
                self.assertTrue(is_date_time(value))

    def test_rejects_iso_forms_rfc3339_does_not_allow(self):
        for value in (
            "2026-10-22",
            "2026-10-22 10:00:00Z",
            "20261022T100000Z",
            "2026-10-22T10:00:00",
            "2026-10-22T10:00Z",
            "2026-10-22T10Z",
            "2026-10-22T10:00:00.Z",
            "2026-10-22T10:00:00,5Z",
            "2026-10-22T10:00:00+0200",
            "2026-10-22T10:00:00+02",
            "2026-W43-4T10:00:00Z",
            "2026-295T10:00:00Z",
            "",
        ):
            with self.subTest(value=value):
                self.assertFalse(is_date_time(value))

    def test_rejects_impossible_dates_times_and_offsets(self):
        for value in (
            *IMPOSSIBLE,
            "2026-10-22T10:00:00+24:00",
            "2026-10-22T10:00:00+05:60",
        ):
            with self.subTest(value=value):
                self.assertFalse(is_date_time(value))

    def test_rejects_non_ascii_digits_and_surrounding_text(self):
        for value in (
            ARABIC_INDIC,
            FULLWIDTH,
            " 2026-10-22T10:00:00Z",
            "2026-10-22T10:00:00Z ",
            "2026-10-22T10:00:00Zx",
        ):
            with self.subTest(value=value):
                self.assertFalse(is_date_time(value))

    def test_rejects_values_that_are_not_strings(self):
        for value in NOT_STRINGS:
            with self.subTest(value=value):
                self.assertFalse(is_date_time(value))


class TestCanonicalFromLegacy(unittest.TestCase):
    """The two whole-second UTC spellings earlier releases wrote, re-spelled."""

    def test_offset_spelling_becomes_z_at_the_same_instant(self):
        legacy = "2026-01-01T00:00:00+00:00"
        canonical = canonical_from_legacy(legacy)
        self.assertEqual(canonical, "2026-01-01T00:00:00Z")
        self.assertEqual(
            datetime.strptime(canonical, CANONICAL_FORMAT).replace(tzinfo=UTC),
            datetime.fromisoformat(legacy),
        )

    def test_canonical_spelling_is_kept(self):
        self.assertEqual(canonical_from_legacy(CANONICAL), CANONICAL)

    def test_anything_else_is_not_converted(self):
        for value in (
            "2026-10-22T10:00:00-00:00",
            "2026-10-22T12:00:00+02:00",
            "2026-10-22T10:00:00.5+00:00",
            "2026-10-22T10:00:00.5Z",
            "2026-10-22t10:00:00+00:00",
            "2026-10-22T10:00:00z",
            "2026-02-30T00:00:00+00:00",
            "2026-02-30T00:00:00Z",
            "2026-10-22T10:00:00+00:00+00:00",
            "2026-10-22T10:00:00Z+00:00",
            "2026-10-22 10:00:00+00:00",
            "2026-10-22",
            "+00:00",
            "Z",
            "",
            ARABIC_INDIC,
        ):
            with self.subTest(value=value):
                self.assertIsNone(canonical_from_legacy(value))

    def test_values_that_are_not_strings_are_not_converted(self):
        for value in NOT_STRINGS:
            with self.subTest(value=value):
                self.assertIsNone(canonical_from_legacy(value))


class TestTrailingNewline(unittest.TestCase):
    """``$`` matches before a final newline in Python; no check is fooled by it."""

    def test_every_check_rejects_a_trailing_newline(self):
        for value in TRAILING_NEWLINE:
            with self.subTest(value=value):
                self.assertFalse(is_canonical(value))
                self.assertFalse(is_date_time(value))
                self.assertIsNone(canonical_from_legacy(value))


if __name__ == "__main__":
    unittest.main()
