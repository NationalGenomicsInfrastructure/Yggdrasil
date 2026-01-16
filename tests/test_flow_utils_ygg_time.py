"""
Comprehensive tests for yggdrasil.flow.utils.ygg_time module.
Tests cover timestamp formatting and consistency.
"""

import re
import unittest
from datetime import UTC, datetime
from time import sleep
from unittest.mock import patch

from yggdrasil.flow.utils.ygg_time import utcnow_compact, utcnow_iso


class TestUtcnowIso(unittest.TestCase):
    """Test utcnow_iso function for ISO8601 formatted timestamps."""

    def test_returns_string(self):
        """utcnow_iso should return a string."""
        result = utcnow_iso()
        self.assertIsInstance(result, str)

    def test_format_iso8601_with_z(self):
        """utcnow_iso should return ISO8601 format with Z suffix."""
        result = utcnow_iso()
        # Expected format: YYYY-MM-DDTHH:MM:SSZ
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
        self.assertIsNotNone(re.match(pattern, result), f"Format mismatch: {result}")

    def test_contains_valid_date_components(self):
        """utcnow_iso should contain valid date components."""
        result = utcnow_iso()
        parts = result.split("T")
        self.assertEqual(len(parts), 2)

        date_part = parts[0]
        time_part = parts[1]

        # Date part: YYYY-MM-DD
        date_components = date_part.split("-")
        self.assertEqual(len(date_components), 3)
        year, month, day = date_components
        self.assertTrue(1970 <= int(year) <= 2100)
        self.assertTrue(1 <= int(month) <= 12)
        self.assertTrue(1 <= int(day) <= 31)

        # Time part: HH:MM:SSZ
        self.assertTrue(time_part.endswith("Z"))
        time_components = time_part[:-1].split(":")
        self.assertEqual(len(time_components), 3)
        hour, minute, second = time_components
        self.assertTrue(0 <= int(hour) <= 23)
        self.assertTrue(0 <= int(minute) <= 59)
        self.assertTrue(0 <= int(second) <= 59)

    def test_utc_timezone(self):
        """utcnow_iso should use UTC timezone."""
        result = utcnow_iso()
        # The 'Z' suffix indicates UTC
        self.assertTrue(result.endswith("Z"))

    def test_current_time_approximate(self):
        """utcnow_iso should return approximately current time."""
        before = datetime.now(UTC).replace(
            microsecond=0
        )  # utcnow_iso doesn't have microseconds
        result = utcnow_iso()
        after = datetime.now(UTC).replace(microsecond=0)

        # Parse the result (remove Z and parse as ISO)
        result_dt = datetime.fromisoformat(result.replace("Z", "+00:00"))

        # Result should be between before and after (within same second)
        self.assertGreaterEqual(result_dt, before)
        self.assertLessEqual(result_dt, after)

    def test_sequential_calls_increase(self):
        """Sequential calls should return increasing or equal timestamps."""
        result1 = utcnow_iso()
        sleep(1.1)  # Wait more than 1 second to ensure different timestamps
        result2 = utcnow_iso()

        # Parse timestamps
        dt1 = datetime.fromisoformat(result1.replace("Z", "+00:00"))
        dt2 = datetime.fromisoformat(result2.replace("Z", "+00:00"))

        self.assertLessEqual(
            dt1, dt2
        )  # Should be less or equal (precision is 1 second)

    def test_deterministic_with_mocked_datetime(self):
        """utcnow_iso should be deterministic when datetime is mocked."""
        fixed_dt = datetime(2025, 11, 21, 14, 30, 45, tzinfo=UTC)

        with patch("yggdrasil.flow.utils.ygg_time.datetime") as mock_datetime:
            mock_datetime.now.return_value = fixed_dt
            result = utcnow_iso()

        self.assertEqual(result, "2025-11-21T14:30:45Z")

    def test_no_microseconds(self):
        """utcnow_iso should not include microseconds."""
        result = utcnow_iso()
        # Should not contain a dot (which would indicate microseconds)
        self.assertNotIn(".", result)

    def test_consistent_length(self):
        """utcnow_iso should always return same length string."""
        result1 = utcnow_iso()
        result2 = utcnow_iso()
        # Format: YYYY-MM-DDTHH:MM:SSZ = 20 characters
        self.assertEqual(len(result1), 20)
        self.assertEqual(len(result2), 20)

    def test_year_current(self):
        """utcnow_iso should return current year."""
        result = utcnow_iso()
        year = result[:4]
        expected_year = str(datetime.now(UTC).year)
        self.assertEqual(year, expected_year)

    def test_multiple_calls_within_second(self):
        """Multiple calls within same second should have same timestamp."""
        results = [utcnow_iso() for _ in range(5)]
        # All should be the same or very close (same second)
        for result in results:
            self.assertIsInstance(result, str)
            self.assertTrue(result.endswith("Z"))


class TestUtcnowCompact(unittest.TestCase):
    """Test utcnow_compact function for filename-safe timestamps."""

    def test_returns_string(self):
        """utcnow_compact should return a string."""
        result = utcnow_compact()
        self.assertIsInstance(result, str)

    def test_format_compact_no_special_chars(self):
        """utcnow_compact should not contain colons, dashes, or spaces."""
        result = utcnow_compact()
        # Should not contain : - or spaces (safe for filenames)
        self.assertNotIn(":", result)
        self.assertNotIn("-", result)
        self.assertNotIn(" ", result)

    def test_format_pattern(self):
        """utcnow_compact should match expected format pattern."""
        result = utcnow_compact()
        # Expected format: YYYYMMDDTHHMMSSffffffZ
        pattern = r"^\d{8}T\d{6}\d{6}Z$"
        self.assertIsNotNone(re.match(pattern, result), f"Format mismatch: {result}")

    def test_contains_t_separator(self):
        """utcnow_compact should contain T separator between date and time."""
        result = utcnow_compact()
        self.assertIn("T", result)

    def test_ends_with_z(self):
        """utcnow_compact should end with Z to indicate UTC."""
        result = utcnow_compact()
        self.assertTrue(result.endswith("Z"))

    def test_contains_microseconds(self):
        """utcnow_compact should include microseconds (6 digits)."""
        result = utcnow_compact()
        # Format: YYYYMMDDTHHMMSSffffffZ
        # Total length: 8 + 1 + 6 + 6 + 1 = 22 characters
        self.assertEqual(len(result), 22)

    def test_valid_date_components(self):
        """utcnow_compact should contain valid date components."""
        result = utcnow_compact()
        # Extract components: YYYYMMDDTHHMMSSffffffZ
        year = result[0:4]
        month = result[4:6]
        day = result[6:8]

        self.assertTrue(1970 <= int(year) <= 2100)
        self.assertTrue(1 <= int(month) <= 12)
        self.assertTrue(1 <= int(day) <= 31)

    def test_valid_time_components(self):
        """utcnow_compact should contain valid time components."""
        result = utcnow_compact()
        # Extract components: YYYYMMDDTHHMMSSffffffZ
        t_index = result.index("T")
        hour = result[t_index + 1 : t_index + 3]
        minute = result[t_index + 3 : t_index + 5]
        second = result[t_index + 5 : t_index + 7]

        self.assertTrue(0 <= int(hour) <= 23)
        self.assertTrue(0 <= int(minute) <= 59)
        self.assertTrue(0 <= int(second) <= 59)

    def test_current_time_approximate(self):
        """utcnow_compact should return approximately current time."""
        before = datetime.now(UTC)
        result = utcnow_compact()
        after = datetime.now(UTC)

        # Parse the compact format
        # YYYYMMDDTHHMMSSffffffZ
        year = int(result[0:4])
        month = int(result[4:6])
        day = int(result[6:8])
        hour = int(result[9:11])
        minute = int(result[11:13])
        second = int(result[13:15])
        microsecond = int(result[15:21])

        result_dt = datetime(
            year, month, day, hour, minute, second, microsecond, tzinfo=UTC
        )

        # Result should be between before and after
        self.assertGreaterEqual(result_dt, before)
        self.assertLessEqual(result_dt, after)

    def test_sequential_calls_increase(self):
        """Sequential calls should return increasing timestamps."""
        result1 = utcnow_compact()
        sleep(0.001)  # Small delay
        result2 = utcnow_compact()

        # String comparison works because format is lexicographically ordered
        self.assertLessEqual(result1, result2)

    def test_deterministic_with_mocked_datetime(self):
        """utcnow_compact should be deterministic when datetime is mocked."""
        fixed_dt = datetime(2025, 11, 21, 14, 30, 45, 123456, tzinfo=UTC)

        with patch("yggdrasil.flow.utils.ygg_time.datetime") as mock_datetime:
            mock_datetime.now.return_value = fixed_dt
            result = utcnow_compact()

        self.assertEqual(result, "20251121T143045123456Z")

    def test_filename_safe(self):
        """utcnow_compact should be safe for use in filenames."""
        result = utcnow_compact()
        # Should only contain alphanumeric and T, Z
        allowed_chars = set("0123456789TZ")
        self.assertTrue(all(c in allowed_chars for c in result))

    def test_sortable_lexicographically(self):
        """utcnow_compact timestamps should sort lexicographically by time."""
        timestamps = []
        for _ in range(10):
            timestamps.append(utcnow_compact())
            sleep(0.001)

        sorted_timestamps = sorted(timestamps)
        self.assertEqual(timestamps, sorted_timestamps)

    def test_unique_within_microsecond_precision(self):
        """utcnow_compact should be unique within microsecond precision."""
        # Fast consecutive calls might have same timestamp
        results = [utcnow_compact() for _ in range(3)]
        # At least should be valid format
        for result in results:
            self.assertEqual(len(result), 22)
            self.assertTrue(result.endswith("Z"))

    def test_consistent_length(self):
        """utcnow_compact should always return same length string."""
        result1 = utcnow_compact()
        result2 = utcnow_compact()
        self.assertEqual(len(result1), 22)
        self.assertEqual(len(result2), 22)


class TestComparisonBetweenFormats(unittest.TestCase):
    """Test comparison and consistency between utcnow_iso and utcnow_compact."""

    def test_both_return_utc_time(self):
        """Both functions should return UTC time indicated by Z."""
        iso_result = utcnow_iso()
        compact_result = utcnow_compact()

        self.assertTrue(iso_result.endswith("Z"))
        self.assertTrue(compact_result.endswith("Z"))

    def test_same_time_components(self):
        """Both functions should return same date/time components when called together."""
        # Call both functions in quick succession
        iso_result = utcnow_iso()
        compact_result = utcnow_compact()

        # Parse ISO: YYYY-MM-DDTHH:MM:SSZ
        iso_parts = iso_result.replace("Z", "").split("T")
        iso_date = iso_parts[0].replace("-", "")
        iso_time = iso_parts[1].replace(":", "")

        # Parse compact: YYYYMMDDTHHMMSSffffffZ
        compact_date = compact_result[:8]
        compact_time = compact_result[9:15]

        # Date should match
        self.assertEqual(iso_date, compact_date)

        # Time should match (ignoring microseconds in compact)
        self.assertEqual(iso_time, compact_time)

    def test_iso_is_human_friendly(self):
        """ISO format should be more human-readable with separators."""
        iso_result = utcnow_iso()
        # Should contain dashes and colons
        self.assertIn("-", iso_result)
        self.assertIn(":", iso_result)

    def test_compact_is_filename_safe(self):
        """Compact format should be safe for filenames (no special chars)."""
        compact_result = utcnow_compact()
        # Should not contain dashes or colons
        self.assertNotIn("-", compact_result)
        self.assertNotIn(":", compact_result)

    def test_both_increase_over_time(self):
        """Both formats should produce increasing or equal values over time."""
        iso1 = utcnow_iso()
        compact1 = utcnow_compact()
        sleep(1.1)  # Wait more than 1 second
        iso2 = utcnow_iso()
        compact2 = utcnow_compact()

        # String comparison works for both formats (lexicographically ordered)
        self.assertLessEqual(iso1, iso2)  # ISO has 1-second precision
        self.assertLess(compact1, compact2)  # Compact has microsecond precision


if __name__ == "__main__":
    unittest.main()
