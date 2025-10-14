from datetime import UTC, datetime


def utcnow_iso() -> str:
    """UTC ISO8601 with Z, human-friendly."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def utcnow_compact() -> str:
    """UTC timestamp safe for filenames/paths (no colons/dashes)."""
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
