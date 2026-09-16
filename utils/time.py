from datetime import datetime, timezone


def utcnow() -> datetime:
    """Current time in UTC, naive (no tzinfo), matching the storage convention."""
    return datetime.now(timezone.utc).replace(tzinfo=None)
