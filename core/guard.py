from dataclasses import dataclass
from statistics import median

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.model import LoadStats
from core.setting import get_settings


@dataclass
class GuardDecision:
    allowed: bool
    reason: str | None = None


def evaluate_closure_guard(session: Session, source: str, received: int, active_count: int, to_close_count: int) -> GuardDecision:
    """SPEC §5.4: block mass-closure of postings that merely reflect a bad snapshot.
    Rules are checked in order (R1, R2, R3); the first one that fires blocks the closure."""
    settings = get_settings()

    if received == 0 and active_count > 0:
        return GuardDecision(False, f"R1: received={received}, active={active_count}")

    history = _recent_received(session, source, settings.guard_history_n)
    if len(history) >= settings.guard_min_history:
        baseline = median(history)
        if received < settings.guard_min_ratio * baseline:
            return GuardDecision(False, f"R2: received={received}, baseline={baseline:g}")

    if active_count >= settings.guard_min_active:
        share = to_close_count / active_count
        if share > settings.guard_max_close_share:
            return GuardDecision(False, f"R3: to_close={to_close_count}, active={active_count}, share={share:.2f}")

    return GuardDecision(True)


def _recent_received(session: Session, source: str, limit: int) -> list[int]:
    stmt = (
        select(LoadStats.received)
        .where(LoadStats.source == source, LoadStats.closures_blocked.is_(False))
        .order_by(LoadStats.snapshot_at.desc())
        .limit(limit)
    )
    return list(session.scalars(stmt))
