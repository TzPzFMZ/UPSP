"""Compatibility exports for deterministic LTM degradation storage."""
from data.ltm_degradation_store import (
    DEFAULT_DECAY_PERIOD_DAYS,
    LTMDegradationFailure,
    LTMDegradationManager,
)

__all__ = [
    "DEFAULT_DECAY_PERIOD_DAYS",
    "LTMDegradationFailure",
    "LTMDegradationManager",
]
