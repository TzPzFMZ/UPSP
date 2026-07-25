"""Shared reaction-round time milestone policy."""


def reaction_time_milestone_seconds(time_limit_seconds=600):
    """Return reminder, warning, auto-relay milestone seconds.

    `round.time_limit` keeps its nominal transaction-window meaning. The
    attention ladder follows the restored Spec594 transaction cadence:
    1.0x reminder, 2.0x warning, 3.0x runtime auto-continue.
    """
    try:
        limit = max(1.0, float(time_limit_seconds or 600))
    except (TypeError, ValueError):
        limit = 600.0
    reminder = max(1, int(round(limit)))
    warning = max(reminder, int(round(limit * 2)))
    auto_relay = max(warning + 1, int(round(limit * 3)))
    return reminder, warning, auto_relay
