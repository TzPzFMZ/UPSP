"""Shared reaction-round time milestone policy."""


def reaction_time_milestone_seconds(time_limit_seconds=600):
    """Return reminder, warning, auto-relay milestone seconds.

    Current callers pass the explicit three-stage tuple from system config.
    A scalar remains accepted only for historical readers/tests and derives
    the former 1x/2x/3x cadence.
    """
    if isinstance(time_limit_seconds, (list, tuple)):
        values = tuple(time_limit_seconds)
    else:
        values = ()
    if len(values) == 3:
        try:
            reminder, warning, auto_relay = (int(value) for value in values)
        except (TypeError, ValueError):
            reminder = warning = auto_relay = 0
        if 0 < reminder < warning < auto_relay:
            return reminder, warning, auto_relay
    try:
        limit = max(1.0, float(time_limit_seconds or 600))
    except (TypeError, ValueError):
        limit = 600.0
    reminder = max(1, int(round(limit)))
    warning = max(reminder, int(round(limit * 2)))
    auto_relay = max(warning + 1, int(round(limit * 3)))
    return reminder, warning, auto_relay
