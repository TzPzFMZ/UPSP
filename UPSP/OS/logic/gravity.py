"""核心、关系引力与动态六轴的纯计算。"""

from constants import COMFORT_ZONE_MAX, COMFORT_ZONE_MIN, DYNAMIC_AXIS_KEYS


CORE_SOURCES = {
    "focus": (("S", 1), ("B", 1)),
    "safety": (("S", 1), ("B", -1)),
    "valence": (("C", 1), ("A", -1)),
    "arousal": (("C", -1), ("A", -1)),
    "mood": (("V", 1), ("R", -1)),
    "humor": (("V", -1), ("R", -1)),
}

RELATION_SOURCES = {
    "valence": ("resonance", "honesty"),
    "arousal": ("value", "investment"),
    "focus": ("trust", "safety"),
    "mood": ("value", "resonance"),
    "humor": ("resonance", "investment"),
    "safety": ("trust", "safety"),
}


def clamp(value, lower=-100, upper=100):
    return max(lower, min(upper, value))


def core_component(value):
    deviation = float(value) - 50
    magnitude = abs(deviation)
    if magnitude <= 10:
        return 0
    strength = 1 if magnitude <= 30 else 2
    return strength if deviation > 0 else -strength


def core_gravity(core_axes):
    pulls = {}
    comfort = {}
    for axis, sources in CORE_SOURCES.items():
        components = [
            core_component(core_axes.get(letter, 50)) * direction
            for letter, direction in sources
        ]
        pulls[axis] = sum(components) / 2
        comfort[axis] = clamp(
            sum(components) * 10, COMFORT_ZONE_MIN, COMFORT_ZONE_MAX)
    return pulls, comfort


def relation_component(value):
    value = float(value)
    if value < -30:
        return -1
    if value >= 30:
        return 1
    return 0


def relation_gravity(relation_axes_by_subject):
    total = {axis: 0 for axis in DYNAMIC_AXIS_KEYS}
    for relation_axes in (relation_axes_by_subject or {}).values():
        for axis, sources in RELATION_SOURCES.items():
            total[axis] += sum(
                relation_component(relation_axes.get(source, 0))
                for source in sources
            )
    return {axis: clamp(value, -2, 2) for axis, value in total.items()}


def apply_dynamic(current, comfort, direct_deltas, core_pulls, relation_pulls,
                  *, natural_return=True):
    """直接感受轴叠加引力；只有入口代谢允许其余轴自然回落。"""
    result = {}
    for axis in DYNAMIC_AXIS_KEYS:
        old = current.get(axis, {}).get("value", 0)
        if direct_deltas.get(axis):
            value = (
                old
                + direct_deltas[axis]
                + core_pulls.get(axis, 0)
                + relation_pulls.get(axis, 0)
            )
        elif natural_return:
            target = comfort.get(axis, 0)
            value = old + (1 if old < target else -1 if old > target else 0)
        else:
            value = old
        result[axis] = {"value": clamp(value)}
    return result
