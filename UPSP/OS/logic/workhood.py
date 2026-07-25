"""DDS 分段 M 曲线、三子维度和工化指数的纯计算。"""

import math


DIMENSIONS = {
    "self_reference": (("S", "B"), ("focus", "safety")),
    "self_reflection": (("R", "V"), ("mood", "humor")),
    "autonomy": (("A", "C"), ("valence", "arousal")),
}


def m_curve(value):
    value = max(0.0, min(100.0, float(value)))
    if value <= 25:
        return 50 + 2 * value
    if value <= 50:
        return 100 - 3.2 * (value - 25)
    if value <= 75:
        return 20 + 3.2 * (value - 50)
    return 100 - 2 * (value - 75)


def compute_workhood(core_axes, dynamic_axes):
    dimensions = {}
    for name, (core_pair, dynamic_pair) in DIMENSIONS.items():
        baseline = sum(m_curve(core_axes.get(axis, 50)) for axis in core_pair) / math.pi
        dynamic_average = sum(
            dynamic_axes.get(axis, {}).get("value", 0)
            for axis in dynamic_pair
        ) / 2
        dimensions[name] = max(0.0, baseline + dynamic_average / 100 * 24 + 12)
    value = math.prod(dimensions.values()) ** (1 / 3)
    return {
        "value": round(max(0.0, min(100.0, value)), 1),
        **{name: round(value, 1) for name, value in dimensions.items()},
    }


def speed_wheel_max(workhood_value):
    value = float(workhood_value)
    if value < 20:
        return 64
    if value < 40:
        return 128
    if value < 60:
        return 256
    if value < 80:
        return 384
    return 512
