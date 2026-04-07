from __future__ import annotations

from typing import Any

from .team_evaluator import ordinal_percentile, percentile_from_values, safe_float


def format_comparison_value(
    value: Any,
    *,
    digits: int = 3,
    integer: bool = False,
) -> str:
    converted = safe_float(value)
    if converted is None:
        return "unknown"
    if integer:
        return str(int(round(converted)))
    if abs(converted) >= 10:
        return f"{converted:.1f}".rstrip("0").rstrip(".")
    return f"{converted:.{digits}f}".rstrip("0").rstrip(".")


def percentile_band(percentile: float | None) -> str:
    if percentile is None:
        return "unclassified"
    if percentile >= 97:
        return "all-time elite"
    if percentile >= 90:
        return "elite"
    if percentile >= 75:
        return "strong"
    if percentile >= 55:
        return "above average"
    if percentile >= 45:
        return "roughly average"
    if percentile >= 25:
        return "below average"
    return "poor"


def build_percentile_blurb(percentile: float | None, population_label: str) -> str:
    if percentile is None:
        return ""
    return (
        f"That lands around the {ordinal_percentile(percentile)} percentile of {population_label}, "
        f"which reads as {percentile_band(percentile)}."
    )


def comparison_gap_sentence(
    *,
    left_label: str,
    right_label: str,
    metric_label: str,
    left_value: float | None,
    right_value: float | None,
    higher_is_better: bool,
    digits: int = 3,
    integer: bool = False,
) -> str:
    if left_value is None or right_value is None:
        return f"The cleanest {metric_label} comparison is unavailable from the loaded evidence."
    difference = left_value - right_value
    if abs(difference) < (1.0 if integer else 0.001):
        return f"They are essentially even by {metric_label}."
    left_better = difference > 0 if higher_is_better else difference < 0
    leader = left_label if left_better else right_label
    trailer = right_label if left_better else left_label
    gap = abs(difference)
    return (
        f"By {metric_label}, {leader} has the clearer edge over {trailer} by "
        f"{format_comparison_value(gap, digits=digits, integer=integer)}."
    )


def percentile_for_population(
    target_value: float | None,
    population_values: list[float | None],
    *,
    higher_is_better: bool,
) -> float | None:
    return percentile_from_values(target_value, population_values, higher_is_better)
