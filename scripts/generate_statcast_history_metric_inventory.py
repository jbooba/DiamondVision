from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mlb_history_bot.season_metric_leaderboards import build_statcast_history_metric_spec
from mlb_history_bot.storage import STATCAST_HISTORY_BATTER_TABLE, STATCAST_HISTORY_PITCHER_TABLE


def categorize_metric(column: str, role: str) -> str:
    normalized = column.lower()
    pitch_prefixes = (
        "ff",
        "si",
        "fc",
        "sl",
        "st",
        "cu",
        "ch",
        "fs",
        "sv",
        "kn",
        "sc",
        "fo",
        "fastball",
        "breaking",
        "offspeed",
    )
    if normalized.startswith("n_") and normalized.endswith("_formatted"):
        return "Pitch Counts"
    if any(normalized.startswith(f"{prefix}_") for prefix in pitch_prefixes) and any(
        token in normalized
        for token in ("avg_spin", "avg_break", "break_x", "break_z", "ivb", "range_speed", "avg_speed")
    ):
        return "Pitch Arsenal Shape"
    if any(
        token in normalized
        for token in ("whiff", "swing", "contact_percent", "in_zone", "out_zone", "edge", "meatball", "f_strike")
    ):
        return "Plate Discipline / Swing Decisions"
    if any(
        token in normalized
        for token in (
            "avg_swing_speed",
            "avg_swing_length",
            "attack_",
            "ideal_angle_rate",
            "vertical_swing_path",
            "blasts",
            "squared_up",
            "swords",
            "fast_swing_rate",
        )
    ):
        return "Bat Tracking / Swing Traits"
    if any(
        token in normalized
        for token in (
            "barrel",
            "hard_hit",
            "sweet_spot",
            "solidcontact",
            "flareburner",
            "poorly",
            "groundballs",
            "flyballs",
            "linedrives",
            "popups",
            "pull_percent",
            "straightaway_percent",
            "opposite_percent",
            "exit_velocity_avg",
            "launch_angle_avg",
            "bacon",
            "xbacon",
            "wobacon",
            "xwobacon",
        )
    ):
        return "Batted Ball Quality"
    if any(
        token in normalized
        for token in (
            "xba",
            "xslg",
            "xwoba",
            "xobp",
            "xiso",
            "woba",
            "isolated_power",
            "batting_avg",
            "slg_percent",
            "on_base_percent",
            "on_base_plus_slg",
            "babip",
            "xbadiff",
            "xslgdiff",
            "wobadiff",
        )
    ):
        return "Rate / Expected Outcome Stats"
    if any(
        token in normalized
        for token in (
            "p_era",
            "opp_",
            "earned_run",
            "run",
            "save",
            "blown_save",
            "win",
            "loss",
            "quality_start",
            "shutout",
            "hold",
            "hit_by_pitch",
            "hit",
            "home_run",
            "double",
            "triple",
            "single",
            "walk",
            "strikeout",
            "rbi",
            "lob",
            "pickoff",
            "balk",
            "wild_pitch",
            "caught_stealing",
            "stolen_base",
            "gnd_into_dp",
            "gnd_into_tp",
            "called_strike",
            "swinging_strike",
            "foul",
            "foul_tip",
            "intent_ball",
            "intent_walk",
            "ball",
            "out",
            "reached_on_error",
            "player_age",
            "game",
            "game_finished",
            "game_in_relief",
            "complete_game",
            "relief_no_out",
            "pinch",
            "beq",
            "inh_runner",
        )
    ):
        return "Result / Counting Stats"
    return "Other"


def inventory_for_table(connection: sqlite3.Connection, table_name: str, role: str) -> dict[str, list[dict[str, object]]]:
    columns = [row["name"] for row in connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()]
    groups: dict[str, list[dict[str, object]]] = defaultdict(list)
    for column in columns:
        spec = build_statcast_history_metric_spec(column=column, table_name=table_name, role=role)
        if spec is None:
            continue
        groups[categorize_metric(column, role)].append(
            {
                "label": spec.label,
                "column": column,
                "sample_basis": spec.sample_basis or "",
                "min_sample_size": spec.min_sample_size,
                "formatter": spec.formatter,
            }
        )
    for group in groups.values():
        group.sort(key=lambda item: str(item["label"]).lower())
    return dict(sorted(groups.items()))


def render_table_rows(rows: list[dict[str, object]]) -> list[str]:
    rendered = [
        "| Metric | Column | Sample Basis | Minimum Sample | Format |",
        "| --- | --- | --- | ---: | --- |",
    ]
    for row in rows:
        rendered.append(
            f"| {row['label']} | `{row['column']}` | `{row['sample_basis']}` | {row['min_sample_size']} | `{row['formatter']}` |"
        )
    return rendered


def generate_inventory(database_path: Path) -> str:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        batter_groups = inventory_for_table(connection, STATCAST_HISTORY_BATTER_TABLE, "hitter")
        pitcher_groups = inventory_for_table(connection, STATCAST_HISTORY_PITCHER_TABLE, "pitcher")
    finally:
        connection.close()

    batter_count = sum(len(rows) for rows in batter_groups.values())
    pitcher_count = sum(len(rows) for rows in pitcher_groups.values())
    total = batter_count + pitcher_count

    lines = [
        "# Statcast History Metric Inventory",
        "",
        "Generated from the imported Statcast custom-history tables used by DiamondVision.",
        "",
        "## Summary",
        "",
        f"- Batter-season queryable metrics: `{batter_count}`",
        f"- Pitcher-season queryable metrics: `{pitcher_count}`",
        f"- Total queryable imported metrics: `{total}`",
        "",
        "Sample basis notes:",
        "- `pa`: plate appearances",
        "- `ab`: at-bats",
        "- `batted_ball`: batted-ball events",
        "- `pitch_count`: total pitches",
        "- `n_*_formatted`: pitch-type specific pitch count",
        "",
        "## Batter History Metrics",
        "",
    ]

    for category, rows in batter_groups.items():
        lines.append(f"### {category} (`{len(rows)}`)")
        lines.append("")
        lines.extend(render_table_rows(rows))
        lines.append("")

    lines.extend(
        [
            "## Pitcher History Metrics",
            "",
        ]
    )

    for category, rows in pitcher_groups.items():
        lines.append(f"### {category} (`{len(rows)}`)")
        lines.append("")
        lines.extend(render_table_rows(rows))
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a categorized inventory of imported Statcast history metrics.")
    parser.add_argument(
        "--database",
        type=Path,
        default=Path("data/processed/mlb_history.sqlite3"),
        help="Path to the SQLite database containing imported Statcast history tables.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/statcast_history_metric_inventory.md"),
        help="Path to write the generated Markdown inventory.",
    )
    args = parser.parse_args()

    markdown = generate_inventory(args.database)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(markdown, encoding="utf-8")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
