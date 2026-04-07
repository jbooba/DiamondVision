from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .metrics import MetricCatalog
from .models import EvidenceSnippet
from .person_query import clean_player_phrase as shared_clean_player_phrase, choose_best_person_match
from .pybaseball_adapter import (
    load_batting_stats,
    load_pitching_stats,
    load_statcast_batter_exitvelo_barrels,
    load_statcast_batter_expected_stats,
    load_statcast_batter_percentile_ranks,
    load_statcast_outs_above_average,
    load_statcast_pitcher_exitvelo_barrels,
    load_statcast_pitcher_expected_stats,
    load_statcast_pitcher_percentile_ranks,
)
from .provider_metrics import (
    SINGLE_GAME_HINTS,
    contains_metric_term,
    find_provider_metric,
)
from .query_utils import extract_explicit_year, normalize_person_name
from .team_evaluator import safe_float


LEADERBOARD_HINTS = {"who ", "who's ", "whos ", "which player", "leader", "leaders", "best", "worst", "highest", "lowest", "most", "least"}
SPECIAL_FIELDING_METRICS = {"OAA"}
POSITION_CODES_WITH_OAA = {1, 3, 4, 5, 6, 7, 8, 9}


@dataclass(slots=True)
class PlayerMetricSpec:
    metric_name: str
    label: str
    source_group: str
    column_name: str
    higher_is_better: bool


@dataclass(slots=True)
class PlayerMetricQuery:
    player_name: str
    player_id: int
    season: int
    metric_name: str
    mode: str
    wants_percentile: bool
    provider_spec: Any | None = None


class PlayerMetricLookupResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_player_metric_query(question, self.live_client, self.catalog, current_season)
        if query is None:
            return None
        if query.metric_name == "OAA":
            result = fetch_oaa_result(self.live_client, query)
        else:
            result = fetch_provider_metric_result(query)
            if result is None:
                result = fetch_statcast_metric_result(query)
        if result is None:
            return None
        return EvidenceSnippet(
            source=result["source"],
            title=result["title"],
            citation=result["citation"],
            summary=result["summary"],
            payload=result["payload"],
        )


def parse_player_metric_query(
    question: str,
    live_client: LiveStatsClient,
    catalog: MetricCatalog,
    current_season: int,
) -> PlayerMetricQuery | None:
    lowered = question.lower().strip()
    if lowered.startswith("show me ") and " off " in lowered and any(
        token in lowered for token in ("curveball", "curveballs", "slider", "sliders", "changeup", "changeups", "fastball", "fastballs")
    ):
        return None
    if any(lowered.startswith(hint) or f" {hint}" in lowered for hint in LEADERBOARD_HINTS):
        return None
    if any(hint in lowered for hint in SINGLE_GAME_HINTS):
        return None
    if any(token in lowered for token in ("team", "roster", "lineup")):
        return None
    metric_name, provider_spec = detect_player_metric(lowered, catalog)
    if metric_name is None:
        return None
    player_query = extract_player_query_for_metric(question, metric_name, provider_spec)
    if not player_query:
        return None
    people = live_client.search_people(player_query)
    if not people:
        return None
    person = choose_best_person_match(people, player_query)
    player_id = int(person.get("id") or 0)
    if not player_id:
        return None
    season = extract_explicit_year(question) or current_season
    mode = "live" if season == current_season else "historical"
    return PlayerMetricQuery(
        player_name=str(person.get("fullName") or player_query).strip(),
        player_id=player_id,
        season=season,
        metric_name=metric_name,
        mode=mode,
        wants_percentile="percentile" in lowered,
        provider_spec=provider_spec,
    )


def detect_player_metric(lowered_question: str, catalog: MetricCatalog) -> tuple[str | None, Any | None]:
    provider_metric = find_provider_metric(lowered_question, catalog)
    if provider_metric is not None:
        return provider_metric.metric_name, provider_metric
    if contains_metric_term(lowered_question, "oaa") or "outs above average" in lowered_question:
        return "OAA", None
    return None, None


def extract_player_query_for_metric(question: str, metric_name: str, provider_spec: Any | None) -> str | None:
    metric_terms = [metric_name]
    if provider_spec is not None:
        metric_terms.extend(provider_spec.aliases)
    from .player_season_analysis import extract_player_query_text

    direct = extract_player_query_text(question)
    normalized_direct = normalize_person_name(direct) if direct else ""
    if direct and normalize_person_name(metric_name) not in normalized_direct and "oaa" not in normalized_direct:
        return direct
    metric_pattern = "|".join(re.escape(term) for term in sorted(metric_terms, key=len, reverse=True))
    stripped = question.strip(" ?.!")
    patterns = (
        re.compile(rf"^(.+?)\s+(?:{metric_pattern})(?:\s|$)", re.IGNORECASE),
        re.compile(rf"what(?:'s| is)\s+(.+?)(?:'s)?\s+(?:{metric_pattern})(?:\s|$)", re.IGNORECASE),
        re.compile(rf"show me\s+(.+?)(?:'s)?\s+(?:{metric_pattern})(?:\s|$)", re.IGNORECASE),
    )
    for pattern in patterns:
        match = pattern.search(stripped)
        if not match:
            continue
        candidate = clean_player_phrase(match.group(1))
        if candidate:
            return candidate
    return None


def clean_player_phrase(value: str) -> str:
    return shared_clean_player_phrase(value)


def fetch_provider_metric_result(query: PlayerMetricQuery) -> dict[str, Any] | None:
    spec = query.provider_spec
    if spec is None:
        return None
    rows = []
    if spec.batting_column:
        rows.extend(match_provider_rows("batting", query.season, spec.batting_column, query.player_name))
    if spec.pitching_column:
        rows.extend(match_provider_rows("pitching", query.season, spec.pitching_column, query.player_name))
    if not rows:
        return None
    selected = choose_best_provider_row(rows, spec.metric_name)
    value = safe_float(selected.get("metric_value"))
    if value is None:
        return None
    team = str(selected.get("team") or "").strip()
    summary = (
        f"{query.player_name} is at {format_metric_value(value)} {spec.metric_name} in {query.season}"
        f"{f' for {team}' if team else ''}."
    )
    if selected["group"] == "batting":
        summary = (
            f"{summary} Public batting context: {format_row_value(selected.get('AVG'))}/"
            f"{format_row_value(selected.get('OBP'))}/{format_row_value(selected.get('SLG'))}, "
            f"{format_row_value(selected.get('PA'))} PA."
        )
    elif selected["group"] == "pitching":
        summary = (
            f"{summary} Public pitching context: {format_row_value(selected.get('ERA'))} ERA over "
            f"{format_row_value(selected.get('IP'))} IP."
        )
    return {
        "source": "FanGraphs via pybaseball",
        "title": f"{query.player_name} {query.season} {spec.metric_name}",
        "citation": "pybaseball FanGraphs batting_stats/pitching_stats player rows",
        "summary": summary,
        "payload": {
            "analysis_type": "player_metric_lookup",
            "mode": query.mode,
            "player": query.player_name,
            "season": query.season,
            "metric": spec.metric_name,
            "source_group": selected["group"],
            "rows": [
                {
                    "player": query.player_name,
                    "season": query.season,
                    "team": team,
                    "group": selected["group"],
                    "metric": spec.metric_name,
                    "value": format_metric_value(value),
                    "context_1": selected.get("context_1", ""),
                    "context_2": selected.get("context_2", ""),
                }
            ],
        },
    }


def match_provider_rows(group: str, season: int, column_name: str, player_name: str) -> list[dict[str, Any]]:
    rows = load_batting_stats(season, season) if group == "batting" else load_pitching_stats(season, season)
    if not rows:
        return []
    normalized_target = normalize_person_name(player_name)
    matches: list[dict[str, Any]] = []
    for row in rows:
        name = str(row.get("Name") or "").strip()
        if normalize_person_name(name) != normalized_target:
            continue
        if column_name not in row or row[column_name] in (None, ""):
            continue
        matches.append(
            {
                "group": group,
                "team": str(row.get("Team") or "").strip(),
                "metric_value": row.get(column_name),
                "AVG": row.get("AVG"),
                "OBP": row.get("OBP"),
                "SLG": row.get("SLG"),
                "PA": row.get("PA"),
                "ERA": row.get("ERA"),
                "IP": row.get("IP"),
                "context_1": build_provider_context_one(group, row),
                "context_2": build_provider_context_two(group, row),
            }
        )
    return matches


def choose_best_provider_row(rows: list[dict[str, Any]], metric_name: str) -> dict[str, Any]:
    if len(rows) == 1:
        return rows[0]
    current_preferred = any(hint in metric_name.lower() for hint in ("fip", "era", "whip", "k/", "bb/"))
    preferred_group = "pitching" if current_preferred else "batting"
    sorted_rows = sorted(rows, key=lambda row: (0 if row["group"] == preferred_group else 1, row["team"]))
    return sorted_rows[0]


def build_provider_context_one(group: str, row: dict[str, Any]) -> str:
    if group == "batting":
        return f"{format_row_value(row.get('AVG'))}/{format_row_value(row.get('OBP'))}/{format_row_value(row.get('SLG'))}"
    return f"{format_row_value(row.get('ERA'))} ERA"


def build_provider_context_two(group: str, row: dict[str, Any]) -> str:
    if group == "batting":
        return f"{format_row_value(row.get('PA'))} PA"
    return f"{format_row_value(row.get('IP'))} IP"


STATCAST_EXPECTED_COLUMN_MAP: dict[str, dict[str, str]] = {
    "xBA": {"batting": "est_ba", "pitching": "est_ba"},
    "xSLG": {"batting": "est_slg", "pitching": "est_slg"},
    "xwOBA": {"batting": "est_woba", "pitching": "est_woba"},
    "xERA": {"pitching": "xera"},
}

STATCAST_CONTACT_COLUMN_MAP: dict[str, dict[str, str]] = {
    "Hard-Hit Rate": {"batting": "ev95percent", "pitching": "ev95percent"},
    "Barrel Rate": {"batting": "brl_percent", "pitching": "brl_percent"},
    "EV": {"batting": "avg_hit_speed", "pitching": "avg_hit_speed"},
    "maxEV": {"batting": "max_hit_speed", "pitching": "max_hit_speed"},
}

STATCAST_PERCENTILE_COLUMN_MAP: dict[str, dict[str, str]] = {
    "xBA": {"batting": "xba", "pitching": "xba"},
    "xSLG": {"batting": "xslg", "pitching": "xslg"},
    "xwOBA": {"batting": "xwoba", "pitching": "xwoba"},
    "Hard-Hit Rate": {"batting": "hard_hit_percent", "pitching": "hard_hit_percent"},
    "Barrel Rate": {"batting": "brl_percent", "pitching": "brl_percent"},
    "EV": {"batting": "exit_velocity", "pitching": "exit_velocity"},
    "maxEV": {"batting": "max_ev", "pitching": "max_ev"},
    "xERA": {"pitching": "xera"},
}


def fetch_statcast_metric_result(query: PlayerMetricQuery) -> dict[str, Any] | None:
    if query.metric_name not in (
        set(STATCAST_EXPECTED_COLUMN_MAP)
        | set(STATCAST_CONTACT_COLUMN_MAP)
        | set(STATCAST_PERCENTILE_COLUMN_MAP)
    ):
        return None
    if query.wants_percentile:
        rows = fetch_statcast_percentile_rows(query)
    else:
        rows = fetch_statcast_raw_rows(query)
    if not rows:
        return None
    selected = choose_best_statcast_row(rows, query.metric_name)
    value = safe_float(selected.get("metric_value"))
    if value is None:
        return None
    team = str(selected.get("team") or "").strip()
    percentile_suffix = " percentile" if query.wants_percentile else ""
    summary = (
        f"{query.player_name} is at {format_metric_value(value)} {query.metric_name}{percentile_suffix} "
        f"in {query.season}{f' for {team}' if team else ''}."
    )
    if selected.get("context_1") or selected.get("context_2"):
        details = [detail for detail in (selected.get("context_1"), selected.get("context_2")) if detail]
        summary = f"{summary} Public Statcast context: {', '.join(details)}."
    citation = (
        "pybaseball Statcast percentile-rank tables"
        if query.wants_percentile
        else "pybaseball Statcast expected-stats and EV/barrel leaderboards"
    )
    return {
        "source": "Baseball Savant via pybaseball",
        "title": f"{query.player_name} {query.season} {query.metric_name}",
        "citation": citation,
        "summary": summary,
        "payload": {
            "analysis_type": "player_metric_lookup",
            "mode": query.mode,
            "player": query.player_name,
            "season": query.season,
            "metric": f"{query.metric_name}{percentile_suffix}",
            "source_group": selected["group"],
            "rows": [
                {
                    "player": query.player_name,
                    "season": query.season,
                    "team": team,
                    "group": selected["group"],
                    "metric": f"{query.metric_name}{percentile_suffix}",
                    "value": format_metric_value(value),
                    "context_1": selected.get("context_1", ""),
                    "context_2": selected.get("context_2", ""),
                }
            ],
        },
    }


def fetch_statcast_raw_rows(query: PlayerMetricQuery) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if query.metric_name in STATCAST_EXPECTED_COLUMN_MAP:
        expected_map = STATCAST_EXPECTED_COLUMN_MAP[query.metric_name]
        if "batting" in expected_map:
            rows.extend(
                match_statcast_expected_rows(
                    load_statcast_batter_expected_stats(query.season, min_pa=1),
                    query.player_id,
                    query.player_name,
                    expected_map["batting"],
                    "batting",
                )
            )
        if "pitching" in expected_map:
            rows.extend(
                match_statcast_expected_rows(
                    load_statcast_pitcher_expected_stats(query.season, min_pa=1),
                    query.player_id,
                    query.player_name,
                    expected_map["pitching"],
                    "pitching",
                )
            )
    if query.metric_name in STATCAST_CONTACT_COLUMN_MAP:
        contact_map = STATCAST_CONTACT_COLUMN_MAP[query.metric_name]
        if "batting" in contact_map:
            rows.extend(
                match_statcast_contact_rows(
                    load_statcast_batter_exitvelo_barrels(query.season, min_bbe=1),
                    query.player_id,
                    query.player_name,
                    contact_map["batting"],
                    "batting",
                )
            )
        if "pitching" in contact_map:
            rows.extend(
                match_statcast_contact_rows(
                    load_statcast_pitcher_exitvelo_barrels(query.season, min_bbe=1),
                    query.player_id,
                    query.player_name,
                    contact_map["pitching"],
                    "pitching",
                )
            )
    return rows


def fetch_statcast_percentile_rows(query: PlayerMetricQuery) -> list[dict[str, Any]]:
    percentile_map = STATCAST_PERCENTILE_COLUMN_MAP.get(query.metric_name)
    if percentile_map is None:
        return []
    rows: list[dict[str, Any]] = []
    if "batting" in percentile_map:
        rows.extend(
            match_statcast_percentile_rows(
                load_statcast_batter_percentile_ranks(query.season),
                query.player_id,
                query.player_name,
                percentile_map["batting"],
                "batting",
            )
        )
    if "pitching" in percentile_map:
        rows.extend(
            match_statcast_percentile_rows(
                load_statcast_pitcher_percentile_ranks(query.season),
                query.player_id,
                query.player_name,
                percentile_map["pitching"],
                "pitching",
            )
        )
    return rows


def match_statcast_expected_rows(
    rows: list[dict[str, Any]],
    player_id: int,
    player_name: str,
    metric_column: str,
    group: str,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for row in iter_matching_statcast_rows(rows, player_id, player_name):
        if metric_column not in row or row[metric_column] in (None, ""):
            continue
        matches.append(
            {
                "group": group,
                "team": "",
                "metric_value": row.get(metric_column),
                "context_1": f"actual {format_metric_value(row.get(context_actual_column(metric_column)))}"
                if context_actual_column(metric_column)
                else "",
                "context_2": f"{format_row_value(row.get('pa'))} PA / {format_row_value(row.get('bip'))} BIP",
            }
        )
    return matches


def match_statcast_contact_rows(
    rows: list[dict[str, Any]],
    player_id: int,
    player_name: str,
    metric_column: str,
    group: str,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for row in iter_matching_statcast_rows(rows, player_id, player_name):
        if metric_column not in row or row[metric_column] in (None, ""):
            continue
        matches.append(
            {
                "group": group,
                "team": "",
                "metric_value": row.get(metric_column),
                "context_1": f"{format_row_value(row.get('attempts'))} BBE / {format_row_value(row.get('barrels'))} barrels",
                "context_2": f"avg EV {format_metric_value(row.get('avg_hit_speed'))}, max EV {format_metric_value(row.get('max_hit_speed'))}",
            }
        )
    return matches


def match_statcast_percentile_rows(
    rows: list[dict[str, Any]],
    player_id: int,
    player_name: str,
    metric_column: str,
    group: str,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for row in iter_matching_statcast_rows(rows, player_id, player_name):
        if metric_column not in row or row[metric_column] in (None, ""):
            continue
        matches.append(
            {
                "group": group,
                "team": "",
                "metric_value": row.get(metric_column),
                "context_1": f"year {format_row_value(row.get('year'))}",
                "context_2": build_percentile_context(row),
            }
        )
    return matches


def iter_matching_statcast_rows(rows: list[dict[str, Any]], player_id: int, player_name: str):
    normalized_target = normalize_person_name(player_name)
    for row in rows:
        row_player_id = safe_float(row.get("player_id") or row.get("pitcher"))
        if row_player_id is not None and int(row_player_id) == player_id:
            yield row
            continue
        raw_name = str(row.get("player_name") or row.get("last_name, first_name") or "").strip()
        matched_name = format_name_for_match(raw_name)
        if matched_name and normalize_person_name(matched_name) == normalized_target:
            yield row


def choose_best_statcast_row(rows: list[dict[str, Any]], metric_name: str) -> dict[str, Any]:
    if len(rows) == 1:
        return rows[0]
    preferred_group = "pitching" if metric_name == "xERA" else "batting"
    sorted_rows = sorted(rows, key=lambda row: (0 if row["group"] == preferred_group else 1, row["group"]))
    return sorted_rows[0]


def context_actual_column(metric_column: str) -> str | None:
    return {
        "est_ba": "ba",
        "est_slg": "slg",
        "est_woba": "woba",
        "xera": "era",
    }.get(metric_column)


def build_percentile_context(row: dict[str, Any]) -> str:
    details = []
    for label, column in (
        ("hard-hit", "hard_hit_percent"),
        ("barrel", "brl_percent"),
        ("exit velo", "exit_velocity"),
        ("OAA", "oaa"),
    ):
        value = safe_float(row.get(column))
        if value is not None:
            details.append(f"{label} {format_metric_value(value)}")
    return ", ".join(details[:3])


def fetch_oaa_result(live_client: LiveStatsClient, query: PlayerMetricQuery) -> dict[str, Any] | None:
    snapshot = live_client.player_season_snapshot(query.player_name, query.season)
    if snapshot is None:
        return None
    fielding = snapshot.get("fielding", {}) or {}
    primary_position = fielding.get("position", {}) or snapshot.get("primary_position", {}) or {}
    position_code = int(primary_position.get("code") or 0)
    position_label = str(primary_position.get("abbreviation") or primary_position.get("name") or "")
    if position_code not in POSITION_CODES_WITH_OAA:
        summary = (
            f"Public Statcast OAA leaderboards do not expose a standard {position_label or 'this position'} view for "
            f"{query.player_name} in {query.season}."
        )
        return {
            "source": "Baseball Savant via pybaseball",
            "title": f"{query.player_name} {query.season} OAA status",
            "citation": "pybaseball statcast_outs_above_average positional leaderboard",
            "summary": summary,
            "payload": {
                "analysis_type": "player_metric_lookup",
                "mode": query.mode,
                "player": query.player_name,
                "season": query.season,
                "metric": "OAA",
                "rows": [],
            },
        }
    rows = load_oaa_rows(query.season, position_code)
    if rows is None or rows.empty:
        return None
    matched = rows[rows["player_id"].astype(int) == query.player_id]
    if matched.empty:
        normalized_target = normalize_person_name(query.player_name)
        matched = rows[
            rows["last_name, first_name"].astype(str).map(format_name_for_match).map(normalize_person_name) == normalized_target
        ]
    if matched.empty:
        return None
    row = matched.iloc[0].to_dict()
    oaa_value = safe_float(row.get("outs_above_average"))
    frp_value = safe_float(row.get("fielding_runs_prevented"))
    actual_sr = row.get("actual_success_rate_formatted")
    expected_sr = row.get("adj_estimated_success_rate_formatted")
    team = str(snapshot.get("current_team") or row.get("display_team_name") or "").strip()
    summary = (
        f"{query.player_name} is at {format_metric_value(oaa_value)} OAA at {position_label or row.get('primary_pos_formatted') or 'his listed position'} "
        f"in {query.season}{f' for {team}' if team else ''}."
    )
    details = []
    if frp_value is not None:
        details.append(f"{format_metric_value(frp_value)} fielding runs prevented")
    if actual_sr and expected_sr:
        details.append(f"{actual_sr} actual success rate versus {expected_sr} estimated")
    if details:
        summary = f"{summary} Public Statcast context: {', '.join(details)}."
    return {
        "source": "Baseball Savant via pybaseball",
        "title": f"{query.player_name} {query.season} OAA",
        "citation": "pybaseball statcast_outs_above_average positional leaderboard",
        "summary": summary,
        "payload": {
            "analysis_type": "player_metric_lookup",
            "mode": query.mode,
            "player": query.player_name,
            "season": query.season,
            "metric": "OAA",
            "source_group": "fielding",
            "rows": [
                {
                    "player": query.player_name,
                    "season": query.season,
                    "team": team,
                    "group": "fielding",
                    "metric": "OAA",
                    "value": format_metric_value(oaa_value),
                    "context_1": f"{position_label or row.get('primary_pos_formatted') or ''} / FRP {format_metric_value(frp_value)}".strip(),
                    "context_2": f"{actual_sr} vs {expected_sr}" if actual_sr and expected_sr else "",
                }
            ],
        },
    }


@lru_cache(maxsize=64)
def load_oaa_rows(season: int, position_code: int):
    return load_statcast_outs_above_average(season, position_code, min_att=0)


def format_name_for_match(last_first_name: str) -> str:
    parts = [part.strip() for part in str(last_first_name or "").split(",", 1)]
    if len(parts) == 2:
        return f"{parts[1]} {parts[0]}".strip()
    return str(last_first_name or "").strip()


def format_metric_value(value: Any) -> str:
    converted = safe_float(value)
    if converted is None:
        return "unknown"
    if abs(converted) >= 10 or converted.is_integer():
        return str(int(round(converted)))
    return f"{converted:.3f}".rstrip("0").rstrip(".")


def format_row_value(value: Any) -> str:
    if value is None or value == "":
        return "unknown"
    text = str(value).strip()
    if text.startswith("."):
        return f"0{text}"
    return text
