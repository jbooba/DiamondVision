from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .award_history import AWARD_DEFINITIONS_BY_KEY
from .cohort_timeline import CohortFilter, load_award_identities, parse_cohort_filter
from .cohort_timeline import resolve_cohort_filter
from .config import Settings
from .metrics import MetricCatalog
from .models import EvidenceSnippet
from .query_utils import extract_minimum_qualifier
from .retrosheet_splits import open_retrosheet_plays_stream
from .storage import (
    clear_retrosheet_player_count_splits,
    clear_retrosheet_player_opponent_pitchers,
    clear_retrosheet_player_opponent_pitcher_cohorts,
    clear_retrosheet_player_reached_count_splits,
    clear_retrosheet_player_opponent_contexts,
    get_connection,
    initialize_database,
    set_metadata_value,
    table_exists,
    upsert_retrosheet_player_count_splits,
    upsert_retrosheet_player_opponent_pitchers,
    upsert_retrosheet_player_opponent_pitcher_cohorts,
    upsert_retrosheet_player_reached_count_splits,
    upsert_retrosheet_player_opponent_contexts,
)


BEST_HINTS = {"best", "highest", "most", "top"}
WORST_HINTS = {"worst", "lowest", "least", "bottom", "fewest"}
MAGNITUDE_HIGH_HINTS = {"highest", "most", "top", "leader", "leaders"}
MAGNITUDE_LOW_HINTS = {"lowest", "least", "fewest", "bottom"}
OFFENSIVE_SUMMARY_HINTS = ("offensive", "offense", "offensively", "at the plate", "hitting")
COUNT_PATTERN = re.compile(r"\b([0-4])\s*-\s*([0-3])\b")
FOLLOWING_COUNT_HINTS = ("following", "after")
ON_COUNT_HINTS = (" on ", " on the ", " on a ")
FORMER_TEAM_HINTS = (
    "former team",
    "teams they were previously on",
    "team they were previously on",
    "previously on",
    "previous team",
)
FUTURE_TEAM_HINTS = (
    "later signed to",
    "later played for",
    "later joined",
    "were later signed to",
)
AGGREGATE_RELATIONSHIP_HINTS = (
    "all former teams",
    "across all former teams",
    "aggregated across all former teams",
    "combined across all former teams",
    "across former teams",
    "all future teams",
    "across all future teams",
    "aggregated across all future teams",
    "combined across all future teams",
    "across future teams",
    "all teams they were previously on",
    "all teams they later joined",
)
COUNT_PLAY_USECOLS = [
    "batter",
    "balls",
    "strikes",
    "pitches",
    "pa",
    "ab",
    "single",
    "double",
    "triple",
    "hr",
    "walk",
    "iw",
    "hbp",
    "sf",
    "k",
    "rbi",
    "date",
    "gametype",
]
OPPONENT_CONTEXT_USECOLS = [
    "id",
    "opp",
    "b_pa",
    "b_ab",
    "b_h",
    "b_d",
    "b_t",
    "b_hr",
    "b_rbi",
    "b_w",
    "b_iw",
    "b_hbp",
    "b_sf",
    "b_k",
    "date",
    "gametype",
]
OPPONENT_PITCHER_COHORT_USECOLS = [
    "batter",
    "pitcher",
    "pa",
    "ab",
    "single",
    "double",
    "triple",
    "hr",
    "walk",
    "iw",
    "hbp",
    "sf",
    "k",
    "rbi",
    "date",
    "gametype",
]
RELATIONAL_OPPONENT_HINTS = (
    " against ",
    " versus ",
    " vs ",
    " facing ",
    " while facing ",
    " when facing ",
)


@dataclass(slots=True, frozen=True)
class ContextMetricSpec:
    key: str
    label: str
    aliases: tuple[str, ...]
    kind: str
    sample_basis: str | None = None
    min_sample_size: int = 0
    higher_is_better: bool = True


@dataclass(slots=True)
class CountSplitQuery:
    count_key: str
    relation: str
    metric_key: str
    descriptor: str
    sort_desc: bool
    min_sample_size: int
    sample_basis: str | None
    is_valid_count: bool


@dataclass(slots=True)
class TeamRelationshipQuery:
    relationship: str
    metric_key: str
    descriptor: str
    sort_desc: bool
    min_sample_size: int
    sample_basis: str | None
    aggregate_scope: str = "opponent"


@dataclass(slots=True)
class OpponentPitcherCohortQuery:
    cohort_kind: str
    cohort_value: str
    cohort_label: str
    metric_key: str
    descriptor: str
    sort_desc: bool
    min_sample_size: int
    sample_basis: str | None
    cohort_filter: CohortFilter | None = None


CONTEXT_METRIC_SPECS = (
    ContextMetricSpec(
        key="ops",
        label="OPS",
        aliases=("ops", "on-base plus slugging", "on base plus slugging"),
        kind="rate",
        sample_basis="plate_appearances",
        min_sample_size=25,
    ),
    ContextMetricSpec(
        key="obp",
        label="OBP",
        aliases=("obp", "on-base percentage", "on base percentage"),
        kind="rate",
        sample_basis="plate_appearances",
        min_sample_size=25,
    ),
    ContextMetricSpec(
        key="slg",
        label="SLG",
        aliases=("slg", "slugging percentage", "slugging"),
        kind="rate",
        sample_basis="at_bats",
        min_sample_size=25,
    ),
    ContextMetricSpec(
        key="ba",
        label="BA",
        aliases=("batting average", "average", " ba "),
        kind="rate",
        sample_basis="at_bats",
        min_sample_size=25,
    ),
    ContextMetricSpec(
        key="home_runs",
        label="HR",
        aliases=("home runs", "home run", "homers", "homer", " hr "),
        kind="count",
    ),
    ContextMetricSpec(
        key="runs_batted_in",
        label="RBI",
        aliases=("runs batted in", " rbi ", " rbis "),
        kind="count",
    ),
    ContextMetricSpec(
        key="strikeouts",
        label="SO",
        aliases=("strikeouts", "strikeout", "struck out", " ks "),
        kind="count",
        higher_is_better=False,
    ),
    ContextMetricSpec(
        key="walks",
        label="BB",
        aliases=("walks", "walk", " free passes ", " free pass "),
        kind="count",
    ),
    ContextMetricSpec(
        key="hits",
        label="Hits",
        aliases=("base hits", "base hit", " hits "),
        kind="count",
    ),
    ContextMetricSpec(
        key="doubles",
        label="2B",
        aliases=("doubles", "double"),
        kind="count",
    ),
    ContextMetricSpec(
        key="triples",
        label="3B",
        aliases=("triples", "triple"),
        kind="count",
    ),
    ContextMetricSpec(
        key="plate_appearances",
        label="PA",
        aliases=("plate appearances", "plate appearance", " pas ", " pa "),
        kind="count",
    ),
    ContextMetricSpec(
        key="at_bats",
        label="AB",
        aliases=("at-bats", "at bats", "at-bat", "at bat", " abs ", " ab "),
        kind="count",
    ),
    ContextMetricSpec(
        key="hit_by_pitch",
        label="HBP",
        aliases=("hit by pitches", "hit by pitch", " hbp "),
        kind="count",
    ),
)
CONTEXT_METRIC_BY_KEY = {spec.key: spec for spec in CONTEXT_METRIC_SPECS}
CONTEXT_MINIMUM_QUALIFIERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("plate_appearances", ("plate appearances", "plate appearance", "pa", "pas")),
    ("at_bats", ("at-bats", "at-bat", "at bats", "at bat", "ab", "abs")),
)



class ContextualPerformanceResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        count_query = parse_count_split_query(question)
        if count_query is not None:
            return self._build_count_split_snippet(connection, count_query)
        opponent_pitcher_cohort_query = parse_opponent_pitcher_cohort_query(question)
        if opponent_pitcher_cohort_query is not None:
            return build_opponent_pitcher_cohort_snippet(connection, opponent_pitcher_cohort_query)
        relationship_query = parse_team_relationship_query(question)
        if relationship_query is not None:
            return build_team_relationship_snippet(connection, relationship_query)
        return None

    def _build_count_split_snippet(self, connection, query: CountSplitQuery) -> EvidenceSnippet | None:
        if not query.is_valid_count:
            return build_invalid_count_snippet(query)
        table_name = "retrosheet_player_reached_count_splits" if query.relation == "after" else "retrosheet_player_count_splits"
        if not table_exists(connection, table_name):
            return build_count_sync_gap_snippet(query)
        rows = fetch_count_split_rows(connection, query, table_name=table_name)
        if not rows:
            return build_count_sync_gap_snippet(query)
        summary = build_count_split_summary(query, rows)
        relation_label = "after" if query.relation == "after" else "on"
        return EvidenceSnippet(
            source="Retrosheet Count Splits",
            title=f"{count_metric_label(query.metric_key)} {relation_label} {query.count_key} counts",
            citation=(
                "Retrosheet plays.csv aggregated to player reached-count split totals"
                if query.relation == "after"
                else "Retrosheet plays.csv aggregated to player terminal-count split totals"
            ),
            summary=summary,
            payload={
                "analysis_type": "player_count_leaderboard",
                "count_key": query.count_key,
                "relation": query.relation,
                "metric": count_metric_label(query.metric_key),
                "leaders": rows,
                "min_sample_size": query.min_sample_size,
                "sample_basis": query.sample_basis,
            },
        )


def parse_count_split_query(question: str) -> CountSplitQuery | None:
    lowered = f" {question.lower()} "
    match = COUNT_PATTERN.search(question)
    if match is None or "count" not in lowered:
        return None
    balls = int(match.group(1))
    strikes = int(match.group(2))
    count_key = f"{balls}-{strikes}"
    relation = "after" if any(token in lowered for token in FOLLOWING_COUNT_HINTS) else "on"
    if relation == "on" and not any(token in lowered for token in ON_COUNT_HINTS) and "counts" not in lowered and "count?" not in lowered:
        relation = "on"
    metric_spec = detect_context_metric_spec(
        lowered,
        default_key="ops" if any(token in lowered for token in OFFENSIVE_SUMMARY_HINTS) else "ba",
    )
    descriptor, sort_desc = resolve_metric_sort(lowered, metric_spec)
    sample_basis, min_sample_size = resolve_context_sample_requirements(question, metric_spec)
    return CountSplitQuery(
        count_key=count_key,
        relation=relation,
        metric_key=metric_spec.key,
        descriptor=descriptor,
        sort_desc=sort_desc,
        min_sample_size=min_sample_size,
        sample_basis=sample_basis,
        is_valid_count=balls <= 3 and strikes <= 2,
    )


def build_invalid_count_snippet(query: CountSplitQuery) -> EvidenceSnippet:
    examples = "Valid baseball counts run from 0-0 through 3-2."
    if query.count_key == "0-3":
        examples = "Valid baseball counts run from 0-0 through 3-2. `0-3` is not possible; you may have meant `3-0` or `0-2`."
    return EvidenceSnippet(
        source="Count-State Planner",
        title=f"Invalid baseball count: {query.count_key}",
        citation="Baseball count-state validator",
        summary=(
            f"`{query.count_key}` is not a legal baseball count, so I should not answer that as a real leaderboard. "
            "A count can have at most 3 balls and 2 strikes. "
            f"{examples}"
        ),
        payload={
            "analysis_type": "contextual_invalid_count",
            "metric": count_metric_label(query.metric_key),
            "count_key": query.count_key,
            "relation": query.relation,
        },
    )


def build_count_sync_gap_snippet(query: CountSplitQuery) -> EvidenceSnippet:
    relation_label = "after" if query.relation == "after" else "on"
    table_description = (
        "pitch-sequence reached-count table"
        if query.relation == "after"
        else "terminal-count split table"
    )
    return EvidenceSnippet(
        source="Count-State Planner",
        title=f"{count_metric_label(query.metric_key)} {relation_label} {query.count_key} counts source gap",
        citation="Retrosheet count-state planner",
        summary=(
            f"I understand this as a leaderboard for {count_metric_phrase(query.metric_key)} {relation_label} {query.count_key} counts. "
            f"The project can answer that once the compact Retrosheet player {table_description} has been built, but that "
            "table is not available in the current database yet. Run `python -m mlb_history_bot sync-retrosheet-counts` "
            "to build it."
        ),
        payload={
            "analysis_type": "contextual_source_gap",
            "metric": count_metric_label(query.metric_key),
            "context": f"{relation_label} {query.count_key} counts",
        },
    )


def fetch_count_split_rows(connection, query: CountSplitQuery, *, table_name: str) -> list[dict[str, Any]]:
    metric_expression = build_count_metric_expression(query.metric_key)
    sample_clause, sample_parameters = build_sample_clause(
        basis=query.sample_basis,
        minimum=query.min_sample_size,
        alias="split",
    )
    parameters: tuple[Any, ...] = (query.count_key, *sample_parameters)
    order_direction = "DESC" if query.sort_desc else "ASC"
    rows = connection.execute(
        f"""
        WITH names AS (
            SELECT
                lower(COALESCE(retroid, '')) AS retroid,
                NULLIF(TRIM(COALESCE(namefirst, '') || ' ' || COALESCE(namelast, '')), '') AS player_name
            FROM lahman_people
            GROUP BY lower(COALESCE(retroid, ''))
        ),
        retro_names AS (
            SELECT
                lower(COALESCE(id, '')) AS retroid,
                NULLIF(TRIM(COALESCE(first, '') || ' ' || COALESCE(last, '')), '') AS player_name
            FROM retrosheet_allplayers
            GROUP BY lower(COALESCE(id, ''))
        )
        SELECT
            split.player_id,
            COALESCE(names.player_name, retro_names.player_name, split.player_id) AS player_name,
            split.plate_appearances,
            split.at_bats,
            split.hits,
            split.doubles,
            split.triples,
            split.home_runs,
            split.walks,
            split.hit_by_pitch,
            split.sacrifice_flies,
            split.strikeouts,
            split.runs_batted_in,
            split.first_season,
            split.last_season,
            {metric_expression} AS metric_value
        FROM {table_name} AS split
        LEFT JOIN names ON names.retroid = lower(split.player_id)
        LEFT JOIN retro_names ON retro_names.retroid = lower(split.player_id)
        WHERE split.count_key = ?
          AND {sample_clause}
          AND ({metric_expression}) IS NOT NULL
        ORDER BY metric_value {order_direction}, split.plate_appearances DESC, player_name ASC
        LIMIT 5
        """,
        parameters,
    ).fetchall()
    return [
        {
            "player_name": str(row["player_name"]),
            "plate_appearances": int(row["plate_appearances"]),
            "at_bats": int(row["at_bats"]),
            "hits": int(row["hits"]),
            "doubles": int(row["doubles"]),
            "triples": int(row["triples"]),
            "home_runs": int(row["home_runs"]),
            "walks": int(row["walks"]),
            "hit_by_pitch": int(row["hit_by_pitch"]),
            "sacrifice_flies": int(row["sacrifice_flies"]),
            "strikeouts": int(row["strikeouts"]),
            "runs_batted_in": int(row["runs_batted_in"]),
            "first_season": int(row["first_season"]),
            "last_season": int(row["last_season"]),
            "metric_value": float(row["metric_value"]),
        }
        for row in rows
    ]


def build_count_split_summary(query: CountSplitQuery, rows: list[dict[str, Any]]) -> str:
    lead = rows[0]
    metric_spec = CONTEXT_METRIC_BY_KEY[query.metric_key]
    relation_phrase = "after reaching" if query.relation == "after" else "on"
    if metric_spec.kind == "rate":
        summary = (
            f"Across imported Retrosheet {'reached-count' if query.relation == 'after' else 'terminal-count'} history, "
            f"the {query.descriptor} {count_metric_phrase(query.metric_key)} {relation_phrase} {query.count_key} counts "
            f"belongs to {lead['player_name']} at {format_context_metric_value(query.metric_key, lead['metric_value'])}. "
            f"That line spans {lead['plate_appearances']} plate appearances between {lead['first_season']} and {lead['last_season']}."
        )
        if query.min_sample_size and query.sample_basis:
            summary = (
                f"{summary} I used a minimum of {query.min_sample_size} "
                f"{sample_basis_phrase(query.sample_basis)} to avoid tiny-sample noise."
            )
        if query.relation == "after":
            summary = f"{summary} Reached-count answers are limited to plate appearances with imported Retrosheet pitch sequences."
    else:
        summary = (
            f"Across imported Retrosheet {'reached-count' if query.relation == 'after' else 'terminal-count'} history, "
            f"the {query.descriptor} {count_metric_phrase(query.metric_key)} {relation_phrase} {query.count_key} counts "
            f"belongs to {lead['player_name']} with {format_context_metric_value(query.metric_key, lead['metric_value'])}."
        )
    trailing = rows[1:4]
    if trailing:
        next_rows = "; ".join(
            f"{row['player_name']} {format_context_metric_value(query.metric_key, row['metric_value'])}"
            for row in trailing
        )
        summary = f"{summary} Next on the board: {next_rows}."
    return summary


def count_metric_label(metric_key: str) -> str:
    spec = CONTEXT_METRIC_BY_KEY.get(metric_key)
    return spec.label if spec is not None else metric_key.upper()


def count_metric_phrase(metric_key: str) -> str:
    phrase_map = {
        "ba": "batting average",
        "obp": "on-base percentage",
        "slg": "slugging percentage",
        "ops": "OPS",
        "home_runs": "home-run total",
        "hits": "hit total",
        "walks": "walk total",
        "strikeouts": "strikeout total",
        "runs_batted_in": "RBI total",
        "doubles": "double total",
        "triples": "triple total",
        "plate_appearances": "plate-appearance total",
        "at_bats": "at-bat total",
        "hit_by_pitch": "hit-by-pitch total",
    }
    return phrase_map.get(metric_key, count_metric_label(metric_key))


def detect_context_metric_spec(lowered_question: str, *, default_key: str) -> ContextMetricSpec:
    for spec in CONTEXT_METRIC_SPECS:
        if any(alias in lowered_question for alias in spec.aliases):
            return spec
    return CONTEXT_METRIC_BY_KEY[default_key]


def resolve_context_sample_requirements(question: str, metric_spec: ContextMetricSpec) -> tuple[str | None, int]:
    for basis, nouns in CONTEXT_MINIMUM_QUALIFIERS:
        value = extract_minimum_qualifier(question, nouns)
        if value is not None:
            return basis, value
    return metric_spec.sample_basis, metric_spec.min_sample_size


def resolve_metric_sort(lowered_question: str, metric_spec: ContextMetricSpec) -> tuple[str, bool]:
    if any(token in lowered_question for token in MAGNITUDE_HIGH_HINTS):
        return ("highest" if metric_spec.kind == "rate" else "most"), True
    if any(token in lowered_question for token in MAGNITUDE_LOW_HINTS):
        return ("lowest" if metric_spec.kind == "rate" else "fewest"), False
    if "worst" in lowered_question:
        return "worst", not metric_spec.higher_is_better
    if "best" in lowered_question:
        return "best", metric_spec.higher_is_better
    if metric_spec.kind == "count":
        return "most", True
    return ("best" if metric_spec.higher_is_better else "worst"), metric_spec.higher_is_better


def sample_basis_phrase(sample_basis: str) -> str:
    phrase_map = {
        "at_bats": "at-bats",
        "plate_appearances": "plate appearances",
    }
    return phrase_map.get(sample_basis, sample_basis.replace("_", " "))


def build_sample_clause(*, basis: str | None, minimum: int, alias: str) -> tuple[str, tuple[Any, ...]]:
    if not basis or minimum <= 0:
        return "1 = 1", tuple()
    return f"{alias}.{basis} >= ?", (minimum,)


def build_count_metric_expression(metric_key: str) -> str:
    singles_expression = "(split.hits - split.doubles - split.triples - split.home_runs)"
    total_bases_expression = (
        f"({singles_expression} + (2 * split.doubles) + (3 * split.triples) + (4 * split.home_runs))"
    )
    if metric_key == "ba":
        return "CAST(split.hits AS REAL) / NULLIF(split.at_bats, 0)"
    if metric_key == "obp":
        return (
            "CAST(split.hits + split.walks + split.hit_by_pitch AS REAL) "
            "/ NULLIF(split.at_bats + split.walks + split.hit_by_pitch + split.sacrifice_flies, 0)"
        )
    if metric_key == "slg":
        return f"CAST({total_bases_expression} AS REAL) / NULLIF(split.at_bats, 0)"
    if metric_key == "ops":
        return (
            "("
            "CAST(split.hits + split.walks + split.hit_by_pitch AS REAL) "
            "/ NULLIF(split.at_bats + split.walks + split.hit_by_pitch + split.sacrifice_flies, 0)"
            ") + ("
            f"CAST({total_bases_expression} AS REAL) / NULLIF(split.at_bats, 0)"
            ")"
        )
    column_map = {
        "home_runs": "split.home_runs",
        "hits": "split.hits",
        "walks": "split.walks",
        "strikeouts": "split.strikeouts",
        "runs_batted_in": "split.runs_batted_in",
        "doubles": "split.doubles",
        "triples": "split.triples",
        "plate_appearances": "split.plate_appearances",
        "at_bats": "split.at_bats",
        "hit_by_pitch": "split.hit_by_pitch",
    }
    column = column_map.get(metric_key, "split.home_runs")
    return f"CAST({column} AS REAL)"


def build_team_relationship_metric_expression(metric_key: str) -> str:
    walks_expression = "(context.walks + context.intentional_walks)"
    singles_expression = "(context.hits - context.doubles - context.triples - context.home_runs)"
    total_bases_expression = (
        f"({singles_expression} + (2 * context.doubles) + (3 * context.triples) + (4 * context.home_runs))"
    )
    if metric_key == "ba":
        return "CAST(context.hits AS REAL) / NULLIF(context.at_bats, 0)"
    if metric_key == "obp":
        return (
            f"CAST(context.hits + {walks_expression} + context.hit_by_pitch AS REAL) "
            f"/ NULLIF(context.at_bats + {walks_expression} + context.hit_by_pitch + context.sacrifice_flies, 0)"
        )
    if metric_key == "slg":
        return f"CAST({total_bases_expression} AS REAL) / NULLIF(context.at_bats, 0)"
    if metric_key == "ops":
        return (
            "("
            f"CAST(context.hits + {walks_expression} + context.hit_by_pitch AS REAL) "
            f"/ NULLIF(context.at_bats + {walks_expression} + context.hit_by_pitch + context.sacrifice_flies, 0)"
            ") + ("
            f"CAST({total_bases_expression} AS REAL) / NULLIF(context.at_bats, 0)"
            ")"
        )
    column_map = {
        "home_runs": "context.home_runs",
        "hits": "context.hits",
        "walks": walks_expression,
        "strikeouts": "context.strikeouts",
        "runs_batted_in": "context.runs_batted_in",
        "doubles": "context.doubles",
        "triples": "context.triples",
        "plate_appearances": "context.plate_appearances",
        "at_bats": "context.at_bats",
        "hit_by_pitch": "context.hit_by_pitch",
    }
    column = column_map.get(metric_key, "context.home_runs")
    return f"CAST({column} AS REAL)"


def parse_opponent_pitcher_cohort_query(question: str) -> OpponentPitcherCohortQuery | None:
    lowered = f" {question.lower()} "
    if not any(token in lowered for token in RELATIONAL_OPPONENT_HINTS):
        return None
    cohort_filter = parse_cohort_filter(question)
    if cohort_filter is None or cohort_filter.kind in {"manager_era", "bat_handedness"}:
        return None
    cohort_kind = cohort_filter.kind
    cohort_value = cohort_filter.label.lower()
    if cohort_filter.kind == "award_winner":
        if not cohort_filter.award_key:
            return None
        definition = AWARD_DEFINITIONS_BY_KEY.get(cohort_filter.award_key)
        if definition is None or definition.role_label not in {"pitchers", "players"}:
            return None
        cohort_kind = "award"
        cohort_value = cohort_filter.award_key
    metric_spec = detect_context_metric_spec(
        lowered,
        default_key="ops" if any(token in lowered for token in OFFENSIVE_SUMMARY_HINTS) else "ops",
    )
    descriptor, sort_desc = resolve_metric_sort(lowered, metric_spec)
    sample_basis, min_sample_size = resolve_context_sample_requirements(question, metric_spec)
    return OpponentPitcherCohortQuery(
        cohort_kind=cohort_kind,
        cohort_value=cohort_value,
        cohort_label=cohort_filter.label,
        metric_key=metric_spec.key,
        descriptor=descriptor,
        sort_desc=sort_desc,
        min_sample_size=min_sample_size,
        sample_basis=sample_basis,
        cohort_filter=cohort_filter,
    )


def build_pitcher_cohort_metric_expression(metric_key: str, alias: str = "cohort", *, aggregate: bool = False) -> str:
    def field(name: str) -> str:
        base = f"{alias}.{name}"
        return f"SUM({base})" if aggregate else base

    walks_expression = f"({field('walks')} + {field('intentional_walks')})"
    singles_expression = f"({field('hits')} - {field('doubles')} - {field('triples')} - {field('home_runs')})"
    total_bases_expression = (
        f"({singles_expression} + (2 * {field('doubles')}) + (3 * {field('triples')}) + (4 * {field('home_runs')}))"
    )
    if metric_key == "ba":
        return f"CAST({field('hits')} AS REAL) / NULLIF({field('at_bats')}, 0)"
    if metric_key == "obp":
        return (
            f"CAST({field('hits')} + {walks_expression} + {field('hit_by_pitch')} AS REAL) "
            f"/ NULLIF({field('at_bats')} + {walks_expression} + {field('hit_by_pitch')} + {field('sacrifice_flies')}, 0)"
        )
    if metric_key == "slg":
        return f"CAST({total_bases_expression} AS REAL) / NULLIF({field('at_bats')}, 0)"
    if metric_key == "ops":
        return (
            "("
            f"CAST({field('hits')} + {walks_expression} + {field('hit_by_pitch')} AS REAL) "
            f"/ NULLIF({field('at_bats')} + {walks_expression} + {field('hit_by_pitch')} + {field('sacrifice_flies')}, 0)"
            ") + ("
            f"CAST({total_bases_expression} AS REAL) / NULLIF({field('at_bats')}, 0)"
            ")"
        )
    column_map = {
        "home_runs": field("home_runs"),
        "hits": field("hits"),
        "walks": walks_expression,
        "strikeouts": field("strikeouts"),
        "runs_batted_in": field("runs_batted_in"),
        "doubles": field("doubles"),
        "triples": field("triples"),
        "plate_appearances": field("plate_appearances"),
        "at_bats": field("at_bats"),
        "hit_by_pitch": field("hit_by_pitch"),
    }
    column = column_map.get(metric_key, field("home_runs"))
    return f"CAST({column} AS REAL)"


def build_contextual_player_name_sql(connection, table_alias: str) -> tuple[str, str, str]:
    cte_parts: list[str] = []
    joins: list[str] = []
    name_parts: list[str] = []
    if table_exists(connection, "lahman_people"):
        cte_parts.append(
            """
            player_names AS (
                SELECT
                    playerid,
                    NULLIF(TRIM(COALESCE(namefirst, '') || ' ' || COALESCE(namelast, '')), '') AS player_name
                FROM lahman_people
                WHERE COALESCE(playerid, '') <> ''
            )
            """
        )
        joins.append(
            f"""
            LEFT JOIN player_names
              ON player_names.playerid = {table_alias}.player_id
            """
        )
        name_parts.append("player_names.player_name")
    if table_exists(connection, "retrosheet_allplayers"):
        cte_parts.append(
            """
            retro_names AS (
                SELECT
                    lower(COALESCE(id, '')) AS retroid,
                    NULLIF(TRIM(COALESCE(first, '') || ' ' || COALESCE(last, '')), '') AS player_name
                FROM retrosheet_allplayers
                GROUP BY lower(COALESCE(id, ''))
            )
            """
        )
        joins.append(
            f"""
            LEFT JOIN retro_names
              ON retro_names.retroid = lower({table_alias}.player_id)
            """
        )
        name_parts.append("retro_names.player_name")
    fallback_name = f"{table_alias}.player_id"
    name_parts.append(fallback_name)
    cte_sql = f"WITH {','.join(cte_parts)}" if cte_parts else ""
    joins_sql = "\n".join(joins)
    name_expression = fallback_name if len(name_parts) == 1 else f"COALESCE({', '.join(name_parts)})"
    return cte_sql, joins_sql, name_expression


def fetch_opponent_pitcher_cohort_rows(connection, query: OpponentPitcherCohortQuery) -> list[dict[str, Any]]:
    metric_expression = build_pitcher_cohort_metric_expression(query.metric_key, "cohort")
    qualification_clause = "AND 1 = 1"
    parameters: list[Any] = [query.cohort_kind, query.cohort_value]
    if query.sample_basis and query.min_sample_size > 0:
        qualification_clause = f"AND cohort.{query.sample_basis} >= ?"
        parameters.append(query.min_sample_size)
    order_direction = "DESC" if query.sort_desc else "ASC"
    cte_sql, joins_sql, name_expression = build_contextual_player_name_sql(connection, "cohort")
    rows = connection.execute(
        f"""
        {cte_sql}
        SELECT
            {name_expression} AS player_name,
            cohort.plate_appearances,
            cohort.at_bats,
            cohort.hits,
            cohort.doubles,
            cohort.triples,
            cohort.home_runs,
            cohort.walks,
            cohort.intentional_walks,
            cohort.hit_by_pitch,
            cohort.sacrifice_flies,
            cohort.strikeouts,
            cohort.runs_batted_in,
            cohort.pitchers_faced,
            cohort.first_season,
            cohort.last_season,
            {metric_expression} AS metric_value
        FROM retrosheet_player_opponent_pitcher_cohorts AS cohort
        {joins_sql}
        WHERE cohort.cohort_kind = ?
          AND cohort.cohort_value = ?
          {qualification_clause}
          AND ({metric_expression}) IS NOT NULL
        ORDER BY metric_value {order_direction}, cohort.plate_appearances DESC, player_name ASC
        LIMIT 5
        """,
        tuple(parameters),
    ).fetchall()
    return [
        {
            "player_name": str(row["player_name"]),
            "plate_appearances": int(row["plate_appearances"]),
            "at_bats": int(row["at_bats"]),
            "hits": int(row["hits"]),
            "doubles": int(row["doubles"]),
            "triples": int(row["triples"]),
            "home_runs": int(row["home_runs"]),
            "walks": int(row["walks"]),
            "intentional_walks": int(row["intentional_walks"]),
            "hit_by_pitch": int(row["hit_by_pitch"]),
            "sacrifice_flies": int(row["sacrifice_flies"]),
            "strikeouts": int(row["strikeouts"]),
            "runs_batted_in": int(row["runs_batted_in"]),
            "pitchers_faced": int(row["pitchers_faced"]),
            "first_season": int(row["first_season"]),
            "last_season": int(row["last_season"]),
            "metric_value": float(row["metric_value"]),
        }
        for row in rows
    ]


def resolve_opponent_pitcher_retroids(connection, query: OpponentPitcherCohortQuery) -> set[str]:
    cohort_filter = query.cohort_filter
    if cohort_filter is None:
        return set()
    resolved = resolve_cohort_filter(connection, cohort_filter)
    if resolved is None:
        return set()
    if not (table_exists(connection, "lahman_people") and table_exists(connection, "lahman_pitching")):
        return set()
    clauses = ["COALESCE(people.retroid, '') <> ''"]
    parameters: list[Any] = []
    identity_clauses: list[str] = []
    if resolved.player_ids:
        placeholders = ",".join("?" for _ in resolved.player_ids)
        identity_clauses.append(f"people.playerid IN ({placeholders})")
        parameters.extend(sorted(resolved.player_ids))
    if resolved.player_names:
        placeholders = ",".join("?" for _ in resolved.player_names)
        identity_clauses.append(
            "lower(trim(COALESCE(people.namefirst, '') || ' ' || COALESCE(people.namelast, ''))) "
            f"IN ({placeholders})"
        )
        parameters.extend(sorted(resolved.player_names))
    if identity_clauses:
        clauses.append("(" + " OR ".join(identity_clauses) + ")")
    rows = connection.execute(
        f"""
        SELECT DISTINCT lower(COALESCE(people.retroid, '')) AS retroid
        FROM lahman_people AS people
        JOIN lahman_pitching AS pitching
          ON pitching.playerid = people.playerid
        WHERE {' AND '.join(clauses)}
        """,
        tuple(parameters),
    ).fetchall()
    return {str(row["retroid"] or "").strip() for row in rows if str(row["retroid"] or "").strip()}


def fetch_opponent_pitcher_rows(
    connection,
    query: OpponentPitcherCohortQuery,
    pitcher_retroids: set[str],
) -> list[dict[str, Any]]:
    if not pitcher_retroids:
        return []
    placeholders = ",".join("?" for _ in pitcher_retroids)
    metric_expression = build_pitcher_cohort_metric_expression(query.metric_key, "matchup", aggregate=True)
    parameters: list[Any] = list(sorted(pitcher_retroids))
    having_clauses = [f"({metric_expression}) IS NOT NULL"]
    if query.sample_basis and query.min_sample_size > 0:
        having_clauses.append(f"SUM(matchup.{query.sample_basis}) >= ?")
        parameters.append(query.min_sample_size)
    order_direction = "DESC" if query.sort_desc else "ASC"
    cte_sql, joins_sql, name_expression = build_contextual_player_name_sql(connection, "matchup")
    rows = connection.execute(
        f"""
        {cte_sql}
        SELECT
            {name_expression} AS player_name,
            SUM(matchup.plate_appearances) AS plate_appearances,
            SUM(matchup.at_bats) AS at_bats,
            SUM(matchup.hits) AS hits,
            SUM(matchup.doubles) AS doubles,
            SUM(matchup.triples) AS triples,
            SUM(matchup.home_runs) AS home_runs,
            SUM(matchup.walks) AS walks,
            SUM(matchup.intentional_walks) AS intentional_walks,
            SUM(matchup.hit_by_pitch) AS hit_by_pitch,
            SUM(matchup.sacrifice_flies) AS sacrifice_flies,
            SUM(matchup.strikeouts) AS strikeouts,
            SUM(matchup.runs_batted_in) AS runs_batted_in,
            COUNT(DISTINCT matchup.pitcher_id) AS pitchers_faced,
            MIN(matchup.first_season) AS first_season,
            MAX(matchup.last_season) AS last_season,
            {metric_expression} AS metric_value
        FROM retrosheet_player_opponent_pitchers AS matchup
        {joins_sql}
        WHERE matchup.pitcher_id IN ({placeholders})
        GROUP BY matchup.player_id
        HAVING {' AND '.join(having_clauses)}
        ORDER BY metric_value {order_direction}, SUM(matchup.plate_appearances) DESC, player_name ASC
        LIMIT 5
        """,
        tuple(parameters),
    ).fetchall()
    return [
        {
            "player_name": str(row["player_name"]),
            "plate_appearances": int(row["plate_appearances"]),
            "at_bats": int(row["at_bats"]),
            "hits": int(row["hits"]),
            "doubles": int(row["doubles"]),
            "triples": int(row["triples"]),
            "home_runs": int(row["home_runs"]),
            "walks": int(row["walks"]),
            "intentional_walks": int(row["intentional_walks"]),
            "hit_by_pitch": int(row["hit_by_pitch"]),
            "sacrifice_flies": int(row["sacrifice_flies"]),
            "strikeouts": int(row["strikeouts"]),
            "runs_batted_in": int(row["runs_batted_in"]),
            "pitchers_faced": int(row["pitchers_faced"]),
            "first_season": int(row["first_season"]),
            "last_season": int(row["last_season"]),
            "metric_value": float(row["metric_value"]),
        }
        for row in rows
    ]


def build_opponent_pitcher_cohort_gap_snippet(query: OpponentPitcherCohortQuery) -> EvidenceSnippet:
    return EvidenceSnippet(
        source="Opponent Pitcher Cohorts",
        title=f"{query.cohort_label} {count_metric_label(query.metric_key)} source gap",
        citation="Historical opponent-pitcher cohort planner",
        summary=(
            f"I understand this as a leaderboard for {count_metric_phrase(query.metric_key)} against {query.cohort_label}. "
            "The project can answer that once the compact opponent-pitcher matchup warehouse has been built. "
            "Run `python -m mlb_history_bot sync-retrosheet-pitcher-cohorts` to build it."
        ),
        payload={
            "analysis_type": "contextual_source_gap",
            "metric": count_metric_label(query.metric_key),
            "context": query.cohort_label,
        },
    )


def build_opponent_pitcher_cohort_snippet(connection, query: OpponentPitcherCohortQuery) -> EvidenceSnippet:
    rows: list[dict[str, Any]] = []
    if table_exists(connection, "retrosheet_player_opponent_pitchers"):
        pitcher_retroids = resolve_opponent_pitcher_retroids(connection, query)
        if pitcher_retroids:
            rows = fetch_opponent_pitcher_rows(connection, query, pitcher_retroids)
    if not rows and query.cohort_kind == "award" and table_exists(connection, "retrosheet_player_opponent_pitcher_cohorts"):
        rows = fetch_opponent_pitcher_cohort_rows(connection, query)
    if not rows:
        return build_opponent_pitcher_cohort_gap_snippet(query)
    lead = rows[0]
    summary = (
        f"Across imported Retrosheet history, the {query.descriptor} {count_metric_phrase(query.metric_key)} "
        f"against {query.cohort_label} belongs to {lead['player_name']} at "
        f"{format_context_metric_value(query.metric_key, lead['metric_value'])}. "
        f"That line spans {lead['plate_appearances']} plate appearances against {lead['pitchers_faced']} qualifying pitchers "
        f"from {lead['first_season']} to {lead['last_season']}."
    )
    if query.min_sample_size and query.sample_basis:
        summary = (
            f"{summary} I used a minimum of {query.min_sample_size} "
            f"{sample_basis_phrase(query.sample_basis)} for stability."
        )
    trailing = rows[1:4]
    if trailing:
        summary = (
            f"{summary} Next on the board: "
            + "; ".join(
                f"{row['player_name']} {format_context_metric_value(query.metric_key, row['metric_value'])}"
                for row in trailing
            )
            + "."
        )
    return EvidenceSnippet(
        source="Opponent Pitcher Cohorts",
        title=f"{query.cohort_label} {count_metric_label(query.metric_key)} leaderboard",
        citation="Retrosheet plays.csv aggregated by hitter against opponent-pitcher award cohorts",
        summary=summary,
        payload={
            "analysis_type": "opponent_pitcher_cohort_leaderboard",
            "mode": "historical",
            "metric": count_metric_label(query.metric_key),
            "cohort_kind": query.cohort_kind,
            "cohort_value": query.cohort_value,
            "cohort_label": query.cohort_label,
            "leaders": rows,
        },
    )


def format_context_metric_value(metric_key: str, value: float) -> str:
    spec = CONTEXT_METRIC_BY_KEY.get(metric_key)
    if spec is not None and spec.kind == "rate":
        return f"{value:.3f}"
    return str(int(round(value)))


def parse_team_relationship_query(question: str) -> TeamRelationshipQuery | None:
    lowered = f" {question.lower()} "
    relationship = None
    if any(token in lowered for token in FORMER_TEAM_HINTS):
        relationship = "former"
    elif any(token in lowered for token in FUTURE_TEAM_HINTS):
        relationship = "future"
    if relationship is None:
        return None
    metric_spec = detect_context_metric_spec(
        lowered,
        default_key="ops" if any(token in lowered for token in OFFENSIVE_SUMMARY_HINTS) else "ops",
    )
    descriptor, sort_desc = resolve_metric_sort(lowered, metric_spec)
    sample_basis, min_sample_size = resolve_context_sample_requirements(question, metric_spec)
    return TeamRelationshipQuery(
        relationship=relationship,
        metric_key=metric_spec.key,
        descriptor=descriptor,
        sort_desc=sort_desc,
        min_sample_size=min_sample_size,
        sample_basis=sample_basis,
        aggregate_scope="player" if any(token in lowered for token in AGGREGATE_RELATIONSHIP_HINTS) else "opponent",
    )


def build_team_relationship_snippet(connection, query: TeamRelationshipQuery) -> EvidenceSnippet | None:
    if table_exists(connection, "retrosheet_player_opponent_contexts"):
        rows = fetch_team_relationship_rows(connection, query)
        if not rows:
            return build_team_relationship_gap_snippet(query)
        return build_team_relationship_evidence(query, rows)
    if not table_exists(connection, "retrosheet_batting") or not table_exists(connection, "lahman_batting"):
        return None
    return build_team_relationship_gap_snippet(query)


def fetch_team_relationship_rows(connection, query: TeamRelationshipQuery) -> list[dict[str, Any]]:
    context_key = "former_team" if query.relationship == "former" else "future_team"
    metric_expression = build_team_relationship_metric_expression(query.metric_key)
    qualification_clause = "AND 1 = 1"
    parameters: list[Any] = [context_key]
    if query.sample_basis and query.min_sample_size > 0:
        qualification_clause = f"AND context.{query.sample_basis} >= ?"
        parameters.append(query.min_sample_size)
    order_direction = "DESC" if query.sort_desc else "ASC"
    if query.aggregate_scope == "player":
        rows = connection.execute(
            f"""
            WITH player_names AS (
                SELECT
                    playerid,
                    NULLIF(TRIM(COALESCE(namefirst, '') || ' ' || COALESCE(namelast, '')), '') AS player_name
                FROM lahman_people
                WHERE COALESCE(playerid, '') <> ''
            ),
            opponent_counts AS (
                SELECT
                    player_id,
                    COUNT(DISTINCT opponent) AS teams_matched,
                    GROUP_CONCAT(DISTINCT opponent) AS opponent_codes,
                    SUM(plate_appearances) AS plate_appearances,
                    SUM(at_bats) AS at_bats,
                    SUM(hits) AS hits,
                    SUM(doubles) AS doubles,
                    SUM(triples) AS triples,
                    SUM(home_runs) AS home_runs,
                    SUM(walks) AS walks,
                    SUM(intentional_walks) AS intentional_walks,
                    SUM(hit_by_pitch) AS hit_by_pitch,
                    SUM(sacrifice_flies) AS sacrifice_flies,
                    SUM(strikeouts) AS strikeouts,
                    SUM(runs_batted_in) AS runs_batted_in,
                    MIN(first_season) AS first_season,
                    MAX(last_season) AS last_season
                FROM retrosheet_player_opponent_contexts
                WHERE context_key = ?
                GROUP BY player_id
            )
            SELECT
                COALESCE(player_names.player_name, context.player_id) AS player_name,
                context.teams_matched,
                context.opponent_codes,
                context.plate_appearances,
                context.at_bats,
                context.hits,
                context.doubles,
                context.triples,
                context.home_runs,
                context.walks,
                context.intentional_walks,
                context.hit_by_pitch,
                context.sacrifice_flies,
                context.strikeouts,
                context.runs_batted_in,
                context.first_season,
                context.last_season,
                {metric_expression.replace('context.', 'context.')} AS metric_value
            FROM opponent_counts AS context
            LEFT JOIN player_names
              ON player_names.playerid = context.player_id
            WHERE 1 = 1
              {qualification_clause}
              AND ({metric_expression.replace('context.', 'context.')}) IS NOT NULL
            ORDER BY metric_value {order_direction}, context.plate_appearances DESC, player_name ASC
            LIMIT 5
            """,
            tuple(parameters),
        ).fetchall()
    else:
        rows = connection.execute(
            f"""
            WITH player_names AS (
                SELECT
                    playerid,
                    NULLIF(TRIM(COALESCE(namefirst, '') || ' ' || COALESCE(namelast, '')), '') AS player_name
                FROM lahman_people
                WHERE COALESCE(playerid, '') <> ''
            ),
            opponent_names AS (
                SELECT
                    teamidretro AS team_code,
                    name,
                    ROW_NUMBER() OVER (PARTITION BY teamidretro ORDER BY CAST(yearid AS INTEGER) DESC) AS rn
                FROM lahman_teams
                WHERE COALESCE(teamidretro, '') <> ''
            )
            SELECT
                COALESCE(player_names.player_name, context.player_id) AS player_name,
                context.opponent,
                COALESCE(onames.name, context.opponent) AS opponent_name,
                context.plate_appearances,
                context.at_bats,
                context.hits,
                context.doubles,
                context.triples,
                context.home_runs,
                context.walks,
                context.intentional_walks,
                context.hit_by_pitch,
                context.sacrifice_flies,
                context.strikeouts,
                context.runs_batted_in,
                context.first_season,
                context.last_season,
                {metric_expression} AS metric_value
            FROM retrosheet_player_opponent_contexts AS context
            LEFT JOIN player_names
              ON player_names.playerid = context.player_id
            LEFT JOIN opponent_names AS onames
              ON onames.team_code = context.opponent
             AND onames.rn = 1
            WHERE context.context_key = ?
              {qualification_clause}
              AND ({metric_expression}) IS NOT NULL
            ORDER BY metric_value {order_direction}, context.plate_appearances DESC, player_name ASC
            LIMIT 5
            """,
            tuple(parameters),
        ).fetchall()
    if not rows:
        return None
    leaders = []
    for row in rows:
        entry = {
            "player_name": str(row["player_name"]),
            "plate_appearances": int(row["plate_appearances"]),
            "at_bats": int(row["at_bats"]),
            "hits": int(row["hits"]),
            "doubles": int(row["doubles"]),
            "triples": int(row["triples"]),
            "home_runs": int(row["home_runs"]),
            "walks": int(row["walks"]),
            "intentional_walks": int(row["intentional_walks"]),
            "hit_by_pitch": int(row["hit_by_pitch"]),
            "sacrifice_flies": int(row["sacrifice_flies"]),
            "strikeouts": int(row["strikeouts"]),
            "runs_batted_in": int(row["runs_batted_in"]),
            "first_season": int(row["first_season"]),
            "last_season": int(row["last_season"]),
            "metric_value": float(row["metric_value"]),
        }
        if query.aggregate_scope == "player":
            entry["teams_matched"] = int(row["teams_matched"])
            entry["opponent_name"] = f"{int(row['teams_matched'])} team(s)"
            entry["opponent_codes"] = str(row["opponent_codes"] or "")
        else:
            entry["opponent_name"] = str(row["opponent_name"])
        leaders.append(entry)
    return leaders


def build_team_relationship_evidence(query: TeamRelationshipQuery, leaders: list[dict[str, Any]]) -> EvidenceSnippet:
    lead = leaders[0]
    relationship_label = "former team" if query.relationship == "former" else "future team"
    metric_spec = CONTEXT_METRIC_BY_KEY[query.metric_key]
    if query.aggregate_scope == "player":
        summary = (
            f"The {query.descriptor} {count_metric_phrase(query.metric_key)} aggregated across all {relationship_label}s belongs to "
            f"{lead['player_name']} at {format_context_metric_value(query.metric_key, lead['metric_value'])} "
            f"across {lead['plate_appearances']} plate appearances versus {lead.get('teams_matched', 0)} matching team(s) "
            f"from {lead['first_season']} to {lead['last_season']}."
        )
    else:
        summary = (
            f"The {query.descriptor} {count_metric_phrase(query.metric_key)} against a {relationship_label} belongs to "
            f"{lead['player_name']} versus {lead['opponent_name']} at "
            f"{format_context_metric_value(query.metric_key, lead['metric_value'])} "
            f"across {lead['plate_appearances']} plate appearances from {lead['first_season']} to {lead['last_season']}."
        )
    if query.sample_basis and query.min_sample_size > 0:
        summary = (
            f"{summary} I used a minimum of {query.min_sample_size} "
            f"{sample_basis_phrase(query.sample_basis)} for relevance."
        )
    if metric_spec.kind == "count":
        summary = (
            f"{summary} That line includes {lead['hits']} hits, {lead['home_runs']} home runs, and "
            f"{lead['runs_batted_in']} RBI."
        )
    trailing = leaders[1:4]
    if trailing:
        if query.aggregate_scope == "player":
            next_rows = "; ".join(
                f"{row['player_name']} {format_context_metric_value(query.metric_key, row['metric_value'])}"
                f" across {row.get('teams_matched', 0)} team(s)"
                for row in trailing
            )
        else:
            next_rows = "; ".join(
                f"{row['player_name']} vs {row['opponent_name']} {format_context_metric_value(query.metric_key, row['metric_value'])}"
                for row in trailing
            )
        summary = f"{summary} Next on the board: {next_rows}."
    return EvidenceSnippet(
        source="Historical Matchup Context",
        title=f"{relationship_label.title()} {count_metric_label(query.metric_key)} leaderboard",
        citation="Retrosheet batting game logs joined to Lahman player-team history",
        summary=summary,
        payload={
            "analysis_type": "player_team_context_leaderboard",
            "relationship": query.relationship,
            "metric": count_metric_label(query.metric_key),
            "aggregate_scope": query.aggregate_scope,
            "leaders": leaders,
        },
    )


def build_team_relationship_gap_snippet(query: TeamRelationshipQuery) -> EvidenceSnippet:
    relationship_label = "former team" if query.relationship == "former" else "future team"
    return EvidenceSnippet(
        source="Historical Matchup Context",
        title=f"{relationship_label.title()} {count_metric_label(query.metric_key)} source gap",
        citation="Historical matchup context planner",
        summary=(
            f"I understand this as a leaderboard for {count_metric_phrase(query.metric_key)} against a {relationship_label}. "
            "The project can answer that once the compact former-team/future-team context table has been built. "
            "Run `python -m mlb_history_bot sync-retrosheet-contexts` to build it."
        ),
        payload={
            "analysis_type": "contextual_source_gap",
            "metric": count_metric_label(query.metric_key),
            "context": relationship_label,
        },
    )


def sync_retrosheet_player_opponent_pitcher_cohorts(
    settings: Settings,
    *,
    retrosheet_dir: Path | None = None,
    chunk_size: int = 250_000,
) -> list[str]:
    source_dir = retrosheet_dir or (settings.raw_data_dir / "retrosheet")
    if not source_dir.exists():
        return [f"Retrosheet directory not found at {source_dir}."]

    connection = get_connection(settings.database_path)
    initialize_database(connection)
    clear_retrosheet_player_opponent_pitcher_cohorts(connection)
    clear_retrosheet_player_opponent_pitchers(connection)
    cohort_sets = load_pitcher_award_cohort_memberships(connection)
    totals: dict[tuple[str, str, str], dict[str, int | str | set[str]]] = {}
    pitcher_totals: dict[tuple[str, str], dict[str, int | str]] = {}
    total_chunks = 0
    total_regular_plays = 0
    source_description = ""
    try:
        with open_retrosheet_plays_stream(source_dir) as handle:
            source_description = handle.name if hasattr(handle, "name") else "plays.csv stream"
            reader = pd.read_csv(
                handle,
                usecols=OPPONENT_PITCHER_COHORT_USECOLS,
                dtype=str,
                chunksize=chunk_size,
                low_memory=False,
            )
            for chunk in reader:
                aggregate_opponent_pitcher_chunk(chunk, pitcher_totals)
                if cohort_sets:
                    aggregate_pitcher_cohort_chunk(chunk, cohort_sets, totals)
                total_chunks += 1
                frame = chunk.copy()
                frame["gametype"] = frame["gametype"].fillna("").str.lower()
                total_regular_plays += int((frame["gametype"] == "regular").sum())
        pitcher_written = upsert_retrosheet_player_opponent_pitchers(connection, pitcher_totals.values())
        for entry in totals.values():
            if isinstance(entry.get("pitchers_faced"), set):
                entry["pitchers_faced"] = len(entry["pitchers_faced"])
        cohort_written = upsert_retrosheet_player_opponent_pitcher_cohorts(connection, totals.values())
        set_metadata_value(connection, "retrosheet_player_opponent_pitcher_cohorts_last_sync", pd.Timestamp.utcnow().isoformat())
    finally:
        connection.close()

    if not pitcher_totals and not totals:
        return [f"No opponent-pitcher matchup rows were built from {source_dir}."]
    return [
        (
            "Built Retrosheet opponent-pitcher matchup totals "
            f"from {source_description} ({total_chunks} chunk(s), {total_regular_plays:,} regular-season plays scanned, "
            f"{pitcher_written:,} pitcher-matchup rows stored, {cohort_written:,} cohort rows stored)."
        )
    ]


def aggregate_opponent_pitcher_chunk(
    chunk: pd.DataFrame,
    totals: dict[tuple[str, str], dict[str, int | str]],
) -> None:
    if chunk.empty:
        return
    frame = chunk.copy()
    frame["gametype"] = frame["gametype"].fillna("").str.lower()
    frame = frame[frame["gametype"] == "regular"]
    if frame.empty:
        return
    for column in ("batter", "pitcher", "date"):
        frame[column] = frame[column].fillna("").astype(str).str.strip().str.lower()
    frame = frame[frame["batter"].ne("") & frame["pitcher"].ne("") & frame["date"].str.match(r"^\d{8}$", na=False)]
    if frame.empty:
        return
    frame["season"] = pd.to_numeric(frame["date"].str[:4], errors="coerce").fillna(0).astype(int)
    for column in ("pa", "ab", "single", "double", "triple", "hr", "walk", "iw", "hbp", "sf", "k", "rbi"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)
    frame["hits"] = frame["single"] + frame["double"] + frame["triple"] + frame["hr"]
    grouped = (
        frame.groupby(["batter", "pitcher"], sort=False)
        .agg(
            plate_appearances=("pa", "sum"),
            at_bats=("ab", "sum"),
            hits=("hits", "sum"),
            doubles=("double", "sum"),
            triples=("triple", "sum"),
            home_runs=("hr", "sum"),
            walks=("walk", "sum"),
            intentional_walks=("iw", "sum"),
            hit_by_pitch=("hbp", "sum"),
            sacrifice_flies=("sf", "sum"),
            strikeouts=("k", "sum"),
            runs_batted_in=("rbi", "sum"),
            first_season=("season", "min"),
            last_season=("season", "max"),
        )
        .reset_index()
    )
    for row in grouped.itertuples(index=False):
        key = (str(row.batter), str(row.pitcher))
        entry = totals.get(key)
        if entry is None:
            totals[key] = {
                "player_id": str(row.batter),
                "pitcher_id": str(row.pitcher),
                "plate_appearances": int(row.plate_appearances),
                "at_bats": int(row.at_bats),
                "hits": int(row.hits),
                "doubles": int(row.doubles),
                "triples": int(row.triples),
                "home_runs": int(row.home_runs),
                "walks": int(row.walks),
                "intentional_walks": int(row.intentional_walks),
                "hit_by_pitch": int(row.hit_by_pitch),
                "sacrifice_flies": int(row.sacrifice_flies),
                "strikeouts": int(row.strikeouts),
                "runs_batted_in": int(row.runs_batted_in),
                "first_season": int(row.first_season),
                "last_season": int(row.last_season),
            }
            continue
        for field in (
            "plate_appearances",
            "at_bats",
            "hits",
            "doubles",
            "triples",
            "home_runs",
            "walks",
            "intentional_walks",
            "hit_by_pitch",
            "sacrifice_flies",
            "strikeouts",
            "runs_batted_in",
        ):
            entry[field] = int(entry[field]) + int(getattr(row, field))
        entry["first_season"] = min(int(entry["first_season"]), int(row.first_season))
        entry["last_season"] = max(int(entry["last_season"]), int(row.last_season))


def load_pitcher_award_cohort_memberships(connection) -> dict[tuple[str, str, str], set[str]]:
    cohort_sets: dict[tuple[str, str, str], set[str]] = {}
    for award_key, definition in AWARD_DEFINITIONS_BY_KEY.items():
        if definition.role_label != "pitchers":
            continue
        retroids = load_award_retroids(connection, award_key)
        if retroids:
            cohort_sets[("award", award_key, definition.label)] = retroids
    return cohort_sets


def load_award_retroids(connection, award_key: str) -> set[str]:
    player_ids, player_names = load_award_identities(connection, award_key)
    retroids: set[str] = set()
    if table_exists(connection, "lahman_people"):
        if player_ids:
            placeholders = ",".join("?" for _ in player_ids)
            rows = connection.execute(
                f"""
                SELECT lower(COALESCE(retroid, '')) AS retroid
                FROM lahman_people
                WHERE playerid IN ({placeholders})
                """,
                tuple(sorted(player_ids)),
            ).fetchall()
            retroids.update(str(row["retroid"] or "").strip() for row in rows if str(row["retroid"] or "").strip())
        if player_names:
            placeholders = ",".join("?" for _ in player_names)
            rows = connection.execute(
                f"""
                SELECT lower(COALESCE(retroid, '')) AS retroid
                FROM lahman_people
                WHERE lower(trim(COALESCE(namefirst, '') || ' ' || COALESCE(namelast, ''))) IN ({placeholders})
                """,
                tuple(sorted(player_names)),
            ).fetchall()
            retroids.update(str(row["retroid"] or "").strip() for row in rows if str(row["retroid"] or "").strip())
    if not retroids and player_names and table_exists(connection, "retrosheet_allplayers"):
        placeholders = ",".join("?" for _ in player_names)
        rows = connection.execute(
            f"""
            SELECT lower(COALESCE(id, '')) AS retroid
            FROM retrosheet_allplayers
            WHERE lower(trim(COALESCE(first, '') || ' ' || COALESCE(last, ''))) IN ({placeholders})
            """,
            tuple(sorted(player_names)),
        ).fetchall()
        retroids.update(str(row["retroid"] or "").strip() for row in rows if str(row["retroid"] or "").strip())
    return retroids


def aggregate_pitcher_cohort_chunk(
    chunk: pd.DataFrame,
    cohort_sets: dict[tuple[str, str, str], set[str]],
    totals: dict[tuple[str, str, str], dict[str, int | str | set[str]]],
) -> None:
    if chunk.empty or not cohort_sets:
        return
    frame = chunk.copy()
    frame["gametype"] = frame["gametype"].fillna("").str.lower()
    frame = frame[frame["gametype"] == "regular"]
    if frame.empty:
        return
    for column in ("batter", "pitcher", "date"):
        frame[column] = frame[column].fillna("").astype(str).str.strip().str.lower()
    frame = frame[frame["batter"].ne("") & frame["pitcher"].ne("") & frame["date"].str.match(r"^\d{8}$", na=False)]
    if frame.empty:
        return
    frame["season"] = pd.to_numeric(frame["date"].str[:4], errors="coerce").fillna(0).astype(int)
    for column in ("pa", "ab", "single", "double", "triple", "hr", "walk", "iw", "hbp", "sf", "k", "rbi"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)
    frame["hits"] = frame["single"] + frame["double"] + frame["triple"] + frame["hr"]

    for (cohort_kind, cohort_value, _cohort_label), retroids in cohort_sets.items():
        cohort_frame = frame[frame["pitcher"].isin(retroids)]
        if cohort_frame.empty:
            continue
        pitcher_sets_by_batter = cohort_frame.groupby("batter", sort=False)["pitcher"].agg(lambda series: set(series.tolist()))
        grouped = (
            cohort_frame.groupby("batter", sort=False)
            .agg(
                plate_appearances=("pa", "sum"),
                at_bats=("ab", "sum"),
                hits=("hits", "sum"),
                doubles=("double", "sum"),
                triples=("triple", "sum"),
                home_runs=("hr", "sum"),
                walks=("walk", "sum"),
                intentional_walks=("iw", "sum"),
                hit_by_pitch=("hbp", "sum"),
                sacrifice_flies=("sf", "sum"),
                strikeouts=("k", "sum"),
                runs_batted_in=("rbi", "sum"),
                first_season=("season", "min"),
                last_season=("season", "max"),
                pitchers_faced=("pitcher", pd.Series.nunique),
            )
            .reset_index()
        )
        for row in grouped.itertuples(index=False):
            key = (str(row.batter), cohort_kind, cohort_value)
            entry = totals.get(key)
            if entry is None:
                totals[key] = {
                    "player_id": str(row.batter),
                    "cohort_kind": cohort_kind,
                    "cohort_value": cohort_value,
                    "plate_appearances": int(row.plate_appearances),
                    "at_bats": int(row.at_bats),
                    "hits": int(row.hits),
                    "doubles": int(row.doubles),
                    "triples": int(row.triples),
                    "home_runs": int(row.home_runs),
                    "walks": int(row.walks),
                    "intentional_walks": int(row.intentional_walks),
                    "hit_by_pitch": int(row.hit_by_pitch),
                    "sacrifice_flies": int(row.sacrifice_flies),
                    "strikeouts": int(row.strikeouts),
                    "runs_batted_in": int(row.runs_batted_in),
                    "pitchers_faced": set(),
                    "first_season": int(row.first_season),
                    "last_season": int(row.last_season),
                }
                entry = totals[key]
            else:
                for field in (
                    "plate_appearances",
                    "at_bats",
                    "hits",
                    "doubles",
                    "triples",
                    "home_runs",
                    "walks",
                    "intentional_walks",
                    "hit_by_pitch",
                    "sacrifice_flies",
                    "strikeouts",
                    "runs_batted_in",
                ):
                    entry[field] = int(entry[field]) + int(getattr(row, field))
                entry["first_season"] = min(int(entry["first_season"]), int(row.first_season))
                entry["last_season"] = max(int(entry["last_season"]), int(row.last_season))
            pitchers_faced = pitcher_sets_by_batter.get(row.batter, set())
            entry["pitchers_faced"] = set(entry["pitchers_faced"]) | pitchers_faced

def ensure_contextual_indexes(connection) -> None:
    statements = [
        "CREATE INDEX IF NOT EXISTS idx_retrosheet_batting_id_opp_date ON retrosheet_batting (id, opp, date)",
        "CREATE INDEX IF NOT EXISTS idx_lahman_people_retroid ON lahman_people (retroid)",
        "CREATE INDEX IF NOT EXISTS idx_lahman_batting_playerid_year_teamid ON lahman_batting (playerid, yearid, teamid)",
        "CREATE INDEX IF NOT EXISTS idx_lahman_teams_year_teamidretro ON lahman_teams (yearid, teamidretro)",
        "CREATE INDEX IF NOT EXISTS idx_lahman_teams_year_teamid ON lahman_teams (yearid, teamid)",
    ]
    for statement in statements:
        try:
            connection.execute(statement)
        except Exception:
            continue
    connection.commit()


def sync_retrosheet_player_count_splits(
    settings: Settings,
    *,
    retrosheet_dir: Path | None = None,
    chunk_size: int = 250_000,
) -> list[str]:
    source_dir = retrosheet_dir or (settings.raw_data_dir / "retrosheet")
    if not source_dir.exists():
        return [f"Retrosheet directory not found at {source_dir}."]

    connection = get_connection(settings.database_path)
    initialize_database(connection)
    clear_retrosheet_player_count_splits(connection)
    clear_retrosheet_player_reached_count_splits(connection)
    totals: dict[tuple[str, str], dict[str, int | str]] = {}
    reached_totals: dict[tuple[str, str], dict[str, int | str]] = {}
    total_chunks = 0
    total_regular_plays = 0
    source_description = ""
    try:
        with open_retrosheet_plays_stream(source_dir) as handle:
            source_description = handle.name if hasattr(handle, "name") else "plays.csv stream"
            reader = pd.read_csv(
                handle,
                usecols=COUNT_PLAY_USECOLS,
                dtype=str,
                chunksize=chunk_size,
                low_memory=False,
            )
            for chunk in reader:
                aggregate_count_chunk(chunk, totals)
                aggregate_reached_count_chunk(chunk, reached_totals)
                total_chunks += 1
                total_regular_plays += int(len(chunk))
        terminal_written = upsert_retrosheet_player_count_splits(connection, totals.values())
        reached_written = upsert_retrosheet_player_reached_count_splits(connection, reached_totals.values())
        set_metadata_value(connection, "retrosheet_player_count_splits_last_sync", pd.Timestamp.utcnow().isoformat())
    finally:
        connection.close()

    if not totals:
        return [f"No regular-season count split rows were built from {source_dir}."]
    return [
        (
            "Built Retrosheet player count split totals "
            f"from {source_description} ({total_chunks} chunk(s), {total_regular_plays:,} raw plays scanned, "
            f"{terminal_written:,} terminal-count rows and {reached_written:,} reached-count rows stored)."
        )
    ]


def sync_retrosheet_player_opponent_contexts(
    settings: Settings,
    *,
    retrosheet_dir: Path | None = None,
    chunk_size: int = 250_000,
) -> list[str]:
    source_dir = retrosheet_dir or (settings.raw_data_dir / "retrosheet")
    batting_path = source_dir / "batting.csv"
    if not batting_path.exists():
        return [f"Retrosheet batting.csv not found at {batting_path}."]

    connection = get_connection(settings.database_path)
    initialize_database(connection)
    clear_retrosheet_player_opponent_contexts(connection)
    retroid_to_playerid, season_extremes = load_player_team_history_maps(connection)
    totals: dict[tuple[str, str, str], dict[str, int | str]] = {}
    total_chunks = 0
    total_rows = 0
    try:
        reader = pd.read_csv(
            batting_path,
            usecols=OPPONENT_CONTEXT_USECOLS,
            dtype=str,
            chunksize=chunk_size,
            low_memory=False,
        )
        for chunk in reader:
            aggregate_opponent_context_chunk(chunk, totals, retroid_to_playerid, season_extremes)
            total_chunks += 1
            total_rows += int(len(chunk))
        written = upsert_retrosheet_player_opponent_contexts(connection, totals.values())
        set_metadata_value(connection, "retrosheet_player_opponent_contexts_last_sync", pd.Timestamp.utcnow().isoformat())
    finally:
        connection.close()

    if not totals:
        return [f"No opponent-context rows were built from {batting_path}."]
    return [
        (
            "Built Retrosheet player opponent context totals "
            f"from {batting_path} ({total_chunks} chunk(s), {total_rows:,} batting rows scanned, {written:,} player-opponent context rows stored)."
        )
    ]


def load_player_team_history_maps(connection) -> tuple[dict[str, str], dict[tuple[str, str], tuple[int, int]]]:
    people_rows = connection.execute(
        """
        SELECT lower(COALESCE(retroid, '')) AS retroid, playerid
        FROM lahman_people
        WHERE COALESCE(retroid, '') <> '' AND COALESCE(playerid, '') <> ''
        """
    ).fetchall()
    retroid_to_playerid = {str(row["retroid"]): str(row["playerid"]) for row in people_rows}
    history_rows = connection.execute(
        """
        SELECT
            batting.playerid,
            COALESCE(teams.teamidretro, teams.teamidbr, teams.teamid) AS team_code,
            MIN(CAST(batting.yearid AS INTEGER)) AS first_season,
            MAX(CAST(batting.yearid AS INTEGER)) AS last_season
        FROM lahman_batting AS batting
        JOIN lahman_teams AS teams
          ON teams.yearid = batting.yearid
         AND teams.teamid = batting.teamid
        WHERE COALESCE(teams.teamidretro, teams.teamidbr, teams.teamid, '') <> ''
        GROUP BY batting.playerid, COALESCE(teams.teamidretro, teams.teamidbr, teams.teamid)
        """
    ).fetchall()
    season_extremes = {
        (str(row["playerid"]), str(row["team_code"])): (int(row["first_season"]), int(row["last_season"]))
        for row in history_rows
    }
    return retroid_to_playerid, season_extremes


def aggregate_opponent_context_chunk(
    chunk: pd.DataFrame,
    totals: dict[tuple[str, str, str], dict[str, int | str]],
    retroid_to_playerid: dict[str, str],
    season_extremes: dict[tuple[str, str], tuple[int, int]],
) -> None:
    if chunk.empty:
        return
    frame = chunk.copy()
    frame["gametype"] = frame["gametype"].fillna("").str.lower()
    frame = frame[frame["gametype"] == "regular"]
    if frame.empty:
        return
    frame["retroid"] = frame["id"].fillna("").astype(str).str.lower()
    frame["player_id"] = frame["retroid"].map(retroid_to_playerid)
    frame = frame[frame["player_id"].notna()].copy()
    if frame.empty:
        return
    frame["season"] = pd.to_numeric(frame["date"].fillna("").str[:4], errors="coerce").fillna(0).astype(int)
    for column in ("b_pa", "b_ab", "b_h", "b_d", "b_t", "b_hr", "b_rbi", "b_w", "b_iw", "b_hbp", "b_sf", "b_k"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)
    for row in frame.itertuples(index=False):
        player_id = str(row.player_id)
        opponent = str(row.opp or "").strip()
        if not opponent:
            continue
        season_pair = season_extremes.get((player_id, opponent))
        if season_pair is None:
            continue
        first_season, last_season = season_pair
        if first_season < int(row.season):
            accumulate_opponent_context(
                totals,
                player_id=player_id,
                opponent=opponent,
                context_key="former_team",
                row=row,
            )
        if last_season > int(row.season):
            accumulate_opponent_context(
                totals,
                player_id=player_id,
                opponent=opponent,
                context_key="future_team",
                row=row,
            )


def accumulate_opponent_context(
    totals: dict[tuple[str, str, str], dict[str, int | str]],
    *,
    player_id: str,
    opponent: str,
    context_key: str,
    row,
) -> None:
    key = (player_id, opponent, context_key)
    entry = totals.setdefault(
        key,
        {
            "player_id": player_id,
            "opponent": opponent,
            "context_key": context_key,
            "plate_appearances": 0,
            "at_bats": 0,
            "hits": 0,
            "doubles": 0,
            "triples": 0,
            "home_runs": 0,
            "walks": 0,
            "intentional_walks": 0,
            "hit_by_pitch": 0,
            "sacrifice_flies": 0,
            "strikeouts": 0,
            "runs_batted_in": 0,
            "first_season": int(row.season),
            "last_season": int(row.season),
        },
    )
    entry["plate_appearances"] = int(entry["plate_appearances"]) + int(row.b_pa)
    entry["at_bats"] = int(entry["at_bats"]) + int(row.b_ab)
    entry["hits"] = int(entry["hits"]) + int(row.b_h)
    entry["doubles"] = int(entry["doubles"]) + int(row.b_d)
    entry["triples"] = int(entry["triples"]) + int(row.b_t)
    entry["home_runs"] = int(entry["home_runs"]) + int(row.b_hr)
    entry["walks"] = int(entry["walks"]) + int(row.b_w)
    entry["intentional_walks"] = int(entry["intentional_walks"]) + int(row.b_iw)
    entry["hit_by_pitch"] = int(entry["hit_by_pitch"]) + int(row.b_hbp)
    entry["sacrifice_flies"] = int(entry["sacrifice_flies"]) + int(row.b_sf)
    entry["strikeouts"] = int(entry["strikeouts"]) + int(row.b_k)
    entry["runs_batted_in"] = int(entry["runs_batted_in"]) + int(row.b_rbi)
    entry["first_season"] = min(int(entry["first_season"]), int(row.season))
    entry["last_season"] = max(int(entry["last_season"]), int(row.season))


def aggregate_count_chunk(chunk: pd.DataFrame, totals: dict[tuple[str, str], dict[str, int | str]]) -> None:
    if chunk.empty:
        return
    frame = chunk.copy()
    frame["gametype"] = frame["gametype"].fillna("").str.lower()
    frame = frame[frame["gametype"] == "regular"]
    if frame.empty:
        return
    frame["balls"] = frame["balls"].fillna("").astype(str).str.strip()
    frame["strikes"] = frame["strikes"].fillna("").astype(str).str.strip()
    frame = frame[frame["balls"].str.fullmatch(r"[0-3]") & frame["strikes"].str.fullmatch(r"[0-2]")]
    if frame.empty:
        return
    frame["count_key"] = frame["balls"] + "-" + frame["strikes"]
    frame["season"] = pd.to_numeric(frame["date"].fillna("").str[:4], errors="coerce").fillna(0).astype(int)
    for column in ("pa", "ab", "single", "double", "triple", "hr", "walk", "iw", "hbp", "sf", "k", "rbi"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)
    frame["hits"] = frame["single"] + frame["double"] + frame["triple"] + frame["hr"]
    frame["walks_total"] = frame["walk"] + frame["iw"]
    grouped = (
        frame.groupby(["batter", "count_key"], sort=False)
        .agg(
            plate_appearances=("pa", "sum"),
            at_bats=("ab", "sum"),
            hits=("hits", "sum"),
            doubles=("double", "sum"),
            triples=("triple", "sum"),
            home_runs=("hr", "sum"),
            walks=("walks_total", "sum"),
            hit_by_pitch=("hbp", "sum"),
            sacrifice_flies=("sf", "sum"),
            strikeouts=("k", "sum"),
            runs_batted_in=("rbi", "sum"),
            first_season=("season", "min"),
            last_season=("season", "max"),
        )
        .reset_index()
    )
    for row in grouped.itertuples(index=False):
        key = (str(row.batter), str(row.count_key))
        existing = totals.get(key)
        if existing is None:
            totals[key] = {
                "player_id": str(row.batter),
                "count_key": str(row.count_key),
                "plate_appearances": int(row.plate_appearances),
                "at_bats": int(row.at_bats),
                "hits": int(row.hits),
                "doubles": int(row.doubles),
                "triples": int(row.triples),
                "home_runs": int(row.home_runs),
                "walks": int(row.walks),
                "hit_by_pitch": int(row.hit_by_pitch),
                "sacrifice_flies": int(row.sacrifice_flies),
                "strikeouts": int(row.strikeouts),
                "runs_batted_in": int(row.runs_batted_in),
                "first_season": int(row.first_season),
                "last_season": int(row.last_season),
            }
            continue
        existing["plate_appearances"] = int(existing["plate_appearances"]) + int(row.plate_appearances)
        existing["at_bats"] = int(existing["at_bats"]) + int(row.at_bats)
        existing["hits"] = int(existing["hits"]) + int(row.hits)
        existing["doubles"] = int(existing["doubles"]) + int(row.doubles)
        existing["triples"] = int(existing["triples"]) + int(row.triples)
        existing["home_runs"] = int(existing["home_runs"]) + int(row.home_runs)
        existing["walks"] = int(existing["walks"]) + int(row.walks)
        existing["hit_by_pitch"] = int(existing["hit_by_pitch"]) + int(row.hit_by_pitch)
        existing["sacrifice_flies"] = int(existing["sacrifice_flies"]) + int(row.sacrifice_flies)
        existing["strikeouts"] = int(existing["strikeouts"]) + int(row.strikeouts)
        existing["runs_batted_in"] = int(existing["runs_batted_in"]) + int(row.runs_batted_in)
        existing["first_season"] = min(int(existing["first_season"]), int(row.first_season))
        existing["last_season"] = max(int(existing["last_season"]), int(row.last_season))


BALL_PITCH_CODES = {"B", "I", "P", "V"}
STRIKE_PITCH_CODES = {"A", "C", "F", "K", "L", "M", "O", "Q", "R", "S", "T"}
IGNORE_PITCH_CODES = {"", ".", "+", "*", ">", "1", "2", "3", "N", "U", "Y", "X", "H", "?"}


def aggregate_reached_count_chunk(chunk: pd.DataFrame, totals: dict[tuple[str, str], dict[str, int | str]]) -> None:
    if chunk.empty:
        return
    frame = chunk.copy()
    frame["gametype"] = frame["gametype"].fillna("").str.lower()
    frame = frame[frame["gametype"] == "regular"]
    if frame.empty:
        return
    frame["season"] = pd.to_numeric(frame["date"].fillna("").str[:4], errors="coerce").fillna(0).astype(int)
    for column in ("pa", "ab", "single", "double", "triple", "hr", "walk", "iw", "hbp", "sf", "k", "rbi"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)
    frame["hits"] = frame["single"] + frame["double"] + frame["triple"] + frame["hr"]
    frame["walks_total"] = frame["walk"] + frame["iw"]
    for row in frame.itertuples(index=False):
        batter = str(row.batter or "").strip()
        pitches = str(row.pitches or "").strip()
        if not batter or not pitches or pitches == "??":
            continue
        reached_counts = extract_reached_counts_from_sequence(pitches)
        if not reached_counts:
            continue
        for count_key in reached_counts:
            key = (batter, count_key)
            existing = totals.get(key)
            if existing is None:
                totals[key] = {
                    "player_id": batter,
                    "count_key": count_key,
                    "plate_appearances": int(row.pa),
                    "at_bats": int(row.ab),
                    "hits": int(row.hits),
                    "doubles": int(row.double),
                    "triples": int(row.triple),
                    "home_runs": int(row.hr),
                    "walks": int(row.walks_total),
                    "hit_by_pitch": int(row.hbp),
                    "sacrifice_flies": int(row.sf),
                    "strikeouts": int(row.k),
                    "runs_batted_in": int(row.rbi),
                    "first_season": int(row.season),
                    "last_season": int(row.season),
                }
                continue
            existing["plate_appearances"] = int(existing["plate_appearances"]) + int(row.pa)
            existing["at_bats"] = int(existing["at_bats"]) + int(row.ab)
            existing["hits"] = int(existing["hits"]) + int(row.hits)
            existing["doubles"] = int(existing["doubles"]) + int(row.double)
            existing["triples"] = int(existing["triples"]) + int(row.triple)
            existing["home_runs"] = int(existing["home_runs"]) + int(row.hr)
            existing["walks"] = int(existing["walks"]) + int(row.walks_total)
            existing["hit_by_pitch"] = int(existing["hit_by_pitch"]) + int(row.hbp)
            existing["sacrifice_flies"] = int(existing["sacrifice_flies"]) + int(row.sf)
            existing["strikeouts"] = int(existing["strikeouts"]) + int(row.k)
            existing["runs_batted_in"] = int(existing["runs_batted_in"]) + int(row.rbi)
            existing["first_season"] = min(int(existing["first_season"]), int(row.season))
            existing["last_season"] = max(int(existing["last_season"]), int(row.season))


def extract_reached_counts_from_sequence(sequence: str) -> set[str]:
    balls = 0
    strikes = 0
    reached: set[str] = set()
    for raw_character in sequence.upper():
        character = raw_character.strip()
        if character in BALL_PITCH_CODES:
            balls += 1
            if balls <= 3:
                reached.add(f"{balls}-{strikes}")
            if balls >= 4:
                break
            continue
        if character in STRIKE_PITCH_CODES:
            if character in {"F", "L", "O", "R"} and strikes >= 2:
                continue
            strikes += 1
            if strikes <= 2:
                reached.add(f"{balls}-{strikes}")
            if strikes >= 3:
                break
            continue
        if character in {"Y", "X", "H"}:
            break
        if character in IGNORE_PITCH_CODES:
            continue
    return {count_key for count_key in reached if count_key not in {"0-0"}}
