from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .query_utils import extract_referenced_season
from .season_metric_leaderboards import find_season_metric


@dataclass(slots=True, frozen=True)
class AwardDefinition:
    key: str
    label: str
    singular_label: str
    award_ids: tuple[str, ...]
    role_label: str
    hints: tuple[str, ...]


AWARD_DEFINITIONS: tuple[AwardDefinition, ...] = (
    AwardDefinition(
        key="cy_young",
        label="Cy Young Award winners",
        singular_label="Cy Young Award",
        award_ids=("ALCY", "NLCY"),
        role_label="pitchers",
        hints=(
            "cy young winner",
            "cy young winners",
            "won the cy young award",
            "have won the cy young award",
            "cy young award",
            "cy young awards",
        ),
    ),
    AwardDefinition(
        key="mvp",
        label="MVP winners",
        singular_label="MVP award",
        award_ids=("ALMVP", "NLMVP"),
        role_label="players",
        hints=(
            "mvp winner",
            "mvp winners",
            "most valuable player",
            "most valuable players",
            "won the mvp award",
            "have won the mvp award",
            "mvp award",
            "mvp awards",
        ),
    ),
    AwardDefinition(
        key="gold_glove",
        label="Gold Glove winners",
        singular_label="Gold Glove award",
        award_ids=("ALGG", "NLGG"),
        role_label="players",
        hints=(
            "gold glove winner",
            "gold glove winners",
            "won the gold glove",
            "have won the gold glove",
            "gold glove",
            "gold gloves",
        ),
    ),
    AwardDefinition(
        key="silver_slugger",
        label="Silver Slugger winners",
        singular_label="Silver Slugger award",
        award_ids=("ALSS", "NLSS"),
        role_label="players",
        hints=(
            "silver slugger winner",
            "silver slugger winners",
            "won the silver slugger",
            "have won the silver slugger",
            "silver slugger",
            "silver sluggers",
        ),
    ),
    AwardDefinition(
        key="rookie_of_the_year",
        label="Rookie of the Year winners",
        singular_label="Rookie of the Year award",
        award_ids=("ALROY", "NLROY"),
        role_label="players",
        hints=(
            "rookie of the year winner",
            "rookie of the year winners",
            "won rookie of the year",
            "have won rookie of the year",
            "rookie of the year",
            "rookie of the year award",
        ),
    ),
)
AWARD_DEFINITIONS_BY_KEY = {definition.key: definition for definition in AWARD_DEFINITIONS}
AWARD_HINTS = {definition.label: definition.hints for definition in AWARD_DEFINITIONS}
RELATIONAL_GAP_HINTS = (
    " against ",
    " versus ",
    " vs ",
    " while facing ",
    " when facing ",
    " facing ",
)
DIRECT_AWARD_LIST_HINTS = (
    "who won",
    "who has won",
    "who have won",
    "which players have won",
    "which pitchers have won",
    "which hitters have won",
    "list",
    "winner",
    "winners",
)


@dataclass(slots=True)
class AwardHistoryQuery:
    definition: AwardDefinition
    season: int | None


class AwardHistoryResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, question: str) -> EvidenceSnippet | None:
        query = parse_award_history_query(question, self.settings)
        if query is None:
            return None
        rows = fetch_award_history_rows(self.live_client, query.definition)
        if query.season is not None:
            rows = [row for row in rows if row["season"] == query.season]
        if not rows:
            return None
        rows.sort(key=lambda row: (-row["season"], row["league"], row["player_name"]))
        summary = build_award_history_summary(query, rows)
        return EvidenceSnippet(
            source="Award History",
            title=query.definition.label,
            citation=build_award_history_citation(query),
            summary=summary,
            payload={
                "analysis_type": "award_history",
                "mode": "historical",
                "award_key": query.definition.key,
                "award_label": query.definition.label,
                "season": query.season,
                "complete": True,
                "total_row_count": len(rows),
                "rows": rows[:40],
            },
        )


def parse_award_history_query(question: str, settings: Settings) -> AwardHistoryQuery | None:
    lowered = f" {question.lower().strip()} "
    if any(token in lowered for token in RELATIONAL_GAP_HINTS):
        return None
    definition = find_award_definition(question)
    if definition is None:
        return None
    if find_season_metric(lowered) is not None:
        return None
    if any(token in lowered for token in (" highest ", " lowest ", " best ", " worst ", " most ", " fewest ", " least ")):
        return None
    if not any(token in lowered for token in DIRECT_AWARD_LIST_HINTS):
        return None
    season = extract_referenced_season(question, settings.live_season or date.today().year)
    return AwardHistoryQuery(definition=definition, season=season)


def find_award_definition(question: str) -> AwardDefinition | None:
    lowered = question.lower()
    for definition in AWARD_DEFINITIONS:
        if any(hint in lowered for hint in definition.hints):
            return definition
    return None


def fetch_award_history_rows(live_client: LiveStatsClient, definition: AwardDefinition) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for award_id in definition.award_ids:
        league = infer_award_league(award_id)
        for recipient in live_client.award_recipients(award_id):
            player = recipient.get("player") or {}
            player_name = str(player.get("nameFirstLast") or player.get("name") or "").strip()
            season_text = str(recipient.get("season") or "").strip()
            if not player_name or not season_text.isdigit():
                continue
            rows.append(
                {
                    "season": int(season_text),
                    "league": league,
                    "award_id": award_id,
                    "player_id": int(player.get("id") or 0),
                    "player_name": player_name,
                    "position": str((player.get("primaryPosition") or {}).get("abbreviation") or ""),
                    "award_date": str(recipient.get("date") or ""),
                }
            )
    return rows


def infer_award_league(award_id: str) -> str:
    normalized = award_id.upper()
    if normalized.startswith("AL"):
        return "AL"
    if normalized.startswith("NL"):
        return "NL"
    return "MLB"


def build_award_history_summary(query: AwardHistoryQuery, rows: list[dict[str, Any]]) -> str:
    if query.season is not None:
        if len(rows) == 1:
            row = rows[0]
            return (
                f"The {query.season} {query.definition.singular_label} winner was {row['player_name']} "
                f"({row['league']})."
            )
        winners = "; ".join(f"{row['league']}: {row['player_name']}" for row in rows[:4])
        return f"The {query.season} {query.definition.singular_label} winners were {winners}."
    unique_winners = sorted({row["player_name"] for row in rows})
    recent = "; ".join(
        f"{row['season']} {row['league']}: {row['player_name']}"
        for row in rows[:6]
    )
    return (
        f"{query.definition.label} are available from the official MLB awards history. "
        f"There are {len(unique_winners)} unique winners in the synced list. "
        f"Recent award seasons: {recent}."
    )


def build_award_history_citation(query: AwardHistoryQuery) -> str:
    if query.season is not None:
        return f"MLB Stats API {query.definition.singular_label} recipients for {query.season}"
    return f"MLB Stats API {query.definition.label}"
