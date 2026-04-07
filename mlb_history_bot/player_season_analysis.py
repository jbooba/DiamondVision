from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .person_query import choose_best_person_match, clean_player_phrase as shared_clean_player_phrase, extract_player_candidate
from .query_utils import extract_explicit_year, question_requests_current_scope
from .team_evaluator import format_float, safe_float, safe_int


PLAYER_SEASON_HINTS = {
    "this year",
    "this season",
    "so far",
    "season so far",
    "current season",
    "current year",
    "stats",
    "stat line",
    "numbers",
    "performance",
    "how has",
    "how is",
    "how's",
    "analyze",
    "analysis",
}
FOCUS_HINTS = {
    "fielding": "fielding",
    "defense": "fielding",
    "defensive": "fielding",
    "glove": "fielding",
    "pitching": "pitching",
    "pitcher": "pitching",
    "hitting": "hitting",
    "batting": "hitting",
    "offense": "hitting",
    "offensive": "hitting",
}
REJECT_HINTS = {"clip", "clips", "video", "videos", "replay", "highlight", "today", "tonight", "yesterday", "last night"}
PLAYER_PATTERNS = (
    re.compile(
        r"(?:analyze|assess|evaluate|break down)\s+(.+?)(?:'s)?\s+(?:performance|season|year|stats?)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"how\s+(?:has|is|was|have)\s+(.+?)\s+(?:been|doing|performing|looking)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"what(?:'s| is)\s+(.+?)(?:'s)?\s+(?:performance|season|year|stats?)\b",
        re.IGNORECASE,
    ),
    re.compile(r"^(.+?)\s+(18\d{2}|19\d{2}|20\d{2})\s+(?:stats?|season|performance)\b", re.IGNORECASE),
    re.compile(r"^(.+?)(?:'s)?\s+(?:stats?|season|performance)\b", re.IGNORECASE),
)


@dataclass(slots=True)
class PlayerSeasonQuery:
    player_query: str
    player_name: str
    season: int
    focus: str


class PlayerSeasonAnalysisResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_player_season_query(question, self.live_client, current_season)
        if query is None:
            return None

        snapshot = self.live_client.player_season_snapshot(query.player_name, query.season)
        if snapshot is None:
            return None
        previous_snapshot = (
            self.live_client.player_season_snapshot(query.player_name, query.season - 1)
            if query.season > 1871
            else None
        )
        summary = build_player_season_summary(query, snapshot, previous_snapshot)
        rows = build_player_season_rows(snapshot, previous_snapshot)
        return EvidenceSnippet(
            source="Player Season Analysis",
            title=f"{snapshot['name']} {query.season} season analysis",
            citation="MLB Stats API people/stats endpoints with prior-season comparison when available",
            summary=summary,
            payload={
                "analysis_type": "player_season_analysis",
                "mode": "live",
                "focus": query.focus,
                "player": snapshot["name"],
                "season": query.season,
                "team": snapshot.get("current_team"),
                "position": snapshot.get("primary_position", {}).get("abbreviation"),
                "rows": rows,
            },
        )


def parse_player_season_query(
    question: str,
    live_client: LiveStatsClient,
    current_season: int,
) -> PlayerSeasonQuery | None:
    lowered = question.lower()
    if any(hint in lowered for hint in REJECT_HINTS):
        return None
    explicit_year = extract_explicit_year(question)
    wants_current_scope = any(hint in lowered for hint in PLAYER_SEASON_HINTS) or question_requests_current_scope(question) or explicit_year == current_season
    if not wants_current_scope:
        return None
    player_candidate = extract_player_query_text(question)
    if not player_candidate:
        return None
    people = live_client.search_people(player_candidate)
    if not people:
        return None
    selected_person = choose_best_person_match(people, player_candidate)
    return PlayerSeasonQuery(
        player_query=player_candidate,
        player_name=str(selected_person.get("fullName") or player_candidate).strip(),
        season=explicit_year or current_season,
        focus=infer_focus(question),
    )
def infer_focus(question: str) -> str:
    lowered = question.lower()
    for token, focus in FOCUS_HINTS.items():
        if token in lowered:
            return focus
    return "overall"


def extract_player_query_text(question: str) -> str | None:
    return extract_player_candidate(question, patterns=PLAYER_PATTERNS, allow_fallback=True)


def clean_player_phrase(value: str) -> str:
    return shared_clean_player_phrase(value)


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().casefold())


def build_player_season_summary(
    query: PlayerSeasonQuery,
    snapshot: dict[str, Any],
    previous_snapshot: dict[str, Any] | None,
) -> str:
    focus = query.focus
    if focus == "pitching":
        return build_pitching_summary(snapshot, previous_snapshot)
    if focus == "fielding":
        return build_fielding_summary(snapshot, previous_snapshot)

    hitting_summary = build_hitting_summary(snapshot, previous_snapshot)
    fielding_summary = build_fielding_blurb(snapshot)
    if focus == "hitting":
        return hitting_summary
    return f"{hitting_summary} {fielding_summary}".strip()


def build_hitting_summary(snapshot: dict[str, Any], previous_snapshot: dict[str, Any] | None) -> str:
    name = str(snapshot.get("name") or "This player")
    season = snapshot.get("season")
    team = str(snapshot.get("current_team") or "").strip()
    hitting = snapshot.get("hitting", {}) or {}
    games = safe_int(hitting.get("gamesPlayed")) or 0
    plate_appearances = safe_int(hitting.get("plateAppearances")) or 0
    ops = safe_float(hitting.get("ops"))
    descriptor = describe_hitting_start(ops, plate_appearances)
    team_phrase = f" for {team}" if team else ""
    summary = (
        f"{name} has had a {descriptor} offensive start in {season}{team_phrase}: "
        f"{normalize_rate_stat(hitting.get('avg'))}/{normalize_rate_stat(hitting.get('obp'))}/{normalize_rate_stat(hitting.get('slg'))} "
        f"({normalize_rate_stat(hitting.get('ops'))} OPS), "
        f"{safe_int(hitting.get('homeRuns')) or 0} HR, {safe_int(hitting.get('rbi')) or 0} RBI, "
        f"{safe_int(hitting.get('baseOnBalls')) or 0} BB, and {safe_int(hitting.get('strikeOuts')) or 0} SO "
        f"in {plate_appearances} PA across {games} game(s)."
    )
    previous_hitting = (previous_snapshot or {}).get("hitting", {}) or {}
    previous_ops = safe_float(previous_hitting.get("ops"))
    if previous_ops is not None and ops is not None:
        delta = ops - previous_ops
        direction = "up" if delta > 0 else "down" if delta < 0 else "flat"
        if direction == "flat":
            summary = (
                f"{summary} That is essentially even with his {int(snapshot['season']) - 1} OPS of "
                f"{normalize_rate_stat(previous_hitting.get('ops'))}."
            )
        else:
            summary = (
                f"{summary} Compared with {int(snapshot['season']) - 1}, his OPS is {direction} "
                f"by {abs(delta):.3f} from {normalize_rate_stat(previous_hitting.get('ops'))}."
            )
    elif plate_appearances < 60:
        summary = f"{summary} The sample is still tiny, so the slash line can move fast over the next week or two."
    return summary


def build_pitching_summary(snapshot: dict[str, Any], previous_snapshot: dict[str, Any] | None) -> str:
    name = str(snapshot.get("name") or "This pitcher")
    season = snapshot.get("season")
    team = str(snapshot.get("current_team") or "").strip()
    pitching = snapshot.get("pitching", {}) or {}
    innings = str(pitching.get("inningsPitched") or "0.0")
    era = safe_float(pitching.get("era"))
    whip = normalize_rate_stat(pitching.get("whip"))
    descriptor = describe_pitching_start(era, safe_float(pitching.get("inningsPitched")))
    team_phrase = f" for {team}" if team else ""
    summary = (
        f"{name} has been {descriptor} on the mound in {season}{team_phrase}: "
        f"{normalize_rate_stat(pitching.get('era'))} ERA, {whip} WHIP, "
        f"{safe_int(pitching.get('strikeOuts')) or 0} SO, and {safe_int(pitching.get('baseOnBalls')) or 0} BB "
        f"over {innings} IP."
    )
    previous_pitching = (previous_snapshot or {}).get("pitching", {}) or {}
    previous_era = safe_float(previous_pitching.get("era"))
    if previous_era is not None and era is not None:
        delta = era - previous_era
        if abs(delta) >= 0.01:
            direction = "lower" if delta < 0 else "higher"
            summary = (
                f"{summary} Relative to {int(snapshot['season']) - 1}, his ERA is {direction} by {abs(delta):.2f}."
            )
    return summary


def build_fielding_summary(snapshot: dict[str, Any], previous_snapshot: dict[str, Any] | None) -> str:
    name = str(snapshot.get("name") or "This player")
    season = snapshot.get("season")
    fielding = snapshot.get("fielding", {}) or {}
    position = str(fielding.get("position", {}).get("abbreviation") or snapshot.get("primary_position", {}).get("abbreviation") or "")
    games = safe_int(fielding.get("gamesPlayed")) or 0
    innings = str(fielding.get("innings") or "")
    summary = (
        f"{name}'s fielding line in {season} at {position or 'his primary spot'} is "
        f"{normalize_rate_stat(fielding.get('fielding'))} over {games} game(s)"
    )
    if innings:
        summary = f"{summary} and {innings} innings"
    summary = f"{summary}, with {safe_int(fielding.get('errors')) or 0} errors."
    catcher_notes = build_catcher_fielding_notes(fielding)
    if catcher_notes:
        summary = f"{summary} {catcher_notes}"
    return summary


def build_fielding_blurb(snapshot: dict[str, Any]) -> str:
    fielding = snapshot.get("fielding", {}) or {}
    games = safe_int(fielding.get("gamesPlayed")) or 0
    if games <= 0:
        return ""
    fielding_pct = normalize_rate_stat(fielding.get("fielding"))
    position = str(fielding.get("position", {}).get("abbreviation") or snapshot.get("primary_position", {}).get("abbreviation") or "")
    sentence = f"Defensively he is at {fielding_pct} in {games} game(s) at {position or 'his primary position'}."
    catcher_notes = build_catcher_fielding_notes(fielding)
    if catcher_notes:
        sentence = f"{sentence} {catcher_notes}"
    return sentence


def build_catcher_fielding_notes(fielding: dict[str, Any]) -> str:
    caught_stealing = safe_int(fielding.get("caughtStealing")) or 0
    stolen_bases = safe_int(fielding.get("stolenBases")) or 0
    if caught_stealing == 0 and stolen_bases == 0:
        return ""
    attempts = caught_stealing + stolen_bases
    return f"He has thrown out {caught_stealing} of {attempts} attempted basestealers."


def describe_hitting_start(ops: float | None, plate_appearances: int) -> str:
    if plate_appearances < 20:
        return "very early"
    if ops is None:
        return "hard-to-read"
    if ops >= 0.900:
        return "excellent"
    if ops >= 0.800:
        return "strong"
    if ops >= 0.725:
        return "solid"
    if ops >= 0.650:
        return "mixed"
    return "rough"


def describe_pitching_start(era: float | None, innings: float | None) -> str:
    if innings is not None and innings < 10.0:
        return "in a very small sample"
    if era is None:
        return "hard to judge"
    if era <= 2.75:
        return "excellent"
    if era <= 3.75:
        return "strong"
    if era <= 4.50:
        return "solid"
    if era <= 5.25:
        return "shaky"
    return "rough"


def normalize_rate_stat(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    if text.startswith("."):
        return f"0{text}"
    return text


def build_player_season_rows(
    snapshot: dict[str, Any],
    previous_snapshot: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    current_row = build_hitting_row(snapshot, "current")
    rows = [current_row] if current_row else []
    previous_row = build_hitting_row(previous_snapshot or {}, "previous")
    if previous_row:
        rows.append(previous_row)
    if rows:
        return rows
    pitching_row = build_pitching_row(snapshot, "current")
    return [pitching_row] if pitching_row else []


def build_hitting_row(snapshot: dict[str, Any], scope: str) -> dict[str, Any] | None:
    hitting = snapshot.get("hitting", {}) or {}
    plate_appearances = safe_int(hitting.get("plateAppearances")) or 0
    if plate_appearances <= 0:
        return None
    return {
        "scope": scope,
        "season": snapshot.get("season"),
        "team": snapshot.get("current_team") if scope == "current" else "",
        "games": safe_int(hitting.get("gamesPlayed")) or 0,
        "pa": plate_appearances,
        "avg": normalize_rate_stat(hitting.get("avg")),
        "obp": normalize_rate_stat(hitting.get("obp")),
        "slg": normalize_rate_stat(hitting.get("slg")),
        "ops": normalize_rate_stat(hitting.get("ops")),
        "hr": safe_int(hitting.get("homeRuns")) or 0,
        "rbi": safe_int(hitting.get("rbi")) or 0,
        "bb": safe_int(hitting.get("baseOnBalls")) or 0,
        "so": safe_int(hitting.get("strikeOuts")) or 0,
    }


def build_pitching_row(snapshot: dict[str, Any], scope: str) -> dict[str, Any] | None:
    pitching = snapshot.get("pitching", {}) or {}
    innings = str(pitching.get("inningsPitched") or "").strip()
    if not innings:
        return None
    return {
        "scope": scope,
        "season": snapshot.get("season"),
        "team": snapshot.get("current_team") if scope == "current" else "",
        "games": safe_int(pitching.get("gamesPlayed")) or 0,
        "innings": innings,
        "era": normalize_rate_stat(pitching.get("era")),
        "whip": normalize_rate_stat(pitching.get("whip")),
        "so": safe_int(pitching.get("strikeOuts")) or 0,
        "bb": safe_int(pitching.get("baseOnBalls")) or 0,
    }
