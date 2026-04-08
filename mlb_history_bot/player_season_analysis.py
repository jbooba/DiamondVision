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
from .storage import table_exists
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

    def build_snippet(self, question: str, connection=None) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_player_season_query(question, self.live_client, current_season)
        if query is None:
            return None

        snapshot = load_historical_player_season_snapshot(connection, query.player_name, query.season)
        mode = "historical" if snapshot is not None or query.season != current_season else "live"
        if snapshot is None:
            snapshot = self.live_client.player_season_snapshot(query.player_name, query.season)
        if snapshot is None:
            return None
        previous_snapshot = (
            load_historical_player_season_snapshot(connection, query.player_name, query.season - 1)
            if query.season > 1871 and mode == "historical"
            else self.live_client.player_season_snapshot(query.player_name, query.season - 1)
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
                "mode": mode,
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
    team_phrase = f" for {team}" if team else ""
    stat_line_label = "regular-season batting line" if season and int(season) < date.today().year else f"{describe_hitting_start(ops, plate_appearances)} offensive start"
    summary = (
        f"{name}'s {season} {stat_line_label}{team_phrase} was: "
        f"G {games}, PA {plate_appearances}, AB {safe_int(hitting.get('atBats')) or 0}, "
        f"R {safe_int(hitting.get('runs')) or 0}, H {safe_int(hitting.get('hits')) or 0}, "
        f"2B {safe_int(hitting.get('doubles')) or 0}, 3B {safe_int(hitting.get('triples')) or 0}, "
        f"HR {safe_int(hitting.get('homeRuns')) or 0}, RBI {safe_int(hitting.get('rbi')) or 0}, "
        f"BB {safe_int(hitting.get('baseOnBalls')) or 0}, SO {safe_int(hitting.get('strikeOuts')) or 0}, "
        f"SB {safe_int(hitting.get('stolenBases')) or 0}, CS {safe_int(hitting.get('caughtStealing')) or 0}, "
        f"HBP {safe_int(hitting.get('hitByPitch')) or 0}, SH {safe_int(hitting.get('sacBunts')) or 0}, "
        f"TB {safe_int(hitting.get('totalBases')) or 0}, "
        f"{normalize_rate_stat(hitting.get('avg'))}/{normalize_rate_stat(hitting.get('obp'))}/{normalize_rate_stat(hitting.get('slg'))} "
        f"({normalize_rate_stat(hitting.get('ops'))} OPS)."
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
        "ab": safe_int(hitting.get("atBats")) or 0,
        "runs": safe_int(hitting.get("runs")) or 0,
        "hits": safe_int(hitting.get("hits")) or 0,
        "doubles": safe_int(hitting.get("doubles")) or 0,
        "triples": safe_int(hitting.get("triples")) or 0,
        "avg": normalize_rate_stat(hitting.get("avg")),
        "obp": normalize_rate_stat(hitting.get("obp")),
        "slg": normalize_rate_stat(hitting.get("slg")),
        "ops": normalize_rate_stat(hitting.get("ops")),
        "hr": safe_int(hitting.get("homeRuns")) or 0,
        "rbi": safe_int(hitting.get("rbi")) or 0,
        "bb": safe_int(hitting.get("baseOnBalls")) or 0,
        "so": safe_int(hitting.get("strikeOuts")) or 0,
        "sb": safe_int(hitting.get("stolenBases")) or 0,
        "cs": safe_int(hitting.get("caughtStealing")) or 0,
        "hbp": safe_int(hitting.get("hitByPitch")) or 0,
        "sh": safe_int(hitting.get("sacBunts")) or 0,
        "tb": safe_int(hitting.get("totalBases")) or 0,
        "babip": normalize_rate_stat(hitting.get("babip")),
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
        "starts": safe_int(pitching.get("gamesStarted")) or 0,
        "wins": safe_int(pitching.get("wins")) or 0,
        "losses": safe_int(pitching.get("losses")) or 0,
        "saves": safe_int(pitching.get("saves")) or 0,
        "innings": innings,
        "era": normalize_rate_stat(pitching.get("era")),
        "whip": normalize_rate_stat(pitching.get("whip")),
        "hits_allowed": safe_int(pitching.get("hits")) or 0,
        "earned_runs": safe_int(pitching.get("earnedRuns")) or 0,
        "home_runs_allowed": safe_int(pitching.get("homeRuns")) or 0,
        "so": safe_int(pitching.get("strikeOuts")) or 0,
        "bb": safe_int(pitching.get("baseOnBalls")) or 0,
    }


def load_historical_player_season_snapshot(connection, player_query: str, season: int) -> dict[str, Any] | None:
    if connection is None or not table_exists(connection, "lahman_people"):
        return None
    rows = connection.execute(
        """
        SELECT *
        FROM lahman_people
        WHERE lower(trim(coalesce(namefirst, '') || ' ' || coalesce(namelast, ''))) LIKE ?
           OR lower(coalesce(namegiven, '')) LIKE ?
        ORDER BY
            CASE
                WHEN lower(trim(coalesce(namefirst, '') || ' ' || coalesce(namelast, ''))) = ? THEN 0
                ELSE 1
            END,
            debut ASC
        LIMIT 25
        """,
        (f"%{player_query.lower()}%", f"%{player_query.lower()}%", player_query.lower()),
    ).fetchall()
    if not rows:
        return None

    def score(candidate) -> tuple[int, int, str]:
        player_id = str(candidate["playerid"] or "")
        activity = 0
        for table_name in ("lahman_batting", "lahman_pitching", "lahman_fielding"):
            if not table_exists(connection, table_name):
                continue
            row = connection.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE playerid = ? AND CAST(yearid AS INTEGER) = ?",
                (player_id, season),
            ).fetchone()
            activity += safe_int(row[0] if row is not None else 0) or 0
        exact = 0 if normalize_text(build_full_name(candidate["namefirst"], candidate["namelast"])) == normalize_text(player_query) else 1
        return (exact, -activity, player_id)

    selected = sorted(rows, key=score)[0]
    player_id = str(selected["playerid"] or "")
    snapshot = {
        "player_id": player_id,
        "name": build_full_name(selected["namefirst"], selected["namelast"]) or player_query,
        "active": False,
        "current_team": resolve_historical_team_name(connection, player_id, season),
        "season": season,
        "current_age": compute_age_for_season(selected, season),
        "primary_position": {},
        "hitting": build_historical_hitting_snapshot(connection, player_id, season),
        "pitching": build_historical_pitching_snapshot(connection, player_id, season),
        "fielding": build_historical_fielding_snapshot(connection, player_id, season),
    }
    fielding_position = snapshot["fielding"].get("position", {}).get("abbreviation")
    if fielding_position:
        snapshot["primary_position"] = {"abbreviation": fielding_position}
    if not any(snapshot[group] for group in ("hitting", "pitching", "fielding")):
        return None
    return snapshot


def build_historical_hitting_snapshot(connection, player_id: str, season: int) -> dict[str, Any]:
    if not table_exists(connection, "lahman_batting"):
        return {}
    row = connection.execute(
        """
        SELECT
            SUM(CAST(COALESCE(g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(ab, '0') AS INTEGER)) AS at_bats,
            SUM(CAST(COALESCE(r, '0') AS INTEGER)) AS runs,
            SUM(CAST(COALESCE(h, '0') AS INTEGER)) AS hits,
            SUM(CAST(COALESCE(c_2b, '0') AS INTEGER)) AS doubles,
            SUM(CAST(COALESCE(c_3b, '0') AS INTEGER)) AS triples,
            SUM(CAST(COALESCE(hr, '0') AS INTEGER)) AS home_runs,
            SUM(CAST(COALESCE(rbi, '0') AS INTEGER)) AS rbi,
            SUM(CAST(COALESCE(sb, '0') AS INTEGER)) AS stolen_bases,
            SUM(CAST(COALESCE(cs, '0') AS INTEGER)) AS caught_stealing,
            SUM(CAST(COALESCE(bb, '0') AS INTEGER)) AS walks,
            SUM(CAST(COALESCE(so, '0') AS INTEGER)) AS strikeouts,
            SUM(CAST(COALESCE(hbp, '0') AS INTEGER)) AS hit_by_pitch,
            SUM(CAST(COALESCE(sh, '0') AS INTEGER)) AS sacrifice_bunts,
            SUM(CAST(COALESCE(sf, '0') AS INTEGER)) AS sacrifice_flies
        FROM lahman_batting
        WHERE playerid = ? AND CAST(yearid AS INTEGER) = ?
        """,
        (player_id, season),
    ).fetchone()
    if row is None:
        return {}
    games = safe_int(row["games"]) or 0
    at_bats = safe_int(row["at_bats"]) or 0
    hits = safe_int(row["hits"]) or 0
    doubles = safe_int(row["doubles"]) or 0
    triples = safe_int(row["triples"]) or 0
    home_runs = safe_int(row["home_runs"]) or 0
    walks = safe_int(row["walks"]) or 0
    strikeouts = safe_int(row["strikeouts"]) or 0
    hit_by_pitch = safe_int(row["hit_by_pitch"]) or 0
    sacrifice_bunts = safe_int(row["sacrifice_bunts"]) or 0
    sacrifice_flies = safe_int(row["sacrifice_flies"]) or 0
    if games <= 0 and at_bats <= 0 and hits <= 0 and home_runs <= 0:
        return {}
    singles = hits - doubles - triples - home_runs
    total_bases = singles + (2 * doubles) + (3 * triples) + (4 * home_runs)
    plate_appearances = at_bats + walks + hit_by_pitch + sacrifice_bunts + sacrifice_flies
    avg = (hits / at_bats) if at_bats else None
    obp_denom = at_bats + walks + hit_by_pitch + sacrifice_flies
    obp = ((hits + walks + hit_by_pitch) / obp_denom) if obp_denom else None
    slg = (total_bases / at_bats) if at_bats else None
    ops = (obp + slg) if obp is not None and slg is not None else None
    babip_denom = at_bats - strikeouts - home_runs + sacrifice_flies
    babip = ((hits - home_runs) / babip_denom) if babip_denom else None
    return {
        "gamesPlayed": games,
        "plateAppearances": plate_appearances,
        "atBats": at_bats,
        "runs": safe_int(row["runs"]) or 0,
        "hits": hits,
        "doubles": doubles,
        "triples": triples,
        "homeRuns": home_runs,
        "rbi": safe_int(row["rbi"]) or 0,
        "stolenBases": safe_int(row["stolen_bases"]) or 0,
        "caughtStealing": safe_int(row["caught_stealing"]) or 0,
        "baseOnBalls": walks,
        "strikeOuts": strikeouts,
        "hitByPitch": hit_by_pitch,
        "sacBunts": sacrifice_bunts,
        "sacFlies": sacrifice_flies,
        "totalBases": total_bases,
        "avg": format_rate(avg),
        "obp": format_rate(obp),
        "slg": format_rate(slg),
        "ops": format_rate(ops),
        "babip": format_rate(babip),
    }


def build_historical_pitching_snapshot(connection, player_id: str, season: int) -> dict[str, Any]:
    if not table_exists(connection, "lahman_pitching"):
        return {}
    row = connection.execute(
        """
        SELECT
            SUM(CAST(COALESCE(w, '0') AS INTEGER)) AS wins,
            SUM(CAST(COALESCE(l, '0') AS INTEGER)) AS losses,
            SUM(CAST(COALESCE(g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(gs, '0') AS INTEGER)) AS starts,
            SUM(CAST(COALESCE(sv, '0') AS INTEGER)) AS saves,
            SUM(CAST(COALESCE(ipouts, '0') AS INTEGER)) AS ipouts,
            SUM(CAST(COALESCE(h, '0') AS INTEGER)) AS hits,
            SUM(CAST(COALESCE(er, '0') AS INTEGER)) AS earned_runs,
            SUM(CAST(COALESCE(hr, '0') AS INTEGER)) AS home_runs,
            SUM(CAST(COALESCE(bb, '0') AS INTEGER)) AS walks,
            SUM(CAST(COALESCE(so, '0') AS INTEGER)) AS strikeouts
        FROM lahman_pitching
        WHERE playerid = ? AND CAST(yearid AS INTEGER) = ?
        """,
        (player_id, season),
    ).fetchone()
    if row is None:
        return {}
    games = safe_int(row["games"]) or 0
    ipouts = safe_int(row["ipouts"]) or 0
    if games <= 0 and ipouts <= 0:
        return {}
    innings = ipouts / 3.0
    era = ((27.0 * (safe_int(row["earned_runs"]) or 0)) / ipouts) if ipouts else None
    whip = (((safe_int(row["hits"]) or 0) + (safe_int(row["walks"]) or 0)) / innings) if innings else None
    return {
        "gamesPlayed": games,
        "gamesStarted": safe_int(row["starts"]) or 0,
        "wins": safe_int(row["wins"]) or 0,
        "losses": safe_int(row["losses"]) or 0,
        "saves": safe_int(row["saves"]) or 0,
        "inningsPitched": f"{innings:.1f}",
        "era": format_rate(era, decimals=2),
        "whip": format_rate(whip, decimals=3),
        "hits": safe_int(row["hits"]) or 0,
        "earnedRuns": safe_int(row["earned_runs"]) or 0,
        "homeRuns": safe_int(row["home_runs"]) or 0,
        "baseOnBalls": safe_int(row["walks"]) or 0,
        "strikeOuts": safe_int(row["strikeouts"]) or 0,
    }


def build_historical_fielding_snapshot(connection, player_id: str, season: int) -> dict[str, Any]:
    if not table_exists(connection, "lahman_fielding"):
        return {}
    rows = connection.execute(
        """
        SELECT
            pos,
            SUM(CAST(COALESCE(g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(po, '0') AS INTEGER)) AS putouts,
            SUM(CAST(COALESCE(a, '0') AS INTEGER)) AS assists,
            SUM(CAST(COALESCE(e, '0') AS INTEGER)) AS errors
        FROM lahman_fielding
        WHERE playerid = ? AND CAST(yearid AS INTEGER) = ?
        GROUP BY pos
        ORDER BY games DESC, pos ASC
        """,
        (player_id, season),
    ).fetchall()
    if not rows:
        return {}
    total_games = sum(safe_int(item["games"]) or 0 for item in rows)
    putouts = sum(safe_int(item["putouts"]) or 0 for item in rows)
    assists = sum(safe_int(item["assists"]) or 0 for item in rows)
    errors = sum(safe_int(item["errors"]) or 0 for item in rows)
    chances = putouts + assists + errors
    return {
        "gamesPlayed": total_games,
        "fielding": format_rate(((putouts + assists) / chances) if chances else None),
        "errors": errors,
        "position": {"abbreviation": str(rows[0]["pos"] or "")},
    }


def resolve_historical_team_name(connection, player_id: str, season: int) -> str:
    team_codes: set[str] = set()
    for table_name in ("lahman_batting", "lahman_pitching", "lahman_fielding"):
        if not table_exists(connection, table_name):
            continue
        codes = connection.execute(
            f"SELECT DISTINCT teamid FROM {table_name} WHERE playerid = ? AND CAST(yearid AS INTEGER) = ?",
            (player_id, season),
        ).fetchall()
        for code in codes:
            if code[0]:
                team_codes.add(str(code[0]))
    if not team_codes:
        return ""
    if not table_exists(connection, "lahman_teams"):
        return ", ".join(sorted(team_codes))
    names: list[str] = []
    for team_code in sorted(team_codes):
        row = connection.execute(
            "SELECT name FROM lahman_teams WHERE teamid = ? AND CAST(yearid AS INTEGER) = ? LIMIT 1",
            (team_code, season),
        ).fetchone()
        if row and row[0]:
            names.append(str(row[0]))
    return ", ".join(names or sorted(team_codes))


def build_full_name(first_name: Any, last_name: Any) -> str:
    return " ".join(part for part in (str(first_name or "").strip(), str(last_name or "").strip()) if part)


def compute_age_for_season(row, season: int) -> int | None:
    if "birthyear" not in row.keys():
        return None
    birth_year = safe_int(row["birthyear"])
    return (season - birth_year) if birth_year is not None else None


def format_rate(value: float | None, *, decimals: int = 3) -> str:
    if value is None:
        return ""
    text = f"{value:.{decimals}f}"
    return text[1:] if text.startswith("0.") else text
