from __future__ import annotations

from dataclasses import dataclass

from .config import Settings
from .metrics import MetricCatalog
from .models import EvidenceSnippet
from .storage import resolve_column, table_exists


AWARD_HINTS = {
    "Cy Young winners": (
        "cy young winner",
        "cy young winners",
        "won the cy young award",
        "have won the cy young award",
        "cy young award",
    ),
    "MVP winners": (
        "mvp winner",
        "mvp winners",
        "most valuable player",
        "won the mvp award",
        "have won the mvp award",
    ),
    "Gold Glove winners": (
        "gold glove winner",
        "gold glove winners",
        "won the gold glove",
        "have won the gold glove",
    ),
    "Silver Slugger winners": (
        "silver slugger winner",
        "silver slugger winners",
        "won the silver slugger",
        "have won the silver slugger",
    ),
    "Rookie of the Year winners": (
        "rookie of the year winner",
        "rookie of the year winners",
        "won rookie of the year",
        "have won rookie of the year",
    ),
}
EXCLUDED_CONTEXT_METRICS = {"WAR"}


@dataclass(slots=True)
class BirthdayHomeRunQuery:
    descriptor: str
    sort_desc: bool


@dataclass(slots=True)
class AwardOpponentGapQuery:
    metric_name: str
    award_label: str


class SpecialLeaderboardResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        birthday_query = parse_birthday_home_run_query(question)
        if birthday_query:
            snippet = build_birthday_home_run_snippet(connection, birthday_query)
            if snippet is not None:
                return snippet
        award_gap_query = parse_award_opponent_gap_query(question, self.catalog)
        if award_gap_query:
            return build_award_opponent_gap_snippet(award_gap_query)
        return None


def parse_birthday_home_run_query(question: str) -> BirthdayHomeRunQuery | None:
    lowered = question.lower()
    if "birthday" not in lowered:
        return None
    if not any(token in lowered for token in ("home run", "home runs", "homer", "homers", "hr")):
        return None
    if any(token in lowered for token in ("most", "highest", "best", "leader")):
        return BirthdayHomeRunQuery(descriptor="most", sort_desc=True)
    if any(token in lowered for token in ("fewest", "lowest", "least")):
        return BirthdayHomeRunQuery(descriptor="fewest", sort_desc=False)
    return BirthdayHomeRunQuery(descriptor="most", sort_desc=True)


def build_birthday_home_run_snippet(connection, query: BirthdayHomeRunQuery) -> EvidenceSnippet | None:
    if not table_exists(connection, "retrosheet_batting") or not table_exists(connection, "lahman_people"):
        return None
    hr_column = resolve_column(connection, "retrosheet_batting", ("b_hr", "hr", "home_runs"))
    if hr_column is None:
        return None
    birthday_map: dict[str, tuple[str, str]] = {}
    people_rows = connection.execute(
        """
        SELECT retroid, namefirst, namelast, birthmonth, birthday
        FROM lahman_people
        WHERE COALESCE(retroid, '') <> ''
          AND COALESCE(birthmonth, '') <> ''
          AND COALESCE(birthday, '') <> ''
        """
    ).fetchall()
    for row in people_rows:
        retroid = str(row["retroid"]).lower()
        player_name = f"{row['namefirst']} {row['namelast']}".strip()
        month_day = f"{int(row['birthmonth']):02d}{int(row['birthday']):02d}"
        birthday_map[retroid] = (player_name, month_day)
    if not birthday_map:
        return None

    totals: dict[str, dict[str, int | str]] = {}
    batting_rows = connection.execute(
        f"""
        SELECT id, date, {hr_column} AS home_runs
        FROM retrosheet_batting
        WHERE COALESCE(gametype, 'regular') IN ('R', 'regular')
          AND CAST(COALESCE({hr_column}, 0) AS INTEGER) > 0
        """
    ).fetchall()
    for row in batting_rows:
        player_id = str(row["id"]).lower()
        player_info = birthday_map.get(player_id)
        if player_info is None:
            continue
        player_name, month_day = player_info
        game_date = str(row["date"] or "")
        if len(game_date) < 8 or game_date[4:8] != month_day:
            continue
        entry = totals.setdefault(
            player_id,
            {
                "player_name": player_name,
                "total": 0,
                "birthday_games": 0,
                "first_season": int(game_date[:4]),
                "last_season": int(game_date[:4]),
            },
        )
        entry["total"] = int(entry["total"]) + int(row["home_runs"])
        entry["birthday_games"] = int(entry["birthday_games"]) + 1
        entry["first_season"] = min(int(entry["first_season"]), int(game_date[:4]))
        entry["last_season"] = max(int(entry["last_season"]), int(game_date[:4]))

    if not totals:
        return None
    leaders = sorted(
        totals.values(),
        key=lambda row: (
            int(row["total"]),
            -int(row["birthday_games"]),
            str(row["player_name"]),
        ),
        reverse=query.sort_desc,
    )[:5]
    leader = leaders[0]
    trailing = "; ".join(
        f"{row['player_name']} ({row['total']})"
        for row in leaders[1:4]
    )
    summary = (
        f"The most home runs a player has hit on his birthday is {leader['total']}, by {leader['player_name']}. "
        f"Those birthday homers came across {leader['birthday_games']} birthday game(s) from {leader['first_season']} to {leader['last_season']}."
    )
    if trailing:
        summary = f"{summary} Next on the board: {trailing}."
    return EvidenceSnippet(
        source="Retrosheet Birthday Leaderboards",
        title="Birthday home runs",
        citation="Retrosheet batting game logs joined to Lahman birth dates",
        summary=summary,
        payload={
            "analysis_type": "birthday_home_run_leaderboard",
            "leaders": leaders,
            "metric": "Birthday HR",
        },
    )


def parse_award_opponent_gap_query(question: str, catalog: MetricCatalog) -> AwardOpponentGapQuery | None:
    lowered = question.lower()
    if "against" not in lowered:
        return None
    award_label = None
    for label, hints in AWARD_HINTS.items():
        if any(hint in lowered for hint in hints):
            award_label = label
            break
    if award_label is None:
        return None
    metric_name = detect_metric_name(question, catalog)
    return AwardOpponentGapQuery(metric_name=metric_name or "requested metric", award_label=award_label)


def detect_metric_name(question: str, catalog: MetricCatalog) -> str | None:
    for metric in catalog.search(question, limit=5):
        if metric.name in EXCLUDED_CONTEXT_METRICS:
            continue
        return metric.name
    return None


def build_award_opponent_gap_snippet(query: AwardOpponentGapQuery) -> EvidenceSnippet:
    return EvidenceSnippet(
        source="Contextual Split Planner",
        title=f"{query.metric_name} vs {query.award_label} source gap",
        citation="Historical split planner plus source-support rules",
        summary=(
            f"I understand this as an opponent-quality split leaderboard for {query.metric_name} against {query.award_label}. "
            "The local historical stack already has game-level batting and pitching rows, but it does not yet keep an "
            "award-winner lookup table joined to opposing pitcher ids, so it cannot ground that exact leaderboard yet. "
            "This should be solved with an imported awards history table or an official award-recipient sync, not by "
            "falling back to an unrelated metric."
        ),
        payload={
            "analysis_type": "contextual_source_gap",
            "metric": query.metric_name,
            "context": query.award_label,
        },
    )
