from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from .config import Settings
from .film_room_research import build_team_matchup, extract_hit_data, format_window, parse_float, serialize_window
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .query_utils import DateWindow, extract_calendar_day_window, extract_date_window, ordinal, question_mentions_explicit_year
from .sporty_research import extract_play_id
from .sporty_video import SportyVideoClient
from .storage import resolve_column, table_exists


TOTAL_HINTS = {
    "how many",
    "number of",
    "total",
    "totals",
    "count of",
}

LEADERBOARD_HINTS = {
    " most ",
    " highest ",
    " leaders ",
    " leader ",
    " record ",
}

PLAYER_TARGET_HINTS = {
    " which player ",
    " who ",
    " batter ",
    " hitter ",
    " hitters ",
    " player ",
}
PITCHER_TARGET_HINTS = {
    " pitcher ",
    " pitchers ",
    " pitching ",
}
PITCHING_PERFORMANCE_HINTS = {
    "best stats",
    "best line",
    "best performance",
    "best game",
    "most dominant",
    "worst stats",
    "worst line",
    "worst performance",
    "worst game",
}


@dataclass(slots=True)
class DailyMetricSpec:
    key: str
    label: str
    aliases: tuple[str, ...]
    table_name: str
    column_candidates: tuple[str, ...]
    live_event_types: tuple[str, ...]
    supports_clips: bool = False


DAILY_METRICS: tuple[DailyMetricSpec, ...] = (
    DailyMetricSpec(
        key="home_runs",
        label="home runs",
        aliases=("home runs", "home run", "homers", "homer", " hr "),
        table_name="retrosheet_batting",
        column_candidates=("b_hr", "hr", "home_runs"),
        live_event_types=("home_run",),
        supports_clips=True,
    ),
    DailyMetricSpec(
        key="hits",
        label="hits",
        aliases=("hits", "base hits"),
        table_name="retrosheet_batting",
        column_candidates=("b_h", "h", "hits"),
        live_event_types=("single", "double", "triple", "home_run"),
    ),
    DailyMetricSpec(
        key="doubles",
        label="doubles",
        aliases=("doubles", "double"),
        table_name="retrosheet_batting",
        column_candidates=("b_d", "doubles"),
        live_event_types=("double",),
    ),
    DailyMetricSpec(
        key="triples",
        label="triples",
        aliases=("triples", "triple"),
        table_name="retrosheet_batting",
        column_candidates=("b_t", "triples"),
        live_event_types=("triple",),
    ),
    DailyMetricSpec(
        key="strikeouts",
        label="strikeouts",
        aliases=("strikeouts", "strikeout", "strike outs", "strike out"),
        table_name="retrosheet_batting",
        column_candidates=("b_k", "strikeouts"),
        live_event_types=("strikeout",),
    ),
    DailyMetricSpec(
        key="walks",
        label="walks",
        aliases=("walks", "walk", "base on balls"),
        table_name="retrosheet_batting",
        column_candidates=("b_w", "walks"),
        live_event_types=("walk", "intent_walk"),
    ),
    DailyMetricSpec(
        key="stolen_bases",
        label="stolen bases",
        aliases=("stolen bases", "stolen base", "steals"),
        table_name="retrosheet_batting",
        column_candidates=("b_sb", "stolen_bases", "sb"),
        live_event_types=("stolen_base",),
    ),
)


@dataclass(slots=True)
class DailyLookupQuery:
    metric: DailyMetricSpec
    date_window: DateWindow


@dataclass(slots=True)
class CalendarDayPlayerLeaderboardQuery:
    metric: DailyMetricSpec
    date_window: DateWindow

    @property
    def month_day_key(self) -> str:
        return self.date_window.start_date.strftime("%m%d")

    @property
    def calendar_label(self) -> str:
        return format_calendar_day(self.date_window.start_date)


@dataclass(slots=True)
class CalendarDayTotalQuery:
    metric: DailyMetricSpec
    date_window: DateWindow

    @property
    def month_day_key(self) -> str:
        return self.date_window.start_date.strftime("%m%d")

    @property
    def calendar_label(self) -> str:
        return format_calendar_day(self.date_window.start_date)


@dataclass(slots=True)
class CalendarDayPitchingPerformanceQuery:
    date_window: DateWindow
    descriptor: str
    sort_desc: bool

    @property
    def month_day_key(self) -> str:
        return self.date_window.start_date.strftime("%m%d")

    @property
    def calendar_label(self) -> str:
        return format_calendar_day(self.date_window.start_date)


@dataclass(slots=True)
class DailyLookupResult:
    mode: str
    total: int
    game_count: int
    summary: str
    citation: str
    clips: list[dict[str, Any]] = field(default_factory=list)
    top_players: list[dict[str, Any]] = field(default_factory=list)


class DailyLookupResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.sporty_video_client = SportyVideoClient(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        pitching_query = parse_calendar_day_pitching_performance_query(
            question,
            self.settings.live_season or date.today().year,
        )
        if pitching_query is not None:
            return self._build_calendar_day_pitching_performance_snippet(connection, pitching_query)

        leaderboard_query = parse_calendar_day_player_leaderboard_query(
            question,
            self.settings.live_season or date.today().year,
        )
        if leaderboard_query is not None:
            return self._build_calendar_day_player_leaderboard_snippet(connection, leaderboard_query)

        total_query = parse_calendar_day_total_query(
            question,
            self.settings.live_season or date.today().year,
        )
        if total_query is not None:
            return self._build_calendar_day_total_snippet(connection, total_query)

        lookup_query = parse_daily_lookup_query(question, self.settings.live_season or date.today().year)
        if lookup_query is None:
            return None

        live_result = self._build_live_result(lookup_query)
        historical_result = self._build_historical_result(connection, lookup_query)
        primary_result = self._choose_primary_result(lookup_query, live_result, historical_result)
        if primary_result is None:
            return None

        summary = primary_result.summary
        validation_note = self._build_validation_note(live_result, historical_result)
        if validation_note:
            summary = f"{summary} {validation_note}"

        return EvidenceSnippet(
            source="Daily Lookup",
            title=f"{format_window(lookup_query.date_window)} {lookup_query.metric.label}",
            citation=primary_result.citation,
            summary=summary,
            payload={
                "analysis_type": "daily_lookup_total",
                "mode": primary_result.mode,
                "metric": lookup_query.metric.key,
                "metric_label": lookup_query.metric.label,
                "date_window": serialize_window(lookup_query.date_window),
                "total": primary_result.total,
                "game_count": primary_result.game_count,
                "top_players": primary_result.top_players,
                "clips": primary_result.clips,
                "validation_note": validation_note,
            },
        )

    def _build_calendar_day_player_leaderboard_snippet(
        self,
        connection,
        leaderboard_query: CalendarDayPlayerLeaderboardQuery,
    ) -> EvidenceSnippet | None:
        table_name = leaderboard_query.metric.table_name
        if not table_exists(connection, table_name):
            return None
        stat_column = resolve_column(connection, table_name, leaderboard_query.metric.column_candidates)
        date_column = resolve_column(connection, table_name, ("date", "game_date", "gamedate"))
        if stat_column is None or date_column is None:
            return None
        stattype_column = resolve_column(connection, table_name, ("stattype",))
        where_clauses = [f"substr(stats.{date_column}, 5, 4) = ?"]
        parameters: list[Any] = [leaderboard_query.month_day_key]
        if stattype_column:
            where_clauses.append(f"stats.{stattype_column} = ?")
            parameters.append("value")
        where_sql = " AND ".join(where_clauses)

        rows = connection.execute(
            f"""
            SELECT
                COALESCE(names.first || ' ' || names.last, stats.id) AS player_name,
                SUM(CAST(stats.{stat_column} AS INTEGER)) AS total
            FROM {table_name} AS stats
            LEFT JOIN (
                SELECT id, MIN(first) AS first, MIN(last) AS last
                FROM retrosheet_allplayers
                GROUP BY id
            ) AS names
                ON names.id = stats.id
            WHERE {where_sql}
            GROUP BY stats.id, player_name
            HAVING SUM(CAST(stats.{stat_column} AS INTEGER)) > 0
            ORDER BY total DESC, player_name ASC
            LIMIT 8
            """,
            tuple(parameters),
        ).fetchall()
        if not rows:
            return None

        totals_row = connection.execute(
            f"""
            SELECT
                COALESCE(SUM(CAST(stats.{stat_column} AS INTEGER)), 0) AS total,
                COUNT(DISTINCT stats.gid) AS game_count
            FROM {table_name} AS stats
            WHERE {where_sql}
            """,
            tuple(parameters),
        ).fetchone()
        total_on_day = int(totals_row["total"] or 0) if totals_row is not None else 0
        game_count = int(totals_row["game_count"] or 0) if totals_row is not None else 0

        leaderboard = [
            {"player_name": str(row["player_name"] or "").strip(), "total": int(row["total"] or 0)}
            for row in rows
            if str(row["player_name"] or "").strip()
        ]
        if not leaderboard:
            return None

        summary = build_calendar_day_leaderboard_summary(
            leaderboard_query.metric.label,
            leaderboard_query.calendar_label,
            leaderboard,
            total_on_day=total_on_day,
            game_count=game_count,
        )
        return EvidenceSnippet(
            source="Daily Lookup",
            title=f"{leaderboard_query.calendar_label} historical {leaderboard_query.metric.label} leaders",
            citation=f"{table_name} imported from Retrosheet",
            summary=summary,
            payload={
                "analysis_type": "calendar_day_player_leaderboard",
                "mode": "historical",
                "metric": leaderboard_query.metric.key,
                "metric_label": leaderboard_query.metric.label,
                "calendar_day": leaderboard_query.calendar_label,
                "month_day_key": leaderboard_query.month_day_key,
                "leaders": leaderboard,
                "total_on_day": total_on_day,
                "game_count": game_count,
                "clips": [],
            },
        )

    def _build_calendar_day_total_snippet(
        self,
        connection,
        total_query: CalendarDayTotalQuery,
    ) -> EvidenceSnippet | None:
        table_name = total_query.metric.table_name
        if not table_exists(connection, table_name):
            return None
        stat_column = resolve_column(connection, table_name, total_query.metric.column_candidates)
        date_column = resolve_column(connection, table_name, ("date", "game_date", "gamedate"))
        if stat_column is None or date_column is None:
            return None
        stattype_column = resolve_column(connection, table_name, ("stattype",))
        where_clauses = [f"substr(stats.{date_column}, 5, 4) = ?"]
        parameters: list[Any] = [total_query.month_day_key]
        if stattype_column:
            where_clauses.append(f"stats.{stattype_column} = ?")
            parameters.append("value")
        where_sql = " AND ".join(where_clauses)
        totals_row = connection.execute(
            f"""
            SELECT
                COALESCE(SUM(CAST(stats.{stat_column} AS INTEGER)), 0) AS total,
                COUNT(DISTINCT stats.gid) AS game_count
            FROM {table_name} AS stats
            WHERE {where_sql}
            """,
            tuple(parameters),
        ).fetchone()
        if totals_row is None or int(totals_row["game_count"] or 0) == 0:
            return None
        top_players = self._historical_top_players_for_calendar_day(
            connection,
            table_name,
            stat_column,
            where_sql,
            parameters,
        )
        summary = (
            f"In the imported Retrosheet data, MLB logged {int(totals_row['total'] or 0)} {total_query.metric.label} "
            f"across {int(totals_row['game_count'] or 0)} game(s) played on {total_query.calendar_label}."
        )
        if top_players:
            next_text = "; ".join(f"{player} {total}" for player, total in top_players.most_common(3))
            summary = f"{summary} Top individual totals on that calendar date: {next_text}."
        return EvidenceSnippet(
            source="Daily Lookup",
            title=f"{total_query.calendar_label} historical {total_query.metric.label} total",
            citation=f"{table_name} imported from Retrosheet",
            summary=summary,
            payload={
                "analysis_type": "calendar_day_total",
                "mode": "historical",
                "metric": total_query.metric.key,
                "metric_label": total_query.metric.label,
                "calendar_day": total_query.calendar_label,
                "month_day_key": total_query.month_day_key,
                "total_on_day": int(totals_row["total"] or 0),
                "game_count": int(totals_row["game_count"] or 0),
                "top_players": serialize_top_players(top_players),
                "clips": [],
            },
        )

    def _build_calendar_day_pitching_performance_snippet(
        self,
        connection,
        pitching_query: CalendarDayPitchingPerformanceQuery,
    ) -> EvidenceSnippet | None:
        table_name = "retrosheet_pitching"
        if not table_exists(connection, table_name):
            return None
        required_columns = {
            "date": resolve_column(connection, table_name, ("date",)),
            "id": resolve_column(connection, table_name, ("id",)),
            "gid": resolve_column(connection, table_name, ("gid",)),
            "team": resolve_column(connection, table_name, ("team",)),
            "opp": resolve_column(connection, table_name, ("opp",)),
            "ipouts": resolve_column(connection, table_name, ("p_ipouts",)),
            "hits": resolve_column(connection, table_name, ("p_h",)),
            "runs": resolve_column(connection, table_name, ("p_r",)),
            "earned_runs": resolve_column(connection, table_name, ("p_er",)),
            "walks": resolve_column(connection, table_name, ("p_w",)),
            "intentional_walks": resolve_column(connection, table_name, ("p_iw",)),
            "strikeouts": resolve_column(connection, table_name, ("p_k",)),
            "starts": resolve_column(connection, table_name, ("p_gs",)),
            "stattype": resolve_column(connection, table_name, ("stattype",)),
            "gametype": resolve_column(connection, table_name, ("gametype",)),
        }
        if any(value is None for value in required_columns.values()):
            return None
        order_direction = "DESC" if pitching_query.sort_desc else "ASC"
        parameters: tuple[Any, ...] = (pitching_query.month_day_key, "value", "regular")
        rows = connection.execute(
            f"""
            WITH names AS (
                SELECT id, MIN(first) AS first, MIN(last) AS last
                FROM retrosheet_allplayers
                GROUP BY id
            ),
            lines AS (
                SELECT
                    stats.{required_columns['id']} AS player_id,
                    COALESCE(names.first || ' ' || names.last, stats.{required_columns['id']}) AS player_name,
                    stats.{required_columns['date']} AS game_date,
                    stats.{required_columns['team']} AS team,
                    stats.{required_columns['opp']} AS opponent,
                    CAST(stats.{required_columns['ipouts']} AS INTEGER) AS ipouts,
                    CAST(stats.{required_columns['hits']} AS INTEGER) AS hits_allowed,
                    CAST(stats.{required_columns['runs']} AS INTEGER) AS runs_allowed,
                    CAST(stats.{required_columns['earned_runs']} AS INTEGER) AS earned_runs,
                    CAST(stats.{required_columns['walks']} AS INTEGER) + CAST(stats.{required_columns['intentional_walks']} AS INTEGER) AS walks_allowed,
                    CAST(stats.{required_columns['strikeouts']} AS INTEGER) AS strikeouts,
                    (
                        50
                        + CAST(stats.{required_columns['ipouts']} AS INTEGER)
                        + CASE
                            WHEN CAST(stats.{required_columns['ipouts']} AS INTEGER) > 12
                                THEN 2 * (CAST(CAST(stats.{required_columns['ipouts']} AS INTEGER) / 3 AS INTEGER) - 4)
                            ELSE 0
                          END
                        + CAST(stats.{required_columns['strikeouts']} AS INTEGER)
                        - (2 * CAST(stats.{required_columns['hits']} AS INTEGER))
                        - (4 * CAST(stats.{required_columns['earned_runs']} AS INTEGER))
                        - (2 * MAX(CAST(stats.{required_columns['runs']} AS INTEGER) - CAST(stats.{required_columns['earned_runs']} AS INTEGER), 0))
                        - (CAST(stats.{required_columns['walks']} AS INTEGER) + CAST(stats.{required_columns['intentional_walks']} AS INTEGER))
                    ) AS game_score
                FROM {table_name} AS stats
                LEFT JOIN names
                  ON names.id = stats.{required_columns['id']}
                WHERE substr(stats.{required_columns['date']}, 5, 4) = ?
                  AND stats.{required_columns['stattype']} = ?
                  AND stats.{required_columns['gametype']} = ?
                  AND CAST(stats.{required_columns['starts']} AS INTEGER) > 0
            )
            SELECT *
            FROM lines
            ORDER BY game_score {order_direction}, strikeouts DESC, ipouts DESC, player_name ASC
            LIMIT 8
            """,
            parameters,
        ).fetchall()
        if not rows:
            return None
        leaders = [
            {
                "player_name": str(row["player_name"] or "").strip(),
                "game_date": format_retrosheet_date(str(row["game_date"] or "")),
                "team": str(row["team"] or ""),
                "opponent": str(row["opponent"] or ""),
                "innings_pitched": round(int(row["ipouts"] or 0) / 3.0, 1),
                "strikeouts": int(row["strikeouts"] or 0),
                "hits_allowed": int(row["hits_allowed"] or 0),
                "walks_allowed": int(row["walks_allowed"] or 0),
                "earned_runs": int(row["earned_runs"] or 0),
                "runs_allowed": int(row["runs_allowed"] or 0),
                "game_score": int(row["game_score"] or 0),
            }
            for row in rows
            if str(row["player_name"] or "").strip()
        ]
        if not leaders:
            return None
        lead = leaders[0]
        summary = (
            f"Using Bill James Game Score as the pitching-line summary, the {pitching_query.descriptor} starting "
            f"pitching performance on {pitching_query.calendar_label} belongs to {lead['player_name']} on "
            f"{lead['game_date']} with a Game Score of {lead['game_score']}. "
            f"Line: {lead['innings_pitched']:.1f} IP, {lead['strikeouts']} SO, {lead['hits_allowed']} H, "
            f"{lead['walks_allowed']} BB, {lead['earned_runs']} ER."
        )
        trailing = leaders[1:4]
        if trailing:
            next_text = "; ".join(
                f"{row['player_name']} {row['game_score']} ({row['game_date']})"
                for row in trailing
            )
            summary = f"{summary} Next on the board: {next_text}."
        return EvidenceSnippet(
            source="Daily Lookup",
            title=f"{pitching_query.calendar_label} historical pitching performances",
            citation="retrosheet_pitching imported from Retrosheet, ranked by Bill James Game Score",
            summary=summary,
            payload={
                "analysis_type": "calendar_day_pitching_leaderboard",
                "mode": "historical",
                "metric": "Game Score",
                "calendar_day": pitching_query.calendar_label,
                "month_day_key": pitching_query.month_day_key,
                "leaders": leaders,
                "descriptor": pitching_query.descriptor,
                "clips": [],
            },
        )

    def _choose_primary_result(
        self,
        lookup_query: DailyLookupQuery,
        live_result: DailyLookupResult | None,
        historical_result: DailyLookupResult | None,
    ) -> DailyLookupResult | None:
        if live_result and self._prefer_live(lookup_query):
            return live_result
        if historical_result:
            return historical_result
        return live_result

    def _prefer_live(self, lookup_query: DailyLookupQuery) -> bool:
        current_year = self.settings.live_season or date.today().year
        if lookup_query.date_window.end_date.year >= current_year:
            return True
        return lookup_query.date_window.label in {"today", "yesterday", "this week", "last week"}

    def _build_validation_note(
        self,
        live_result: DailyLookupResult | None,
        historical_result: DailyLookupResult | None,
    ) -> str:
        if live_result is None or historical_result is None:
            return ""
        if live_result.total == historical_result.total:
            return "Live game feeds and the imported historical tables agree on that total."
        return (
            f"Cross-check note: live game feeds show {live_result.total}, while the imported historical tables "
            f"show {historical_result.total}."
        )

    def _build_live_result(self, lookup_query: DailyLookupQuery) -> DailyLookupResult | None:
        total = 0
        game_count = 0
        top_players: Counter[str] = Counter()
        home_run_candidates: list[dict[str, Any]] = []

        for target_date in iter_dates(lookup_query.date_window):
            schedule = self.live_client.schedule(target_date.isoformat())
            for day in schedule.get("dates", []):
                for game in day.get("games", []):
                    if game.get("status", {}).get("codedGameState") in {"S", "P"}:
                        continue
                    game_count += 1
                    game_pk = int(game["gamePk"])
                    feed = self.live_client.game_feed(game_pk)
                    team_matchup = build_team_matchup(feed)
                    for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
                        event_type = str(play.get("result", {}).get("eventType") or "").lower()
                        if event_type not in lookup_query.metric.live_event_types:
                            continue
                        total += 1
                        batter_name = str(play.get("matchup", {}).get("batter", {}).get("fullName") or "").strip()
                        if batter_name:
                            top_players[batter_name] += 1
                        if lookup_query.metric.key == "home_runs":
                            home_run_candidates.append(
                                {
                                    "game_date": target_date.isoformat(),
                                    "game_pk": game_pk,
                                    "team_matchup": team_matchup,
                                    "play": play,
                                    "play_id": extract_play_id(play),
                                    "batter_name": batter_name,
                                    "pitcher_name": str(
                                        play.get("matchup", {}).get("pitcher", {}).get("fullName") or ""
                                    ).strip(),
                                    "distance": parse_float(extract_hit_data(play).get("totalDistance")),
                                }
                            )

        if game_count == 0:
            return None

        summary = build_total_summary(
            metric_label=lookup_query.metric.label,
            total=total,
            game_count=game_count,
            date_window=lookup_query.date_window,
            top_players=top_players,
        )
        clips = self._build_home_run_clips(home_run_candidates) if lookup_query.metric.supports_clips else []
        if clips:
            longest = clips[0]
            distance = longest.get("hit_distance")
            if distance is not None:
                summary = (
                    f"{summary} The longest one I found went {int(round(float(distance)))} feet by "
                    f"{longest.get('batter_name') or longest.get('actor_name') or 'an MLB hitter'}."
                )
        return DailyLookupResult(
            mode="live",
            total=total,
            game_count=game_count,
            summary=summary,
            citation="MLB Stats API game feeds plus Baseball Savant sporty-videos when available",
            clips=clips,
            top_players=serialize_top_players(top_players),
        )

    def _build_home_run_clips(self, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ranked = sorted(
            candidates,
            key=lambda item: (
                -(item["distance"] or -1),
                item["game_date"],
                item["game_pk"],
            ),
        )
        clips: list[dict[str, Any]] = []
        for rank, candidate in enumerate(ranked[:5], start=1):
            play_id = str(candidate.get("play_id") or "")
            if not play_id:
                continue
            sporty_page = self.sporty_video_client.fetch(play_id)
            if sporty_page is None or not (sporty_page.savant_url or sporty_page.mp4_url):
                continue
            play = candidate["play"]
            title = sporty_page.title or str(play.get("result", {}).get("description") or "")
            distance = sporty_page.hit_distance if sporty_page.hit_distance is not None else candidate["distance"]
            distance_text = f"{int(round(float(distance)))} ft" if distance is not None else "an untracked distance"
            explanation = f"Relevant because this was the No. {rank} longest home run in that window at {distance_text}."
            if rank == 1:
                explanation = f"Relevant because this was the longest home run in that window at {distance_text}."
            if sporty_page.hr_parks is not None:
                explanation = f"{explanation} Statcast graded it as a homer in {sporty_page.hr_parks}/30 parks."
            clips.append(
                {
                    "play_id": play_id,
                    "title": title,
                    "description": str(play.get("result", {}).get("description") or ""),
                    "explanation": explanation,
                    "game_date": candidate["game_date"],
                    "team_matchup": sporty_page.matchup or candidate["team_matchup"],
                    "batter_name": sporty_page.batter or candidate["batter_name"],
                    "pitcher_name": sporty_page.pitcher or candidate["pitcher_name"],
                    "fielder_name": "",
                    "actor_name": sporty_page.batter or candidate["batter_name"],
                    "actor_roles": ["batter"],
                    "match_tags": ["home run", "daily total context"],
                    "inning": int(play.get("about", {}).get("inning") or 0),
                    "half_inning": str(play.get("about", {}).get("halfInning") or ""),
                    "hit_distance": distance,
                    "exit_velocity": sporty_page.exit_velocity,
                    "launch_angle": sporty_page.launch_angle,
                    "hr_parks": sporty_page.hr_parks,
                    "savant_url": sporty_page.savant_url,
                    "mp4_url": sporty_page.mp4_url,
                }
            )
        return clips

    def _build_historical_result(self, connection, lookup_query: DailyLookupQuery) -> DailyLookupResult | None:
        table_name = lookup_query.metric.table_name
        if not table_exists(connection, table_name):
            return None
        stat_column = resolve_column(connection, table_name, lookup_query.metric.column_candidates)
        date_column = resolve_column(connection, table_name, ("date", "game_date", "gamedate"))
        if stat_column is None or date_column is None:
            return None
        stattype_column = resolve_column(connection, table_name, ("stattype",))
        where_clauses = [f"{date_column} >= ?", f"{date_column} <= ?"]
        parameters: list[Any] = [lookup_query.date_window.start_date.strftime("%Y%m%d"), lookup_query.date_window.end_date.strftime("%Y%m%d")]
        if stattype_column:
            where_clauses.append(f"{stattype_column} = ?")
            parameters.append("value")
        where_sql = " AND ".join(where_clauses)
        row = connection.execute(
            f"""
            SELECT
                COALESCE(SUM(CAST({stat_column} AS INTEGER)), 0) AS total,
                COUNT(DISTINCT gid) AS game_count
            FROM {table_name}
            WHERE {where_sql}
            """,
            tuple(parameters),
        ).fetchone()
        if row is None or int(row["game_count"] or 0) == 0:
            return None

        summary = build_total_summary(
            metric_label=lookup_query.metric.label,
            total=int(row["total"] or 0),
            game_count=int(row["game_count"] or 0),
            date_window=lookup_query.date_window,
            top_players=self._historical_top_players(connection, table_name, stat_column, where_sql, parameters),
        )
        return DailyLookupResult(
            mode="historical",
            total=int(row["total"] or 0),
            game_count=int(row["game_count"] or 0),
            summary=summary,
            citation=f"{table_name} imported from Retrosheet",
        )

    def _historical_top_players(
        self,
        connection,
        table_name: str,
        stat_column: str,
        where_sql: str,
        parameters: list[Any],
    ) -> Counter[str]:
        top_players: Counter[str] = Counter()
        rows = connection.execute(
            f"""
            SELECT
                COALESCE(names.first || ' ' || names.last, stats.id) AS player_name,
                SUM(CAST(stats.{stat_column} AS INTEGER)) AS total
            FROM {table_name} AS stats
            LEFT JOIN (
                SELECT id, MIN(first) AS first, MIN(last) AS last
                FROM retrosheet_allplayers
                GROUP BY id
            ) AS names
                ON names.id = stats.id
            WHERE {where_sql}
            GROUP BY stats.id, player_name
            HAVING SUM(CAST(stats.{stat_column} AS INTEGER)) > 0
            ORDER BY total DESC, player_name ASC
            LIMIT 5
            """,
            tuple(parameters),
        ).fetchall()
        for row in rows:
            player_name = str(row["player_name"] or "").strip()
            if player_name:
                top_players[player_name] = int(row["total"] or 0)
        return top_players

    def _historical_top_players_for_calendar_day(
        self,
        connection,
        table_name: str,
        stat_column: str,
        where_sql: str,
        parameters: list[Any],
    ) -> Counter[str]:
        return self._historical_top_players(connection, table_name, stat_column, where_sql, parameters)


def parse_daily_lookup_query(question: str, default_year: int) -> DailyLookupQuery | None:
    lowered = f" {question.lower()} "
    if not any(hint in lowered for hint in TOTAL_HINTS):
        return None
    if extract_calendar_day_window(question, default_year) is not None and not question_mentions_explicit_year(question):
        return None
    date_window = extract_date_window(question, default_year)
    if date_window is None:
        return None
    metric = match_daily_metric(lowered)
    if metric is None:
        return None
    return DailyLookupQuery(metric=metric, date_window=date_window)


def parse_calendar_day_player_leaderboard_query(
    question: str,
    default_year: int,
) -> CalendarDayPlayerLeaderboardQuery | None:
    lowered = f" {question.lower()} "
    if not any(hint in lowered for hint in LEADERBOARD_HINTS):
        return None
    if not any(hint in lowered for hint in PLAYER_TARGET_HINTS):
        return None
    if question_mentions_explicit_year(question):
        return None
    date_window = extract_calendar_day_window(question, default_year)
    if date_window is None or not date_window.is_single_day:
        return None
    metric = match_daily_metric(lowered)
    if metric is None:
        return None
    return CalendarDayPlayerLeaderboardQuery(metric=metric, date_window=date_window)


def parse_calendar_day_total_query(
    question: str,
    default_year: int,
) -> CalendarDayTotalQuery | None:
    lowered = f" {question.lower()} "
    if not any(hint in lowered for hint in TOTAL_HINTS):
        return None
    if question_mentions_explicit_year(question):
        return None
    date_window = extract_calendar_day_window(question, default_year)
    if date_window is None or not date_window.is_single_day:
        return None
    metric = match_daily_metric(lowered)
    if metric is None:
        return None
    return CalendarDayTotalQuery(metric=metric, date_window=date_window)


def parse_calendar_day_pitching_performance_query(
    question: str,
    default_year: int,
) -> CalendarDayPitchingPerformanceQuery | None:
    lowered = f" {question.lower()} "
    if question_mentions_explicit_year(question):
        return None
    date_window = extract_calendar_day_window(question, default_year)
    if date_window is None or not date_window.is_single_day:
        return None
    if not any(hint in lowered for hint in PITCHER_TARGET_HINTS):
        return None
    if not any(hint in lowered for hint in PITCHING_PERFORMANCE_HINTS):
        return None
    sort_desc = not any(hint in lowered for hint in ("worst",))
    descriptor = "best" if sort_desc else "worst"
    return CalendarDayPitchingPerformanceQuery(
        date_window=date_window,
        descriptor=descriptor,
        sort_desc=sort_desc,
    )


def wants_historical_calendar_day_leaderboard(question: str, default_year: int) -> bool:
    return any(
        query is not None
        for query in (
            parse_calendar_day_player_leaderboard_query(question, default_year),
            parse_calendar_day_total_query(question, default_year),
            parse_calendar_day_pitching_performance_query(question, default_year),
        )
    )


def match_daily_metric(lowered_question: str) -> DailyMetricSpec | None:
    best_match: tuple[int, DailyMetricSpec] | None = None
    for metric in DAILY_METRICS:
        for alias in metric.aliases:
            alias_text = alias if alias.startswith(" ") else f" {alias} "
            if alias_text not in lowered_question:
                continue
            score = len(alias.strip())
            if best_match is None or score > best_match[0]:
                best_match = (score, metric)
    return best_match[1] if best_match else None


def build_total_summary(
    *,
    metric_label: str,
    total: int,
    game_count: int,
    date_window: DateWindow,
    top_players: Counter[str],
) -> str:
    window_text = f"on {date_window.start_date.isoformat()}"
    if not date_window.is_single_day:
        window_text = f"from {date_window.start_date.isoformat()} through {date_window.end_date.isoformat()}"
    summary = f"There were {total} {metric_label} across {game_count} MLB game(s) {window_text}."
    leaders = []
    for index, (player_name, player_total) in enumerate(top_players.most_common(3), start=1):
        leaders.append(f"{index}. {player_name} {player_total}")
    if leaders:
        summary = f"{summary} Top individual totals: {'; '.join(leaders)}."
    return summary


def serialize_top_players(top_players: Counter[str]) -> list[dict[str, Any]]:
    return [
        {"player_name": player_name, "total": total}
        for player_name, total in top_players.most_common(5)
    ]


def format_calendar_day(target_date: date) -> str:
    return f"{target_date.strftime('%B')} {target_date.day}"


def build_calendar_day_leaderboard_summary(
    metric_label: str,
    calendar_label: str,
    leaderboard: list[dict[str, Any]],
    *,
    total_on_day: int,
    game_count: int,
) -> str:
    top_total = int(leaderboard[0]["total"])
    leaders = [entry["player_name"] for entry in leaderboard if int(entry["total"]) == top_total]
    if len(leaders) == 1:
        summary = (
            f"Historically on {calendar_label}, {leaders[0]} has the most {metric_label} with {top_total}."
        )
    else:
        leader_text = join_names(leaders)
        summary = (
            f"Historically on {calendar_label}, {leader_text} are tied for the most {metric_label} with {top_total} each."
        )

    runners_up = [entry for entry in leaderboard if int(entry["total"]) < top_total][:3]
    if runners_up:
        next_text = "; ".join(f"{entry['player_name']} {entry['total']}" for entry in runners_up)
        summary = f"{summary} Next on the list: {next_text}."
    if total_on_day or game_count:
        summary = (
            f"{summary} In the imported Retrosheet data, MLB logged {total_on_day} {metric_label} across "
            f"{game_count} game(s) played on that calendar date."
        )
    return summary


def join_names(names: list[str]) -> str:
    if len(names) <= 1:
        return names[0] if names else ""
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{', '.join(names[:-1])}, and {names[-1]}"


def format_retrosheet_date(value: str) -> str:
    cleaned = str(value or "").strip()
    if len(cleaned) == 8 and cleaned.isdigit():
        return f"{cleaned[:4]}-{cleaned[4:6]}-{cleaned[6:]}"
    return cleaned


def iter_dates(window: DateWindow):
    current = window.start_date
    while current <= window.end_date:
        yield current
        current += timedelta(days=1)
