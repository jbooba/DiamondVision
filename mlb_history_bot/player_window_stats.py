from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .live_game_research import build_homer_play_card
from .models import EvidenceSnippet
from .person_query import choose_best_person_match, extract_player_candidate
from .query_utils import DateWindow, RECENT_WINDOW_LABELS, extract_recent_window
from .sporty_video import SportyVideoClient
from .team_evaluator import safe_int


HIT_EVENTS = {"single", "double", "triple", "home_run"}
WALK_EVENTS = {"walk", "intent_walk"}
WINDOW_REJECT_HINTS = {"clip", "clips", "video", "videos", "replay", "highlight", "highlights"}


@dataclass(slots=True)
class PlayerWindowMetricSpec:
    key: str
    label: str
    aliases: tuple[str, ...]


@dataclass(slots=True)
class PlayerWindowMetricQuery:
    player_query: str
    player_name: str
    player_id: int
    metric: PlayerWindowMetricSpec
    date_window: DateWindow


WINDOW_METRICS: tuple[PlayerWindowMetricSpec, ...] = (
    PlayerWindowMetricSpec("home_runs", "home runs", ("home run", "home runs", "homer", "homers", "homerun", "homeruns", "homered")),
    PlayerWindowMetricSpec("hits", "hits", ("hits", "hit")),
    PlayerWindowMetricSpec("walks", "walks", ("walks", "walk")),
    PlayerWindowMetricSpec("strikeouts", "strikeouts", ("strikeouts", "strikeout", "strike outs", "struck out")),
    PlayerWindowMetricSpec("rbi", "RBI", ("rbi", "rbis", "runs batted in")),
)


class PlayerWindowStatsResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.sporty_video_client = SportyVideoClient(settings)

    def build_snippet(self, question: str) -> EvidenceSnippet | None:
        current_year = self.settings.live_season or date.today().year
        query = parse_player_window_metric_query(question, self.live_client, current_year)
        if query is None:
            return None

        person_details = self.live_client.person_details(query.player_id) or {}
        current_team = person_details.get("currentTeam", {}) if isinstance(person_details.get("currentTeam"), dict) else {}
        current_team_id = int(current_team.get("id") or 0)
        current_team_name = str(current_team.get("name") or "").strip()

        schedule = (
            self.live_client.schedule_range(
                query.date_window.start_date.isoformat(),
                query.date_window.end_date.isoformat(),
                team_id=current_team_id or None,
            )
            if current_team_id and query.date_window.end_date.year >= current_year
            else self.live_client.schedule_range(
                query.date_window.start_date.isoformat(),
                query.date_window.end_date.isoformat(),
            )
        )

        total = 0
        plate_appearances = 0
        hits = 0
        walks = 0
        strikeouts = 0
        rbi = 0
        games_with_appearance = 0
        scheduled_games = 0
        started_games = 0
        clips: list[dict[str, Any]] = []
        rows: list[dict[str, Any]] = []

        for day in schedule.get("dates", []):
            for game in day.get("games", []):
                status_code = str(game.get("status", {}).get("codedGameState") or "")
                if status_code in {"S", "P"}:
                    scheduled_games += 1
                    continue
                started_games += 1
                game_pk = int(game.get("gamePk") or 0)
                if not game_pk:
                    continue
                feed = self.live_client.game_feed(game_pk)
                game_total = 0
                game_pa = 0
                game_hits = 0
                game_walks = 0
                game_strikeouts = 0
                game_rbi = 0
                team_matchup = build_team_matchup(feed)
                for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
                    batter = play.get("matchup", {}).get("batter", {})
                    if int(batter.get("id") or 0) != query.player_id:
                        continue
                    game_pa += 1
                    event_type = str(play.get("result", {}).get("eventType") or "").lower()
                    if event_type in HIT_EVENTS:
                        game_hits += 1
                    if event_type in WALK_EVENTS:
                        game_walks += 1
                    if "strikeout" in event_type:
                        game_strikeouts += 1
                    game_rbi += safe_int(play.get("result", {}).get("rbi")) or 0
                    game_total += metric_increment(query.metric.key, event_type, play)
                    if query.metric.key == "home_runs" and event_type == "home_run":
                        clips.append(build_homer_play_card(day["date"], game_pk, play, self.sporty_video_client))
                if game_pa <= 0:
                    continue
                games_with_appearance += 1
                total += game_total
                plate_appearances += game_pa
                hits += game_hits
                walks += game_walks
                strikeouts += game_strikeouts
                rbi += game_rbi
                if game_total or query.metric.key == "home_runs":
                    rows.append(
                        {
                            "game_date": day["date"],
                            "matchup": team_matchup,
                            "metric_total": game_total,
                            "plate_appearances": game_pa,
                            "hits": game_hits,
                            "walks": game_walks,
                            "strikeouts": game_strikeouts,
                            "rbi": game_rbi,
                        }
                    )

        summary = build_player_window_summary(
            query,
            total=total,
            games_with_appearance=games_with_appearance,
            plate_appearances=plate_appearances,
            hits=hits,
            walks=walks,
            strikeouts=strikeouts,
            rbi=rbi,
            scheduled_games=scheduled_games,
            started_games=started_games,
            current_team_name=current_team_name,
            rows=rows,
        )
        return EvidenceSnippet(
            source="Player Window Stats",
            title=f"{query.player_name} {query.date_window.label} {query.metric.label}",
            citation="MLB Stats API schedule range and live game feeds",
            summary=summary,
            payload={
                "analysis_type": "player_window_metric",
                "mode": "live",
                "player": query.player_name,
                "metric": query.metric.label,
                "window_label": query.date_window.label,
                "window_start": query.date_window.start_date.isoformat(),
                "window_end": query.date_window.end_date.isoformat(),
                "rows": rows or [
                    {
                        "game_date": "",
                        "matchup": "",
                        "metric_total": total,
                        "plate_appearances": plate_appearances,
                        "hits": hits,
                        "walks": walks,
                        "strikeouts": strikeouts,
                        "rbi": rbi,
                    }
                ],
                "clips": clips[:6],
            },
        )

def parse_player_window_metric_query(
    question: str,
    live_client: LiveStatsClient,
    current_year: int,
) -> PlayerWindowMetricQuery | None:
    lowered = question.lower()
    if any(hint in lowered for hint in WINDOW_REJECT_HINTS):
        return None
    date_window = extract_recent_window(question, current_year, allowed_labels=RECENT_WINDOW_LABELS)
    if date_window is None:
        return None
    metric = find_window_metric(lowered)
    if metric is None:
        return None
    player_query = extract_player_query(question, metric)
    if not player_query:
        return None
    people = live_client.search_people(player_query)
    if not people:
        return None
    selected = choose_best_person_match(people, player_query)
    player_id = int(selected.get("id") or 0)
    if not player_id:
        return None
    return PlayerWindowMetricQuery(
        player_query=player_query,
        player_name=str(selected.get("fullName") or player_query).strip(),
        player_id=player_id,
        metric=metric,
        date_window=date_window,
    )


def find_window_metric(lowered_question: str) -> PlayerWindowMetricSpec | None:
    for metric in WINDOW_METRICS:
        if any(alias in lowered_question for alias in metric.aliases):
            return metric
    return None


def extract_player_query(question: str, metric: PlayerWindowMetricSpec) -> str | None:
    alias_pattern = "|".join(re.escape(alias) for alias in sorted(metric.aliases, key=len, reverse=True))
    patterns = (
        re.compile(rf"^(?:did|has|have)\s+(.+?)\s+(?:hit|get|collect|draw|record|have)\s+(?:any\s+)?(?:{alias_pattern})\b", re.IGNORECASE),
        re.compile(rf"^how many\s+(?:{alias_pattern})\s+(?:did|has|have)\s+(.+?)\b", re.IGNORECASE),
        re.compile(rf"^(.+?)\s+(?:{alias_pattern})\s+(?:today|tonight|yesterday|last night|this week|last week)\b", re.IGNORECASE),
    )
    return extract_player_candidate(question, patterns=patterns)


def clean_player_phrase(value: str) -> str:
    from .person_query import clean_player_phrase as shared_clean_player_phrase

    return shared_clean_player_phrase(value)


def metric_increment(metric_key: str, event_type: str, play: dict[str, Any]) -> int:
    if metric_key == "home_runs":
        return 1 if event_type == "home_run" else 0
    if metric_key == "hits":
        return 1 if event_type in HIT_EVENTS else 0
    if metric_key == "walks":
        return 1 if event_type in WALK_EVENTS else 0
    if metric_key == "strikeouts":
        return 1 if "strikeout" in event_type else 0
    if metric_key == "rbi":
        return safe_int(play.get("result", {}).get("rbi")) or 0
    return 0


def build_player_window_summary(
    query: PlayerWindowMetricQuery,
    *,
    total: int,
    games_with_appearance: int,
    plate_appearances: int,
    hits: int,
    walks: int,
    strikeouts: int,
    rbi: int,
    scheduled_games: int,
    started_games: int,
    current_team_name: str,
    rows: list[dict[str, Any]],
) -> str:
    window_text = (
        query.date_window.start_date.isoformat()
        if query.date_window.is_single_day
        else f"{query.date_window.start_date.isoformat()} through {query.date_window.end_date.isoformat()}"
    )
    if started_games == 0 and scheduled_games > 0:
        team_phrase = f"{current_team_name} games" if current_team_name else f"{query.player_name}'s games"
        window_label = query.date_window.label[:1].upper() + query.date_window.label[1:]
        return (
            f"Not yet. {window_label} currently covers {window_text}, and "
            f"{team_phrase} in that window have not started yet."
        )
    if games_with_appearance == 0:
        return f"I did not find any plate appearances for {query.player_name} from {window_text}."
    if total > 0:
        if query.metric.key == "home_runs":
            details = "; ".join(
                f"{row['game_date']} vs {row['matchup']} ({row['metric_total']} HR)"
                for row in rows
                if row["metric_total"] > 0
            )
            return (
                f"Yes. {query.player_name} has {total} {query.metric.label} from {window_text} across "
                f"{games_with_appearance} game(s) and {plate_appearances} plate appearances. {details}"
            )
        return (
            f"{query.player_name} has {total} {query.metric.label} from {window_text} across "
            f"{games_with_appearance} game(s) and {plate_appearances} plate appearances. "
            f"Context: {hits} H, {walks} BB, {strikeouts} SO, {rbi} RBI."
        )
    return (
        f"No. {query.player_name} has 0 {query.metric.label} from {window_text} across "
        f"{games_with_appearance} game(s) and {plate_appearances} plate appearances. "
        f"Context: {hits} H, {walks} BB, {strikeouts} SO, {rbi} RBI."
    )


def build_team_matchup(feed: dict[str, Any]) -> str:
    teams = feed.get("gameData", {}).get("teams", {})
    away = str(teams.get("away", {}).get("abbreviation") or teams.get("away", {}).get("name") or "").strip()
    home = str(teams.get("home", {}).get("abbreviation") or teams.get("home", {}).get("name") or "").strip()
    return f"{away} @ {home}".strip()
