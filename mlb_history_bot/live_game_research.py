from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from .config import Settings
from .film_room_research import extract_hit_data, parse_float
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .person_query import choose_best_person_match, extract_player_candidate
from .query_utils import DateWindow, extract_recent_window, ordinal
from .sporty_research import extract_play_id
from .sporty_video import SportyVideoClient
from .team_evaluator import TeamIdentity, resolve_team_from_question, safe_float, safe_int


TEAM_GAME_HINTS = (
    "how did",
    "did the",
    "did ",
    "how were",
    "how was",
    "play today",
    "play tonight",
    "play yesterday",
    "play last night",
)
PLAYER_HOME_RUN_HINTS = (
    " homer",
    " homered",
    " home run",
    " home runs",
    " homers",
)
DATE_LABELS = {"today", "yesterday", "last night", "tonight"}
SCORING_BREAKDOWN_HINTS = (
    "how did",
    "score their runs",
    "score its runs",
    "how were the runs scored",
    "how was the scoring done",
    "scoring plays",
)


@dataclass(slots=True)
class TeamGameQuery:
    team: TeamIdentity
    date_window: DateWindow
    wants_scoring_breakdown: bool = False


@dataclass(slots=True)
class PlayerDayHomeRunQuery:
    player_query: str
    date_window: DateWindow


class LiveGameResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.sporty_video_client = SportyVideoClient(settings)

    def build_snippet(self, question: str) -> EvidenceSnippet | None:
        current_year = self.settings.live_season or date.today().year
        team_query = parse_team_game_query(question, self.live_client, current_year)
        if team_query is not None:
            return self._build_team_game_snippet(team_query)
        player_query = parse_player_day_home_run_query(question, current_year)
        if player_query is not None:
            return self._build_player_home_run_snippet(player_query)
        return None

    def _build_team_game_snippet(self, query: TeamGameQuery) -> EvidenceSnippet | None:
        target_date = query.date_window.start_date.isoformat()
        schedule = self.live_client.schedule(target_date, hydrate="linescore,team,decisions")
        for day in schedule.get("dates", []):
            for game in day.get("games", []):
                teams = game.get("teams", {})
                away_team_id = int(teams.get("away", {}).get("team", {}).get("id") or 0)
                home_team_id = int(teams.get("home", {}).get("team", {}).get("id") or 0)
                if query.team.team_id not in {away_team_id, home_team_id}:
                    continue
                feed = self.live_client.game_feed(int(game["gamePk"]))
                return build_team_game_snippet(query.team, target_date, game, feed)
        return None

    def _build_player_home_run_snippet(self, query: PlayerDayHomeRunQuery) -> EvidenceSnippet | None:
        target_date = query.date_window.start_date.isoformat()
        people = self.live_client.search_people(query.player_query)
        if not people:
            return None
        selected_player = choose_best_person_match(people, query.player_query)
        player_id = int(selected_player.get("id") or 0)
        player_name = str(selected_player.get("fullName") or query.player_query).strip()
        schedule = self.live_client.schedule(target_date)
        homer_plays: list[dict[str, Any]] = []
        appearances = 0
        team_name = ""
        game_status = ""
        for day in schedule.get("dates", []):
            for game in day.get("games", []):
                feed = self.live_client.game_feed(int(game["gamePk"]))
                team_name = team_name or current_team_name_for_player(feed, player_id)
                game_status = str(game.get("status", {}).get("detailedState") or game_status)
                for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
                    matchup = play.get("matchup", {})
                    batter = matchup.get("batter", {})
                    if int(batter.get("id") or 0) != player_id:
                        continue
                    appearances += 1
                    event_type = str(play.get("result", {}).get("eventType") or "").lower()
                    if event_type == "home_run":
                        homer_plays.append(build_homer_play_card(target_date, int(game["gamePk"]), play, self.sporty_video_client))
        if not appearances and not homer_plays:
            return EvidenceSnippet(
                source="Live Game Research",
                title=f"{player_name} {target_date} home run status",
                citation="MLB Stats API schedule and game feeds",
                summary=f"I did not find an MLB game for {player_name} on {target_date}.",
                payload={
                    "analysis_type": "player_day_home_runs",
                    "mode": "live",
                    "player": player_name,
                    "target_date": target_date,
                    "home_run_count": 0,
                    "clips": [],
                    "rows": [],
                },
            )

        count = len(homer_plays)
        if count:
            first_play = homer_plays[0]
            summary = (
                f"Yes. {player_name} hit {count} home run(s) on {target_date}"
                f"{f' for {team_name}' if team_name else ''}."
            )
            details = []
            for index, play in enumerate(homer_plays[:3], start=1):
                distance = f"{int(round(float(play['hit_distance'])))} ft" if play.get("hit_distance") is not None else "distance unavailable"
                details.append(
                    f"{index}. {play['title']} in the {ordinal(int(play['inning']))} {play['half_inning']} ({distance})"
                )
            summary = f"{summary} {' '.join(details)}"
        else:
            summary = (
                f"No. {player_name} did not homer on {target_date}"
                f"{f' for {team_name}' if team_name else ''}."
            )
            if game_status:
                summary = f"{summary} Game status: {game_status}."
        return EvidenceSnippet(
            source="Live Game Research",
            title=f"{player_name} {target_date} home runs",
            citation="MLB Stats API game feeds plus Baseball Savant sporty-videos when available",
            summary=summary,
            payload={
                "analysis_type": "player_day_home_runs",
                "mode": "live",
                "player": player_name,
                "target_date": target_date,
                "home_run_count": count,
                "clips": homer_plays,
                "rows": [
                    {
                        "title": play["title"],
                        "inning": f"{ordinal(int(play['inning']))} {play['half_inning']}",
                        "distance": format_optional_distance(play.get("hit_distance")),
                        "matchup": play["team_matchup"],
                    }
                    for play in homer_plays
                ],
            },
        )


def parse_team_game_query(question: str, live_client: LiveStatsClient, current_year: int) -> TeamGameQuery | None:
    lowered = question.lower()
    date_window = extract_recent_window(question, current_year, allowed_labels=DATE_LABELS)
    wants_scoring_breakdown = any(hint in lowered for hint in SCORING_BREAKDOWN_HINTS) and "run" in lowered
    if date_window is None and ("right now" in lowered or wants_scoring_breakdown):
        today = date.today()
        date_window = DateWindow(start_date=today, end_date=today, label="today")
    if date_window is None or not date_window.is_single_day:
        return None
    if not any(hint in lowered for hint in TEAM_GAME_HINTS) and not wants_scoring_breakdown:
        return None
    if any(hint in lowered for hint in PLAYER_HOME_RUN_HINTS):
        return None
    team = resolve_team_from_question(question, live_client.teams(current_year))
    if team is None:
        return None
    return TeamGameQuery(team=team, date_window=date_window, wants_scoring_breakdown=wants_scoring_breakdown)


def parse_player_day_home_run_query(question: str, current_year: int) -> PlayerDayHomeRunQuery | None:
    lowered = question.lower()
    date_window = extract_recent_window(question, current_year, allowed_labels=DATE_LABELS)
    if date_window is None or not date_window.is_single_day:
        return None
    if not any(hint in lowered for hint in PLAYER_HOME_RUN_HINTS):
        return None
    player_query = extract_player_lookup_phrase(question)
    if not player_query:
        return None
    return PlayerDayHomeRunQuery(player_query=player_query, date_window=date_window)


def extract_player_lookup_phrase(question: str) -> str | None:
    patterns = (
        re.compile(r"did\s+(.+?)\s+homer(?:ed)?\b", re.IGNORECASE),
        re.compile(r"(.+?)\s+home\s+runs?\s+(?:today|tonight|yesterday|last night)\b", re.IGNORECASE),
        re.compile(r"(.+?)\s+homers?\s+(?:today|tonight|yesterday|last night)\b", re.IGNORECASE),
    )
    return extract_player_candidate(question, patterns=patterns)


def build_team_game_snippet(team: TeamIdentity, target_date: str, game: dict[str, Any], feed: dict[str, Any]) -> EvidenceSnippet:
    teams = game.get("teams", {})
    away = teams.get("away", {})
    home = teams.get("home", {})
    away_team = away.get("team", {})
    home_team = home.get("team", {})
    away_name = str(away_team.get("name") or "")
    home_name = str(home_team.get("name") or "")
    away_score = safe_int(away.get("score"))
    home_score = safe_int(home.get("score"))
    team_is_home = int(home_team.get("id") or 0) == team.team_id
    team_score = home_score if team_is_home else away_score
    opponent_score = away_score if team_is_home else home_score
    opponent_name = away_name if team_is_home else home_name
    status = str(game.get("status", {}).get("detailedState") or "")
    result_word = describe_team_game_result(team_score, opponent_score, status)
    top_hitters = summarize_top_hitters(feed, team.team_id)
    top_pitchers = summarize_top_pitchers(feed, team.team_id)
    scoring_clips = collect_scoring_clips(target_date, int(game["gamePk"]), feed)
    scoring_plays = collect_scoring_play_rows(feed, away_name=away_name, home_name=home_name, away_team_id=int(away_team.get("id") or 0), home_team_id=int(home_team.get("id") or 0))
    team_scoring_plays = [row for row in scoring_plays if row["batting_team_id"] == team.team_id]
    summary = (
        f"The {team.club_name} {result_word} on {target_date}: "
        f"{away_name} {away_score if away_score is not None else 0} at {home_name} {home_score if home_score is not None else 0} ({status})."
    )
    if team_scoring_plays:
        scoring_text = "; ".join(
            f"{ordinal(int(row['inning']))} {row['half_inning']}: {row['description']}"
            for row in team_scoring_plays[:4]
        )
        summary = f"{summary} Scoring plays for {team.club_name}: {scoring_text}."
    if top_hitters:
        summary = f"{summary} Best bats: {'; '.join(top_hitters)}."
    if top_pitchers:
        summary = f"{summary} Pitching line: {'; '.join(top_pitchers)}."
    return EvidenceSnippet(
        source="Live Game Research",
        title=f"{team.name} {target_date} game",
        citation="MLB Stats API schedule, live feed, and boxscore",
        summary=summary,
        payload={
            "analysis_type": "team_game_result",
            "mode": "live",
            "team": team.name,
            "target_date": target_date,
            "status": status,
            "opponent": opponent_name,
            "team_score": team_score,
            "opponent_score": opponent_score,
            "clips": scoring_clips,
            "scoring_plays": team_scoring_plays,
            "rows": [
                {
                    "team": away_name,
                    "runs": away_score if away_score is not None else 0,
                    "opponent": home_name,
                    "status": status,
                },
                {
                    "team": home_name,
                    "runs": home_score if home_score is not None else 0,
                    "opponent": away_name,
                    "status": status,
                },
            ],
        },
    )


def describe_team_game_result(team_score: int | None, opponent_score: int | None, status: str) -> str:
    if team_score is None or opponent_score is None:
        return f"played {status.lower()}" if status else "played"
    if "Final" not in status and "Game Over" not in status:
        if team_score > opponent_score:
            return "are leading"
        if team_score < opponent_score:
            return "are trailing"
        return "are tied"
    if team_score > opponent_score:
        return "won"
    if team_score < opponent_score:
        return "lost"
    return "tied"


def summarize_top_hitters(feed: dict[str, Any], team_id: int) -> list[str]:
    players = team_boxscore_players(feed, team_id)
    hitters: list[tuple[float, str]] = []
    for player in players:
        batting = player.get("stats", {}).get("batting", {})
        hits = safe_int(batting.get("hits")) or 0
        home_runs = safe_int(batting.get("homeRuns")) or 0
        rbi = safe_int(batting.get("rbi")) or 0
        at_bats = safe_int(batting.get("atBats")) or 0
        if not any((hits, home_runs, rbi, at_bats)):
            continue
        score = (home_runs * 6) + (hits * 2) + rbi
        name = str(player.get("person", {}).get("fullName") or "")
        line = f"{name} {hits}-{at_bats}"
        extras = []
        if home_runs:
            extras.append(f"{home_runs} HR")
        if rbi:
            extras.append(f"{rbi} RBI")
        if extras:
            line = f"{line}, {', '.join(extras)}"
        hitters.append((score, line))
    hitters.sort(key=lambda item: (-item[0], item[1]))
    return [line for _, line in hitters[:3]]


def summarize_top_pitchers(feed: dict[str, Any], team_id: int) -> list[str]:
    players = team_boxscore_players(feed, team_id)
    pitchers: list[tuple[float, str]] = []
    for player in players:
        pitching = player.get("stats", {}).get("pitching", {})
        outs = innings_text_to_outs(pitching.get("inningsPitched"))
        if not outs:
            continue
        earned_runs = safe_int(pitching.get("earnedRuns")) or 0
        strikeouts = safe_int(pitching.get("strikeOuts")) or 0
        name = str(player.get("person", {}).get("fullName") or "")
        innings_text = str(pitching.get("inningsPitched") or "0.0")
        era_score = (earned_runs * 9) - strikeouts
        line = f"{name} {innings_text} IP, {earned_runs} ER, {strikeouts} SO"
        pitchers.append((era_score, line))
    pitchers.sort(key=lambda item: (item[0], item[1]))
    return [line for _, line in pitchers[:2]]


def team_boxscore_players(feed: dict[str, Any], team_id: int) -> list[dict[str, Any]]:
    boxscore = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
    for side in ("away", "home"):
        side_payload = boxscore.get(side, {})
        if int(side_payload.get("team", {}).get("id") or 0) == team_id:
            return list((side_payload.get("players") or {}).values())
    return []


def collect_scoring_play_rows(
    feed: dict[str, Any],
    *,
    away_name: str,
    home_name: str,
    away_team_id: int,
    home_team_id: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
        runs_scored = sum(1 for runner in play.get("runners", []) if runner.get("details", {}).get("isScoringEvent"))
        if runs_scored <= 0:
            continue
        half_inning = str(play.get("about", {}).get("halfInning") or "")
        batting_team_id = away_team_id if half_inning == "top" else home_team_id
        batting_team_name = away_name if half_inning == "top" else home_name
        rows.append(
            {
                "inning": int(play.get("about", {}).get("inning") or 0),
                "half_inning": half_inning,
                "description": str(play.get("result", {}).get("description") or ""),
                "runs_scored": runs_scored,
                "batting_team_id": batting_team_id,
                "batting_team_name": batting_team_name,
                "batter_name": str(play.get("matchup", {}).get("batter", {}).get("fullName") or ""),
                "pitcher_name": str(play.get("matchup", {}).get("pitcher", {}).get("fullName") or ""),
            }
        )
    return rows


def innings_text_to_outs(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    if "." not in text:
        try:
            return int(float(text) * 3)
        except ValueError:
            return 0
    whole, fraction = text.split(".", 1)
    try:
        return (int(whole) * 3) + int(fraction[:1] or "0")
    except ValueError:
        return 0


def collect_scoring_clips(target_date: str, game_pk: int, feed: dict[str, Any]) -> list[dict[str, Any]]:
    clips: list[dict[str, Any]] = []
    for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
        if not is_scoring_or_highlight_play(play):
            continue
        play_id = extract_play_id(play)
        if not play_id:
            continue
        clips.append(build_clip_payload(target_date, game_pk, play, play_id))
        if len(clips) >= 4:
            break
    return clips


def is_scoring_or_highlight_play(play: dict[str, Any]) -> bool:
    runs_scored = sum(1 for runner in play.get("runners", []) if runner.get("details", {}).get("isScoringEvent"))
    event_type = str(play.get("result", {}).get("eventType") or "").lower()
    return runs_scored > 0 or event_type == "home_run"


def build_homer_play_card(
    target_date: str,
    game_pk: int,
    play: dict[str, Any],
    sporty_video_client: SportyVideoClient,
) -> dict[str, Any]:
    play_id = extract_play_id(play) or ""
    sporty_page = sporty_video_client.fetch(play_id) if play_id else None
    hit_data = extract_hit_data(play)
    return {
        "play_id": play_id,
        "game_pk": game_pk,
        "game_date": target_date,
        "title": sporty_page.title if sporty_page and sporty_page.title else str(play.get("result", {}).get("description") or ""),
        "description": str(play.get("result", {}).get("description") or ""),
        "inning": int(play.get("about", {}).get("inning") or 0),
        "half_inning": str(play.get("about", {}).get("halfInning") or ""),
        "team_matchup": sporty_page.matchup if sporty_page and sporty_page.matchup else "",
        "batter_name": sporty_page.batter if sporty_page and sporty_page.batter else str(play.get("matchup", {}).get("batter", {}).get("fullName") or ""),
        "pitcher_name": sporty_page.pitcher if sporty_page and sporty_page.pitcher else str(play.get("matchup", {}).get("pitcher", {}).get("fullName") or ""),
        "fielder_name": "",
        "actor_name": str(play.get("matchup", {}).get("batter", {}).get("fullName") or ""),
        "actor_roles": ["batter"],
        "match_tags": ["home run"],
        "savant_url": sporty_page.savant_url if sporty_page else None,
        "mp4_url": sporty_page.mp4_url if sporty_page else None,
        "hit_distance": sporty_page.hit_distance if sporty_page and sporty_page.hit_distance is not None else parse_float(hit_data.get("totalDistance")),
        "exit_velocity": sporty_page.exit_velocity if sporty_page and sporty_page.exit_velocity is not None else parse_float(hit_data.get("launchSpeed")),
        "launch_angle": sporty_page.launch_angle if sporty_page and sporty_page.launch_angle is not None else parse_float(hit_data.get("launchAngle")),
        "hr_parks": sporty_page.hr_parks if sporty_page else None,
        "explanation": "Relevant because it was a same-day home run by the requested player.",
    }


def build_clip_payload(target_date: str, game_pk: int, play: dict[str, Any], play_id: str) -> dict[str, Any]:
    hit_data = extract_hit_data(play)
    return {
        "play_id": play_id,
        "game_pk": game_pk,
        "game_date": target_date,
        "title": str(play.get("result", {}).get("description") or ""),
        "description": str(play.get("result", {}).get("description") or ""),
        "inning": int(play.get("about", {}).get("inning") or 0),
        "half_inning": str(play.get("about", {}).get("halfInning") or ""),
        "team_matchup": "",
        "batter_name": str(play.get("matchup", {}).get("batter", {}).get("fullName") or ""),
        "pitcher_name": str(play.get("matchup", {}).get("pitcher", {}).get("fullName") or ""),
        "fielder_name": "",
        "actor_name": str(play.get("matchup", {}).get("batter", {}).get("fullName") or ""),
        "actor_roles": ["batter"],
        "match_tags": ["scoring play"],
        "savant_url": f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}",
        "mp4_url": None,
        "hit_distance": parse_float(hit_data.get("totalDistance")),
        "exit_velocity": parse_float(hit_data.get("launchSpeed")),
        "launch_angle": parse_float(hit_data.get("launchAngle")),
        "hr_parks": None,
        "explanation": "Relevant because it was a decisive scoring play from the requested game.",
    }


def current_team_name_for_player(feed: dict[str, Any], player_id: int) -> str:
    boxscore = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
    for side in ("away", "home"):
        side_payload = boxscore.get(side, {})
        for player in (side_payload.get("players") or {}).values():
            if int(player.get("person", {}).get("id") or 0) == player_id:
                return str(side_payload.get("team", {}).get("name") or "").strip()
    return ""


def format_optional_distance(value: Any) -> str:
    converted = safe_float(value)
    return f"{int(round(converted))} ft" if converted is not None else ""
