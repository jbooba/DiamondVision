from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .query_utils import extract_name_candidates, extract_target_date, normalize_person_name, ordinal
from .sporty_video import SportyVideoClient
from .storage import get_connection, table_exists


VISUAL_HINTS = {"clip", "clips", "video", "videos", "replay", "replays", "tape", "highlight", "highlights", "watch"}
STAT_QUERY_HINTS = {
    "how many",
    "which player",
    "who has",
    "most",
    "least",
    "highest",
    "lowest",
    "leader",
    "leaders",
    "historically",
    "all time",
    "career",
}
FIELDER_DESCRIPTION_PATTERN = (
    r"(?:pitcher|catcher|first baseman|second baseman|third baseman|shortstop|left fielder|center fielder|right fielder)"
)
TAG_PATTERNS = {
    "home run": ("home run", "homer", "homers", "homerun", "homeruns", "go-ahead shot", "solo shot", "grand slam"),
    "robbery": ("rob ", "robbed", "robbery", "robs", "took away"),
    "defense": (
        "defense",
        "defensive",
        "fielding",
        "drs",
        "def ",
        "rarm",
        "rpm",
        "rhr",
        "arm",
        "assist",
        "double play",
    ),
    "catch": ("catch", "catches", "flies out", "lineout", "grab"),
    "strikeout": ("strikeout", "strike out", "struck out", "called out on strikes", "punchout"),
    "single": ("single", "singles"),
    "double": ("double", "doubles"),
    "triple": ("triple", "triples"),
    "walk": ("walk", "walks", "base on balls"),
    "stolen base": ("stolen base", "stole", "steal", "swipe"),
    "rbi": ("rbi", "runs batted in"),
}


@dataclass(slots=True)
class ReplayQuery:
    start_date: str
    end_date: str
    date_label: str
    player_queries: list[str] = field(default_factory=list)
    replay_tags: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SportyReplayClip:
    play_id: str
    game_pk: int
    game_date: str
    title: str
    description: str
    inning: int
    half_inning: str
    team_matchup: str
    batter_name: str
    pitcher_name: str
    fielder_name: str
    actor_name: str
    actor_roles: list[str]
    match_tags: list[str]
    relevance_score: int
    relevance_reason: str
    savant_url: str
    mp4_url: str | None
    exit_velocity: float | None
    launch_angle: float | None
    hit_distance: float | None
    hr_parks: int | None


@dataclass(slots=True)
class ReplayScore:
    score: int
    actor_name: str
    actor_roles: list[str]
    match_tags: list[str]
    reason: str


class SportyReplayFinder:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.sporty_video_client = SportyVideoClient(settings)

    def build_snippets(self, question: str) -> list[EvidenceSnippet]:
        replay_query = build_replay_query(question, self.settings.live_season or date.today().year)
        if replay_query is None:
            return []
        return self._build_snippets_for_query(replay_query)

    def build_recent_player_snippets(self, question: str) -> list[EvidenceSnippet]:
        replay_query = build_recent_player_replay_query(question, self.settings.live_season or date.today().year)
        if replay_query is None:
            return []
        return self._build_snippets_for_query(replay_query)

    def _build_snippets_for_query(self, replay_query: ReplayQuery) -> list[EvidenceSnippet]:
        clips = self.find_relevant_clips(replay_query)
        if not clips:
            return [self._empty_result_snippet(replay_query)]

        lines = []
        for index, clip in enumerate(clips[:4], start=1):
            detail_parts = []
            if clip.hr_parks is not None:
                detail_parts.append(f"{clip.hr_parks}/30 HR parks")
            if clip.hit_distance is not None:
                detail_parts.append(f"{int(round(float(clip.hit_distance)))} ft")
            details = f" ({', '.join(detail_parts)})" if detail_parts else ""
            actor = clip.actor_name or clip.batter_name or clip.fielder_name or clip.pitcher_name or "the player"
            lines.append(
                f"{index}. {clip.title or clip.description} [{actor}, {ordinal(clip.inning)} {clip.half_inning}{details}]"
            )

        focus = ", ".join(replay_query.player_queries) if replay_query.player_queries else "the requested theme"
        summary = (
            f"Found {len(clips)} public Baseball Savant replay clip(s) tied to {focus} "
            f"{format_replay_window_phrase(replay_query)}. "
            + " ".join(lines)
        )
        return [
            EvidenceSnippet(
                source="Sporty Replay",
                title=f"{replay_query.date_label} replay matches",
                citation="Baseball Savant sporty-videos pages matched against MLB Stats API game feeds",
                summary=summary,
                payload={
                    "start_date": replay_query.start_date,
                    "end_date": replay_query.end_date,
                    "date_label": replay_query.date_label,
                    "player_queries": replay_query.player_queries,
                    "replay_tags": replay_query.replay_tags,
                    "clip_count": len(clips),
                    "clips": [asdict(clip) for clip in clips],
                },
            )
        ]

    def find_relevant_clips(self, replay_query: ReplayQuery) -> list[SportyReplayClip]:
        players_by_id = self._resolve_players(replay_query.player_queries)
        player_ids = set(players_by_id)
        exact_names = {normalize_person_name(name) for name in replay_query.player_queries}
        exact_names.update(normalize_person_name(name) for name in players_by_id.values())
        if replay_query.player_queries and not exact_names:
            return []

        schedule = self._schedule_payload(replay_query, players_by_id)
        clips: list[SportyReplayClip] = []
        seen_play_ids: set[str] = set()
        for day in schedule.get("dates", []):
            game_date = str(day.get("date") or replay_query.end_date)
            for game in day.get("games", []):
                if game.get("status", {}).get("codedGameState") in {"S", "P"}:
                    continue
                game_pk = int(game["gamePk"])
                feed = self.live_client.game_feed(game_pk)
                for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
                    preflight_text = build_search_text(play, None)
                    rough_tags = sorted(extract_matching_tags(preflight_text, replay_query.replay_tags))
                    player_matches = player_ids and play_involves_player(play, player_ids)
                    if not player_matches and not rough_tags and not replay_query.player_queries:
                        continue
                    if not player_matches and replay_query.player_queries:
                        continue

                    play_id = extract_play_id(play)
                    if not play_id or play_id in seen_play_ids:
                        continue

                    sporty_page = self.sporty_video_client.fetch(play_id)
                    if sporty_page is None or not (sporty_page.title or sporty_page.mp4_url):
                        continue

                    score = score_clip_match(
                        play,
                        sporty_page,
                        requested_player_ids=player_ids,
                        requested_exact_names=exact_names,
                        requested_tags=replay_query.replay_tags,
                    )
                    if score.score <= 0:
                        continue

                    seen_play_ids.add(play_id)
                    clips.append(
                        SportyReplayClip(
                            play_id=play_id,
                            game_pk=game_pk,
                            game_date=game_date,
                            title=sporty_page.title or str(play.get("result", {}).get("description") or ""),
                            description=str(play.get("result", {}).get("description") or ""),
                            inning=int(play.get("about", {}).get("inning") or 0),
                            half_inning=str(play.get("about", {}).get("halfInning") or ""),
                            team_matchup=sporty_page.matchup,
                            batter_name=sporty_page.batter
                            or str(play.get("matchup", {}).get("batter", {}).get("fullName") or "").strip(),
                            pitcher_name=sporty_page.pitcher
                            or str(play.get("matchup", {}).get("pitcher", {}).get("fullName") or "").strip(),
                            fielder_name=find_primary_fielder_name(play),
                            actor_name=score.actor_name,
                            actor_roles=score.actor_roles,
                            match_tags=score.match_tags,
                            relevance_score=score.score,
                            relevance_reason=score.reason,
                            savant_url=sporty_page.savant_url,
                            mp4_url=sporty_page.mp4_url,
                            exit_velocity=sporty_page.exit_velocity,
                            launch_angle=sporty_page.launch_angle,
                            hit_distance=sporty_page.hit_distance,
                            hr_parks=sporty_page.hr_parks,
                        )
                    )
        clips.sort(
            key=lambda clip: (
                -clip.relevance_score,
                clip.game_date,
                clip.game_pk,
                clip.inning,
                clip.half_inning,
                clip.title,
            )
        )
        if clips:
            return clips[:6]
        return self._fallback_recent_home_run_clips(replay_query, players_by_id)[:6]

    def _schedule_payload(self, replay_query: ReplayQuery, players_by_id: dict[int, str]) -> dict[str, Any]:
        if replay_query.start_date == replay_query.end_date:
            return self.live_client.schedule(replay_query.start_date)
        team_id = self._resolve_current_team_id(players_by_id)
        if replay_query.player_queries and not team_id:
            return {"dates": []}
        return self.live_client.schedule_range(
            replay_query.start_date,
            replay_query.end_date,
            team_id=team_id,
        )

    def _resolve_players(self, player_queries: list[str]) -> dict[int, str]:
        players_by_id: dict[int, str] = {}
        for player_query in player_queries[:2]:
            for person in self.live_client.search_people(player_query):
                player_id = int(person.get("id") or 0)
                full_name = str(person.get("fullName") or "").strip()
                if player_id and full_name:
                    players_by_id.setdefault(player_id, full_name)
        return players_by_id

    def _resolve_current_team_id(self, players_by_id: dict[int, str]) -> int | None:
        for player_id in players_by_id:
            details = self.live_client.person_details(player_id)
            if not details:
                continue
            team = details.get("currentTeam") or {}
            team_id = int(team.get("id") or 0)
            if team_id:
                return team_id
        return None

    def _empty_result_snippet(self, replay_query: ReplayQuery) -> EvidenceSnippet:
        schedule = self._schedule_payload(replay_query, self._resolve_players(replay_query.player_queries))
        games = [game for day in schedule.get("dates", []) for game in day.get("games", [])]
        all_not_started = games and all(game.get("status", {}).get("codedGameState") in {"S", "P"} for game in games)
        focus = ", ".join(replay_query.player_queries) if replay_query.player_queries else "that query"
        summary = f"No public Baseball Savant replay pages matched {focus} {format_replay_window_phrase(replay_query)}."
        if all_not_started:
            if replay_query.start_date == replay_query.end_date:
                summary = f"{summary} MLB games for that date have not started yet."
            else:
                summary = f"{summary} MLB games in that recent search window have not started yet."
        return EvidenceSnippet(
            source="Sporty Replay",
            title=f"{replay_query.date_label} replay status",
            citation="Baseball Savant sporty-videos pages checked against MLB Stats API game feeds",
            summary=summary,
            payload={
                "start_date": replay_query.start_date,
                "end_date": replay_query.end_date,
                "date_label": replay_query.date_label,
                "player_queries": replay_query.player_queries,
                "replay_tags": replay_query.replay_tags,
                "clip_count": 0,
                "clips": [],
            },
        )

    def _fallback_recent_home_run_clips(
        self,
        replay_query: ReplayQuery,
        players_by_id: dict[int, str],
    ) -> list[SportyReplayClip]:
        if not self._is_simple_recent_home_run_request(replay_query, players_by_id):
            return []
        connection = get_connection(self.settings.database_path)
        try:
            if not table_exists(connection, "statcast_events"):
                return []
            candidate_rows = []
            for player_id in players_by_id:
                candidate_rows.extend(
                    connection.execute(
                        """
                        SELECT
                            game_pk,
                            game_date,
                            at_bat_number,
                            batter_id,
                            batter_name,
                            pitcher_name,
                            away_team || ' @ ' || home_team AS team_matchup,
                            launch_speed,
                            launch_angle,
                            hit_distance
                        FROM statcast_events
                        WHERE batter_id = ?
                          AND event = 'home_run'
                        ORDER BY season DESC, game_date DESC, game_pk DESC, at_bat_number DESC
                        LIMIT 12
                        """,
                        (player_id,),
                    ).fetchall()
                )
        finally:
            connection.close()

        for row in candidate_rows:
            game_pk = int(row["game_pk"] or 0)
            if not game_pk:
                continue
            feed = self.live_client.game_feed(game_pk)
            play = self._find_matching_home_run_play(feed, int(row["batter_id"] or 0), int(row["at_bat_number"] or 0))
            if play is None:
                continue
            play_id = extract_play_id(play)
            sporty_page = self.sporty_video_client.fetch(play_id) if play_id else None
            title = sporty_page.title if sporty_page and sporty_page.title else str(play.get("result", {}).get("description") or "")
            batter_name = (
                sporty_page.batter
                if sporty_page and sporty_page.batter
                else str(row["batter_name"] or play.get("matchup", {}).get("batter", {}).get("fullName") or "").strip()
            )
            pitcher_name = (
                sporty_page.pitcher
                if sporty_page and sporty_page.pitcher
                else str(row["pitcher_name"] or play.get("matchup", {}).get("pitcher", {}).get("fullName") or "").strip()
            )
            return [
                SportyReplayClip(
                    play_id=play_id or "",
                    game_pk=game_pk,
                    game_date=str(row["game_date"] or ""),
                    title=title,
                    description=str(play.get("result", {}).get("description") or title),
                    inning=int(play.get("about", {}).get("inning") or 0),
                    half_inning=str(play.get("about", {}).get("halfInning") or ""),
                    team_matchup=str(row["team_matchup"] or ""),
                    batter_name=batter_name,
                    pitcher_name=pitcher_name,
                    fielder_name=find_primary_fielder_name(play),
                    actor_name=batter_name,
                    actor_roles=["batter"],
                    match_tags=["home run"],
                    relevance_score=250,
                    relevance_reason="Recent Statcast home run fallback for a generic replay request.",
                    savant_url=sporty_page.savant_url if sporty_page else (f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}" if play_id else ""),
                    mp4_url=sporty_page.mp4_url if sporty_page else None,
                    exit_velocity=(sporty_page.exit_velocity if sporty_page and sporty_page.exit_velocity is not None else float(row["launch_speed"]) if row["launch_speed"] is not None else None),
                    launch_angle=(sporty_page.launch_angle if sporty_page and sporty_page.launch_angle is not None else float(row["launch_angle"]) if row["launch_angle"] is not None else None),
                    hit_distance=(sporty_page.hit_distance if sporty_page and sporty_page.hit_distance is not None else float(row["hit_distance"]) if row["hit_distance"] is not None else None),
                    hr_parks=sporty_page.hr_parks if sporty_page else None,
                )
            ]
        return []

    def _is_simple_recent_home_run_request(
        self,
        replay_query: ReplayQuery,
        players_by_id: dict[int, str],
    ) -> bool:
        if replay_query.start_date == replay_query.end_date:
            return False
        if len(replay_query.player_queries) != 1 or not players_by_id:
            return False
        replay_tags = set(replay_query.replay_tags)
        return bool(replay_tags) and replay_tags <= {"home run"}

    @staticmethod
    def _find_matching_home_run_play(feed: dict[str, Any], batter_id: int, at_bat_number: int) -> dict[str, Any] | None:
        for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
            if int(play.get("matchup", {}).get("batter", {}).get("id") or 0) != batter_id:
                continue
            if int(play.get("about", {}).get("atBatIndex") or -1) == at_bat_number - 1:
                event_type = str(play.get("result", {}).get("eventType") or "").lower()
                if event_type == "home_run":
                    return play
            if int(play.get("atBatIndex") or -1) == at_bat_number - 1:
                event_type = str(play.get("result", {}).get("eventType") or "").lower()
                if event_type == "home_run":
                    return play
        return None


def build_replay_query(question: str, default_year: int) -> ReplayQuery | None:
    target_date = extract_target_date(question, default_year)
    player_queries = extract_name_candidates(question)
    replay_tags = sorted(extract_replay_tags(question))
    has_visual_intent = contains_visual_hint(question)
    if not has_visual_intent and looks_like_statistical_query(question):
        return None
    if target_date is None and not player_queries:
        return None
    if target_date is None and not has_visual_intent and not replay_tags:
        return None
    if not has_visual_intent and not player_queries:
        return None
    if not player_queries and not replay_tags and not has_visual_intent:
        return None
    if target_date is None:
        end_date = resolve_recent_replay_end_date(default_year)
        start_date = end_date - timedelta(days=120)
        return ReplayQuery(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            date_label=f"{start_date.isoformat()} through {end_date.isoformat()}",
            player_queries=player_queries[:2],
            replay_tags=replay_tags,
        )
    return ReplayQuery(
        start_date=target_date.isoformat(),
        end_date=target_date.isoformat(),
        date_label=target_date.isoformat(),
        player_queries=player_queries[:2],
        replay_tags=replay_tags,
    )


def build_recent_player_replay_query(question: str, default_year: int) -> ReplayQuery | None:
    player_queries = extract_name_candidates(question)
    if not player_queries:
        return None
    end_date = resolve_recent_replay_end_date(default_year)
    start_date = end_date - timedelta(days=120)
    return ReplayQuery(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        date_label=f"{start_date.isoformat()} through {end_date.isoformat()}",
        player_queries=player_queries[:2],
        replay_tags=[],
    )


def wants_sporty_replay(question: str, default_year: int) -> bool:
    return build_replay_query(question, default_year) is not None


def resolve_recent_replay_end_date(default_year: int) -> date:
    today = date.today()
    if today.year == default_year:
        return today
    if today.year > default_year:
        return date(default_year, 12, 31)
    return date(default_year, 4, 1)


def format_replay_window_phrase(replay_query: ReplayQuery) -> str:
    if replay_query.start_date == replay_query.end_date:
        return f"on {replay_query.start_date}"
    return f"from {replay_query.start_date} through {replay_query.end_date}"


def extract_replay_tags(question: str) -> set[str]:
    lowered = question.lower()
    tags = {tag for tag, patterns in TAG_PATTERNS.items() if any(pattern in lowered for pattern in patterns)}
    if "field" in lowered or "outfield" in lowered or "infield" in lowered:
        tags.add("defense")
    return tags


def contains_visual_hint(question: str) -> bool:
    lowered = question.lower()
    return any(hint in lowered for hint in VISUAL_HINTS)


def looks_like_statistical_query(question: str) -> bool:
    lowered = question.lower()
    return any(hint in lowered for hint in STAT_QUERY_HINTS)


def extract_play_id(play: dict[str, Any]) -> str | None:
    seen_ids: set[str] = set()
    for event in reversed(play.get("playEvents", [])):
        play_id = str(event.get("playId") or "").strip()
        if not play_id or play_id in seen_ids:
            continue
        seen_ids.add(play_id)
        return play_id
    return None


def build_search_text(play: dict[str, Any], sporty_page: Any | None) -> str:
    fragments = [
        str(play.get("result", {}).get("description") or ""),
        str(play.get("result", {}).get("event") or ""),
        str(play.get("result", {}).get("eventType") or ""),
        str(play.get("matchup", {}).get("batter", {}).get("fullName") or ""),
        str(play.get("matchup", {}).get("pitcher", {}).get("fullName") or ""),
    ]
    if sporty_page is not None:
        fragments.extend(
            [
                str(getattr(sporty_page, "title", "") or ""),
                str(getattr(sporty_page, "batter", "") or ""),
                str(getattr(sporty_page, "pitcher", "") or ""),
            ]
        )
    fielder_name = find_primary_fielder_name(play)
    if fielder_name:
        fragments.append(fielder_name)
    return " ".join(fragment for fragment in fragments if fragment).lower()


def extract_matching_tags(search_text: str, requested_tags: list[str]) -> set[str]:
    if not requested_tags:
        return set()
    matched: set[str] = set()
    for tag in requested_tags:
        patterns = TAG_PATTERNS.get(tag, (tag,))
        if any(pattern in search_text for pattern in patterns):
            matched.add(tag)
    return matched


def play_involves_player(play: dict[str, Any], player_ids: set[int]) -> bool:
    return bool(find_player_roles(play, player_ids)[1])


def find_player_roles(play: dict[str, Any], player_ids: set[int]) -> tuple[str, list[str]]:
    matchup = play.get("matchup", {})
    batter = matchup.get("batter", {})
    pitcher = matchup.get("pitcher", {})
    if int(batter.get("id") or 0) in player_ids:
        return str(batter.get("fullName") or ""), ["batter"]
    if int(pitcher.get("id") or 0) in player_ids:
        return str(pitcher.get("fullName") or ""), ["pitcher"]

    role_order = [
        ("f_putout", "fielder"),
        ("f_assist", "fielder"),
        ("f_assist_of", "fielder"),
        ("f_fielded_ball", "fielder"),
        ("f_deflection", "fielder"),
        ("f_throwing_error", "fielder"),
    ]
    roles: list[str] = []
    actor_name = ""
    seen_roles: set[str] = set()
    for runner in play.get("runners", []):
        details = runner.get("details", {})
        runner_person = details.get("runner", {})
        if int(runner_person.get("id") or 0) in player_ids:
            actor_name = str(runner_person.get("fullName") or actor_name)
            if "runner" not in seen_roles:
                roles.append("runner")
                seen_roles.add("runner")
        for credit in runner.get("credits", []):
            player = credit.get("player", {})
            if int(player.get("id") or 0) not in player_ids:
                continue
            actor_name = str(player.get("fullName") or actor_name)
            credit_type = str(credit.get("credit") or "")
            role_label = next((label for prefix, label in role_order if credit_type == prefix), "fielder")
            if role_label not in seen_roles:
                roles.append(role_label)
                seen_roles.add(role_label)
    if roles and not actor_name:
        actor_name = find_primary_fielder_name(play)
    return actor_name, roles


def find_primary_fielder_name(play: dict[str, Any]) -> str:
    for runner in play.get("runners", []):
        for credit in runner.get("credits", []):
            credit_type = str(credit.get("credit") or "")
            if not credit_type.startswith("f_"):
                continue
            player_id = int(credit.get("player", {}).get("id") or 0)
            if not player_id:
                continue
            person = credit.get("player", {})
            if person.get("fullName"):
                return str(person["fullName"])
    description = str(play.get("result", {}).get("description") or "")
    pattern = rf"\b(?:{FIELDER_DESCRIPTION_PATTERN})\s+(.+?)(?=\.|,\s| to | scores| out at|$)"
    match = re.search(pattern, description, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""


def score_clip_match(
    play: dict[str, Any],
    sporty_page: Any,
    *,
    requested_player_ids: set[int],
    requested_exact_names: set[str],
    requested_tags: list[str],
) -> ReplayScore:
    actor_name, actor_roles = find_player_roles(play, requested_player_ids)
    normalized_title = normalize_person_name(str(getattr(sporty_page, "title", "") or ""))
    normalized_description = normalize_person_name(str(play.get("result", {}).get("description") or ""))
    if requested_player_ids and not actor_roles:
        search_text = build_search_text(play, sporty_page)
        if not any(name in search_text for name in requested_exact_names):
            return ReplayScore(score=0, actor_name="", actor_roles=[], match_tags=[], reason="")
        actor_name = actor_name or guess_actor_name(play, sporty_page)

    search_text = build_search_text(play, sporty_page)
    matched_tags = sorted(extract_matching_tags(search_text, requested_tags))
    if "defense" in requested_tags and "fielder" in actor_roles and "defense" not in matched_tags:
        matched_tags.append("defense")
    if "defense" in requested_tags and actor_roles and "fielder" not in actor_roles and not matched_tags:
        return ReplayScore(score=0, actor_name="", actor_roles=[], match_tags=[], reason="")
    if requested_tags and not matched_tags and not actor_roles:
        return ReplayScore(score=0, actor_name="", actor_roles=[], match_tags=[], reason="")

    score = 0
    reasons: list[str] = []
    if actor_roles:
        score += 60
        role_text = "/".join(actor_roles)
        reasons.append(f"it directly involves {actor_name or 'the requested player'} as the {role_text}")
    if matched_tags:
        score += 14 * len(matched_tags)
        reasons.append(f"it matches the question's {', '.join(matched_tags)} angle")
    if requested_exact_names and any(name in normalized_title for name in requested_exact_names):
        score += 24
        reasons.append("the clip title itself centers the requested player")
    if normalized_title and normalized_title != normalized_description:
        score += 12
        reasons.append("Savant elevated it as a named highlight rather than a plain play log line")
    if getattr(sporty_page, "mp4_url", None):
        score += 5
    bonus, bonus_reason = highlight_importance_bonus(play, sporty_page)
    if bonus != 0:
        score += bonus
        if bonus_reason:
            reasons.append(bonus_reason)
    if not requested_tags and not actor_roles:
        return ReplayScore(score=0, actor_name="", actor_roles=[], match_tags=[], reason="")

    actor_name = actor_name or guess_actor_name(play, sporty_page)
    if not reasons:
        reasons.append("it lined up with the requested replay search")
    reason = f"Relevant because {' and '.join(reasons)}."
    return ReplayScore(
        score=score,
        actor_name=actor_name,
        actor_roles=actor_roles,
        match_tags=matched_tags,
        reason=reason,
    )


def highlight_importance_bonus(play: dict[str, Any], sporty_page: Any) -> tuple[int, str]:
    event_type = str(play.get("result", {}).get("eventType") or "").lower()
    if getattr(sporty_page, "is_home_run_robbery", False):
        return 56, "it was a true home run robbery highlight"
    if event_type == "home_run":
        return 48, "it was a home run rather than a routine plate appearance"
    if event_type == "triple":
        return 28, "it was a triple"
    if event_type == "double":
        return 18, "it was an extra-base hit"
    if event_type == "double_play":
        return 22, "it created a double play"
    if event_type in {"hit_by_pitch", "walk", "intent_walk"}:
        return -26, "it was a routine plate appearance, so it ranks below actual highlights"
    if event_type in {"groundout", "field_out", "single"}:
        return -14, "it was a routine result, so it ranks below bigger moments"
    return 8, "it was more notable than a routine result"


def guess_actor_name(play: dict[str, Any], sporty_page: Any) -> str:
    batter_name = str(getattr(sporty_page, "batter", "") or play.get("matchup", {}).get("batter", {}).get("fullName") or "")
    fielder_name = find_primary_fielder_name(play)
    pitcher_name = str(getattr(sporty_page, "pitcher", "") or play.get("matchup", {}).get("pitcher", {}).get("fullName") or "")
    for candidate in (fielder_name, batter_name, pitcher_name):
        if candidate:
            return candidate
    return ""
