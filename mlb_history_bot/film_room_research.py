from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .query_utils import DateWindow, extract_date_window, extract_name_candidates, normalize_person_name, ordinal
from .sporty_research import extract_play_id, find_player_roles, find_primary_fielder_name, play_involves_player
from .sporty_video import SportyVideoClient, SportyVideoPage


COOLEST_HINTS = {"cool", "coolest", "best", "sick", "awesome", "wild", "crazy", "nastiest"}
WEIRD_HINTS = {"weird", "weirdest", "unusual", "strange", "odd", "bizarre"}
HOME_RUN_DISTANCE_HINTS = {"farthest", "furthest", "longest"}
DEFENSIVE_HINTS = {"defense", "defensive", "fielding", "glove", "robbery", "robberies", "catch", "catches", "arm", "assist"}
OUTFIELD_POSITIONS = {"LF", "CF", "RF"}
WEIRD_KEYWORDS = {
    "reviewed": 18,
    "interference": 60,
    "obstruction": 60,
    "balk": 44,
    "wild pitch": 26,
    "passed ball": 26,
    "throwing error": 36,
    "fan interference": 75,
    "deflected": 16,
    "force out": 18,
    "triple play": 95,
    "inside-the-park": 90,
    "appeal": 30,
}


@dataclass(slots=True)
class StoryQuery:
    kind: str
    date_window: DateWindow
    player_queries: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PlayCandidate:
    game_date: str
    game_pk: int
    team_matchup: str
    play: dict[str, Any]
    base_score: float
    reasons: list[str]
    play_id: str | None


@dataclass(slots=True)
class StoryPlay:
    play_id: str
    game_pk: int
    game_date: str
    team_matchup: str
    title: str
    description: str
    explanation: str
    batter_name: str
    pitcher_name: str
    fielder_name: str
    actor_name: str
    actor_roles: list[str]
    match_tags: list[str]
    story_score: float
    inning: int
    half_inning: str
    event_type: str
    hit_distance: float | None
    exit_velocity: float | None
    launch_angle: float | None
    hr_parks: int | None
    savant_url: str | None
    mp4_url: str | None


class FilmRoomResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.sporty_video_client = SportyVideoClient(settings)

    def build_snippets(self, question: str) -> list[EvidenceSnippet]:
        story_query = parse_story_query(question, self.settings.live_season or date.today().year)
        if story_query is None:
            return []

        if story_query.kind == "home_run_distance":
            return self._home_run_distance_snippets(story_query)
        if story_query.kind == "defensive_performance":
            return self._defensive_performance_snippets(story_query)
        if story_query.kind == "defensive_plays":
            return self._roundup_snippets(story_query, label="best defensive", score_fn=score_defensive_play)
        if story_query.kind == "coolest_plays":
            return self._roundup_snippets(story_query, label="coolest", score_fn=score_cool_play)
        if story_query.kind == "weird_plays":
            return self._roundup_snippets(story_query, label="weirdest", score_fn=score_weird_play)
        return []

    def _home_run_distance_snippets(self, story_query: StoryQuery) -> list[EvidenceSnippet]:
        player_ids = self._resolve_player_ids(story_query.player_queries)
        candidates: list[PlayCandidate] = []
        for game_date, game_pk, feed in self._window_games(story_query.date_window):
            team_matchup = build_team_matchup(feed)
            for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
                event_type = str(play.get("result", {}).get("eventType") or "").lower()
                if event_type != "home_run":
                    continue
                if player_ids and not play_involves_player(play, player_ids):
                    continue
                hit_data = extract_hit_data(play)
                distance = parse_float(hit_data.get("totalDistance")) if hit_data else None
                if distance is None:
                    continue
                reasons = [f"{int(round(distance))} ft"]
                exit_velocity = parse_float(hit_data.get("launchSpeed")) if hit_data else None
                if exit_velocity is not None:
                    reasons.append(f"{exit_velocity:.1f} mph EV")
                candidates.append(
                    PlayCandidate(
                        game_date=game_date,
                        game_pk=game_pk,
                        team_matchup=team_matchup,
                        play=play,
                        base_score=distance,
                        reasons=reasons,
                        play_id=extract_play_id(play),
                    )
                )

        candidates.sort(key=lambda candidate: (-candidate.base_score, candidate.game_date, candidate.game_pk))
        if not candidates:
            return [
                EvidenceSnippet(
                    source="Film Room Research",
                    title=f"{story_query.date_window.label} home run distance status",
                    citation="MLB Stats API game feeds",
                    summary=f"No tracked home runs were found from {format_window(story_query.date_window)}.",
                    payload={"clips": [], "plays": []},
                )
            ]

        top_candidates = candidates[:5]
        story_plays = [self._build_home_run_story_play(index + 1, candidate) for index, candidate in enumerate(top_candidates)]
        winner = story_plays[0]
        others = "; ".join(
            f"{index}. {play.actor_name} {int(round(play.hit_distance or 0))} ft"
            for index, play in enumerate(story_plays[1:5], start=2)
        )
        summary = (
            f"On {story_query.date_window.start_date.isoformat()}, {winner.actor_name} hit the farthest home run in MLB "
            f"at {int(round(winner.hit_distance or 0))} feet for {winner.team_matchup} in the {ordinal(winner.inning)} {winner.half_inning}."
        )
        if others:
            summary = f"{summary} Next longest: {others}."
        summary = f"{summary} I found {len(candidates)} tracked home run(s) in that window."
        return [
            EvidenceSnippet(
                source="Film Room Research",
                title=f"{story_query.date_window.start_date.isoformat()} farthest home runs",
                citation="MLB Stats API game feeds plus Baseball Savant sporty-videos when available",
                summary=summary,
                payload={
                    "analysis_type": "home_run_distance",
                    "date_window": serialize_window(story_query.date_window),
                    "clip_count": len([play for play in story_plays if play.savant_url or play.mp4_url]),
                    "plays": [asdict(play) for play in story_plays],
                    "clips": [asdict(play) for play in story_plays if play.savant_url or play.mp4_url],
                },
            )
        ]

    def _roundup_snippets(
        self,
        story_query: StoryQuery,
        *,
        label: str,
        score_fn,
    ) -> list[EvidenceSnippet]:
        player_ids = self._resolve_player_ids(story_query.player_queries)
        candidates: list[PlayCandidate] = []
        for game_date, game_pk, feed in self._window_games(story_query.date_window):
            team_matchup = build_team_matchup(feed)
            for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
                if player_ids and not play_involves_player(play, player_ids):
                    continue
                if player_ids and story_query.kind == "defensive_plays":
                    matched_actor_name, matched_roles = find_player_roles(play, player_ids)
                    if "fielder" not in matched_roles:
                        continue
                score, reasons = score_fn(play)
                if score <= 0:
                    continue
                play_id = extract_play_id(play)
                if story_query.kind == "defensive_plays" and play_id:
                    sporty_page = self.sporty_video_client.fetch(play_id)
                    if sporty_page and sporty_page.is_home_run_robbery:
                        score += 140
                        reasons = [*reasons, "Statcast/Savant labeled it as a home run robbery"]
                candidates.append(
                    PlayCandidate(
                        game_date=game_date,
                        game_pk=game_pk,
                        team_matchup=team_matchup,
                        play=play,
                        base_score=score,
                        reasons=reasons,
                        play_id=play_id,
                    )
                )

        candidates.sort(key=lambda candidate: (-candidate.base_score, candidate.game_date, candidate.game_pk))
        if not candidates:
            return [
                EvidenceSnippet(
                    source="Film Room Research",
                    title=f"{story_query.date_window.label} {label} play status",
                    citation="MLB Stats API game feeds",
                    summary=f"I did not find any standout {label} plays from {format_window(story_query.date_window)}.",
                    payload={"clips": [], "plays": []},
                )
            ]

        top_candidates = candidates[:24]
        player_name_override = story_query.player_queries[0] if len(story_query.player_queries) == 1 else ""
        story_plays = [
            self._build_roundup_story_play(
                story_query.kind,
                candidate,
                player_ids=player_ids,
                player_name_override=player_name_override,
            )
            for candidate in top_candidates
        ]
        story_plays.sort(key=lambda play: (-play.story_score, play.game_date, play.game_pk, play.inning))
        top_story_plays = story_plays[:6]
        clips = [play for play in top_story_plays if play.savant_url or play.mp4_url]
        summary_lines = []
        for index, play in enumerate(top_story_plays[:5], start=1):
            summary_lines.append(
                f"{index}. {play.title} on {play.game_date} ({play.team_matchup}) in the {ordinal(play.inning)} {play.half_inning}: {play.explanation}"
            )
        focus_text = f" for {', '.join(story_query.player_queries)}" if story_query.player_queries else ""
        prefix = (
            f"Here are the {label} plays I found{focus_text} from {format_window(story_query.date_window)}."
            if story_query.date_window.start_date != story_query.date_window.end_date
            else f"Here are the {label} plays I found{focus_text} on {story_query.date_window.start_date.isoformat()}."
        )
        summary = f"{prefix} {' '.join(summary_lines)}"
        return [
            EvidenceSnippet(
                source="Film Room Research",
                title=f"{format_window(story_query.date_window)} {label} plays",
                citation="MLB Stats API game feeds plus Baseball Savant sporty-videos when available",
                summary=summary,
                payload={
                    "analysis_type": story_query.kind,
                    "date_window": serialize_window(story_query.date_window),
                    "clip_count": len(clips),
                    "plays": [asdict(play) for play in top_story_plays],
                    "clips": [asdict(play) for play in clips],
                },
            )
        ]

    def _defensive_performance_snippets(self, story_query: StoryQuery) -> list[EvidenceSnippet]:
        player_ids = self._resolve_player_ids(story_query.player_queries)
        performers: dict[str, dict[str, Any]] = {}
        for game_date, game_pk, feed in self._window_games(story_query.date_window):
            team_matchup = build_team_matchup(feed)
            for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
                if player_ids and not play_involves_player(play, player_ids):
                    continue
                if player_ids:
                    matched_actor_name, matched_roles = find_player_roles(play, player_ids)
                    if "fielder" not in matched_roles:
                        continue
                score, reasons = score_defensive_play(play)
                if score <= 0:
                    continue
                play_id = extract_play_id(play)
                if play_id:
                    sporty_page = self.sporty_video_client.fetch(play_id)
                    if sporty_page and sporty_page.is_home_run_robbery:
                        score += 140
                        reasons = [*reasons, "Statcast/Savant labeled it as a home run robbery"]
                candidate = PlayCandidate(
                    game_date=game_date,
                    game_pk=game_pk,
                    team_matchup=team_matchup,
                    play=play,
                    base_score=score,
                    reasons=reasons,
                    play_id=play_id,
                )
                story_play = self._build_roundup_story_play(
                    "defensive_plays",
                    candidate,
                    player_ids=player_ids,
                    player_name_override="",
                )
                performer_name = story_play.actor_name or story_play.fielder_name
                if not performer_name or "fielder" not in story_play.actor_roles:
                    continue
                entry = performers.setdefault(
                    performer_name,
                    {"score": 0.0, "plays": []},
                )
                entry["score"] += story_play.story_score
                entry["plays"].append(story_play)

        if not performers:
            return [
                EvidenceSnippet(
                    source="Film Room Research",
                    title=f"{story_query.date_window.label} defensive performance status",
                    citation="MLB Stats API game feeds",
                    summary=f"I did not find a clear defensive standout from {format_window(story_query.date_window)}.",
                    payload={"clips": [], "plays": []},
                )
            ]

        ranked = sorted(
            performers.items(),
            key=lambda item: (
                -item[1]["score"],
                -max(play.story_score for play in item[1]["plays"]),
                item[0],
            ),
        )
        winner_name, winner_entry = ranked[0]
        winner_plays = sorted(
            winner_entry["plays"],
            key=lambda play: (-play.story_score, play.game_date, play.game_pk, play.inning),
        )
        clips = [play for play in winner_plays if play.savant_url or play.mp4_url][:6]
        summary_lines = [
            f"{index}. {play.title} in the {ordinal(play.inning)} {play.half_inning} on {play.game_date} ({play.team_matchup})"
            for index, play in enumerate(winner_plays[:4], start=1)
        ]
        summary = (
            f"{winner_name} had the strongest defensive performance from {format_window(story_query.date_window)} "
            f"with {len(winner_plays)} standout play(s) and a replay-score total of {winner_entry['score']:.1f}. "
            f"Top plays: {' '.join(summary_lines)}"
        )
        return [
            EvidenceSnippet(
                source="Film Room Research",
                title=f"{format_window(story_query.date_window)} best defensive performer",
                citation="MLB Stats API game feeds plus Baseball Savant sporty-videos when available",
                summary=summary,
                payload={
                    "analysis_type": "defensive_performance",
                    "date_window": serialize_window(story_query.date_window),
                    "performer": winner_name,
                    "performer_score": round(float(winner_entry["score"]), 1),
                    "rows": [
                        {
                            "player": name,
                            "play_count": len(entry["plays"]),
                            "score": round(float(entry["score"]), 1),
                        }
                        for name, entry in ranked[:8]
                    ],
                    "plays": [asdict(play) for play in winner_plays[:6]],
                    "clips": [asdict(play) for play in clips],
                },
            )
        ]

    def _build_home_run_story_play(self, rank: int, candidate: PlayCandidate) -> StoryPlay:
        play = candidate.play
        sporty_page = self.sporty_video_client.fetch(candidate.play_id) if candidate.play_id else None
        hit_data = extract_hit_data(play)
        distance = parse_float(hit_data.get("totalDistance")) if hit_data else None
        exit_velocity = parse_float(hit_data.get("launchSpeed")) if hit_data else None
        launch_angle = parse_float(hit_data.get("launchAngle")) if hit_data else None
        batter_name = str(play.get("matchup", {}).get("batter", {}).get("fullName") or "").strip()
        title = sporty_page.title if sporty_page and sporty_page.title else str(play.get("result", {}).get("description") or "")
        hr_parks = sporty_page.hr_parks if sporty_page else None
        distance_text = f"{int(round(distance))} ft" if distance is not None else "an untracked distance"
        if rank == 1:
            explanation = f"This was the farthest home run in MLB that day at {distance_text}."
        else:
            explanation = f"This was the No. {rank} farthest home run in MLB that day at {distance_text}."
        if hr_parks is not None:
            explanation = f"{explanation} Statcast graded it as a homer in {hr_parks}/30 parks."
        return StoryPlay(
            play_id=candidate.play_id or "",
            game_pk=candidate.game_pk,
            game_date=candidate.game_date,
            team_matchup=sporty_page.matchup if sporty_page and sporty_page.matchup else candidate.team_matchup,
            title=title,
            description=str(play.get("result", {}).get("description") or ""),
            explanation=explanation,
            batter_name=sporty_page.batter if sporty_page and sporty_page.batter else batter_name,
            pitcher_name=sporty_page.pitcher if sporty_page and sporty_page.pitcher else str(play.get("matchup", {}).get("pitcher", {}).get("fullName") or "").strip(),
            fielder_name=find_primary_fielder_name(play),
            actor_name=batter_name,
            actor_roles=["batter"],
            match_tags=["home run"],
            story_score=candidate.base_score,
            inning=int(play.get("about", {}).get("inning") or 0),
            half_inning=str(play.get("about", {}).get("halfInning") or ""),
            event_type=str(play.get("result", {}).get("eventType") or ""),
            hit_distance=sporty_page.hit_distance if sporty_page and sporty_page.hit_distance is not None else distance,
            exit_velocity=sporty_page.exit_velocity if sporty_page and sporty_page.exit_velocity is not None else exit_velocity,
            launch_angle=sporty_page.launch_angle if sporty_page and sporty_page.launch_angle is not None else launch_angle,
            hr_parks=hr_parks,
            savant_url=sporty_page.savant_url if sporty_page else None,
            mp4_url=sporty_page.mp4_url if sporty_page else None,
        )

    def _build_roundup_story_play(
        self,
        kind: str,
        candidate: PlayCandidate,
        *,
        player_ids: set[int] | None = None,
        player_name_override: str = "",
    ) -> StoryPlay:
        play = candidate.play
        sporty_page = self.sporty_video_client.fetch(candidate.play_id) if candidate.play_id else None
        hit_data = extract_hit_data(play)
        distance = parse_float(hit_data.get("totalDistance")) if hit_data else None
        exit_velocity = parse_float(hit_data.get("launchSpeed")) if hit_data else None
        launch_angle = parse_float(hit_data.get("launchAngle")) if hit_data else None
        title = sporty_page.title if sporty_page and sporty_page.title else str(play.get("result", {}).get("description") or "")
        actor_name, actor_roles = derive_primary_actor(play, sporty_page)
        if player_ids:
            matched_actor_name, matched_roles = find_player_roles(play, player_ids)
            if matched_roles:
                if (
                    player_name_override
                    and normalize_person_name(matched_actor_name)
                    and normalize_person_name(matched_actor_name) != normalize_person_name(player_name_override)
                ):
                    actor_name = player_name_override
                else:
                    actor_name = matched_actor_name or player_name_override or actor_name
                actor_roles = matched_roles
        story_score = candidate.base_score
        reasons = list(candidate.reasons)
        if sporty_page and sporty_page.mp4_url:
            story_score += 8
            reasons.append("a public Savant clip is available")
        if sporty_page and sporty_page.title and sporty_page.title != str(play.get("result", {}).get("description") or ""):
            story_score += 10
            reasons.append("Savant promoted it as a named highlight")
        if sporty_page and sporty_page.is_home_run_robbery:
            story_score += 22 if kind == "coolest_plays" else 10
            reasons.append("it was a home run robbery")
        explanation = f"Relevant because {'; '.join(reasons[:3])}."
        event_type = str(play.get("result", {}).get("eventType") or "")
        if kind == "defensive_plays":
            tags = ["defense"]
        else:
            tags = ["highlight" if kind == "coolest_plays" else "unusual"]
        if sporty_page and sporty_page.is_home_run_robbery:
            tags.append("robbery")
        return StoryPlay(
            play_id=candidate.play_id or "",
            game_pk=candidate.game_pk,
            game_date=candidate.game_date,
            team_matchup=sporty_page.matchup if sporty_page and sporty_page.matchup else candidate.team_matchup,
            title=title,
            description=str(play.get("result", {}).get("description") or ""),
            explanation=explanation,
            batter_name=sporty_page.batter if sporty_page and sporty_page.batter else str(play.get("matchup", {}).get("batter", {}).get("fullName") or "").strip(),
            pitcher_name=sporty_page.pitcher if sporty_page and sporty_page.pitcher else str(play.get("matchup", {}).get("pitcher", {}).get("fullName") or "").strip(),
            fielder_name=find_primary_fielder_name(play),
            actor_name=actor_name,
            actor_roles=actor_roles,
            match_tags=tags,
            story_score=story_score,
            inning=int(play.get("about", {}).get("inning") or 0),
            half_inning=str(play.get("about", {}).get("halfInning") or ""),
            event_type=event_type,
            hit_distance=sporty_page.hit_distance if sporty_page and sporty_page.hit_distance is not None else distance,
            exit_velocity=sporty_page.exit_velocity if sporty_page and sporty_page.exit_velocity is not None else exit_velocity,
            launch_angle=sporty_page.launch_angle if sporty_page and sporty_page.launch_angle is not None else launch_angle,
            hr_parks=sporty_page.hr_parks if sporty_page else None,
            savant_url=sporty_page.savant_url if sporty_page else None,
            mp4_url=sporty_page.mp4_url if sporty_page else None,
        )

    def _resolve_player_ids(self, player_queries: list[str]) -> set[int]:
        player_ids: set[int] = set()
        for player_query in player_queries[:2]:
            for person in self.live_client.search_people(player_query):
                person_id = int(person.get("id") or 0)
                if person_id:
                    player_ids.add(person_id)
        return player_ids

    def _window_games(self, window: DateWindow):
        current = window.start_date
        while current <= window.end_date:
            schedule = self.live_client.schedule(current.isoformat())
            for day in schedule.get("dates", []):
                for game in day.get("games", []):
                    if game.get("status", {}).get("codedGameState") in {"S", "P"}:
                        continue
                    game_pk = int(game["gamePk"])
                    yield current.isoformat(), game_pk, self.live_client.game_feed(game_pk)
            current += timedelta(days=1)


def parse_story_query(question: str, default_year: int) -> StoryQuery | None:
    date_window = extract_date_window(question, default_year)
    if date_window is None:
        return None

    lowered = question.lower()
    player_queries = extract_story_player_queries(question)[:2]
    if any(hint in lowered for hint in HOME_RUN_DISTANCE_HINTS) and any(term in lowered for term in {"home run", "homer"}):
        return StoryQuery(kind="home_run_distance", date_window=date_window, player_queries=player_queries)

    highlight_shape = any(hint in lowered for hint in COOLEST_HINTS) and (
        "play" in lowered or "happen" in lowered or "anything" in lowered
    )
    weird_shape = any(hint in lowered for hint in WEIRD_HINTS) and (
        "play" in lowered or "happen" in lowered or "anything" in lowered
    )
    defensive_performance_shape = any(hint in lowered for hint in DEFENSIVE_HINTS) and (
        "performance" in lowered or ("who had" in lowered and "defensive" in lowered)
    )
    defensive_shape = any(hint in lowered for hint in DEFENSIVE_HINTS) and (
        "play" in lowered or "plays" in lowered or "happen" in lowered or "anything" in lowered or "performance" in lowered
    )
    if weird_shape:
        return StoryQuery(kind="weird_plays", date_window=date_window, player_queries=player_queries)
    if defensive_performance_shape:
        return StoryQuery(kind="defensive_performance", date_window=date_window, player_queries=player_queries)
    if defensive_shape:
        return StoryQuery(kind="defensive_plays", date_window=date_window, player_queries=player_queries)
    if highlight_shape:
        return StoryQuery(kind="coolest_plays", date_window=date_window, player_queries=player_queries)
    return None


def score_cool_play(play: dict[str, Any]) -> tuple[float, list[str]]:
    event_type = str(play.get("result", {}).get("eventType") or "").lower()
    description = str(play.get("result", {}).get("description") or "")
    score = 0.0
    reasons: list[str] = []
    hit_data = extract_hit_data(play)
    distance = parse_float(hit_data.get("totalDistance")) if hit_data else None
    exit_velocity = parse_float(hit_data.get("launchSpeed")) if hit_data else None
    runs_scored = count_runs_scored(play)

    if event_type == "home_run":
        score += 75
        reasons.append("it was a home run")
        if distance is not None:
            score += min(distance / 7.0, 70)
            if distance >= 400:
                reasons.append(f"it traveled {int(round(distance))} feet")
        if exit_velocity is not None and exit_velocity >= 110:
            score += 16
            reasons.append(f"it came off the bat at {exit_velocity:.1f} mph")
    if event_type == "triple":
        score += 54
        reasons.append("it was a triple")
    if event_type == "double_play":
        score += 34
        reasons.append("it turned a double play")
    if has_outfield_assist(play):
        score += 52
        reasons.append("an outfielder cut down a runner")
    if is_home_run_robbery_candidate(play):
        score += 82
        reasons.append("a deep reviewed fly ball still turned into an out")
    if "walk-off" in description.lower():
        score += 68
        reasons.append("it was a walk-off moment")
    if runs_scored >= 2:
        score += runs_scored * 9
        reasons.append(f"it drove in {runs_scored} run(s)")
    return score, reasons


def score_weird_play(play: dict[str, Any]) -> tuple[float, list[str]]:
    description = str(play.get("result", {}).get("description") or "")
    lowered = description.lower()
    event_type = str(play.get("result", {}).get("eventType") or "").lower()
    score = 0.0
    reasons: list[str] = []

    for keyword, keyword_score in WEIRD_KEYWORDS.items():
        if keyword in lowered:
            score += keyword_score
            reasons.append(f"the description includes '{keyword}'")
    if has_outfield_assist(play) and "force out" in lowered:
        score += 45
        reasons.append("an outfielder recorded a force-out assist")
    if is_home_run_robbery_candidate(play):
        score += 38
        reasons.append("a reviewed near-home-run still ended as an out")
    if event_type not in {"single", "double", "triple", "home_run", "strikeout", "walk", "field_out", "groundout", "flyout", "lineout"}:
        score += 20
        reasons.append(f"the event type '{event_type}' is less common")
    return score, reasons


def score_defensive_play(play: dict[str, Any]) -> tuple[float, list[str]]:
    event_type = str(play.get("result", {}).get("eventType") or "").lower()
    description = str(play.get("result", {}).get("description") or "")
    lowered = description.lower()
    hit_data = extract_hit_data(play)
    distance = parse_float(hit_data.get("totalDistance")) if hit_data else None
    exit_velocity = parse_float(hit_data.get("launchSpeed")) if hit_data else None
    score = 0.0
    reasons: list[str] = []

    if event_type == "home_run":
        return 0.0, []
    if is_home_run_robbery_candidate(play):
        score += 140
        reasons.append("it took away a likely home run")
    if has_outfield_assist(play):
        score += 95
        reasons.append("an outfielder cut down a runner")
    if has_infield_assist(play):
        score += 26
        reasons.append("an infielder made a credited assist on the play")
    if event_type == "double_play":
        score += 80
        reasons.append("it turned a double play")
    if has_outfield_putout(play):
        score += 30
        reasons.append("it finished as an outfield catch")
        if distance is not None and distance >= 360:
            score += 42
            reasons.append(f"the ball carried {int(round(distance))} feet")
        if exit_velocity is not None and exit_velocity >= 100:
            score += min((exit_velocity - 95) * 2.4, 30)
            reasons.append(f"it came off the bat at {exit_velocity:.1f} mph")
    text_bonuses = {
        "diving": 44,
        "leaping": 40,
        "sliding": 18,
        "barehanded": 55,
        "sprawling": 32,
        "robbed": 34,
        "snow cone": 34,
    }
    for keyword, bonus in text_bonuses.items():
        if keyword in lowered:
            score += bonus
            reasons.append(f"the description flags it as a {keyword} play")
    if event_type in {"field_out", "flyout", "lineout", "groundout"} and not reasons:
        score += 8
    return score, reasons


def extract_story_player_queries(question: str) -> list[str]:
    candidates = extract_name_candidates(question)
    if candidates:
        return candidates
    stripped = question.strip(" ?.!")
    patterns = (
        re.compile(r"^(?:show me\s+)?(.+?)\s+(?:defensive|fielding|glove)\s+plays?\b", re.IGNORECASE),
        re.compile(r"^(?:show me\s+)?(.+?)\s+(?:best|coolest|weirdest|weird|sick|awesome|wild|crazy|nastiest)\s+plays?\b", re.IGNORECASE),
        re.compile(r"^who\s+had\s+the\s+best\s+defensive\s+performance\s+(?:for\s+)?(.+?)\b", re.IGNORECASE),
    )
    for pattern in patterns:
        match = pattern.search(stripped)
        if not match:
            continue
        candidate = normalize_story_player_phrase(match.group(1))
        if candidate:
            return [candidate]
    return []


def normalize_story_player_phrase(value: str) -> str:
    cleaned = value.strip(" ?.!,'\"")
    cleaned = re.sub(r"^(?:the|all|just)\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(on|for|from)\b.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    if not cleaned or " " not in cleaned:
        return ""
    return " ".join(part.capitalize() for part in cleaned.split())


def extract_hit_data(play: dict[str, Any]) -> dict[str, Any]:
    for event in reversed(play.get("playEvents", [])):
        hit_data = event.get("hitData")
        if hit_data:
            return hit_data
    return {}


def count_runs_scored(play: dict[str, Any]) -> int:
    return sum(1 for runner in play.get("runners", []) if runner.get("details", {}).get("isScoringEvent"))


def has_outfield_assist(play: dict[str, Any]) -> bool:
    for runner in play.get("runners", []):
        for credit in runner.get("credits", []):
            if credit.get("credit") not in {"f_assist", "f_assist_of"}:
                continue
            if credit.get("position", {}).get("abbreviation") in OUTFIELD_POSITIONS:
                return True
    return False


def has_infield_assist(play: dict[str, Any]) -> bool:
    for runner in play.get("runners", []):
        for credit in runner.get("credits", []):
            if credit.get("credit") not in {"f_assist", "f_assist_of"}:
                continue
            if credit.get("position", {}).get("abbreviation") in {"1B", "2B", "3B", "SS"}:
                return True
    return False


def is_home_run_robbery_candidate(play: dict[str, Any]) -> bool:
    description = str(play.get("result", {}).get("description") or "").lower()
    if "reviewed (home run)" in description or any(keyword in description for keyword in ("robs", "robbed", "at the wall")):
        return True
    hit_data = extract_hit_data(play)
    distance = parse_float(hit_data.get("totalDistance")) if hit_data else None
    launch_angle = parse_float(hit_data.get("launchAngle")) if hit_data else None
    return (
        has_outfield_putout(play)
        and distance is not None
        and distance >= 360
        and launch_angle is not None
        and launch_angle >= 30
    )


def has_outfield_putout(play: dict[str, Any]) -> bool:
    for runner in play.get("runners", []):
        for credit in runner.get("credits", []):
            if credit.get("credit") != "f_putout":
                continue
            if credit.get("position", {}).get("abbreviation") in OUTFIELD_POSITIONS:
                return True
    return False


def has_any_fielder_putout(play: dict[str, Any]) -> bool:
    for runner in play.get("runners", []):
        for credit in runner.get("credits", []):
            if credit.get("credit") == "f_putout":
                return True
    return False


def build_team_matchup(feed: dict[str, Any]) -> str:
    teams = feed.get("gameData", {}).get("teams", {})
    away = str(teams.get("away", {}).get("abbreviation") or teams.get("away", {}).get("name") or "").strip()
    home = str(teams.get("home", {}).get("abbreviation") or teams.get("home", {}).get("name") or "").strip()
    return f"{away} @ {home}".strip()


def derive_primary_actor(play: dict[str, Any], sporty_page: SportyVideoPage | None) -> tuple[str, list[str]]:
    event_type = str(play.get("result", {}).get("eventType") or "").lower()
    batter_name = str(play.get("matchup", {}).get("batter", {}).get("fullName") or "").strip()
    pitcher_name = str(play.get("matchup", {}).get("pitcher", {}).get("fullName") or "").strip()
    fielder_name = find_primary_fielder_name(play)

    if (
        is_home_run_robbery_candidate(play)
        or has_outfield_assist(play)
        or has_outfield_putout(play)
        or has_infield_assist(play)
        or has_any_fielder_putout(play)
    ):
        actor_name = fielder_name or batter_name or pitcher_name
        return actor_name, ["fielder"]
    if event_type in {"strikeout", "walk", "wild_pitch", "passed_ball", "balk"}:
        actor_name = pitcher_name or batter_name or fielder_name
        return actor_name, ["pitcher"] if pitcher_name else ["batter"]
    actor_name = sporty_page.batter if sporty_page and sporty_page.batter else batter_name or fielder_name or pitcher_name
    return actor_name, ["batter"] if actor_name == batter_name or not actor_name else ["batter"]


def format_window(window: DateWindow) -> str:
    if window.start_date == window.end_date:
        return window.start_date.isoformat()
    return f"{window.start_date.isoformat()} through {window.end_date.isoformat()}"


def serialize_window(window: DateWindow) -> dict[str, str]:
    return {
        "label": window.label,
        "start_date": window.start_date.isoformat(),
        "end_date": window.end_date.isoformat(),
    }


def parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
