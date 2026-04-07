from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import re
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .query_utils import extract_name_candidates, extract_target_date, normalize_person_name, ordinal
from .sporty_video import SportyVideoClient


RHR_PROXY_RUN_VALUE = 1.6
CANDIDATE_POSITIONS = {"LF", "CF", "RF"}


@dataclass(slots=True)
class HomeRunRobberyPlay:
    play_id: str
    game_pk: int
    game_date: str
    fielder_name: str
    batter_name: str
    team_matchup: str
    inning: int
    half_inning: str
    title: str
    description: str
    hit_distance: float | None
    hr_parks: int | None
    savant_url: str
    mp4_url: str | None
    pitcher_name: str
    exit_velocity: float | None
    launch_angle: float | None
    proxy_runs: float = RHR_PROXY_RUN_VALUE


class HomeRunRobberyProxy:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.sporty_video_client = SportyVideoClient(settings)

    def build_snippets(self, question: str) -> list[EvidenceSnippet]:
        season = extract_season(question, self.settings.live_season or date.today().year)
        target_date = extract_target_date(question, season)
        candidate_names = extract_name_candidates(question)
        player_query = candidate_names[0] if candidate_names else extract_player_query(question)
        if target_date is None:
            if player_query:
                return self._season_to_date_player_snippets(player_query, season)
            return self._season_to_date_leaderboard_snippets(season)

        plays = self.find_home_run_robberies(target_date.isoformat(), player_query=player_query)
        if not plays:
            return [self._empty_result_snippet(target_date.isoformat(), player_query)]

        if player_query:
            total_proxy = sum(play.proxy_runs for play in plays)
            lines = [
                (
                    f"{play.game_date} {play.fielder_name}: verified home run robbery in the "
                    f"{ordinal(play.inning)} {play.half_inning} off {play.batter_name}"
                    + (
                        f" ({play.hr_parks}/30 HR parks, {int(play.hit_distance)} ft)"
                        if play.hr_parks is not None and play.hit_distance is not None
                        else ""
                    )
                )
                for play in plays[:5]
            ]
            summary = (
                f"{plays[0].fielder_name} had {len(plays)} Savant-verified home run robbery play(s) on {target_date.isoformat()}, "
                f"for an rHR proxy of {total_proxy:.1f} runs using +{RHR_PROXY_RUN_VALUE:.1f} runs per robbery."
            )
            if lines:
                summary = f"{summary} {' '.join(lines)}"
            return [
                EvidenceSnippet(
                    source="rHR Proxy",
                    title=f"{plays[0].fielder_name} {target_date.isoformat()} rHR proxy",
                    citation=(
                        "Baseball Savant sporty-videos play pages verified against MLB Stats API game feeds; "
                        "proxy uses +1.6 runs per robbery from SIS commentary"
                    ),
                    summary=summary,
                    payload={
                        "target_date": target_date.isoformat(),
                        "player": plays[0].fielder_name,
                        "robbery_count": len(plays),
                        "proxy_runs": round(total_proxy, 1),
                        "plays": [asdict(play) for play in plays],
                        "clips": [asdict(play) for play in plays],
                    },
                )
            ]

        leaderboard: dict[str, list[HomeRunRobberyPlay]] = {}
        for play in plays:
            leaderboard.setdefault(play.fielder_name, []).append(play)
        ranked = sorted(
            leaderboard.items(),
            key=lambda item: (-len(item[1]), -sum(play.proxy_runs for play in item[1]), item[0]),
        )
        lines = [
            f"{index}. {fielder}: {len(player_plays)} robbery(ies), {sum(play.proxy_runs for play in player_plays):.1f} rHR proxy"
            for index, (fielder, player_plays) in enumerate(ranked[:5], start=1)
        ]
        return [
            EvidenceSnippet(
                source="rHR Proxy",
                title=f"{target_date.isoformat()} home run robbery proxy leaders",
                citation=(
                    "Baseball Savant sporty-videos play pages verified against MLB Stats API game feeds; "
                    "proxy uses +1.6 runs per robbery from SIS commentary"
                ),
                summary=" ".join(lines),
                payload={
                    "target_date": target_date.isoformat(),
                    "leaders": [
                        {
                            "fielder_name": fielder,
                            "robbery_count": len(player_plays),
                            "proxy_runs": round(sum(play.proxy_runs for play in player_plays), 1),
                        }
                        for fielder, player_plays in ranked
                    ],
                },
            )
        ]

    def _season_to_date_player_snippets(self, player_query: str, season: int) -> list[EvidenceSnippet]:
        end_date = season_end_date(season, self.settings.live_season or date.today().year)
        team_id = self._current_team_id(player_query, season)
        plays = self.find_home_run_robberies_in_window(
            start_date=f"{season}-03-01",
            end_date=end_date,
            player_query=player_query,
            team_id=team_id,
        )
        if not plays:
            return [
                EvidenceSnippet(
                    source="rHR Proxy",
                    title=f"{player_query} {season} home run robbery status",
                    citation=(
                        "Baseball Savant sporty-videos play pages checked against MLB Stats API game feeds; "
                        "proxy uses +1.6 runs per robbery from SIS commentary"
                    ),
                    summary=f"I did not find any Savant-verified home run robbery clips for {player_query} in {season} season-to-date play data.",
                    payload={
                        "season": season,
                        "player_query": player_query,
                        "robbery_count": 0,
                        "proxy_runs": 0.0,
                        "clips": [],
                    },
                )
            ]

        player_name = plays[0].fielder_name
        total_proxy = sum(play.proxy_runs for play in plays)
        lines = [
            (
                f"{play.game_date}: {play.title}"
                + (
                    f" ({play.hr_parks}/30 HR parks, {int(round(float(play.hit_distance)))} ft)"
                    if play.hr_parks is not None and play.hit_distance is not None
                    else ""
                )
            )
            for play in plays[:5]
        ]
        summary = (
            f"{player_name} has {len(plays)} Savant-verified home run robbery clip(s) in {season} season-to-date data, "
            f"for an rHR proxy of {total_proxy:.1f} runs at +{RHR_PROXY_RUN_VALUE:.1f} per robbery."
        )
        if lines:
            summary = f"{summary} {' '.join(lines)}"
        return [
            EvidenceSnippet(
                source="rHR Proxy",
                title=f"{player_name} {season} home run robberies",
                citation=(
                    "Baseball Savant sporty-videos play pages verified against MLB Stats API game feeds; "
                    "proxy uses +1.6 runs per robbery from SIS commentary"
                ),
                summary=summary,
                payload={
                    "season": season,
                    "player": player_name,
                    "robbery_count": len(plays),
                    "proxy_runs": round(total_proxy, 1),
                    "plays": [asdict(play) for play in plays],
                    "clips": [asdict(play) for play in plays],
                },
            )
        ]

    def _season_to_date_leaderboard_snippets(self, season: int) -> list[EvidenceSnippet]:
        end_date = season_end_date(season, self.settings.live_season or date.today().year)
        plays = self.find_home_run_robberies_in_window(
            start_date=f"{season}-03-01",
            end_date=end_date,
        )
        if not plays:
            return [
                EvidenceSnippet(
                    source="rHR Proxy",
                    title=f"{season} home run robbery status",
                    citation=(
                        "Baseball Savant sporty-videos play pages checked against MLB Stats API game feeds; "
                        "proxy uses +1.6 runs per robbery from SIS commentary"
                    ),
                    summary=f"I did not find any Savant-verified home run robbery clips in the {season} season-to-date window.",
                    payload={
                        "season": season,
                        "robbery_count": 0,
                        "proxy_runs": 0.0,
                        "leaders": [],
                        "plays": [],
                        "clips": [],
                    },
                )
            ]

        leaderboard: dict[str, list[HomeRunRobberyPlay]] = {}
        for play in plays:
            leaderboard.setdefault(play.fielder_name, []).append(play)
        ranked = sorted(
            leaderboard.items(),
            key=lambda item: (-len(item[1]), -sum(play.proxy_runs for play in item[1]), item[0]),
        )
        total_proxy = sum(play.proxy_runs for play in plays)
        leader_lines = [
            f"{index}. {fielder}: {len(player_plays)} robbery(ies), {sum(play.proxy_runs for play in player_plays):.1f} rHR proxy"
            for index, (fielder, player_plays) in enumerate(ranked[:5], start=1)
        ]
        play_lines = [
            f"{play.game_date}: {play.title} ({play.team_matchup})"
            for play in plays[:6]
        ]
        summary = (
            f"So far in {season}, I found {len(plays)} Savant-verified home run robbery play(s), "
            f"worth {total_proxy:.1f} total proxy runs at +{RHR_PROXY_RUN_VALUE:.1f} each."
        )
        if leader_lines:
            summary = f"{summary} Leaders: {' '.join(leader_lines)}"
        if play_lines:
            summary = f"{summary} Clips: {' '.join(play_lines)}"
        return [
            EvidenceSnippet(
                source="rHR Proxy",
                title=f"{season} home run robberies",
                citation=(
                    "Baseball Savant sporty-videos play pages verified against MLB Stats API game feeds; "
                    "proxy uses +1.6 runs per robbery from SIS commentary"
                ),
                summary=summary,
                payload={
                    "analysis_type": "home_run_robbery_proxy",
                    "season": season,
                    "robbery_count": len(plays),
                    "proxy_runs": round(total_proxy, 1),
                    "leaders": [
                        {
                            "fielder_name": fielder,
                            "robbery_count": len(player_plays),
                            "proxy_runs": round(sum(play.proxy_runs for play in player_plays), 1),
                        }
                        for fielder, player_plays in ranked
                    ],
                    "plays": [asdict(play) for play in plays],
                    "clips": [asdict(play) for play in plays],
                    "rows": [
                        {
                            "fielder_name": fielder,
                            "robbery_count": len(player_plays),
                            "proxy_runs": round(sum(play.proxy_runs for play in player_plays), 1),
                        }
                        for fielder, player_plays in ranked
                    ],
                },
            )
        ]

    def _empty_result_snippet(self, target_date: str, player_query: str | None) -> EvidenceSnippet:
        schedule = self.live_client.schedule(target_date)
        games = [game for day in schedule.get("dates", []) for game in day.get("games", [])]
        all_not_started = games and all(game.get("status", {}).get("codedGameState") in {"S", "P"} for game in games)
        if player_query:
            summary = f"No Savant-verified home run robbery plays were found for {player_query} on {target_date}."
        else:
            summary = f"No Savant-verified home run robbery plays were found on {target_date}."
        if all_not_started:
            summary = f"{summary} MLB games for that date have not started yet."
        return EvidenceSnippet(
            source="rHR Proxy",
            title=f"{target_date} home run robbery proxy status",
            citation=(
                "Baseball Savant sporty-videos play pages checked against MLB Stats API game feeds; "
                "proxy uses +1.6 runs per robbery from SIS commentary"
            ),
            summary=summary,
            payload={"target_date": target_date, "player_query": player_query, "robbery_count": 0, "proxy_runs": 0.0},
        )

    def find_home_run_robberies(self, target_date: str, *, player_query: str | None = None) -> list[HomeRunRobberyPlay]:
        return self.find_home_run_robberies_in_window(
            start_date=target_date,
            end_date=target_date,
            player_query=player_query,
        )

    def find_home_run_robberies_in_window(
        self,
        *,
        start_date: str,
        end_date: str,
        player_query: str | None = None,
        team_id: int | None = None,
    ) -> list[HomeRunRobberyPlay]:
        people = self.live_client.search_people(player_query) if player_query else []
        exact_names = (
            {normalize_person_name(str(person.get("fullName") or "")) for person in people if person.get("fullName")}
            if people
            else set()
        )
        schedule = self.live_client.schedule_range(start_date, end_date, team_id=team_id)
        plays: list[HomeRunRobberyPlay] = []
        candidates: list[tuple[str, int, dict[str, Any], str]] = []
        for day in schedule.get("dates", []):
            game_date = str(day.get("date") or start_date)
            for game in day.get("games", []):
                if game.get("status", {}).get("codedGameState") in {"S", "P"}:
                    continue
                game_pk = int(game["gamePk"])
                feed = self.live_client.game_feed(game_pk)
                for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
                    candidate = self._candidate_pitch_event(play)
                    if candidate is None:
                        continue
                    candidates.append((game_date, game_pk, play, candidate["playId"]))

        pages_by_id = self._fetch_pages_for_ids([play_id for _, _, _, play_id in candidates])
        for game_date, game_pk, play, play_id in candidates:
            sporty_page = pages_by_id.get(play_id)
            if sporty_page is None or not sporty_page.is_home_run_robbery:
                continue
            fielder_name = parse_fielder_name(sporty_page.title)
            if exact_names and normalize_person_name(fielder_name) not in exact_names:
                continue
            plays.append(
                HomeRunRobberyPlay(
                    play_id=play_id,
                    game_pk=game_pk,
                    game_date=game_date,
                    fielder_name=fielder_name or self._fallback_fielder_name(play),
                    batter_name=sporty_page.batter or str(play.get("matchup", {}).get("batter", {}).get("fullName") or "").strip(),
                    team_matchup=sporty_page.matchup,
                    inning=int(play.get("about", {}).get("inning") or 0),
                    half_inning=str(play.get("about", {}).get("halfInning") or ""),
                    title=sporty_page.title,
                    description=str(play.get("result", {}).get("description") or ""),
                    hit_distance=sporty_page.hit_distance,
                    hr_parks=sporty_page.hr_parks,
                    savant_url=sporty_page.savant_url,
                    mp4_url=sporty_page.mp4_url,
                    pitcher_name=sporty_page.pitcher,
                    exit_velocity=sporty_page.exit_velocity,
                    launch_angle=sporty_page.launch_angle,
                )
            )
        plays.sort(key=lambda play: (play.game_date, play.game_pk, play.inning, play.half_inning))
        return plays

    def _fetch_pages_for_ids(self, play_ids: list[str]) -> dict[str, Any]:
        unique_ids = list(dict.fromkeys(play_id for play_id in play_ids if play_id))
        if not unique_ids:
            return {}
        if len(unique_ids) <= 12:
            return {play_id: self.sporty_video_client.fetch(play_id) for play_id in unique_ids}
        with ThreadPoolExecutor(max_workers=12) as executor:
            pages = list(executor.map(self.sporty_video_client.fetch, unique_ids))
        return {play_id: page for play_id, page in zip(unique_ids, pages, strict=False)}

    def _current_team_id(self, player_query: str, season: int) -> int | None:
        people = self.live_client.search_people(player_query)
        if not people:
            return None
        details = self.live_client.person_details(int(people[0].get("id") or 0))
        current_team_id = int(details.get("currentTeam", {}).get("id") or 0) if details else 0
        if current_team_id:
            return current_team_id
        snapshot = self.live_client.player_season_snapshot(player_query, season)
        if not snapshot:
            return None
        current_team_name = str(snapshot.get("current_team") or "").strip().lower()
        for team in self.live_client.teams(season):
            if current_team_name and str(team.get("name") or "").strip().lower() == current_team_name:
                return int(team.get("id") or 0) or None
        return None

    def _candidate_pitch_event(self, play: dict[str, Any]) -> dict[str, Any] | None:
        if not play.get("result", {}).get("isOut"):
            return None
        if not self._has_outfield_putout(play):
            return None
        pitch_event = next(
            (event for event in reversed(play.get("playEvents", [])) if event.get("isPitch") and event.get("hitData")),
            None,
        )
        if pitch_event is None:
            return None
        hit_data = pitch_event.get("hitData", {})
        distance = float(hit_data.get("totalDistance") or 0)
        launch_angle = float(hit_data.get("launchAngle") or 0)
        trajectory = str(hit_data.get("trajectory") or "")
        if distance < 340:
            return None
        if launch_angle < 25:
            return None
        if trajectory and trajectory != "fly_ball":
            return None
        location = str(hit_data.get("location") or "")
        if location and location not in {"7", "8", "9"}:
            return None
        return pitch_event

    def _has_outfield_putout(self, play: dict[str, Any]) -> bool:
        for runner in play.get("runners", []):
            for credit in runner.get("credits", []):
                if credit.get("credit") != "f_putout":
                    continue
                if credit.get("position", {}).get("abbreviation") in CANDIDATE_POSITIONS:
                    return True
        return False

    def _play_has_player_credit(self, play: dict[str, Any], player_ids: set[int]) -> bool:
        for runner in play.get("runners", []):
            for credit in runner.get("credits", []):
                if credit.get("credit") != "f_putout":
                    continue
                if int(credit.get("player", {}).get("id") or 0) in player_ids:
                    return True
        return False

    def _fallback_fielder_name(self, play: dict[str, Any]) -> str:
        description = str(play.get("result", {}).get("description") or "")
        match = re.search(r"to\s+(?:left|center|right)\s+fielder\s+(.+?)\.", description, re.IGNORECASE)
        return match.group(1).strip() if match else "Unknown fielder"

def parse_fielder_name(title: str) -> str:
    lowered = title.lower()
    suffix = "'s home run robbery"
    if lowered.endswith(suffix):
        return title[: -len(suffix)].strip()
    for pattern in (
        re.compile(r"^(.+?) robs\b", re.IGNORECASE),
        re.compile(r"^(.+?) steals a home run\b", re.IGNORECASE),
        re.compile(r"^(.+?) home run robbery\b", re.IGNORECASE),
    ):
        match = pattern.search(title.strip())
        if match:
            return match.group(1).strip()
    return ""


def extract_player_query(question: str) -> str | None:
    patterns = (
        re.compile(r"show me\s+(.+?)\s+home run robber", re.IGNORECASE),
        re.compile(r"(.+?)\s+rhr\b", re.IGNORECASE),
    )
    for pattern in patterns:
        match = pattern.search(question.strip())
        if match:
            candidate = match.group(1).strip(" ?.!,'\"")
            if candidate:
                return " ".join(part.capitalize() for part in candidate.split())
    return None


def extract_season(question: str, default_season: int) -> int:
    match = re.search(r"\b(18\d{2}|19\d{2}|20\d{2})\b", question)
    return int(match.group(1)) if match else default_season


def season_end_date(season: int, current_season: int) -> str:
    if season >= current_season:
        return date.today().isoformat()
    return f"{season}-12-31"
