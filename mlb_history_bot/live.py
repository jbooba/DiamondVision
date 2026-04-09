from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import date
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import Settings


class LiveStatsClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._json_cache: dict[str, dict[str, Any]] = {}

    def fetch_json(self, url: str) -> dict[str, Any]:
        if url in self._json_cache:
            return self._json_cache[url]
        request = Request(
            url,
            headers={
                "User-Agent": self.settings.user_agent,
                "Accept": "application/json",
            },
        )
        try:
            with urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (OSError, TimeoutError, json.JSONDecodeError, UnicodeDecodeError):
            payload = {}
        self._json_cache[url] = payload
        return payload

    def scoreboard(self, target_date: str | None = None) -> dict[str, Any]:
        current_date = target_date or date.today().isoformat()
        payload = self.schedule(current_date, hydrate="linescore,team,decisions")
        games: list[dict[str, Any]] = []
        for day in payload.get("dates", []):
            for game in day.get("games", []):
                teams = game.get("teams", {})
                away = teams.get("away", {})
                home = teams.get("home", {})
                games.append(
                    {
                        "gamePk": game.get("gamePk"),
                        "date": current_date,
                        "status": game.get("status", {}).get("detailedState"),
                        "away": away.get("team", {}).get("name"),
                        "away_score": away.get("score"),
                        "home": home.get("team", {}).get("name"),
                        "home_score": home.get("score"),
                    }
                )
        return {"date": current_date, "games": games}

    def schedule(self, target_date: str, *, hydrate: str | None = None) -> dict[str, Any]:
        query_payload: dict[str, Any] = {"sportId": 1, "date": target_date}
        if hydrate:
            query_payload["hydrate"] = hydrate
        query = urlencode(query_payload)
        return self.fetch_json(f"https://statsapi.mlb.com/api/v1/schedule?{query}")

    def schedule_range(
        self,
        start_date: str,
        end_date: str,
        *,
        hydrate: str | None = None,
        team_id: int | None = None,
    ) -> dict[str, Any]:
        query_payload: dict[str, Any] = {"sportId": 1, "startDate": start_date, "endDate": end_date}
        if hydrate:
            query_payload["hydrate"] = hydrate
        if team_id:
            query_payload["teamId"] = team_id
        query = urlencode(query_payload)
        return self.fetch_json(f"https://statsapi.mlb.com/api/v1/schedule?{query}")

    def game_feed(self, game_pk: int) -> dict[str, Any]:
        return self.fetch_json(f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live")

    def teams(self, season: int | None = None) -> list[dict[str, Any]]:
        selected_season = season or self.settings.live_season or date.today().year
        query = urlencode({"sportId": 1, "season": selected_season})
        payload = self.fetch_json(f"https://statsapi.mlb.com/api/v1/teams?{query}")
        return payload.get("teams", [])

    def all_team_group_stats(self, group: str, season: int | None = None) -> list[dict[str, Any]]:
        selected_season = season or self.settings.live_season or date.today().year
        query = urlencode({"sportId": 1, "stats": "season", "group": group, "season": selected_season})
        payload = self.fetch_json(f"https://statsapi.mlb.com/api/v1/teams/stats?{query}")
        stats_groups = payload.get("stats") or []
        first_group = stats_groups[0] if stats_groups else {}
        return first_group.get("splits", [])

    def team_game_logs(self, team_id: int, group: str, season: int | None = None) -> list[dict[str, Any]]:
        selected_season = season or self.settings.live_season or date.today().year
        query = urlencode({"stats": "gameLog", "group": group, "season": selected_season})
        payload = self.fetch_json(f"https://statsapi.mlb.com/api/v1/teams/{team_id}/stats?{query}")
        stats_groups = payload.get("stats") or []
        first_group = stats_groups[0] if stats_groups else {}
        return first_group.get("splits", [])

    def team_roster(
        self,
        team_id: int,
        *,
        season: int | None = None,
        roster_type: str = "active",
    ) -> list[dict[str, Any]]:
        selected_season = season or self.settings.live_season or date.today().year
        query = urlencode({"rosterType": roster_type, "season": selected_season})
        payload = self.fetch_json(f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?{query}")
        return payload.get("roster", [])

    def standings(self, season: int | None = None) -> dict[str, Any]:
        selected_season = season or self.settings.live_season or date.today().year
        query = urlencode(
            {
                "leagueId": "103,104",
                "season": selected_season,
                "standingsTypes": "regularSeason",
            }
        )
        payload = self.fetch_json(f"https://statsapi.mlb.com/api/v1/standings?{query}")
        rows: list[dict[str, Any]] = []
        for record in payload.get("records", []):
            division = record.get("division", {}).get("name")
            for team_record in record.get("teamRecords", []):
                team = team_record.get("team", {})
                rows.append(
                    {
                        "team": team.get("name"),
                        "division": division,
                        "wins": team_record.get("wins"),
                        "losses": team_record.get("losses"),
                        "pct": team_record.get("winningPercentage"),
                        "games_back": team_record.get("gamesBack"),
                    }
                )
        return {"season": selected_season, "standings": rows}

    def search_people(self, query: str) -> list[dict[str, Any]]:
        payload = self.fetch_json(
            f"https://statsapi.mlb.com/api/v1/people/search?{urlencode({'names': query})}"
        )
        return payload.get("people", [])

    def person_details(self, player_id: int) -> dict[str, Any] | None:
        payload = self.fetch_json(
            f"https://statsapi.mlb.com/api/v1/people/{player_id}?{urlencode({'hydrate': 'currentTeam'})}"
        )
        people = payload.get("people") or []
        return people[0] if people else None

    def award_recipients(self, award_id: str) -> list[dict[str, Any]]:
        payload = self.fetch_json(f"https://statsapi.mlb.com/api/v1/awards/{award_id}/recipients")
        return payload.get("awards") or []

    def player_season_snapshot(self, player_query: str, season: int | None = None) -> dict[str, Any] | None:
        people = self.search_people(player_query)
        if not people:
            return None
        player = choose_best_people_match(people, player_query)
        player_id = player.get("id")
        selected_season = season or self.settings.live_season or date.today().year
        groups = {}
        for group in ("hitting", "pitching", "fielding"):
            payload = self.fetch_json(
                "https://statsapi.mlb.com/api/v1/people/"
                f"{player_id}/stats?{urlencode({'stats': 'season', 'group': group, 'season': selected_season})}"
            )
            stats_groups = payload.get("stats") or []
            first_group = stats_groups[0] if stats_groups else {}
            splits = first_group.get("splits", [])
            if group != "fielding":
                groups[group] = splits[0].get("stat", {}) if splits else {}
            else:
                best_split = max(
                    splits,
                    key=lambda split: safe_float(split.get("stat", {}).get("gamesPlayed")) or 0.0,
                    default=None,
                )
                groups[group] = best_split.get("stat", {}) if best_split else {}
        details = self.person_details(int(player_id)) if player_id else None
        return {
            "player_id": player_id,
            "name": player.get("fullName"),
            "active": player.get("active"),
            "current_team": (details or {}).get("currentTeam", {}).get("name"),
            "season": selected_season,
            "current_age": player.get("currentAge"),
            "primary_position": player.get("primaryPosition", {}),
            "hitting": groups["hitting"],
            "pitching": groups["pitching"],
            "fielding": groups["fielding"],
        }

    def people_with_stats(
        self,
        person_ids: Iterable[int],
        *,
        season: int | None = None,
        groups: tuple[str, ...] = ("hitting", "pitching", "fielding"),
        chunk_size: int = 12,
    ) -> list[dict[str, Any]]:
        selected_season = season or self.settings.live_season or date.today().year
        normalized_ids = [str(int(person_id)) for person_id in person_ids if int(person_id)]
        if not normalized_ids:
            return []
        people: list[dict[str, Any]] = []
        hydrate = f"stats(group=[{','.join(groups)}],type=[season],season={selected_season})"
        for start in range(0, len(normalized_ids), chunk_size):
            chunk = normalized_ids[start : start + chunk_size]
            query = urlencode({"personIds": ",".join(chunk), "hydrate": hydrate})
            payload = self.fetch_json(f"https://statsapi.mlb.com/api/v1/people?{query}")
            people.extend(payload.get("people", []))
        return people

    def player_game_logs(
        self,
        player_id: int,
        *,
        season: int | None = None,
        group: str = "hitting",
    ) -> list[dict[str, Any]]:
        selected_season = season or self.settings.live_season or date.today().year
        query = urlencode({"stats": "gameLog", "group": group, "season": selected_season})
        payload = self.fetch_json(f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?{query}")
        stats_groups = payload.get("stats") or []
        first_group = stats_groups[0] if stats_groups else {}
        return first_group.get("splits", []) or []

    def statcast_batter_summary(
        self,
        player_query: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any] | None:
        try:
            from pybaseball import playerid_lookup, statcast_batter
        except ImportError:
            return None
        name_parts = player_query.strip().split()
        if len(name_parts) < 2:
            return None
        lookup = playerid_lookup(name_parts[-1], " ".join(name_parts[:-1]))
        if lookup.empty:
            return None
        mlbam_id = int(lookup.iloc[0]["key_mlbam"])
        frame = statcast_batter(start_date, end_date, mlbam_id)
        if frame.empty:
            return {
                "player": player_query,
                "start_date": start_date,
                "end_date": end_date,
                "events": 0,
            }
        return {
            "player": player_query,
            "start_date": start_date,
            "end_date": end_date,
            "events": int(frame.shape[0]),
            "hits": int(frame["events"].isin(["single", "double", "triple", "home_run"]).sum()),
            "home_runs": int((frame["events"] == "home_run").sum()),
            "avg_exit_velocity": round(float(frame["launch_speed"].dropna().mean()), 2)
            if "launch_speed" in frame
            else None,
        }


def choose_best_people_match(people: list[dict[str, Any]], requested_name: str) -> dict[str, Any]:
    normalized_requested = normalize_person_name(requested_name)

    def sort_key(person: dict[str, Any]) -> tuple[int, int, int, int, str]:
        full_name = normalize_person_name(str(person.get("fullName") or ""))
        return (
            0 if full_name == normalized_requested else 1,
            0 if person.get("active") else 1,
            0 if person.get("isPlayer") else 1,
            0 if person.get("isVerified") else 1,
            str(person.get("fullName") or ""),
        )

    return sorted(people, key=sort_key)[0]


def normalize_person_name(value: str) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {".---", "-.--", "---"}:
        return None
    if text.startswith("."):
        text = f"0{text}"
    try:
        return float(text)
    except ValueError:
        return None
