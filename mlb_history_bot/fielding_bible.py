from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import Settings
from .storage import (
    get_connection,
    initialize_database,
    replace_fielding_bible_player_drs,
    replace_fielding_bible_team_drs,
)


PLAYER_SOURCE_NAME = "Fielding Bible / SIS DRS"
TEAM_SOURCE_NAME = "Fielding Bible / SIS Team DRS"


class FieldingBibleClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def fetch_json(self, path: str, *, query: dict[str, Any] | None = None) -> Any:
        base_url = f"{self.settings.fielding_bible_api_base}/{path.lstrip('/')}"
        if query:
            base_url = f"{base_url}?{urlencode(query)}"
        request = Request(
            base_url,
            headers={
                "User-Agent": self.settings.user_agent,
                "Accept": "application/json",
            },
        )
        with urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))

    def player_drs(self, season: int) -> list[dict[str, Any]]:
        payload = self.fetch_json("DRS", query={"season": season})
        if not isinstance(payload, list):
            raise RuntimeError("Fielding Bible player DRS response was not a list")
        return [row for row in payload if isinstance(row, dict)]

    def team_drs(self, season: int) -> list[dict[str, Any]]:
        payload = self.fetch_json("TeamDRS", query={"season": season})
        if not isinstance(payload, list):
            raise RuntimeError("Fielding Bible team DRS response was not a list")
        return [row for row in payload if isinstance(row, dict)]


def sync_fielding_bible_data(
    settings: Settings,
    *,
    start_season: int | None = None,
    end_season: int | None = None,
    snapshot_current: bool = False,
) -> list[str]:
    selected_start = start_season or settings.fielding_bible_start_season
    selected_end = end_season or settings.live_season or date.today().year
    if selected_end < selected_start:
        raise ValueError("end_season must be greater than or equal to start_season")

    settings.ensure_directories()
    client = FieldingBibleClient(settings)
    connection = get_connection(settings.database_path)
    initialize_database(connection)
    player_rows = 0
    team_rows = 0
    try:
        for season in range(selected_start, selected_end + 1):
            player_rows += replace_fielding_bible_player_drs(
                connection,
                season=season,
                rows=client.player_drs(season),
                source_name=PLAYER_SOURCE_NAME,
            )
            team_rows += replace_fielding_bible_team_drs(
                connection,
                season=season,
                rows=client.team_drs(season),
                source_name=TEAM_SOURCE_NAME,
            )
        notes = [
            (
                "Synced Fielding Bible/SIS player DRS for "
                f"{selected_start}-{selected_end} ({player_rows} rows)"
            ),
            (
                "Synced Fielding Bible/SIS team DRS for "
                f"{selected_start}-{selected_end} ({team_rows} rows)"
            ),
        ]
        if snapshot_current:
            snapshot_notes = snapshot_current_drs_leaderboards(
                settings,
                season=settings.live_season or date.today().year,
                connection=connection,
                client=client,
            )
            notes.extend(snapshot_notes)
        return notes
    finally:
        connection.close()


def snapshot_current_drs_leaderboards(
    settings: Settings,
    *,
    season: int | None = None,
    connection=None,
    client: FieldingBibleClient | None = None,
) -> list[str]:
    selected_season = season or settings.live_season or date.today().year
    active_client = client or FieldingBibleClient(settings)
    active_connection = connection or get_connection(settings.database_path)
    initialize_database(active_connection)
    snapshot_at = _snapshot_timestamp()
    player_rows = replace_fielding_bible_player_drs(
        active_connection,
        season=selected_season,
        rows=active_client.player_drs(selected_season),
        snapshot_at=snapshot_at,
        source_name=PLAYER_SOURCE_NAME,
    )
    team_rows = replace_fielding_bible_team_drs(
        active_connection,
        season=selected_season,
        rows=active_client.team_drs(selected_season),
        snapshot_at=snapshot_at,
        source_name=TEAM_SOURCE_NAME,
    )
    if connection is None:
        active_connection.close()
    return [
        f"Snapshotted current Fielding Bible/SIS player DRS leaderboard for {selected_season} at {snapshot_at} ({player_rows} rows)",
        f"Snapshotted current Fielding Bible/SIS team DRS leaderboard for {selected_season} at {snapshot_at} ({team_rows} rows)",
    ]


def _snapshot_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
