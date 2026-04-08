from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from mlb_history_bot.config import Settings
from mlb_history_bot.live import LiveStatsClient


class FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return b"first byte timeout"


def test_live_client_invalid_json_falls_back_to_empty_payload() -> None:
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    client = LiveStatsClient(settings)
    with patch("mlb_history_bot.live.urlopen", return_value=FakeResponse()):
        payload = client.fetch_json("https://example.com/bad-json")
    assert payload == {}
