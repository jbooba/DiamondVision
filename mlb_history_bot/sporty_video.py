from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen

from .config import Settings


TITLE_PATTERN = re.compile(r"<title>\s*(.*?)\s*\|\s*Baseball Savant Videos", re.IGNORECASE | re.DOTALL)
SOURCE_PATTERN = re.compile(r'<source\s+src="([^"]+\.mp4)"', re.IGNORECASE)
BATTER_PATTERN = re.compile(r"<li>\s*<strong>Batter:</strong>\s*([^<]+)</li>", re.IGNORECASE)
PITCHER_PATTERN = re.compile(r"<li>\s*<strong>Pitcher:</strong>\s*([^<]+)</li>", re.IGNORECASE)
EXIT_VELO_PATTERN = re.compile(r"<li>\s*<strong>Exit Velocity:</strong>\s*([0-9.]+)\s*</li>", re.IGNORECASE)
LAUNCH_ANGLE_PATTERN = re.compile(r"<li>\s*<strong>Launch Angle:</strong>\s*([0-9.-]+)\s*</li>", re.IGNORECASE)
HIT_DISTANCE_PATTERN = re.compile(r"<li>\s*<strong>Hit Distance:</strong>\s*([0-9.]+)\s*</li>", re.IGNORECASE)
HR_PARKS_PATTERN = re.compile(r"<li>\s*<strong>HR:</strong>\s*<span[^>]*>\s*(\d+)/30\s*</span>\s*parks\s*</li>", re.IGNORECASE)
MATCHUP_PATTERN = re.compile(r"<li>\s*<strong>Matchup:</strong>\s*([A-Z]{2,3}\s*@\s*[A-Z]{2,3})\s*</li>", re.IGNORECASE)
DATE_PATTERN = re.compile(r"<li>\s*<strong>Date:</strong>\s*(\d{4}-\d{2}-\d{2})\s*</li>", re.IGNORECASE)


@dataclass(slots=True)
class SportyVideoPage:
    play_id: str
    title: str
    savant_url: str
    mp4_url: str | None
    batter: str
    pitcher: str
    exit_velocity: float | None
    launch_angle: float | None
    hit_distance: float | None
    hr_parks: int | None
    matchup: str
    page_date: str

    @property
    def is_home_run_robbery(self) -> bool:
        lowered = self.title.lower()
        return "home run robbery" in lowered or ("robs" in lowered and ("home run" in lowered or "homer" in lowered))


class SportyVideoClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._cache: dict[str, SportyVideoPage | None] = {}

    def fetch(self, play_id: str) -> SportyVideoPage | None:
        if play_id in self._cache:
            return self._cache[play_id]

        savant_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}"
        request = Request(savant_url, headers={"User-Agent": self.settings.user_agent})
        try:
            with urlopen(request, timeout=30) as response:
                html_text = response.read().decode("utf-8", errors="replace")
        except Exception:
            self._cache[play_id] = None
            return None

        title_match = TITLE_PATTERN.search(html_text)
        page = SportyVideoPage(
            play_id=play_id,
            title=clean_fragment(title_match.group(1)) if title_match else "",
            savant_url=savant_url,
            mp4_url=parse_string(SOURCE_PATTERN.search(html_text)),
            batter=clean_fragment(parse_string(BATTER_PATTERN.search(html_text)) or ""),
            pitcher=clean_fragment(parse_string(PITCHER_PATTERN.search(html_text)) or ""),
            exit_velocity=parse_float(EXIT_VELO_PATTERN.search(html_text)),
            launch_angle=parse_float(LAUNCH_ANGLE_PATTERN.search(html_text)),
            hit_distance=parse_float(HIT_DISTANCE_PATTERN.search(html_text)),
            hr_parks=parse_int(HR_PARKS_PATTERN.search(html_text)),
            matchup=clean_fragment(parse_string(MATCHUP_PATTERN.search(html_text)) or ""),
            page_date=parse_string(DATE_PATTERN.search(html_text)) or "",
        )
        self._cache[play_id] = page
        return page


def clean_fragment(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def parse_string(match: re.Match[str] | None) -> str | None:
    if match is None:
        return None
    return match.group(1)


def parse_int(match: re.Match[str] | None) -> int | None:
    if match is None:
        return None
    return int(match.group(1))


def parse_float(match: re.Match[str] | None) -> float | None:
    if match is None:
        return None
    return float(match.group(1))
