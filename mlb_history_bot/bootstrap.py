from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from zipfile import ZipFile

from .config import Settings


RETROSHEET_MAIN_CSV_URL = "https://www.retrosheet.org/downloads/csvdownloads.zip"
LAHMAN_SHARE_URL = "https://sabr.box.com/s/y1prhc795jk8zvmelfd3jq7tl389y6cd"
LAHMAN_SHARE_DOWNLOAD_URL = "https://sabr.app.box.com/index.php"


@dataclass(slots=True)
class BootstrapResult:
    lahman_dir: Path
    retrosheet_dir: Path


def bootstrap_datasets(settings: Settings, *, include_retrosheet_plays: bool = False) -> BootstrapResult:
    settings.ensure_directories()
    lahman_dir = settings.raw_data_dir / "lahman"
    retrosheet_dir = settings.raw_data_dir / "retrosheet"
    lahman_dir.mkdir(parents=True, exist_ok=True)
    retrosheet_dir.mkdir(parents=True, exist_ok=True)
    download_lahman_csvs(settings, lahman_dir)
    download_retrosheet_csvs(settings, retrosheet_dir, include_plays=include_retrosheet_plays)
    return BootstrapResult(lahman_dir=lahman_dir, retrosheet_dir=retrosheet_dir)


def download_retrosheet_csvs(
    settings: Settings,
    destination_dir: Path,
    *,
    include_plays: bool = False,
) -> None:
    zip_path = destination_dir / "csvdownloads.zip"
    _download_file(RETROSHEET_MAIN_CSV_URL, zip_path, settings.user_agent)
    allowed = {
        "allplayers.csv",
        "gameinfo.csv",
        "teamstats.csv",
        "batting.csv",
        "pitching.csv",
        "fielding.csv",
    }
    if include_plays:
        allowed.add("plays.csv")
    with ZipFile(zip_path) as archive:
        for member in archive.infolist():
            name = Path(member.filename).name.lower()
            if name not in allowed:
                continue
            target_path = destination_dir / Path(member.filename).name
            with archive.open(member) as source, target_path.open("wb") as target:
                shutil.copyfileobj(source, target)


def download_lahman_csvs(settings: Settings, destination_dir: Path) -> None:
    destination_dir.mkdir(parents=True, exist_ok=True)
    for item in _fetch_lahman_manifest(settings):
        if item.get("type") != "file":
            continue
        name = str(item.get("name", ""))
        if not name.lower().endswith(".csv"):
            continue
        query = urlencode(
            {
                "rm": "box_download_shared_file",
                "shared_name": urlparse(LAHMAN_SHARE_URL).path.rstrip("/").split("/")[-1],
                "file_id": f"f_{item['id']}",
            }
        )
        _download_file(
            f"{LAHMAN_SHARE_DOWNLOAD_URL}?{query}",
            destination_dir / name,
            settings.user_agent,
        )


def _fetch_lahman_manifest(settings: Settings) -> list[dict]:
    html = _fetch_text(LAHMAN_SHARE_URL, settings.user_agent)
    payloads = _extract_box_payloads(html)
    for payload in payloads:
        shared_folder = payload.get("/app-api/enduserapp/shared-folder")
        if isinstance(shared_folder, dict) and isinstance(shared_folder.get("items"), list):
            return shared_folder["items"]
        for value in payload.values():
            if isinstance(value, dict):
                if isinstance(value.get("items"), list):
                    return value["items"]
                item_collection = value.get("itemCollection")
                if isinstance(item_collection, dict) and isinstance(item_collection.get("entries"), list):
                    return item_collection["entries"]
    raise RuntimeError("Unable to locate Lahman Box folder metadata")


def _extract_box_payloads(html: str) -> list[dict]:
    decoder = json.JSONDecoder()
    payloads: list[dict] = []
    # Current Box pages expose both payloads; the folder listing lives in postStreamData.
    for marker in ("Box.postStreamData = ", "Box.prefetchedData = "):
        if marker not in html:
            continue
        start = html.index(marker) + len(marker)
        payload, _ = decoder.raw_decode(html[start:])
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def _fetch_text(url: str, user_agent: str) -> str:
    request = Request(url, headers={"User-Agent": user_agent})
    with urlopen(request, timeout=60) as response:
        return response.read().decode("utf-8", errors="replace")


def _download_file(url: str, destination: Path, user_agent: str) -> None:
    request = Request(url, headers={"User-Agent": user_agent})
    destination.parent.mkdir(parents=True, exist_ok=True)
    with urlopen(request, timeout=120) as response, destination.open("wb") as handle:
        shutil.copyfileobj(response, handle)
