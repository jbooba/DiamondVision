from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _maybe_load_dotenv(project_root: Path) -> None:
    env_path = project_root / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(env_path)


@dataclass(slots=True)
class Settings:
    project_root: Path
    raw_data_dir: Path
    processed_data_dir: Path
    database_path: Path
    sabr_docs_dir: Path
    openai_model: str
    openai_reasoning_effort: str
    live_season: int | None
    user_agent: str
    fielding_bible_api_base: str
    fielding_bible_start_season: int

    @classmethod
    def from_env(cls, project_root: Path | None = None) -> "Settings":
        root = project_root or Path(__file__).resolve().parent.parent
        _maybe_load_dotenv(root)
        raw_data_dir = Path(os.getenv("MLB_HISTORY_RAW_DATA_DIR", root / "data" / "raw"))
        processed_data_dir = Path(os.getenv("MLB_HISTORY_PROCESSED_DIR", root / "data" / "processed"))
        database_path = Path(
            os.getenv("MLB_HISTORY_DATABASE_PATH", processed_data_dir / "mlb_history.sqlite3")
        )
        sabr_docs_dir = Path(os.getenv("MLB_HISTORY_SABR_DIR", raw_data_dir / "sabr"))
        live_season_value = os.getenv("MLB_HISTORY_LIVE_SEASON", "").strip()
        live_season = int(live_season_value) if live_season_value else None
        return cls(
            project_root=root,
            raw_data_dir=raw_data_dir,
            processed_data_dir=processed_data_dir,
            database_path=database_path,
            sabr_docs_dir=sabr_docs_dir,
            openai_model=os.getenv("OPENAI_MODEL", "gpt-5.4"),
            openai_reasoning_effort=os.getenv("OPENAI_REASONING_EFFORT", "medium"),
            live_season=live_season,
            user_agent=os.getenv("MLB_HISTORY_USER_AGENT", "mlb-history-chatbot/0.1"),
            fielding_bible_api_base=os.getenv(
                "MLB_HISTORY_FIELDING_BIBLE_API_BASE",
                "https://api.sportsinfosolutions.com/api/v2/fieldingbible",
            ).rstrip("/"),
            fielding_bible_start_season=int(os.getenv("MLB_HISTORY_FIELDING_BIBLE_START_SEASON", "2003")),
        )

    def ensure_directories(self) -> None:
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
        self.sabr_docs_dir.mkdir(parents=True, exist_ok=True)
