from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel

from .chat import BaseballChatbot
from .config import Settings
from .models import EvidenceSnippet


class ChatRequest(BaseModel):
    question: str
    session_id: str | None = None


def create_app() -> FastAPI:
    settings = Settings.from_env()
    bot = BaseballChatbot(settings)
    app = FastAPI(title="MLB History Chatbot")
    index_path = settings.project_root / "mlb_history_bot" / "static" / "index.html"

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(index_path)

    @app.get("/health")
    def health() -> dict:
        size = settings.database_path.stat().st_size if settings.database_path.exists() else 0
        return {
            "ok": True,
            "database_path": str(settings.database_path),
            "database_exists": settings.database_path.exists(),
            "database_size_bytes": size,
        }

    @app.post("/chat")
    def chat(request: ChatRequest) -> dict:
        result = bot.answer(request.question, session_id=request.session_id)
        return {
            "answer": result.answer,
            "citations": result.citations,
            "warnings": result.warnings,
            "classification": result.context.classification,
            "evidence": {
                "glossary": [serialize_snippet(snippet) for snippet in result.context.glossary_entries],
                "historical": [serialize_snippet(snippet) for snippet in result.context.historical_evidence],
                "replays": [serialize_snippet(snippet) for snippet in result.context.replay_evidence],
                "live": [serialize_snippet(snippet) for snippet in result.context.live_evidence],
            },
            "clips": extract_clip_cards(result.context.all_snippets()),
        }

    return app


app = create_app()


def serialize_snippet(snippet: EvidenceSnippet) -> dict:
    return {
        "source": snippet.source,
        "title": snippet.title,
        "citation": snippet.citation,
        "summary": snippet.summary,
        "payload": snippet.payload,
        "display": build_snippet_display(snippet),
    }


def extract_clip_cards(snippets: list[EvidenceSnippet]) -> list[dict]:
    clip_cards: list[dict] = []
    seen_keys: set[str] = set()
    for snippet in snippets:
        payload = snippet.payload
        candidate_lists = []
        if isinstance(payload.get("clips"), list):
            candidate_lists.append(payload["clips"])
        if isinstance(payload.get("plays"), list):
            candidate_lists.append(payload["plays"])
        for candidate_list in candidate_lists:
            for item in candidate_list:
                if not isinstance(item, dict):
                    continue
                play_id = str(item.get("play_id") or "")
                mp4_url = str(item.get("mp4_url") or "")
                savant_url = str(item.get("savant_url") or "")
                if not (mp4_url or savant_url):
                    continue
                dedupe_key = play_id or savant_url or mp4_url
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                clip_cards.append(
                    {
                        "play_id": play_id,
                        "title": str(item.get("title") or snippet.title),
                        "explanation": build_clip_explanation(snippet, item),
                        "mp4_url": mp4_url or None,
                        "savant_url": savant_url or None,
                        "fielder_name": str(item.get("fielder_name") or ""),
                        "batter_name": str(item.get("batter_name") or ""),
                        "pitcher_name": str(item.get("pitcher_name") or ""),
                        "game_date": str(item.get("game_date") or ""),
                        "team_matchup": str(item.get("team_matchup") or ""),
                        "actor_name": str(item.get("actor_name") or ""),
                        "actor_roles": item.get("actor_roles") if isinstance(item.get("actor_roles"), list) else [],
                        "match_tags": item.get("match_tags") if isinstance(item.get("match_tags"), list) else [],
                        "hit_distance": item.get("hit_distance"),
                        "exit_velocity": item.get("exit_velocity"),
                        "launch_angle": item.get("launch_angle"),
                        "hr_parks": item.get("hr_parks"),
                        "source_title": snippet.title,
                        "source_type": snippet.source,
                    }
                )
    return clip_cards


def build_clip_explanation(snippet: EvidenceSnippet, item: dict) -> str:
    if item.get("explanation"):
        return str(item["explanation"])
    if item.get("relevance_reason"):
        return str(item["relevance_reason"])
    if snippet.source == "rHR Proxy":
        fielder = str(item.get("fielder_name") or "This fielder")
        batter = str(item.get("batter_name") or "the batter")
        inning = item.get("inning")
        half = str(item.get("half_inning") or "")
        details = []
        if item.get("hr_parks") is not None:
            details.append(f"{item['hr_parks']}/30 HR parks")
        if item.get("hit_distance") is not None:
            details.append(f"{int(float(item['hit_distance']))} ft")
        detail_text = f" ({', '.join(details)})" if details else ""
        inning_text = f" in the {inning}{ordinal_suffix(int(inning))} {half}" if inning else ""
        return f"Relevant because {fielder} robbed a likely home run off {batter}{inning_text}{detail_text}."
    return snippet.summary


def ordinal_suffix(value: int) -> str:
    if 10 <= value % 100 <= 20:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")


def build_snippet_display(snippet: EvidenceSnippet) -> dict[str, Any] | None:
    payload = snippet.payload
    if not isinstance(payload, dict):
        return None
    analysis_type = str(payload.get("analysis_type") or "")

    if analysis_type in {"team_window_ranking", "statcast_team_window_ranking", "team_split_window_ranking"}:
        leaders = payload.get("leaders")
        metric_label = str(payload.get("metric_label") or payload.get("metric") or "Metric")
        if isinstance(leaders, list):
            columns = [
                {"key": "rank", "label": "#", "align": "right"},
                {"key": "season", "label": "Season", "align": "right"},
                {"key": "team_name", "label": "Team", "align": "left"},
                {"key": "games_played", "label": "G", "align": "right"},
                {"key": "metric_value", "label": metric_label, "align": "right"},
            ]
            if analysis_type == "team_split_window_ranking":
                columns.extend(
                    [
                        {"key": "at_bats", "label": "AB", "align": "right"},
                        {"key": "hits", "label": "H", "align": "right"},
                    ]
                )
            return build_table_display(columns, leaders)

    if analysis_type == "provider_metric_leaderboard":
        leaders = payload.get("leaders")
        metric_label = str(payload.get("metric") or "Metric")
        if isinstance(leaders, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "name", "label": "Player", "align": "left"},
                    {"key": "team", "label": "Team", "align": "left"},
                    {"key": "season", "label": "Season", "align": "right"},
                    {"key": "group", "label": "Group", "align": "left"},
                    {"key": "metric_value", "label": metric_label, "align": "right"},
                ],
                leaders,
            )

    if analysis_type == "pitch_arsenal_leaderboard":
        leaders = payload.get("leaders")
        metric_label = str(payload.get("metric") or "Metric")
        if isinstance(leaders, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "pitcher_name", "label": "Pitcher", "align": "left"},
                    {"key": "team", "label": "Team", "align": "left"},
                    {"key": "season", "label": "Season", "align": "right"},
                    {"key": "pitch_label", "label": "Pitch", "align": "left"},
                    {"key": "metric_value", "label": metric_label, "align": "right"},
                    {"key": "pitch_count", "label": "Pitches", "align": "right"},
                ],
                leaders,
            )

    if analysis_type == "statcast_relationship_events":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "game_date", "label": "Date", "align": "left"},
                    {"key": "batter", "label": "Batter", "align": "left"},
                    {"key": "pitcher", "label": "Pitcher", "align": "left"},
                    {"key": "pitch_name", "label": "Pitch", "align": "left"},
                    {"key": "event", "label": "Result", "align": "left"},
                    {"key": "launch_speed", "label": "EV", "align": "right"},
                    {"key": "release_speed", "label": "Velo", "align": "right"},
                    {"key": "release_spin_rate", "label": "Spin", "align": "right"},
                    {"key": "team_matchup", "label": "Matchup", "align": "left"},
                ],
                rows,
            )

    if analysis_type == "statcast_relationship_aggregate":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "pitcher", "label": "Pitcher", "align": "left"},
                    {"key": "count", "label": "Count", "align": "right"},
                    {"key": "top_metric", "label": "Top Metric", "align": "right"},
                    {"key": "latest_date", "label": "Latest", "align": "left"},
                ],
                rows,
            )

    if analysis_type == "calendar_day_player_leaderboard":
        leaders = payload.get("leaders")
        metric_label = str(payload.get("metric") or "Total")
        if isinstance(leaders, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player_name", "label": "Player", "align": "left"},
                    {"key": "total", "label": metric_label, "align": "right"},
                ],
                leaders,
            )

    if analysis_type == "calendar_day_total":
        top_players = payload.get("top_players")
        metric_label = str(payload.get("metric_label") or "Total")
        if isinstance(top_players, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player_name", "label": "Player", "align": "left"},
                    {"key": "total", "label": metric_label, "align": "right"},
                ],
                top_players,
            )

    if analysis_type == "calendar_day_pitching_leaderboard":
        leaders = payload.get("leaders")
        if isinstance(leaders, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player_name", "label": "Pitcher", "align": "left"},
                    {"key": "game_date", "label": "Date", "align": "left"},
                    {"key": "team", "label": "Team", "align": "left"},
                    {"key": "opponent", "label": "Opp", "align": "left"},
                    {"key": "game_score", "label": "GmSc", "align": "right"},
                    {"key": "innings_pitched", "label": "IP", "align": "right"},
                    {"key": "strikeouts", "label": "SO", "align": "right"},
                    {"key": "hits_allowed", "label": "H", "align": "right"},
                    {"key": "walks_allowed", "label": "BB", "align": "right"},
                    {"key": "earned_runs", "label": "ER", "align": "right"},
                ],
                leaders,
            )

    if analysis_type == "daily_lookup_total":
        top_players = payload.get("top_players")
        if isinstance(top_players, list) and top_players:
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player_name", "label": "Player", "align": "left"},
                    {"key": "total", "label": "Total", "align": "right"},
                ],
                top_players,
            )

    if analysis_type == "birthday_home_run_leaderboard":
        leaders = payload.get("leaders")
        if isinstance(leaders, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player_name", "label": "Player", "align": "left"},
                    {"key": "total", "label": "Birthday HR", "align": "right"},
                    {"key": "birthday_games", "label": "Birthday G", "align": "right"},
                    {"key": "first_season", "label": "First", "align": "right"},
                    {"key": "last_season", "label": "Last", "align": "right"},
                ],
                leaders,
            )

    if analysis_type == "player_count_leaderboard":
        leaders = payload.get("leaders")
        metric_label = str(payload.get("metric") or "Metric")
        if isinstance(leaders, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player_name", "label": "Player", "align": "left"},
                    {"key": "metric_value", "label": metric_label, "align": "right"},
                    {"key": "at_bats", "label": "AB", "align": "right"},
                    {"key": "hits", "label": "H", "align": "right"},
                    {"key": "plate_appearances", "label": "PA", "align": "right"},
                    {"key": "home_runs", "label": "HR", "align": "right"},
                    {"key": "first_season", "label": "First", "align": "right"},
                    {"key": "last_season", "label": "Last", "align": "right"},
                ],
                leaders,
            )

    if analysis_type == "player_team_context_leaderboard":
        leaders = payload.get("leaders")
        metric_label = str(payload.get("metric") or "Metric")
        if isinstance(leaders, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player_name", "label": "Player", "align": "left"},
                    {"key": "opponent_name", "label": "Opponent", "align": "left"},
                    {"key": "metric_value", "label": metric_label, "align": "right"},
                    {"key": "plate_appearances", "label": "PA", "align": "right"},
                    {"key": "at_bats", "label": "AB", "align": "right"},
                    {"key": "hits", "label": "H", "align": "right"},
                    {"key": "home_runs", "label": "HR", "align": "right"},
                    {"key": "runs_batted_in", "label": "RBI", "align": "right"},
                ],
                leaders,
            )

    if analysis_type == "player_situational_leaderboard":
        leaders = payload.get("leaders")
        metric_label = str(payload.get("metric") or "Metric")
        if isinstance(leaders, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player_name", "label": "Player", "align": "left"},
                    {"key": "team", "label": "Team", "align": "left"},
                    {"key": "metric_value", "label": metric_label, "align": "right"},
                    {"key": "plate_appearances", "label": "PA", "align": "right"},
                    {"key": "at_bats", "label": "AB", "align": "right"},
                    {"key": "hits", "label": "H", "align": "right"},
                    {"key": "home_runs", "label": "HR", "align": "right"},
                    {"key": "walks", "label": "BB", "align": "right"},
                    {"key": "strikeouts", "label": "SO", "align": "right"},
                ],
                leaders,
            )

    if analysis_type == "team_season_comparison":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "team", "label": "Team-Season", "align": "left"},
                    {"key": "scope", "label": "Scope", "align": "left"},
                    {"key": "games", "label": "G", "align": "right"},
                    {"key": "record", "label": "Record", "align": "left"},
                    {"key": "win_pct", "label": "Win%", "align": "right"},
                    {"key": "runs_per_game", "label": "R/G", "align": "right"},
                    {"key": "runs_allowed_per_game", "label": "RA/G", "align": "right"},
                    {"key": "run_diff_per_game", "label": "RD/G", "align": "right"},
                    {"key": "ops", "label": "OPS", "align": "right"},
                    {"key": "era", "label": "ERA", "align": "right"},
                ],
                rows,
            )

    if analysis_type == "historical_team_analysis":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "team", "label": "Team-Season", "align": "left"},
                    {"key": "games", "label": "G", "align": "right"},
                    {"key": "record", "label": "Record", "align": "left"},
                    {"key": "win_pct", "label": "Win%", "align": "right"},
                    {"key": "runs_per_game", "label": "R/G", "align": "right"},
                    {"key": "runs_allowed_per_game", "label": "RA/G", "align": "right"},
                    {"key": "run_diff_per_game", "label": "RD/G", "align": "right"},
                    {"key": "ops", "label": "OPS", "align": "right"},
                    {"key": "era", "label": "ERA", "align": "right"},
                    {"key": "fielding_pct", "label": "Fld%", "align": "right"},
                ],
                rows,
            )

    if analysis_type == "historical_manager_lookup":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "manager", "label": "Manager", "align": "left"},
                    {"key": "games", "label": "G", "align": "right"},
                    {"key": "wins", "label": "W", "align": "right"},
                    {"key": "losses", "label": "L", "align": "right"},
                    {"key": "finish", "label": "Finish", "align": "right"},
                    {"key": "player_manager", "label": "Plyr-Mgr", "align": "left"},
                ],
                rows,
            )

    if analysis_type in {"manager_era_offense", "manager_era_defense"}:
        rows = payload.get("rows")
        if isinstance(rows, list):
            if analysis_type == "manager_era_offense":
                return build_table_display(
                    [
                        {"key": "rank", "label": "#", "align": "right"},
                        {"key": "player", "label": "Player", "align": "left"},
                        {"key": "plate_appearances", "label": "PA", "align": "right"},
                        {"key": "ops", "label": "OPS", "align": "right"},
                        {"key": "wrc_plus", "label": "wRC+", "align": "right"},
                        {"key": "home_runs", "label": "HR", "align": "right"},
                        {"key": "runs_batted_in", "label": "RBI", "align": "right"},
                        {"key": "war", "label": "WAR", "align": "right"},
                    ],
                    rows,
                )
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player", "label": "Player", "align": "left"},
                    {"key": "positions", "label": "Pos", "align": "left"},
                    {"key": "games", "label": "G", "align": "right"},
                    {"key": "drs", "label": "DRS", "align": "right"},
                    {"key": "def", "label": "Def", "align": "right"},
                    {"key": "oaa", "label": "OAA", "align": "right"},
                    {"key": "innings", "label": "Inn", "align": "right"},
                ],
                rows,
            )

    if analysis_type == "career_earnings_leaderboard":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player", "label": "Player", "align": "left"},
                    {"key": "birthcountry", "label": "Birth Country", "align": "left"},
                    {"key": "career_earnings", "label": "Earnings", "align": "right"},
                    {"key": "first_year", "label": "First", "align": "right"},
                    {"key": "last_year", "label": "Last", "align": "right"},
                ],
                rows,
            )

    if analysis_type == "player_salary_analysis":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "season", "label": "Season", "align": "right"},
                    {"key": "salary", "label": "Salary", "align": "right"},
                    {"key": "games", "label": "G", "align": "right"},
                    {"key": "hits", "label": "H", "align": "right"},
                    {"key": "runs", "label": "R", "align": "right"},
                    {"key": "home_runs", "label": "HR", "align": "right"},
                    {"key": "ops", "label": "OPS", "align": "right"},
                    {"key": "salary_per_game", "label": "$/G", "align": "right"},
                    {"key": "salary_per_hit", "label": "$/H", "align": "right"},
                    {"key": "salary_per_run", "label": "$/R", "align": "right"},
                ],
                rows,
            )

    if analysis_type == "statcast_relationship_aggregate":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "pitcher", "label": "Pitcher", "align": "left"},
                    {"key": "count", "label": "Count", "align": "right"},
                    {"key": "top_metric", "label": "Top Metric", "align": "right"},
                    {"key": "latest_date", "label": "Latest", "align": "left"},
                ],
                rows,
            )

    if analysis_type == "statcast_relationship_events":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "game_date", "label": "Date", "align": "left"},
                    {"key": "batter", "label": "Batter", "align": "left"},
                    {"key": "pitcher", "label": "Pitcher", "align": "left"},
                    {"key": "pitch_name", "label": "Pitch", "align": "left"},
                    {"key": "event", "label": "Event", "align": "left"},
                    {"key": "release_speed", "label": "Velo", "align": "right"},
                    {"key": "release_spin_rate", "label": "Spin", "align": "right"},
                    {"key": "description", "label": "Play", "align": "left"},
                ],
                rows,
            )

    if analysis_type == "player_season_analysis":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "scope", "label": "Scope", "align": "left"},
                    {"key": "season", "label": "Season", "align": "right"},
                    {"key": "team", "label": "Team", "align": "left"},
                    {"key": "games", "label": "G", "align": "right"},
                    {"key": "pa", "label": "PA", "align": "right"},
                    {"key": "avg", "label": "AVG", "align": "right"},
                    {"key": "obp", "label": "OBP", "align": "right"},
                    {"key": "slg", "label": "SLG", "align": "right"},
                    {"key": "ops", "label": "OPS", "align": "right"},
                    {"key": "hr", "label": "HR", "align": "right"},
                    {"key": "rbi", "label": "RBI", "align": "right"},
                    {"key": "bb", "label": "BB", "align": "right"},
                    {"key": "so", "label": "SO", "align": "right"},
                ],
                rows,
            )

    if analysis_type == "player_start_comparison":
        rows = payload.get("rows")
        metric_label = str(payload.get("metric") or "Metric")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "season", "label": "Season", "align": "right"},
                    {"key": "scope", "label": "Scope", "align": "left"},
                    {"key": "team", "label": "Team", "align": "left"},
                    {"key": "games", "label": "G", "align": "right"},
                    {"key": "metric_value", "label": metric_label, "align": "right"},
                    {"key": "avg", "label": "AVG", "align": "right"},
                    {"key": "obp", "label": "OBP", "align": "right"},
                    {"key": "slg", "label": "SLG", "align": "right"},
                    {"key": "ops", "label": "OPS", "align": "right"},
                    {"key": "hr", "label": "HR", "align": "right"},
                    {"key": "rbi", "label": "RBI", "align": "right"},
                    {"key": "bb", "label": "BB", "align": "right"},
                    {"key": "so", "label": "SO", "align": "right"},
                    {"key": "pa", "label": "PA", "align": "right"},
                ],
                rows,
            )

    if analysis_type == "player_season_comparison":
        rows = payload.get("rows")
        metric_label = str(payload.get("metric") or "Metric")
        role = str(payload.get("role") or "")
        if isinstance(rows, list):
            if role == "pitching":
                columns = [
                    {"key": "player", "label": "Player", "align": "left"},
                    {"key": "season", "label": "Season", "align": "right"},
                    {"key": "scope", "label": "Scope", "align": "left"},
                    {"key": "team", "label": "Team", "align": "left"},
                    {"key": "metric_value", "label": metric_label, "align": "right"},
                    {"key": "season_rank", "label": "Rank", "align": "right"},
                    {"key": "historical_percentile", "label": "Pctile", "align": "right"},
                    {"key": "ip", "label": "IP", "align": "right"},
                    {"key": "era", "label": "ERA", "align": "right"},
                    {"key": "whip", "label": "WHIP", "align": "right"},
                    {"key": "k_per_9", "label": "K/9", "align": "right"},
                    {"key": "bb_per_9", "label": "BB/9", "align": "right"},
                    {"key": "wins", "label": "W", "align": "right"},
                    {"key": "saves", "label": "SV", "align": "right"},
                ]
            else:
                columns = [
                    {"key": "player", "label": "Player", "align": "left"},
                    {"key": "season", "label": "Season", "align": "right"},
                    {"key": "scope", "label": "Scope", "align": "left"},
                    {"key": "team", "label": "Team", "align": "left"},
                    {"key": "metric_value", "label": metric_label, "align": "right"},
                    {"key": "season_rank", "label": "Rank", "align": "right"},
                    {"key": "historical_percentile", "label": "Pctile", "align": "right"},
                    {"key": "pa", "label": "PA", "align": "right"},
                    {"key": "avg", "label": "AVG", "align": "right"},
                    {"key": "obp", "label": "OBP", "align": "right"},
                    {"key": "slg", "label": "SLG", "align": "right"},
                    {"key": "ops", "label": "OPS", "align": "right"},
                    {"key": "hr", "label": "HR", "align": "right"},
                    {"key": "rbi", "label": "RBI", "align": "right"},
                    {"key": "sb", "label": "SB", "align": "right"},
                ]
            return build_table_display(columns, rows)

    if analysis_type == "player_metric_lookup":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "player", "label": "Player", "align": "left"},
                    {"key": "season", "label": "Season", "align": "right"},
                    {"key": "team", "label": "Team", "align": "left"},
                    {"key": "group", "label": "Group", "align": "left"},
                    {"key": "metric", "label": "Metric", "align": "left"},
                    {"key": "value", "label": "Value", "align": "right"},
                    {"key": "context_1", "label": "Context", "align": "left"},
                    {"key": "context_2", "label": "Extra", "align": "left"},
                ],
                rows,
            )

    if analysis_type == "player_window_metric":
        rows = payload.get("rows")
        metric_label = str(payload.get("metric") or "Metric")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "game_date", "label": "Date", "align": "left"},
                    {"key": "matchup", "label": "Matchup", "align": "left"},
                    {"key": "metric_total", "label": metric_label, "align": "right"},
                    {"key": "plate_appearances", "label": "PA", "align": "right"},
                    {"key": "hits", "label": "H", "align": "right"},
                    {"key": "walks", "label": "BB", "align": "right"},
                    {"key": "strikeouts", "label": "SO", "align": "right"},
                    {"key": "rbi", "label": "RBI", "align": "right"},
                ],
                rows,
            )

    if analysis_type == "statcast_event_leaderboard":
        leaders = payload.get("leaders")
        metric_label = str(payload.get("metric") or "Metric")
        aggregate_mode = str(payload.get("aggregate_mode") or "events")
        if isinstance(leaders, list):
            if aggregate_mode == "player":
                columns = [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player_name", "label": "Player", "align": "left"},
                    {"key": "metric_value", "label": metric_label, "align": "right"},
                    {"key": "game_date", "label": "Date", "align": "left"},
                    {"key": "team_matchup", "label": "Game", "align": "left"},
                    {"key": "event", "label": "Event", "align": "left"},
                ]
            else:
                columns = [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player_name", "label": "Player", "align": "left"},
                    {"key": "game_date", "label": "Date", "align": "left"},
                    {"key": "team_matchup", "label": "Game", "align": "left"},
                    {"key": "event", "label": "Event", "align": "left"},
                    {"key": "metric_value", "label": metric_label, "align": "right"},
                    {"key": "hit_distance", "label": "Dist", "align": "right"},
                    {"key": "launch_angle", "label": "LA", "align": "right"},
                ]
            return build_table_display(columns, leaders)

    if analysis_type == "roster_comparison":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "team", "label": "Team-Season", "align": "left"},
                    {"key": "scope", "label": "Scope", "align": "left"},
                    {"key": "roster_size", "label": "Roster", "align": "right"},
                    {"key": "hitters", "label": "Hitters", "align": "right"},
                    {"key": "pitchers", "label": "Pitchers", "align": "right"},
                    {"key": "avg_age", "label": "Avg Age", "align": "right"},
                    {"key": "lineup_depth_ops", "label": "Top-6 OPS", "align": "right"},
                    {"key": "rotation_era", "label": "Rotation ERA", "align": "right"},
                    {"key": "bullpen_era", "label": "Bullpen ERA", "align": "right"},
                    {"key": "top_hitter", "label": "Top Bat", "align": "left"},
                    {"key": "top_hitter_ops", "label": "Bat OPS", "align": "right"},
                    {"key": "top_arm", "label": "Top Arm", "align": "left"},
                    {"key": "top_arm_era", "label": "Arm ERA", "align": "right"},
                ],
                rows,
            )

    if analysis_type == "home_run_robbery_proxy":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "fielder_name", "label": "Fielder", "align": "left"},
                    {"key": "robbery_count", "label": "Robberies", "align": "right"},
                    {"key": "proxy_runs", "label": "rHR Proxy", "align": "right"},
                ],
                rows,
            )

    if analysis_type == "defensive_performance":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "player", "label": "Player", "align": "left"},
                    {"key": "play_count", "label": "Plays", "align": "right"},
                    {"key": "score", "label": "Score", "align": "right"},
                ],
                rows,
            )

    if analysis_type in {"defensive_plays", "coolest_plays", "weird_plays", "home_run_distance"}:
        plays = payload.get("plays")
        if isinstance(plays, list):
            return build_table_display(
                [
                    {"key": "actor_name", "label": "Player", "align": "left"},
                    {"key": "game_date", "label": "Date", "align": "left"},
                    {"key": "team_matchup", "label": "Matchup", "align": "left"},
                    {"key": "inning_label", "label": "Inning", "align": "left"},
                    {"key": "title", "label": "Play", "align": "left"},
                    {"key": "explanation", "label": "Why It Mattered", "align": "left"},
                ],
                [
                    {
                        "actor_name": play.get("actor_name", ""),
                        "game_date": play.get("game_date", ""),
                        "team_matchup": play.get("team_matchup", ""),
                        "inning_label": f"{int(play.get('inning') or 0)}{ordinal_suffix(int(play.get('inning') or 0))} {play.get('half_inning', '')}".strip()
                        if play.get("inning")
                        else "",
                        "title": play.get("title", ""),
                        "explanation": play.get("explanation", ""),
                    }
                    for play in plays
                ],
            )

    if analysis_type == "team_start_similarity":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "rank", "label": "#", "align": "right"},
                    {"key": "team", "label": "Team-Season", "align": "left"},
                    {"key": "record", "label": "Record", "align": "left"},
                    {"key": "games", "label": "G", "align": "right"},
                    {"key": "win_pct", "label": "Win%", "align": "right"},
                    {"key": "runs_per_game", "label": "R/G", "align": "right"},
                    {"key": "runs_allowed_per_game", "label": "RA/G", "align": "right"},
                    {"key": "ops", "label": "OPS", "align": "right"},
                    {"key": "era", "label": "ERA", "align": "right"},
                    {"key": "similarity_score", "label": "Similarity", "align": "right"},
                ],
                rows,
            )

    if analysis_type == "pitching_staff_comparison":
        rows = payload.get("rows")
        if isinstance(rows, list):
            return build_table_display(
                [
                    {"key": "team", "label": "Team-Season", "align": "left"},
                    {"key": "innings", "label": "IP", "align": "right"},
                    {"key": "era", "label": "ERA", "align": "right"},
                    {"key": "whip", "label": "WHIP", "align": "right"},
                    {"key": "strikeouts_per_9", "label": "K/9", "align": "right"},
                    {"key": "walks_per_9", "label": "BB/9", "align": "right"},
                ],
                rows,
            )

    rows = payload.get("rows")
    if isinstance(rows, list) and rows and all(isinstance(row, dict) for row in rows):
        return build_generic_rows_display(snippet, rows)
    return None


def build_table_display(columns: list[dict[str, str]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    formatted_rows: list[dict[str, str]] = []
    for index, row in enumerate(rows, start=1):
        rendered: dict[str, str] = {"rank": str(index)}
        for column in columns:
            key = column["key"]
            if key == "rank":
                continue
            rendered[key] = format_display_value(row.get(key))
        formatted_rows.append(rendered)
    return {"kind": "table", "columns": columns, "rows": formatted_rows}


def build_generic_rows_display(snippet: EvidenceSnippet, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    ordered_keys = preferred_row_keys(rows[0], snippet.source)
    if not ordered_keys:
        return None
    columns = [{"key": "rank", "label": "#", "align": "right"}]
    for key in ordered_keys:
        columns.append({"key": key, "label": humanize_column_label(key), "align": infer_alignment(key, rows)})
    return build_table_display(columns, rows)


def preferred_row_keys(row: dict[str, Any], source: str) -> list[str]:
    preferred_sequences = [
        ["player", "season", "pos_abbr", "total"],
        ["nickname", "season", "total"],
        ["player", "career_value", "first_season", "last_season"],
        ["player", "pos_abbr", "games", "innings", "total"],
    ]
    existing_keys = [key for key in row.keys() if not key.endswith("_id") and key not in {"snapshot_at", "source_name"}]
    for sequence in preferred_sequences:
        present = [key for key in sequence if key in row]
        if present:
            extras = [key for key in existing_keys if key not in present][:2]
            return [*present, *extras]
    if "Fielding Bible" in source:
        return existing_keys[:5]
    return existing_keys[:5]


def humanize_column_label(key: str) -> str:
    overrides = {
        "pos_abbr": "Pos",
        "games": "G",
        "innings": "Inn",
        "career_value": "Career",
        "metric_value": "Value",
        "team_name": "Team",
        "player_name": "Player",
    }
    if key in overrides:
        return overrides[key]
    return key.replace("_", " ").title()


def infer_alignment(key: str, rows: list[dict[str, Any]]) -> str:
    if any(term in key for term in ("name", "team", "player", "pos", "group")):
        return "left"
    for row in rows[:3]:
        value = row.get(key)
        if isinstance(value, (int, float)):
            return "right"
        if isinstance(value, str):
            try:
                float(value)
                return "right"
            except ValueError:
                continue
    return "left"


def format_display_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if abs(value) >= 100 or value.is_integer():
            return str(int(value))
        text = f"{value:.3f}"
        return text.rstrip("0").rstrip(".")
    return str(value)
