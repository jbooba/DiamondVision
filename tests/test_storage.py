from __future__ import annotations

import sqlite3

from mlb_history_bot.storage import audit_statcast_history_table


def test_audit_statcast_history_table_reports_player_year_gaps() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE statcast_history_pitcher_seasons (
            last_name_first_name TEXT,
            player_id TEXT,
            year TEXT,
            meatball_percent TEXT
        )
        """
    )
    connection.executemany(
        """
        INSERT INTO statcast_history_pitcher_seasons(
            last_name_first_name, player_id, year, meatball_percent
        ) VALUES (?, ?, ?, ?)
        """,
        [
            ("Eovaldi, Nathan", "543135", "2021", "8.2"),
            ("Eovaldi, Nathan", "543135", "2024", "6.8"),
            ("Eovaldi, Nathan", "543135", "2026", "5.4"),
            ("Skubal, Tarik", "669373", "2025", "7.1"),
        ],
    )
    connection.commit()

    audit = audit_statcast_history_table(
        connection,
        "statcast_history_pitcher_seasons",
        player_name="Nathan Eovaldi",
    )

    assert audit["exists"] is True
    assert audit["row_count"] == 4
    assert audit["year_counts"] == [
        {"season": 2021, "row_count": 1},
        {"season": 2024, "row_count": 1},
        {"season": 2025, "row_count": 1},
        {"season": 2026, "row_count": 1},
    ]
    assert len(audit["player_matches"]) == 1
    match = audit["player_matches"][0]
    assert match["player_name"] == "Eovaldi, Nathan"
    assert match["seasons"] == [2021, 2024, 2026]
    assert match["missing_between"] == [2022, 2023, 2025]

    connection.close()
