from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date, timedelta


MONTH_DAY_YEAR_PATTERN = re.compile(
    r"\b("
    r"january|february|march|april|may|june|july|august|september|october|november|december"
    r")\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s+(\d{4}))?\b",
    re.IGNORECASE,
)
SLASH_DATE_PATTERN = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b")
YEAR_PATTERN = re.compile(r"\b(18\d{2}|19\d{2}|20\d{2})\b")
FIRST_N_GAMES_PATTERN = re.compile(
    r"\b(?:through|thru|over|in|across)\s+(?:the\s+)?first\s+([a-z0-9-]+(?:\s+[a-z0-9-]+){0,3})\s+games?\b"
    r"|\bfirst\s+([a-z0-9-]+(?:\s+[a-z0-9-]+){0,3})\s+games?\b",
    re.IGNORECASE,
)
MINIMUM_QUALIFIER_PATTERNS = (
    re.compile(
        r"\b(?:minimum|min)\s+(?:of\s+)?([a-z0-9-]+(?:\s+[a-z0-9-]+){0,3})\s+({terms})\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bat\s+least\s+([a-z0-9-]+(?:\s+[a-z0-9-]+){0,3})\s+({terms})\b",
        re.IGNORECASE,
    ),
)
LAST_N_SEASONS_PATTERN = re.compile(
    r"\b(?P<kind>last|past|previous)\s+(?P<count>[a-z0-9-]+(?:\s+[a-z0-9-]+){0,2})\s+(?P<unit>years?|seasons?)\b",
    re.IGNORECASE,
)
SINCE_SEASON_PATTERN = re.compile(r"\bsince\s+(18\d{2}|19\d{2}|20\d{2})\b", re.IGNORECASE)
FROM_TO_SEASON_PATTERN = re.compile(
    r"\bfrom\s+(18\d{2}|19\d{2}|20\d{2})\s+(?:through|thru|to|-)\s+(18\d{2}|19\d{2}|20\d{2})\b",
    re.IGNORECASE,
)
BETWEEN_AND_SEASON_PATTERN = re.compile(
    r"\bbetween\s+(18\d{2}|19\d{2}|20\d{2})\s+and\s+(18\d{2}|19\d{2}|20\d{2})\b",
    re.IGNORECASE,
)

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

NUMBER_WORDS = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
    "hundred": 100,
}

LEADING_QUESTION_WORDS = {
    "Was",
    "Were",
    "Is",
    "Are",
    "Did",
    "Does",
    "Do",
    "Can",
    "Could",
    "Should",
    "Would",
    "Will",
    "Has",
    "Have",
    "Had",
    "What",
    "Who",
    "Compare",
    "Show",
    "Analyze",
    "Assess",
    "Evaluate",
    "Break",
}
LOWERCASE_NAME_LEADIN_PATTERNS = (
    re.compile(
        r"\bwho\s+(?:is|was)\s+(?!the\b|a\b|an\b)([a-z][a-z'.-]+(?:\s+[a-z][a-z'.-]+){1,3})\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\btell\s+me\s+about\s+(?!the\b|a\b|an\b)([a-z][a-z'.-]+(?:\s+[a-z][a-z'.-]+){1,3})\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bshow\s+me\s+(?:a|an|the)?\s*(?:clip|clips|video|videos|replay|replays|highlight|highlights)\s+of\s+"
        r"(?:(?:the|a|an)\s+)?([a-z][a-z'.-]+(?:\s+[a-z][a-z'.-]+){1,3})"
        r"(?=\s+(?:home\s+runs?|homeruns?|homers?|clips?|videos?|replays?|highlights?|hits?|singles?|doubles?|triples?|"
        r"strikeouts?|walks?|stolen\s+bases?|defensive|fielding|batting|pitching)\b|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bshow\s+me\s+(?!the\b|a\b|an\b)([a-z][a-z'.-]+(?:\s+[a-z][a-z'.-]+){1,3})"
        r"(?=\s+(?:home\s+runs?|homeruns?|homers?|clips?|videos?|replays?|highlights?|hits?|singles?|doubles?|triples?|"
        r"strikeouts?|walks?|stolen\s+bases?|defensive|fielding|batting|pitching))",
        re.IGNORECASE,
    ),
)
CURRENT_SCOPE_HINTS = {
    "today",
    "tonight",
    "yesterday",
    "last night",
    "this week",
    "last week",
    "current",
    "right now",
    "latest",
    "this season",
    "season so far",
    "this year",
    "current season",
    "current year",
    "to date",
    "so far",
}
RECENT_WINDOW_LABELS = {"today", "tonight", "yesterday", "last night", "this week", "last week"}


@dataclass(slots=True)
class DateWindow:
    start_date: date
    end_date: date
    label: str

    @property
    def is_single_day(self) -> bool:
        return self.start_date == self.end_date


@dataclass(slots=True)
class SeasonSpan:
    start_season: int
    end_season: int
    label: str
    aggregate: bool = True


def extract_target_date(question: str, default_year: int) -> date | None:
    window = extract_date_window(question, default_year)
    if window is None or not window.is_single_day:
        return None
    return window.start_date


def question_mentions_explicit_year(question: str) -> bool:
    if re.search(r"\b\d{4}-\d{2}-\d{2}\b", question):
        return True
    if SLASH_DATE_PATTERN.search(question):
        return True
    if YEAR_PATTERN.search(question):
        return True
    return False


def extract_explicit_year(question: str) -> int | None:
    match = YEAR_PATTERN.search(question)
    return int(match.group(1)) if match else None


def extract_referenced_season(question: str, current_season: int) -> int | None:
    explicit_year = extract_explicit_year(question)
    if explicit_year is not None:
        return explicit_year
    lowered = question.lower()
    if "last year" in lowered or "last season" in lowered or "previous season" in lowered:
        return current_season - 1
    if "this year" in lowered or "this season" in lowered or "current season" in lowered or "current year" in lowered:
        return current_season
    return None


def extract_season_span(question: str, current_season: int) -> SeasonSpan | None:
    lowered = question.lower()
    match = FROM_TO_SEASON_PATTERN.search(lowered)
    if match:
        start_season = int(match.group(1))
        end_season = int(match.group(2))
        if start_season > end_season:
            start_season, end_season = end_season, start_season
        return SeasonSpan(start_season, end_season, f"{start_season}-{end_season}")

    match = BETWEEN_AND_SEASON_PATTERN.search(lowered)
    if match:
        start_season = int(match.group(1))
        end_season = int(match.group(2))
        if start_season > end_season:
            start_season, end_season = end_season, start_season
        return SeasonSpan(start_season, end_season, f"{start_season}-{end_season}")

    match = SINCE_SEASON_PATTERN.search(lowered)
    if match:
        start_season = int(match.group(1))
        end_season = current_season
        if start_season > end_season:
            return None
        return SeasonSpan(start_season, end_season, f"since {start_season}")

    match = LAST_N_SEASONS_PATTERN.search(lowered)
    if match:
        count = parse_number_token(match.group("count"))
        if count is None or count <= 0:
            return None
        kind = match.group("kind").lower()
        if kind == "previous":
            end_season = current_season - 1
            start_season = end_season - count + 1
        else:
            end_season = current_season
            start_season = end_season - count + 1
        if start_season > end_season:
            return None
        label = f"{start_season}-{end_season}"
        return SeasonSpan(start_season, end_season, label)

    return None


def question_requests_current_scope(question: str) -> bool:
    lowered = question.lower()
    return any(token in lowered for token in CURRENT_SCOPE_HINTS)


def question_mentions_specific_date_reference(question: str) -> bool:
    lowered = question.lower()
    if any(token in lowered for token in ("today", "tonight", "yesterday", "last night")):
        return True
    if re.search(r"\b\d{4}-\d{2}-\d{2}\b", question):
        return True
    if SLASH_DATE_PATTERN.search(question):
        return True
    if MONTH_DAY_YEAR_PATTERN.search(question):
        return True
    return False


def question_mentions_yearless_month_day(question: str) -> bool:
    match = MONTH_DAY_YEAR_PATTERN.search(question)
    return bool(match and not match.group(3))


def extract_first_n_games(question: str) -> int | None:
    match = FIRST_N_GAMES_PATTERN.search(question)
    if match is None:
        return None
    value = match.group(1) or match.group(2)
    return parse_number_token(value) if value else None


def extract_minimum_qualifier(question: str, nouns: tuple[str, ...]) -> int | None:
    noun_pattern = "|".join(re.escape(noun) for noun in nouns if noun)
    if not noun_pattern:
        return None
    for template in MINIMUM_QUALIFIER_PATTERNS:
        pattern = re.compile(template.pattern.replace("{terms}", noun_pattern), re.IGNORECASE)
        match = pattern.search(question)
        if match is None:
            continue
        value = parse_number_token(match.group(1))
        if value is not None:
            return value
    return None


def extract_date_window(
    question: str,
    default_year: int,
    *,
    today: date | None = None,
    include_yearless_month_day: bool = False,
) -> DateWindow | None:
    reference_today = today or date.today()
    lowered = question.lower()

    if "today" in lowered or "tonight" in lowered:
        return DateWindow(reference_today, reference_today, "today")
    if "yesterday" in lowered or "last night" in lowered:
        previous_day = reference_today - timedelta(days=1)
        return DateWindow(previous_day, previous_day, "yesterday")
    if "this week" in lowered:
        week_start = reference_today - timedelta(days=reference_today.weekday())
        return DateWindow(week_start, reference_today, "this week")
    if "last week" in lowered:
        this_week_start = reference_today - timedelta(days=reference_today.weekday())
        week_end = this_week_start - timedelta(days=1)
        week_start = week_end - timedelta(days=6)
        return DateWindow(week_start, week_end, "last week")

    iso_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", question)
    if iso_match:
        target = date.fromisoformat(iso_match.group(1))
        return DateWindow(target, target, target.isoformat())

    slash_match = SLASH_DATE_PATTERN.search(question)
    if slash_match:
        month = int(slash_match.group(1))
        day_value = int(slash_match.group(2))
        year_value = normalize_slash_year(slash_match.group(3), default_year)
        target = date(year_value, month, day_value)
        return DateWindow(target, target, target.isoformat())

    mdy_match = MONTH_DAY_YEAR_PATTERN.search(question)
    if mdy_match:
        month = MONTHS[mdy_match.group(1).lower()]
        day_value = int(mdy_match.group(2))
        if not mdy_match.group(3) and not include_yearless_month_day:
            return None
        year_value = int(mdy_match.group(3)) if mdy_match.group(3) else default_year
        target = date(year_value, month, day_value)
        return DateWindow(target, target, target.isoformat())

    return None


def normalize_slash_year(raw_year: str, default_year: int) -> int:
    year_value = int(raw_year)
    if len(raw_year) == 4:
        return year_value
    cutoff = (default_year % 100) + 1
    return 2000 + year_value if year_value <= cutoff else 1900 + year_value


def extract_calendar_day_window(question: str, default_year: int) -> DateWindow | None:
    match = MONTH_DAY_YEAR_PATTERN.search(question)
    if not match or match.group(3):
        return None
    month = MONTHS[match.group(1).lower()]
    day_value = int(match.group(2))
    target = date(default_year, month, day_value)
    return DateWindow(target, target, f"{target.strftime('%B')} {target.day}")


def extract_recent_window(
    question: str,
    default_year: int,
    *,
    allowed_labels: set[str] | None = None,
    today: date | None = None,
) -> DateWindow | None:
    window = extract_date_window(question, default_year, today=today)
    if window is None:
        return None
    if allowed_labels is not None and window.label not in allowed_labels:
        return None
    return window


def extract_name_candidates(question: str) -> list[str]:
    matches = re.findall(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b", question)
    unique: list[str] = []
    for match in matches:
        candidate = normalize_name_candidate(match)
        if candidate and candidate not in unique:
            unique.append(candidate)
    if unique:
        return unique
    for pattern in LOWERCASE_NAME_LEADIN_PATTERNS:
        for match in pattern.finditer(question):
            candidate = normalize_name_candidate(match.group(1).title())
            if candidate and candidate not in unique:
                unique.append(candidate)
    return unique


def normalize_name_candidate(candidate: str) -> str:
    candidate = re.sub(
        r"^(?:clip|clips|video|videos|replay|replays|highlight|highlights)\s+of\s+",
        "",
        candidate,
        flags=re.IGNORECASE,
    ).strip()
    candidate = re.sub(
        r"\s+(?:home\s+runs?|homeruns?|homers?|clips?|videos?|replays?|highlights?|hits?|singles?|doubles?|triples?|"
        r"strikeouts?|walks?|stolen\s+bases?|defensive|fielding|batting|pitching)\b.*$",
        "",
        candidate,
        flags=re.IGNORECASE,
    ).strip()
    words = candidate.split()
    if len(words) >= 3 and words[0] in LEADING_QUESTION_WORDS:
        words = words[1:]
    return " ".join(words)


def normalize_person_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = "".join(character for character in normalized if not unicodedata.combining(character))
    return ascii_only.casefold().strip()


def ordinal(value: int) -> str:
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def parse_number_token(value: str) -> int | None:
    token = value.strip().lower()
    if not token:
        return None
    if token.isdigit():
        return int(token)

    parts = token.replace("-", " ").split()
    if not parts:
        return None

    total = 0
    current = 0
    for part in parts:
        if part not in NUMBER_WORDS:
            return None
        amount = NUMBER_WORDS[part]
        if amount == 100:
            current = max(current, 1) * amount
        else:
            current += amount
    total += current
    return total or None
