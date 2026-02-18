"""ESPN Hidden API client for Six Nations rugby data extraction."""

import csv
import json
import os
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/rugby"
LEAGUE_ID = "180659"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")

# Six Nations seasons: the "2000-01" season has matches in calendar year 2001, etc.
# Exception: "2000" season = matches in Feb-Apr 2000 (the first Six Nations)
SEASONS = []
for year in range(2000, 2026):
    SEASONS.append(year)


def fetch_json(url, retries=3):
    """Fetch JSON from a URL with retries."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            print(f"  Attempt {attempt+1}/{retries} failed for {url}: {type(e).__name__}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
    return None


def get_season_matches(year, league_id=None):
    """Get all match IDs and basic info for a given calendar year.

    The Six Nations typically runs Feb-Mar, but some years had delays:
    - 2001: foot-and-mouth postponed Ireland matches to Sep-Oct
    - 2020: COVID postponed some matches to Oct
    Uses a single full-year query which returns all matches.
    """
    lid = league_id or LEAGUE_ID
    url = f"{BASE_URL}/{lid}/scoreboard?dates={year}0101-{year}1231"
    data = fetch_json(url)
    if not data or "events" not in data:
        return []

    return data["events"]


def parse_scoreboard_event(event, year):
    """Extract match info and events from a scoreboard event."""
    comp = event["competitions"][0]

    # Determine home/away teams
    home_team = away_team = None
    home_score = away_score = None
    for competitor in comp["competitors"]:
        team_name = competitor["team"]["displayName"]
        score = competitor.get("score", "0")
        if competitor["homeAway"] == "home":
            home_team = team_name
            home_score = int(score) if score else 0
        else:
            away_team = team_name
            away_score = int(score) if score else 0

    venue = comp.get("venue", {}).get("fullName", "")
    date_str = event.get("date", "")

    # Parse match events (subs, tries, etc.)
    details = comp.get("details", [])
    match_events = []
    for detail in details:
        event_type = detail.get("type", {}).get("text", "")
        clock = detail.get("clock", {})
        minute_str = clock.get("displayValue", "")
        minute_val = clock.get("value", 0)
        team_id = detail.get("team", {}).get("id", "")

        athletes = []
        for athlete in detail.get("athletesInvolved", []):
            athletes.append({
                "id": athlete.get("id", ""),
                "name": athlete.get("fullName", ""),
                "position": athlete.get("position", ""),
            })

        match_events.append({
            "type": event_type,
            "minute": minute_str.replace("'", ""),
            "minute_seconds": minute_val,
            "team_id": team_id,
            "athletes": athletes,
        })

    # Determine season label
    # Six Nations "season" naming: matches in 2001 = "2000-01" season, etc.
    # But the first Six Nations in 2000 = "2000" or "1999-00" season
    season_label = f"{year-1}-{str(year)[-2:]}" if year > 2000 else "1999-00"

    return {
        "espn_match_id": event["id"],
        "season": season_label,
        "date": date_str[:10] if date_str else "",
        "home_team": home_team,
        "away_team": away_team,
        "home_score": home_score,
        "away_score": away_score,
        "venue": venue,
        "match_events": match_events,
    }


def get_match_roster(event_id, league_id=None):
    """Get the full roster for a match from the summary endpoint.

    Uses the 'rosters' section which has data for all seasons (2000-2025),
    unlike 'boxscore.players' which is empty for older matches.
    """
    lid = league_id or LEAGUE_ID
    url = f"{BASE_URL}/{lid}/summary?event={event_id}"
    data = fetch_json(url)
    if not data:
        return None

    rosters = {}  # team_name -> list of players

    # Primary source: "rosters" section (works for all seasons)
    for roster_section in data.get("rosters", []):
        team_info = roster_section.get("team", {})
        team_name = team_info.get("displayName", team_info.get("name", "Unknown"))
        home_away = roster_section.get("homeAway", "")

        players = []
        for entry in roster_section.get("roster", []):
            athlete = entry.get("athlete", {})
            position = entry.get("position", {})
            pos_abbr = position.get("abbreviation", "") if isinstance(position, dict) else str(position)

            player = {
                "id": str(athlete.get("id", "")),
                "name": athlete.get("displayName", athlete.get("fullName", "")),
                "jersey": str(entry.get("jersey", "")),
                "position": pos_abbr,
                "home_away": home_away,
            }
            players.append(player)

        rosters[team_name] = players

    # Fallback: try "boxscore" -> "players" if rosters section is empty
    if not rosters or all(len(p) == 0 for p in rosters.values()):
        boxscore = data.get("boxscore", {})
        for section in boxscore.get("players", []):
            team_info = section.get("team", {})
            team_name = team_info.get("displayName", team_info.get("name", "Unknown"))

            players = []
            for stat_group in section.get("statistics", []):
                for athlete_entry in stat_group.get("athletes", []):
                    athlete = athlete_entry.get("athlete", {})
                    player = {
                        "id": str(athlete.get("id", "")),
                        "name": athlete.get("displayName", athlete.get("fullName", "")),
                        "jersey": str(athlete.get("jersey", "")),
                        "position": athlete.get("position", {}).get("abbreviation", "") if isinstance(athlete.get("position"), dict) else str(athlete.get("position", "")),
                        "home_away": "",
                    }
                    if not any(p["id"] == player["id"] for p in players):
                        players.append(player)

            if players:
                rosters[team_name] = players

    return rosters


def calculate_minutes(match_info, rosters):
    """Calculate minutes played for each player based on match events."""
    appearances = []
    match_events = match_info.get("match_events", [])

    # Build substitution map: who came on, who went off, and when
    sub_on_events = {}   # player_id -> minute
    sub_off_events = {}  # player_id -> minute

    for event in match_events:
        etype = event["type"].lower()
        minute = event["minute"]
        try:
            minute_int = int(minute)
        except (ValueError, TypeError):
            continue

        for athlete in event["athletes"]:
            pid = athlete["id"]
            if "substitute on" in etype or "sub on" in etype:
                sub_on_events[pid] = minute_int
            elif "substitute off" in etype or "sub off" in etype:
                sub_off_events[pid] = minute_int

    for team_name, players in rosters.items():
        for player in players:
            pid = player["id"]
            jersey = player["jersey"]

            try:
                jersey_num = int(jersey)
            except (ValueError, TypeError):
                jersey_num = 0

            is_starter = 1 <= jersey_num <= 15

            if is_starter:
                sub_off_min = sub_off_events.get(pid)
                if sub_off_min is not None:
                    minutes_played = sub_off_min
                    sub_minute_off = sub_off_min
                else:
                    minutes_played = 80
                    sub_minute_off = ""
                sub_minute_on = 0
            else:
                sub_on_min = sub_on_events.get(pid)
                if sub_on_min is not None:
                    minutes_played = 80 - sub_on_min
                    sub_minute_on = sub_on_min
                else:
                    # Replacement who didn't come on
                    minutes_played = 0
                    sub_minute_on = ""
                sub_minute_off = ""

            appearances.append({
                "season": match_info["season"],
                "date": match_info["date"],
                "home_team": match_info["home_team"],
                "away_team": match_info["away_team"],
                "team": team_name,
                "player_name": player["name"],
                "shirt_number": jersey,
                "position": player["position"],
                "is_starter": is_starter,
                "sub_minute_off": sub_minute_off,
                "sub_minute_on": sub_minute_on,
                "minutes_played": minutes_played,
                "source": "espn",
            })

    return appearances


def save_matches_csv(matches, filepath):
    """Save match data to CSV."""
    if not matches:
        return
    fieldnames = ["season", "date", "home_team", "away_team", "home_score", "away_score",
                  "venue", "espn_match_id"]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in matches:
            writer.writerow({k: m[k] for k in fieldnames})


def save_appearances_csv(appearances, filepath):
    """Save player appearance data to CSV."""
    if not appearances:
        return
    fieldnames = ["season", "date", "home_team", "away_team", "team", "player_name",
                  "shirt_number", "position", "is_starter", "sub_minute_off",
                  "sub_minute_on", "minutes_played", "source"]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for a in appearances:
            writer.writerow(a)


def scrape_all():
    """Scrape all Six Nations data from ESPN API."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_matches = []
    all_appearances = []

    for year in SEASONS:
        season_label = f"{year-1}-{str(year)[-2:]}" if year > 2000 else "1999-00"
        print(f"\n{'='*60}")
        print(f"ESPN: Scraping season {season_label} (calendar year {year})")
        print(f"{'='*60}")

        events = get_season_matches(year)
        if not events:
            print(f"  No matches found for {year}")
            continue

        print(f"  Found {len(events)} matches")

        for i, event in enumerate(events):
            match_info = parse_scoreboard_event(event, year)
            print(f"  [{i+1}/{len(events)}] {match_info['home_team']} {match_info['home_score']}-{match_info['away_score']} {match_info['away_team']}")

            # Get full roster from summary
            time.sleep(0.5)
            rosters = get_match_roster(event["id"])

            if rosters:
                player_count = sum(len(p) for p in rosters.values())
                print(f"    Roster: {player_count} players")
                appearances = calculate_minutes(match_info, rosters)
                all_appearances.extend(appearances)
            else:
                print(f"    WARNING: No roster data available")

            # Remove match_events before saving (not needed in CSV)
            match_info_clean = {k: v for k, v in match_info.items() if k != "match_events"}
            all_matches.append(match_info_clean)

        # Incremental save after each season
        save_matches_csv(all_matches, os.path.join(OUTPUT_DIR, "espn_matches.csv"))
        save_appearances_csv(all_appearances, os.path.join(OUTPUT_DIR, "espn_appearances.csv"))
        print(f"  Saved: {len(all_matches)} matches, {len(all_appearances)} appearances so far")

    print(f"\n{'='*60}")
    print(f"ESPN COMPLETE: {len(all_matches)} matches, {len(all_appearances)} appearances")
    print(f"{'='*60}")

    return all_matches, all_appearances


if __name__ == "__main__":
    scrape_all()
