"""Sky Sports HTML scraper for Six Nations rugby data extraction."""

import csv
import os
import re
import time
from html.parser import HTMLParser
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

BASE_URL = "https://www.skysports.com"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")

# Seasons with data on Sky Sports (verified during exploration)
AVAILABLE_SEASONS = [
    "2009-10", "2010-11", "2011-12", "2012-13", "2013-14",
    "2014-15", "2015-16", "2016-17", "2017-18", "2018-19",
    "2019-20", "2020-21", "2023-24", "2024-25",
]


def fetch_html(url, retries=3):
    """Fetch HTML from a URL with retries."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
            })
            with urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError) as e:
            print(f"  Attempt {attempt+1}/{retries} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def parse_results_page(html):
    """Parse results page HTML to extract match info.

    The HTML structure has:
    - <div class="fixres__item"> containing match blocks
    - <a href="https://www.skysports.com/rugby-union/{slug}/{id}" class="matches__item matches__link">
    - <span class="swap-text__target">TeamName</span> for team names
    - <span class="matches__teamscores-side">XX</span> for scores
    - <h4 class="fixres__header2">Day DDth Month</h4> for dates
    """
    matches = []

    # Extract each match block: from <div class="fixres__item"> to closing </div> + </a>
    # The match link contains the slug and ID in the href
    # Use the full match block pattern
    block_pattern = re.compile(
        r'<div\s+class="fixres__item">\s*'
        r'<a\s+href="(?:https://www\.skysports\.com)?(/rugby-union/([a-z-]+-vs-[a-z-]+)/(\d+))"'
        r'[^>]*>(.+?)</a>',
        re.DOTALL | re.IGNORECASE
    )

    for block_match in block_pattern.finditer(html):
        href = block_match.group(1)
        slug = block_match.group(2)
        match_id = block_match.group(3)
        block_html = block_match.group(4)

        # Extract team names from swap-text__target spans
        team_names = re.findall(
            r'<span\s+class="swap-text__target">([^<]+)</span>',
            block_html
        )

        # Extract scores from matches__teamscores-side spans
        scores = re.findall(
            r'<span\s+class="matches__teamscores-side">\s*(\d+)\s*</span>',
            block_html
        )

        if len(team_names) >= 2:
            home_team = team_names[0].strip()
            away_team = team_names[1].strip()
        else:
            # Fallback: derive from slug
            parts = slug.split("-vs-")
            if len(parts) == 2:
                team_name_map = {
                    "england": "England", "france": "France", "ireland": "Ireland",
                    "italy": "Italy", "scotland": "Scotland", "wales": "Wales",
                }
                home_team = team_name_map.get(parts[0], parts[0].title())
                away_team = team_name_map.get(parts[1], parts[1].title())
            else:
                continue

        home_score = int(scores[0]) if len(scores) >= 2 else None
        away_score = int(scores[1]) if len(scores) >= 2 else None

        matches.append({
            "href": href,
            "slug": slug,
            "match_id": match_id,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
        })

    return matches


def parse_teams_page(html):
    """Parse the teams tab HTML to extract player lineups and sub minutes.

    HTML structure:
    - <div class="team-lineups__list-team"> for each team section
    - <h3 class="block-header__title">TeamName</h3> for team name
    - <li class="team-lineups__list-player" data-player-id="...">
    -   <span class="team-lineups__list-player-number">15</span>
    -   <span class="team-lineups__list-player-name">Chris Paterson</span>
    -   <span class="team-lineups__list-events">19'</span>  (sub minute)
    - <li class="team-lineups__list-subtitle">Subs</li> separates starters from subs
    """
    teams = {}  # team_name -> list of players

    # Split into team sections
    team_section_pattern = re.compile(
        r'<div\s+class="team-lineups__list-team">(.*?)(?=<div\s+class="team-lineups__list-team">|$)',
        re.DOTALL
    )

    for section_match in team_section_pattern.finditer(html):
        section = section_match.group(1)

        # Get team name from header
        name_match = re.search(r'<h3\s+class="block-header__title">([^<]+)</h3>', section)
        if not name_match:
            continue
        team_name = name_match.group(1).strip()

        players = []

        # Find all player entries
        player_pattern = re.compile(
            r'<li\s+class="team-lineups__list-player"[^>]*>.*?</li>',
            re.DOTALL
        )

        for player_match in player_pattern.finditer(section):
            item_html = player_match.group(0)

            # Extract player data
            number_match = re.search(
                r'team-lineups__list-player-number[^>]*>\s*(\d{1,2})\s*<',
                item_html
            )
            pname_match = re.search(
                r'team-lineups__list-player-name[^>]*>\s*(.+?)\s*<',
                item_html
            )

            if not (number_match and pname_match):
                continue

            jersey = number_match.group(1)
            name = pname_match.group(1).strip()

            # Parse sub minute from events, using icon type to determine on/off
            # substitution_off.svg = starter subbed off
            # substitution_on.svg = sub coming on
            sub_minute_off = None
            sub_minute_on = None

            # Search the entire player <li> block for substitution icons + minutes
            if 'substitution_off' in item_html:
                minute_match = re.search(
                    r'substitution_off\.svg[^>]*>\s*(\d+)',
                    item_html
                )
                if minute_match:
                    sub_minute_off = int(minute_match.group(1))

            if 'substitution_on' in item_html:
                minute_match = re.search(
                    r'substitution_on\.svg[^>]*>\s*(\d+)',
                    item_html
                )
                if minute_match:
                    sub_minute_on = int(minute_match.group(1))

            jersey_num = int(jersey)
            is_starter = 1 <= jersey_num <= 15

            players.append({
                "jersey": jersey,
                "name": name,
                "is_starter": is_starter,
                "sub_minute_off": sub_minute_off,
                "sub_minute_on": sub_minute_on,
            })

        if players:
            teams[team_name] = players

    return teams


def get_season_results(season):
    """Get all match results for a season from Sky Sports."""
    url = f"{BASE_URL}/rugby-union/competitions/six-nations/results/{season}"
    print(f"  Fetching results page: {url}")
    html = fetch_html(url)
    if not html:
        return []

    matches = parse_results_page(html)
    print(f"  Found {len(matches)} matches")
    return matches


def get_match_teams(slug, match_id):
    """Get team lineups from the Teams tab of a match page."""
    url = f"{BASE_URL}/rugby-union/{slug}/teams/{match_id}"
    html = fetch_html(url)
    if not html:
        return {}

    return parse_teams_page(html)


def calculate_appearances(match_info, teams_data, season):
    """Calculate minutes played for each player."""
    appearances = []

    for team_name, players in teams_data.items():
        for player in players:
            is_starter = player.get("is_starter", int(player["jersey"]) <= 15)
            sub_off = player.get("sub_minute_off")
            sub_on = player.get("sub_minute_on")

            if is_starter:
                sub_minute_on = 0
                if sub_off is not None:
                    minutes_played = sub_off
                    sub_minute_off = sub_off
                else:
                    minutes_played = 80
                    sub_minute_off = ""
            else:
                sub_minute_off = ""
                if sub_on is not None:
                    minutes_played = 80 - sub_on
                    sub_minute_on = sub_on
                else:
                    # Sub who didn't come on
                    minutes_played = 0
                    sub_minute_on = ""

            appearances.append({
                "season": season,
                "date": match_info.get("date", ""),
                "home_team": match_info["home_team"],
                "away_team": match_info["away_team"],
                "team": team_name,
                "player_name": player["name"],
                "shirt_number": player["jersey"],
                "position": "",
                "is_starter": is_starter,
                "sub_minute_off": sub_minute_off,
                "sub_minute_on": sub_minute_on,
                "minutes_played": minutes_played,
                "source": "sky_sports",
            })

    return appearances


def save_matches_csv(matches, filepath):
    """Save match data to CSV."""
    if not matches:
        return
    fieldnames = ["season", "date", "home_team", "away_team", "home_score", "away_score",
                  "sky_match_id", "slug"]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in matches:
            writer.writerow({k: m.get(k, "") for k in fieldnames})


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
    """Scrape all available Six Nations data from Sky Sports."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_matches = []
    all_appearances = []

    for season in AVAILABLE_SEASONS:
        print(f"\n{'='*60}")
        print(f"Sky Sports: Scraping season {season}")
        print(f"{'='*60}")

        matches = get_season_results(season)
        if not matches:
            print(f"  No matches found for {season}")
            continue

        for i, match in enumerate(matches):
            print(f"  [{i+1}/{len(matches)}] {match['home_team']} {match.get('home_score', '?')}-{match.get('away_score', '?')} {match['away_team']}")

            # Fetch teams page
            time.sleep(1.5)
            teams_data = get_match_teams(match["slug"], match["match_id"])

            if teams_data:
                player_count = sum(len(p) for p in teams_data.values())
                print(f"    Teams data: {player_count} players")
                appearances = calculate_appearances(match, teams_data, season)
                all_appearances.extend(appearances)
            else:
                print(f"    WARNING: No teams data available")

            match_record = {
                "season": season,
                "date": "",
                "home_team": match["home_team"],
                "away_team": match["away_team"],
                "home_score": match.get("home_score", ""),
                "away_score": match.get("away_score", ""),
                "sky_match_id": match["match_id"],
                "slug": match["slug"],
            }
            all_matches.append(match_record)

        # Incremental save
        save_matches_csv(all_matches, os.path.join(OUTPUT_DIR, "sky_matches.csv"))
        save_appearances_csv(all_appearances, os.path.join(OUTPUT_DIR, "sky_appearances.csv"))
        print(f"  Saved: {len(all_matches)} matches, {len(all_appearances)} appearances so far")

    print(f"\n{'='*60}")
    print(f"SKY SPORTS COMPLETE: {len(all_matches)} matches, {len(all_appearances)} appearances")
    print(f"{'='*60}")

    return all_matches, all_appearances


if __name__ == "__main__":
    scrape_all()
