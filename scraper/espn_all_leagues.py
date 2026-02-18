"""ESPN multi-league scraper for all men's international rugby since 2000."""

import csv
import json
import os
import time

from scraper.espn_api import (
    BASE_URL, OUTPUT_DIR, fetch_json, parse_scoreboard_event,
    get_match_roster, calculate_minutes,
)

# League configurations
LEAGUES = {
    "rugby_championship": {
        "id": "244293",
        "name": "Rugby Championship / Tri Nations",
        "years": list(range(2000, 2026)),
        "query_strategy": "monthly_rc",
        "season_label": lambda year: str(year),
    },
    "rugby_world_cup": {
        "id": "164205",
        "name": "Rugby World Cup",
        "years": [2003, 2007, 2011, 2015, 2019, 2023],
        "query_strategy": "yearly",
        "season_label": lambda year: str(year),
    },
    "lions": {
        "id": "268565",
        "name": "British & Irish Lions",
        "years": [2017, 2021],
        "query_strategy": "yearly",
        "season_label": lambda year: str(year),
    },
    "test_matches": {
        "id": "289234",
        "name": "International Test Match",
        "years": list(range(2015, 2026)),
        "query_strategy": "monthly_full",
        "season_label": lambda year: str(year),
    },
    "tri_nations_2020": {
        "id": "289274",
        "name": "2020 Tri Nations",
        "years": [2020],
        "query_strategy": "yearly",
        "season_label": lambda year: str(year),
    },
}

PROGRESS_FILE = os.path.join(OUTPUT_DIR, "scrape_progress.json")


def load_progress():
    """Load scraping progress from disk."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_progress(progress):
    """Save scraping progress to disk."""
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2)


def get_events_yearly(league_id, year):
    """Single full-year scoreboard query."""
    url = f"{BASE_URL}/{league_id}/scoreboard?dates={year}0101-{year}1231"
    data = fetch_json(url)
    if not data or "events" not in data:
        return []
    return data["events"]


def get_events_monthly_rc(league_id, year):
    """Monthly queries for Rugby Championship to avoid API truncation.

    Pre-2012 (Tri Nations, 6 matches): full-year query is fine.
    2012+ (Rugby Championship, 12 matches): API truncates, so query Jul-Dec monthly.
    """
    if year <= 2011:
        return get_events_yearly(league_id, year)

    all_events = {}
    for month in range(7, 13):  # Jul through Dec
        last_day = 31 if month in (7, 8, 10, 12) else 30
        start = f"{year}{month:02d}01"
        end = f"{year}{month:02d}{last_day}"
        url = f"{BASE_URL}/{league_id}/scoreboard?dates={start}-{end}"
        data = fetch_json(url)
        time.sleep(0.5)
        if data and "events" in data:
            for event in data["events"]:
                eid = event["id"]
                if eid not in all_events:
                    all_events[eid] = event
    return list(all_events.values())


def get_events_monthly_full(league_id, year):
    """Monthly queries for the full year to avoid API 100-event cap.

    Used for International Test Matches which can have 100+ events per year.
    """
    all_events = {}
    for month in range(1, 13):
        last_day = 31 if month in (1, 3, 5, 7, 8, 10, 12) else (30 if month != 2 else 28)
        start = f"{year}{month:02d}01"
        end = f"{year}{month:02d}{last_day}"
        url = f"{BASE_URL}/{league_id}/scoreboard?dates={start}-{end}"
        data = fetch_json(url)
        time.sleep(0.5)
        if data and "events" in data:
            for event in data["events"]:
                eid = event["id"]
                if eid not in all_events:
                    all_events[eid] = event
    return list(all_events.values())


def save_league_matches_csv(matches, league_key):
    """Save match data for a single league."""
    if not matches:
        return
    filepath = os.path.join(OUTPUT_DIR, f"espn_{league_key}_matches.csv")
    fieldnames = ["season", "tournament", "date", "home_team", "away_team",
                  "home_score", "away_score", "venue", "espn_match_id"]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in matches:
            writer.writerow({k: m.get(k, "") for k in fieldnames})


def save_league_appearances_csv(appearances, league_key):
    """Save player appearance data for a single league."""
    if not appearances:
        return
    filepath = os.path.join(OUTPUT_DIR, f"espn_{league_key}_appearances.csv")
    fieldnames = ["season", "tournament", "date", "home_team", "away_team",
                  "team", "player_name", "shirt_number", "position", "is_starter",
                  "sub_minute_off", "sub_minute_on", "minutes_played",
                  "espn_match_id", "source"]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for a in appearances:
            writer.writerow({k: a.get(k, "") for k in fieldnames})


def scrape_league(league_key, league_config, progress):
    """Scrape all matches for a single league."""
    league_id = league_config["id"]
    league_name = league_config["name"]
    strategy = league_config["query_strategy"]
    season_label_fn = league_config["season_label"]

    all_matches = []
    all_appearances = []

    # Load any previously saved data for this league
    matches_path = os.path.join(OUTPUT_DIR, f"espn_{league_key}_matches.csv")
    apps_path = os.path.join(OUTPUT_DIR, f"espn_{league_key}_appearances.csv")
    if os.path.exists(matches_path):
        all_matches = list(read_csv(matches_path))
    if os.path.exists(apps_path):
        all_appearances = list(read_csv(apps_path))

    league_progress = progress.get(league_key, {})

    for year in league_config["years"]:
        year_str = str(year)
        if league_progress.get(year_str):
            print(f"  Year {year}: already done, skipping")
            continue

        print(f"\n  Year {year}...")

        # Get events using appropriate strategy
        if strategy == "monthly_rc":
            events = get_events_monthly_rc(league_id, year)
        elif strategy == "monthly_full":
            events = get_events_monthly_full(league_id, year)
        else:
            events = get_events_yearly(league_id, year)

        if not events:
            print(f"    No matches found")
            league_progress[year_str] = True
            progress[league_key] = league_progress
            save_progress(progress)
            continue

        print(f"    Found {len(events)} matches")

        for i, event in enumerate(events):
            match_info = parse_scoreboard_event(event, year)
            # Override season label and add tournament
            match_info["season"] = season_label_fn(year)
            match_info["tournament"] = league_name

            print(f"    [{i+1}/{len(events)}] {match_info['home_team']} "
                  f"{match_info['home_score']}-{match_info['away_score']} "
                  f"{match_info['away_team']}")

            # Fetch roster
            time.sleep(0.5)
            rosters = get_match_roster(event["id"], league_id)

            if rosters:
                player_count = sum(len(p) for p in rosters.values())
                print(f"      Roster: {player_count} players")
                apps = calculate_minutes(match_info, rosters)
                # Inject extra fields into each appearance
                for app in apps:
                    app["tournament"] = league_name
                    app["espn_match_id"] = match_info["espn_match_id"]
                all_appearances.extend(apps)
            else:
                print(f"      WARNING: No roster data")

            match_clean = {k: v for k, v in match_info.items() if k != "match_events"}
            all_matches.append(match_clean)

        # Incremental save after each year
        save_league_matches_csv(all_matches, league_key)
        save_league_appearances_csv(all_appearances, league_key)
        league_progress[year_str] = True
        progress[league_key] = league_progress
        save_progress(progress)
        print(f"    Saved: {len(all_matches)} matches, {len(all_appearances)} appearances so far")

    return all_matches, all_appearances


def read_csv(filepath):
    """Read a CSV file and return a list of dicts."""
    rows = []
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


def deduplicate_matches(all_matches):
    """Deduplicate matches across leagues by ESPN event ID.

    Named tournaments take priority over the generic ITM league.
    """
    priority = {
        "Six Nations": 1,
        "Rugby Championship / Tri Nations": 1,
        "Rugby World Cup": 1,
        "British & Irish Lions": 1,
        "2020 Tri Nations": 1,
        "International Test Match": 2,
    }

    seen_ids = {}
    for match in all_matches:
        eid = match["espn_match_id"]
        if eid not in seen_ids:
            seen_ids[eid] = match
        else:
            existing_pri = priority.get(seen_ids[eid].get("tournament", ""), 99)
            new_pri = priority.get(match.get("tournament", ""), 99)
            if new_pri < existing_pri:
                seen_ids[eid] = match

    # Secondary dedup by (date, sorted teams) for safety
    sig_seen = {}
    result = []
    for match in seen_ids.values():
        date = match.get("date", "")[:10]
        teams = tuple(sorted([match.get("home_team", ""), match.get("away_team", "")]))
        sig = (date, teams)
        if sig not in sig_seen:
            sig_seen[sig] = match
            result.append(match)
        else:
            existing_pri = priority.get(sig_seen[sig].get("tournament", ""), 99)
            new_pri = priority.get(match.get("tournament", ""), 99)
            if new_pri < existing_pri:
                # Replace existing with higher priority
                result = [m for m in result if m is not sig_seen[sig]]
                result.append(match)
                sig_seen[sig] = match

    return result


def deduplicate_appearances(all_appearances, valid_match_ids):
    """Keep only appearances for deduplicated matches."""
    seen = set()
    result = []
    for app in all_appearances:
        eid = app.get("espn_match_id", "")
        if eid not in valid_match_ids:
            continue
        key = (eid, app.get("player_name", ""), app.get("team", ""))
        if key not in seen:
            seen.add(key)
            result.append(app)
    return result


def combine_all_leagues():
    """Read per-league CSVs + existing Six Nations, deduplicate, write combined output."""
    all_matches = []
    all_appearances = []

    # Include existing Six Nations data
    six_nations_matches_path = os.path.join(OUTPUT_DIR, "espn_matches.csv")
    six_nations_apps_path = os.path.join(OUTPUT_DIR, "espn_appearances.csv")

    if os.path.exists(six_nations_matches_path):
        sn_matches = read_csv(six_nations_matches_path)
        # Build lookup for injecting espn_match_id into appearances
        sn_match_lookup = {}
        for m in sn_matches:
            m["tournament"] = "Six Nations"
            key = (m.get("date", "")[:10], m.get("home_team", ""), m.get("away_team", ""))
            sn_match_lookup[key] = m.get("espn_match_id", "")
        all_matches.extend(sn_matches)

        if os.path.exists(six_nations_apps_path):
            sn_apps = read_csv(six_nations_apps_path)
            for a in sn_apps:
                a["tournament"] = "Six Nations"
                key = (a.get("date", "")[:10], a.get("home_team", ""), a.get("away_team", ""))
                a["espn_match_id"] = sn_match_lookup.get(key, "")
            all_appearances.extend(sn_apps)

    # Read each new league's output
    for league_key in LEAGUES:
        m_path = os.path.join(OUTPUT_DIR, f"espn_{league_key}_matches.csv")
        a_path = os.path.join(OUTPUT_DIR, f"espn_{league_key}_appearances.csv")
        if os.path.exists(m_path):
            all_matches.extend(read_csv(m_path))
        if os.path.exists(a_path):
            all_appearances.extend(read_csv(a_path))

    # Deduplicate
    deduped_matches = deduplicate_matches(all_matches)
    valid_ids = {m["espn_match_id"] for m in deduped_matches}
    deduped_apps = deduplicate_appearances(all_appearances, valid_ids)

    # Sort by date
    deduped_matches.sort(key=lambda m: m.get("date", ""))
    deduped_apps.sort(key=lambda a: (a.get("date", ""), a.get("home_team", ""),
                                      a.get("team", ""), a.get("shirt_number", "")))

    # Save combined files
    combined_matches_path = os.path.join(OUTPUT_DIR, "espn_all_international_matches.csv")
    combined_apps_path = os.path.join(OUTPUT_DIR, "espn_all_international_appearances.csv")

    if deduped_matches:
        fieldnames = ["season", "tournament", "date", "home_team", "away_team",
                      "home_score", "away_score", "venue", "espn_match_id"]
        with open(combined_matches_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for m in deduped_matches:
                writer.writerow({k: m.get(k, "") for k in fieldnames})

    if deduped_apps:
        fieldnames = ["season", "tournament", "date", "home_team", "away_team",
                      "team", "player_name", "shirt_number", "position", "is_starter",
                      "sub_minute_off", "sub_minute_on", "minutes_played",
                      "espn_match_id", "source"]
        with open(combined_apps_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for a in deduped_apps:
                writer.writerow({k: a.get(k, "") for k in fieldnames})

    # Print summary
    tournaments = {}
    for m in deduped_matches:
        t = m.get("tournament", "Unknown")
        tournaments[t] = tournaments.get(t, 0) + 1

    print(f"\n{'='*60}")
    print(f"COMBINED OUTPUT: {len(deduped_matches)} matches, {len(deduped_apps)} appearances")
    print(f"{'='*60}")
    for t, count in sorted(tournaments.items()):
        print(f"  {t}: {count} matches")

    return deduped_matches, deduped_apps


def scrape_all_leagues(league_filter=None):
    """Scrape all configured leagues (or a single one if filtered)."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    progress = load_progress()

    leagues_to_scrape = {}
    if league_filter:
        if league_filter in LEAGUES:
            leagues_to_scrape[league_filter] = LEAGUES[league_filter]
        else:
            print(f"Unknown league: {league_filter}")
            print(f"Available: {', '.join(LEAGUES.keys())}")
            return
    else:
        leagues_to_scrape = LEAGUES

    for league_key, config in leagues_to_scrape.items():
        print(f"\n{'='*60}")
        print(f"Scraping: {config['name']} (league {config['id']})")
        print(f"{'='*60}")
        scrape_league(league_key, config, progress)

    # Combine all leagues after scraping
    print(f"\n{'='*60}")
    print(f"Combining all leagues...")
    print(f"{'='*60}")
    combine_all_leagues()


if __name__ == "__main__":
    scrape_all_leagues()
