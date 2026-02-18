"""Orchestrator for Six Nations data extraction from ESPN + Sky Sports."""

import csv
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scraper.espn_api import scrape_all as scrape_espn
from scraper.sky_sports import scrape_all as scrape_sky
from scraper.espn_all_leagues import scrape_all_leagues, combine_all_leagues

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")


def merge_data():
    """Merge ESPN and Sky Sports data into final CSVs."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Read ESPN data
    espn_matches = read_csv(os.path.join(OUTPUT_DIR, "espn_matches.csv"))
    espn_appearances = read_csv(os.path.join(OUTPUT_DIR, "espn_appearances.csv"))

    # Read Sky Sports data
    sky_matches = read_csv(os.path.join(OUTPUT_DIR, "sky_matches.csv"))
    sky_appearances = read_csv(os.path.join(OUTPUT_DIR, "sky_appearances.csv"))

    print(f"\nMerging data:")
    print(f"  ESPN: {len(espn_matches)} matches, {len(espn_appearances)} appearances")
    print(f"  Sky:  {len(sky_matches)} matches, {len(sky_appearances)} appearances")

    # Build final matches CSV (combine both sources)
    final_matches = []
    for m in espn_matches:
        final_matches.append({
            "season": m.get("season", ""),
            "date": m.get("date", ""),
            "home_team": m.get("home_team", ""),
            "away_team": m.get("away_team", ""),
            "home_score": m.get("home_score", ""),
            "away_score": m.get("away_score", ""),
            "venue": m.get("venue", ""),
            "espn_match_id": m.get("espn_match_id", ""),
            "sky_match_id": "",
        })

    # Add Sky Sports match IDs to matching ESPN matches
    for sm in sky_matches:
        matched = False
        for fm in final_matches:
            if (normalize_team(fm["home_team"]) == normalize_team(sm.get("home_team", "")) and
                normalize_team(fm["away_team"]) == normalize_team(sm.get("away_team", "")) and
                fm["season"] == sm.get("season", "")):
                fm["sky_match_id"] = sm.get("sky_match_id", "")
                matched = True
                break

        if not matched:
            # Sky Sports match not in ESPN data - add it
            final_matches.append({
                "season": sm.get("season", ""),
                "date": sm.get("date", ""),
                "home_team": sm.get("home_team", ""),
                "away_team": sm.get("away_team", ""),
                "home_score": sm.get("home_score", ""),
                "away_score": sm.get("away_score", ""),
                "venue": "",
                "espn_match_id": "",
                "sky_match_id": sm.get("sky_match_id", ""),
            })

    # Sort by season then date
    final_matches.sort(key=lambda m: (m["season"], m["date"]))

    # Build final appearances: prefer ESPN data (has positions), supplement with Sky Sports
    final_appearances = []

    # Add all ESPN appearances
    for a in espn_appearances:
        final_appearances.append(a)

    # Add Sky Sports appearances that don't have ESPN equivalents
    for sa in sky_appearances:
        has_espn = any(
            a["source"] == "espn" and
            normalize_team(a["home_team"]) == normalize_team(sa.get("home_team", "")) and
            normalize_team(a["away_team"]) == normalize_team(sa.get("away_team", "")) and
            a["season"] == sa.get("season", "") and
            normalize_name(a["player_name"]) == normalize_name(sa.get("player_name", ""))
            for a in espn_appearances
        )
        if not has_espn:
            final_appearances.append(sa)

    # Save final CSVs
    match_fields = ["season", "date", "home_team", "away_team", "home_score", "away_score",
                    "venue", "espn_match_id", "sky_match_id"]
    write_csv(final_matches, match_fields, os.path.join(OUTPUT_DIR, "matches.csv"))

    appearance_fields = ["season", "date", "home_team", "away_team", "team", "player_name",
                         "shirt_number", "position", "is_starter", "sub_minute_off",
                         "sub_minute_on", "minutes_played", "source"]
    write_csv(final_appearances, appearance_fields, os.path.join(OUTPUT_DIR, "appearances.csv"))

    # Print summary
    seasons = set(m["season"] for m in final_matches)
    print(f"\n{'='*60}")
    print(f"FINAL OUTPUT:")
    print(f"  Seasons: {len(seasons)}")
    print(f"  Matches: {len(final_matches)}")
    print(f"  Appearances: {len(final_appearances)}")
    print(f"  Files: output/matches.csv, output/appearances.csv")
    print(f"{'='*60}")

    # Per-season summary
    print(f"\nPer-season breakdown:")
    for s in sorted(seasons):
        s_matches = [m for m in final_matches if m["season"] == s]
        s_apps = [a for a in final_appearances if a["season"] == s]
        print(f"  {s}: {len(s_matches)} matches, {len(s_apps)} appearances")


def normalize_team(name):
    """Normalize team name for comparison."""
    return name.strip().lower()


def normalize_name(name):
    """Normalize player name for comparison."""
    return name.strip().lower().replace("'", "'")


def read_csv(filepath):
    """Read a CSV file into a list of dicts."""
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(rows, fieldnames, filepath):
    """Write a list of dicts to CSV."""
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main():
    """Run the full extraction pipeline."""
    import argparse

    parser = argparse.ArgumentParser(description="Rugby Data Extractor")
    parser.add_argument("--espn-only", action="store_true", help="Only run ESPN Six Nations scraper")
    parser.add_argument("--sky-only", action="store_true", help="Only run Sky Sports scraper")
    parser.add_argument("--merge-only", action="store_true", help="Only merge existing Six Nations data")
    parser.add_argument("--all-leagues", action="store_true", help="Extract all international leagues from ESPN")
    parser.add_argument("--league", type=str, help="Extract a specific league (e.g., rugby_championship)")
    parser.add_argument("--combine-only", action="store_true", help="Only combine existing league CSVs")
    args = parser.parse_args()

    if args.combine_only:
        combine_all_leagues()
        return

    if args.all_leagues:
        scrape_all_leagues()
        return

    if args.league:
        scrape_all_leagues(league_filter=args.league)
        return

    if args.merge_only:
        merge_data()
        return

    if not args.sky_only:
        print("=" * 60)
        print("PHASE 1: ESPN API Extraction")
        print("=" * 60)
        scrape_espn()

    if not args.espn_only:
        print("\n" + "=" * 60)
        print("PHASE 2: Sky Sports Extraction")
        print("=" * 60)
        scrape_sky()

    print("\n" + "=" * 60)
    print("PHASE 3: Merging Data")
    print("=" * 60)
    merge_data()


if __name__ == "__main__":
    main()
