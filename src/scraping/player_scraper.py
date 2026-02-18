"""
Rugby player data scraper for rugbypass.com.

Two-phase approach:
1. Paginate through the listing API to collect all player slugs.
2. Concurrently scrape individual player pages for bio details.
"""

import csv
import json
import logging
import re
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://www.rugbypass.com"
PLAYERS_LIST_URL = f"{BASE_URL}/players/"
REQUEST_TIMEOUT = 30
CONCURRENCY = 5
MAX_RETRIES = 3
PLAYERS_PER_PAGE = 150

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

AJAX_HEADERS = {
    **HEADERS,
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": BASE_URL,
    "Referer": f"{BASE_URL}/players/",
}

AJAX_URL = f"{BASE_URL}/players"

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data"
ESPN_APPEARANCES_PATH = Path(__file__).resolve().parents[2] / "output" / "espn_appearances.csv"

CSV_COLUMNS = ["name", "nationality", "age", "position", "height", "weight", "team", "slug"]

# Thread-local storage for per-thread sessions
_thread_local = threading.local()


@dataclass
class Player:
    name: str
    slug: str
    position: Optional[str] = None
    team: Optional[str] = None
    nationality: Optional[str] = None
    age: Optional[int] = None
    height: Optional[str] = None
    weight: Optional[str] = None


def _get_session() -> requests.Session:
    """Get or create a per-thread requests session."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update(HEADERS)
    return _thread_local.session


def _normalize_name(name: str) -> str:
    """Normalize a player name for matching: lowercase, strip accents, replace spaces with hyphens."""
    # Remove accents
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = nfkd.encode("ascii", "ignore").decode("ascii")
    # Lowercase, replace spaces/special chars with hyphens
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name.lower()).strip("-")
    return slug


def _parse_player_entry(entry: dict) -> Player:
    """Convert a raw listing entry to a Player with basic info."""
    slug = entry.get("l", "").strip("/").split("/")[-1]
    return Player(
        name=entry.get("n", "Unknown"),
        slug=slug,
        position=entry.get("p"),
        team=entry.get("t"),
    )


def _fetch_page_via_api(session: requests.Session, page: int) -> list[dict]:
    """Fetch a single page of players via the AJAX API."""
    resp = session.post(
        AJAX_URL,
        data={
            "isContent": 1,
            "action": "load-players",
            "page": page,
            "sortType": "name",
            "sortDir": "asc",
            "currentSquad": 0,
        },
        headers=AJAX_HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise ValueError(f"API returned success=false for page {page}")
    return data.get("players", [])


def _discover_total_pages(session: requests.Session) -> int:
    """Discover total pages by fetching the listing page HTML."""
    resp = session.get(PLAYERS_LIST_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    match = re.search(r'total\s*:\s*(\d+)', resp.text)
    return int(match.group(1)) if match else 118


def fetch_player_list() -> list[Player]:
    """
    Phase 1: Collect all player slugs by paginating through the AJAX API.

    Pages are 0-indexed. The API returns 150 players per page.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    total_pages = _discover_total_pages(session)
    logger.info("Phase 1: Fetching players from %d pages...", total_pages + 1)

    all_entries = []
    for page in range(total_pages + 1):  # 0-indexed, inclusive
        try:
            entries = _fetch_page_via_api(session, page)
            all_entries.extend(entries)

            if page % 10 == 0 or page == total_pages:
                logger.info("Fetched page %d/%d (%d players so far)", page, total_pages, len(all_entries))
            time.sleep(0.2)
        except Exception as e:
            logger.error("Failed to fetch page %d: %s", page, e)

    # Deduplicate by slug
    seen = set()
    players = []
    for entry in all_entries:
        player = _parse_player_entry(entry)
        if player.slug and player.slug not in seen:
            seen.add(player.slug)
            players.append(player)

    logger.info("Phase 1 complete: %d unique players collected", len(players))
    return players


def load_espn_player_names(espn_path: Path = ESPN_APPEARANCES_PATH) -> set[str]:
    """Load unique player names from ESPN appearances CSV."""
    if not espn_path.exists():
        logger.warning("ESPN appearances file not found: %s", espn_path)
        return set()
    names = set()
    with open(espn_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("player_name", "").strip()
            if name:
                names.add(name)
    logger.info("Loaded %d unique ESPN player names", len(names))
    return names


def prioritize_espn_players(players: list[Player], espn_names: set[str]) -> list[Player]:
    """Sort players so ESPN-matched players come first."""
    # Build a set of normalized ESPN names for matching
    espn_normalized = {_normalize_name(n) for n in espn_names}

    espn_players = []
    other_players = []
    for p in players:
        if p.slug in espn_normalized or _normalize_name(p.name) in espn_normalized:
            espn_players.append(p)
        else:
            other_players.append(p)

    logger.info(
        "Prioritized %d ESPN players (out of %d ESPN names), %d others",
        len(espn_players), len(espn_names), len(other_players),
    )
    return espn_players + other_players


def parse_player_details(html: str, player: Player) -> Player:
    """Parse bio details from an individual player page's HTML."""
    soup = BeautifulSoup(html, "lxml")

    for h3 in soup.find_all("h3"):
        label = h3.get_text(strip=True).lower()

        if label == "nationality":
            img = h3.find_next("img")
            if img and img.get("alt"):
                player.nationality = img["alt"]
            else:
                sibling = h3.find_next_sibling()
                if sibling:
                    text = sibling.get_text(strip=True)
                    if text:
                        player.nationality = text

        elif label == "age":
            sibling = h3.find_next_sibling()
            if sibling:
                text = sibling.get_text(strip=True)
            else:
                text = h3.next_sibling
                if text:
                    text = str(text).strip()
            if text and str(text).isdigit():
                player.age = int(text)

        elif label == "height":
            sibling = h3.find_next_sibling()
            if sibling:
                player.height = sibling.get_text(strip=True)
            else:
                text = h3.next_sibling
                if text:
                    player.height = str(text).strip()

        elif label == "weight":
            sibling = h3.find_next_sibling()
            if sibling:
                player.weight = sibling.get_text(strip=True)
            else:
                text = h3.next_sibling
                if text:
                    player.weight = str(text).strip()

    return player


def _fetch_single_player(player: Player) -> Player:
    """Fetch and parse a single player's detail page with retry (uses per-thread session)."""
    session = _get_session()
    url = f"{BASE_URL}/players/{player.slug}/"
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return parse_player_details(resp.text, player)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            raise


def _load_already_scraped(csv_path: Path) -> set[str]:
    """Load slugs already present in the CSV for resume capability."""
    if not csv_path.exists():
        return set()
    scraped = set()
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("slug"):
                scraped.add(row["slug"])
    logger.info("Found %d already-scraped players in CSV", len(scraped))
    return scraped


def _append_to_csv(csv_path: Path, players: list[Player], write_header: bool) -> None:
    """Append player rows to the CSV file."""
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if write_header:
            writer.writeheader()
        for player in players:
            row = asdict(player)
            writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})


def scrape_player_details(
    players: list[Player],
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> list[Player]:
    """
    Phase 2: Scrape individual player pages for bio details using thread pool.

    Uses per-thread sessions for better performance.
    Saves incrementally to CSV and supports resuming.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "players.csv"
    failed_path = output_dir / "failed_players.txt"

    # Resume support
    already_scraped = _load_already_scraped(csv_path)
    remaining = [p for p in players if p.slug not in already_scraped]
    write_header = not csv_path.exists() or csv_path.stat().st_size == 0

    if not remaining:
        logger.info("All players already scraped!")
        return players

    logger.info(
        "Phase 2: Scraping details for %d players (%d already done, %d remaining)",
        len(players), len(already_scraped), len(remaining),
    )

    completed = []
    failed = []
    total_processed = 0
    batch_size = 100

    for chunk_start in range(0, len(remaining), batch_size):
        chunk = remaining[chunk_start:chunk_start + batch_size]
        chunk_completed = []

        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            future_to_player = {executor.submit(_fetch_single_player, p): p for p in chunk}

            for future in as_completed(future_to_player):
                player = future_to_player[future]
                try:
                    result = future.result()
                    chunk_completed.append(result)
                    completed.append(result)
                except Exception as e:
                    failed.append(player.slug)
                    if "404" not in str(e):
                        logger.error("Failed to scrape %s: %s", player.slug, e)

        # Save chunk to CSV
        if chunk_completed:
            _append_to_csv(csv_path, chunk_completed, write_header)
            write_header = False

        total_processed += len(chunk)
        logger.info(
            "[%d/%d] Progress: %d completed, %d failed",
            total_processed, len(remaining), len(completed), len(failed),
        )

    # Save failed slugs
    if failed:
        with open(failed_path, "w", encoding="utf-8") as f:
            f.write("\n".join(failed))
        logger.warning("Saved %d failed slugs to %s", len(failed), failed_path)

    logger.info(
        "Phase 2 complete: %d scraped, %d failed", len(completed), len(failed),
    )
    return completed


def save_slugs_checkpoint(players: list[Player], output_dir: Path = DEFAULT_OUTPUT_DIR) -> None:
    """Save player slugs as a JSON checkpoint."""
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = output_dir / "player_slugs.json"
    data = [{"name": p.name, "slug": p.slug, "position": p.position, "team": p.team} for p in players]
    with open(checkpoint_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info("Saved %d player slugs to %s", len(data), checkpoint_path)


def load_slugs_checkpoint(output_dir: Path = DEFAULT_OUTPUT_DIR) -> list[Player] | None:
    """Load player slugs from a previous checkpoint."""
    checkpoint_path = output_dir / "player_slugs.json"
    if not checkpoint_path.exists():
        return None
    with open(checkpoint_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    players = [Player(name=d["name"], slug=d["slug"], position=d.get("position"), team=d.get("team")) for d in data]
    logger.info("Loaded %d players from checkpoint", len(players))
    return players


def scrape_all_players(output_dir: Path = DEFAULT_OUTPUT_DIR) -> list[Player]:
    """Full scrape: collect player list, then fetch details for all players."""
    # Try loading from checkpoint first
    players = load_slugs_checkpoint(output_dir)
    if players is None:
        players = fetch_player_list()
        save_slugs_checkpoint(players, output_dir)

    # Prioritize ESPN players
    espn_names = load_espn_player_names()
    if espn_names:
        players = prioritize_espn_players(players, espn_names)

    completed = scrape_player_details(players, output_dir)

    total_in_csv = len(_load_already_scraped(output_dir / "players.csv"))
    logger.info("Done! Total players in CSV: %d", total_in_csv)
    return completed
