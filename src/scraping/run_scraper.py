"""Entry point for running the rugby player scraper."""

import logging

from src.scraping.player_scraper import scrape_all_players


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    players = scrape_all_players()
    print(f"\nDone. Scraped {len(players)} players.")


if __name__ == "__main__":
    main()
