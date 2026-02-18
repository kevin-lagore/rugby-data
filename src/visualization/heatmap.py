"""
Generate height vs weight density heatmaps for Six Nations players.

Usage:
    python -m src.visualization.heatmap              # static Round 1 2025 heatmap
    python -m src.visualization.heatmap --animate    # animated GIF across all seasons
"""

import argparse
import datetime as dt
import logging
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.animation import FuncAnimation, PillowWriter
import seaborn as sns

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
APPEARANCES_PATH = PROJECT_ROOT / "output" / "espn_appearances.csv"
ALL_INTERNATIONAL_PATH = PROJECT_ROOT / "output" / "espn_all_international_appearances.csv"
PLAYERS_PATH = PROJECT_ROOT / "data" / "players.csv"
OUTPUT_DIR = PROJECT_ROOT / "output"

TEAM_NATIONALITY = {
    "France": "France",
    "Wales": "Wales",
    "Scotland": "Scotland",
    "Italy": "Italy",
    "Ireland": "Ireland",
    "England": "England",
    "New Zealand": "New Zealand",
    "Australia": "Australia",
    "South Africa": "South Africa",
    "Argentina": "Argentina",
    "Japan": "Japan",
    "Fiji": "Fiji",
}

T1_TEAMS = set(TEAM_NATIONALITY.keys())

FORWARD_POSITIONS = {"P", "H", "L", "FL", "N8"}
BACK_POSITIONS = {"FB", "W", "C", "FH", "SH"}


def classify_position_group(position: str, shirt_number: str) -> str:
    """Classify a player as 'Forward' or 'Back' from position code and shirt number."""
    if position in FORWARD_POSITIONS:
        return "Forward"
    if position in BACK_POSITIONS:
        return "Back"
    # Replacements: use shirt number (16-20 forwards, 21-23 backs)
    try:
        num = int(shirt_number)
        return "Forward" if num <= 20 else "Back"
    except (ValueError, TypeError):
        return "Unknown"


def load_appearances() -> pd.DataFrame:
    """Load the full appearances CSV."""
    return pd.read_csv(APPEARANCES_PATH, dtype=str)


def load_player_biometrics() -> pd.DataFrame:
    """Load players.csv and parse height/weight to numeric columns."""
    df = pd.read_csv(PLAYERS_PATH, dtype=str)
    df["height_cm"] = pd.to_numeric(
        df["height"].str.replace("cm", "", regex=False), errors="coerce"
    )
    df["weight_kg"] = pd.to_numeric(
        df["weight"].str.replace("kg", "", regex=False), errors="coerce"
    )
    return df


def merge_players(players_df: pd.DataFrame, bio_df: pd.DataFrame) -> pd.DataFrame:
    """Join players with biometrics, disambiguating duplicate names."""
    merged = players_df.merge(
        bio_df, left_on="player_name", right_on="name", how="left", suffixes=("", "_bio")
    )

    merged["nationality_match"] = merged.apply(
        lambda row: row.get("nationality") == TEAM_NATIONALITY.get(row["team"], ""),
        axis=1,
    )
    merged["has_biometrics"] = merged["height_cm"].notna() & merged["weight_kg"].notna()

    merged = merged.sort_values(
        ["player_name", "nationality_match", "has_biometrics"],
        ascending=[True, False, False],
    )
    merged = merged.drop_duplicates(subset=["player_name"], keep="first")

    no_match = merged["name"].isna()
    good = ~no_match & merged["has_biometrics"]

    keep_cols = ["player_name", "team", "height_cm", "weight_kg"]
    if "position_group" in merged.columns:
        keep_cols.append("position_group")
    return merged.loc[good, keep_cols].reset_index(drop=True)


def group_into_rounds(apps_df: pd.DataFrame) -> list[dict]:
    """Group appearances into rounds by season and date clusters.

    Dates within 3 days of each other within a season form one round.
    Returns a list of dicts with 'season', 'round', 'dates', sorted chronologically.
    """
    rounds = []
    for season in sorted(apps_df["season"].unique()):
        season_df = apps_df[apps_df["season"] == season]
        dates = sorted(season_df["date"].unique())
        parsed = [dt.date.fromisoformat(d) for d in dates]

        # Cluster dates within 3 days of each other
        clusters: list[list[str]] = []
        current_cluster = [dates[0]]
        for i in range(1, len(dates)):
            if (parsed[i] - parsed[i - 1]).days <= 3:
                current_cluster.append(dates[i])
            else:
                clusters.append(current_cluster)
                current_cluster = [dates[i]]
        clusters.append(current_cluster)

        for round_num, date_cluster in enumerate(clusters, 1):
            rounds.append({
                "season": season,
                "round": round_num,
                "dates": date_cluster,
            })

    return rounds


def _format_season_label(season: str) -> str:
    """Convert '2024-25' to '2024/25' for display."""
    return season.replace("-", "/")


def create_heatmap(df: pd.DataFrame, output_path: Path) -> None:
    """Render a filled KDE heatmap with scatter overlay and save to disk."""
    fig, ax = plt.subplots(figsize=(10, 8))

    sns.kdeplot(
        data=df,
        x="weight_kg",
        y="height_cm",
        fill=True,
        cmap="YlOrRd",
        levels=15,
        alpha=0.7,
        ax=ax,
    )

    ax.scatter(
        df["weight_kg"],
        df["height_cm"],
        color="black",
        s=15,
        alpha=0.6,
        zorder=5,
        edgecolors="white",
        linewidths=0.5,
    )

    ax.set_xlabel("Weight (kg)", fontsize=13)
    ax.set_ylabel("Height (cm)", fontsize=13)
    ax.set_title("2025 Six Nations Round 1 \u2014 Player Height vs Weight", fontsize=15, pad=15)

    ax.xaxis.set_major_locator(ticker.MultipleLocator(10))
    ax.yaxis.set_major_locator(ticker.MultipleLocator(5))

    ax.text(
        0.5,
        -0.08,
        f"{len(df)} players across 6 teams (starters and substitutes)",
        transform=ax.transAxes,
        ha="center",
        fontsize=10,
        color="gray",
    )

    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved heatmap to %s", output_path)


def _median_cog(df: pd.DataFrame) -> tuple[float, float]:
    """Return (median weight, median height) or (nan, nan) if empty."""
    if len(df) >= 1:
        return df["weight_kg"].median(), df["height_cm"].median()
    return float("nan"), float("nan")


def _draw_trail_and_marker(
    ax,
    trail: list[tuple[float, float]],
    frame_idx: int,
    trail_length: int,
    color: str,
    label_text: str,
    y_offset: int = -15,
) -> None:
    """Draw a fading trail and current CoG marker for one group."""
    trail_start = max(0, frame_idx - trail_length + 1)
    trail_segment = trail[trail_start : frame_idx + 1]
    for i in range(len(trail_segment) - 1):
        age = len(trail_segment) - 1 - i
        alpha = max(0.08, 1.0 - age / trail_length)
        w0, h0 = trail_segment[i]
        w1, h1 = trail_segment[i + 1]
        ax.plot([w0, w1], [h0, h1], color=color, linewidth=2, alpha=alpha, zorder=9)

    cog_w, cog_h = trail[frame_idx]
    ax.scatter(
        cog_w, cog_h, color=color, s=200, marker="X",
        zorder=10, edgecolors="white", linewidths=1.5,
    )
    ax.annotate(
        label_text,
        (cog_w, cog_h),
        xytext=(12, y_offset),
        textcoords="offset points",
        fontsize=11,
        fontweight="bold",
        color="white",
        zorder=11,
        bbox=dict(boxstyle="round,pad=0.3", fc=color, alpha=0.8),
    )


def create_animation(output_path: Path, split_position: bool = False) -> None:
    """Create an animated GIF with one frame per round across all seasons."""
    apps_df = load_appearances()
    bio_df = load_player_biometrics()
    rounds = group_into_rounds(apps_df)

    # Pre-compute merged data for each round
    round_data: list[tuple[str, pd.DataFrame]] = []
    for r in rounds:
        mask = (apps_df["season"] == r["season"]) & (apps_df["date"].isin(r["dates"]))
        round_apps = apps_df.loc[mask]
        players = round_apps[["player_name", "team"]].drop_duplicates()

        if split_position:
            # Build position_group per player from their appearance data
            pos_map = {}
            for _, row in round_apps.iterrows():
                name = row["player_name"]
                if name not in pos_map:
                    pos_map[name] = classify_position_group(
                        row.get("position", ""), row.get("shirt_number", "")
                    )
            players = players.copy()
            players["position_group"] = players["player_name"].map(pos_map)

        merged = merge_players(players, bio_df)
        year = r["dates"][0][:4]
        label = f"{year} R{r['round']}"
        round_data.append((label, merged))
        logger.info("%s: %d players with biometrics", label, len(merged))

    # Compute fixed axis limits from all data with tight padding
    all_merged = pd.concat([d for _, d in round_data], ignore_index=True)
    weight_min = all_merged["weight_kg"].min() - 5
    weight_max = all_merged["weight_kg"].max() + 5
    height_min = all_merged["height_cm"].min() - 3
    height_max = all_merged["height_cm"].max() + 3

    TRAIL_LENGTH = 20

    if split_position:
        fwd_trail = [_median_cog(d[d["position_group"] == "Forward"]) for _, d in round_data]
        back_trail = [_median_cog(d[d["position_group"] == "Back"]) for _, d in round_data]
    else:
        combined_trail = [_median_cog(d) for _, d in round_data]

    fig, ax = plt.subplots(figsize=(10, 8))
    fig.subplots_adjust(top=0.90, bottom=0.12)

    def draw_frame(frame_idx: int) -> None:
        ax.clear()
        label, df = round_data[frame_idx]

        if len(df) >= 2:
            sns.kdeplot(
                data=df,
                x="weight_kg",
                y="height_cm",
                fill=True,
                cmap="YlOrRd",
                levels=15,
                alpha=0.7,
                ax=ax,
            )

        ax.scatter(
            df["weight_kg"],
            df["height_cm"],
            color="black",
            s=15,
            alpha=0.6,
            zorder=5,
            edgecolors="white",
            linewidths=0.5,
        )

        if split_position:
            fw = fwd_trail[frame_idx]
            bk = back_trail[frame_idx]
            _draw_trail_and_marker(
                ax, fwd_trail, frame_idx, TRAIL_LENGTH,
                color="red",
                label_text=f"Fwd {fw[0]:.0f}kg, {fw[1]:.0f}cm",
                y_offset=-18,
            )
            _draw_trail_and_marker(
                ax, back_trail, frame_idx, TRAIL_LENGTH,
                color="green",
                label_text=f"Back {bk[0]:.0f}kg, {bk[1]:.0f}cm",
                y_offset=12,
            )
        else:
            cog = combined_trail[frame_idx]
            _draw_trail_and_marker(
                ax, combined_trail, frame_idx, TRAIL_LENGTH,
                color="blue",
                label_text=f"{cog[0]:.0f}kg, {cog[1]:.0f}cm",
            )

        ax.set_xlim(weight_min, weight_max)
        ax.set_ylim(height_min, height_max)
        ax.set_xlabel("Weight (kg)", fontsize=13)
        ax.set_ylabel("Height (cm)", fontsize=13)
        ax.set_title(
            f"Six Nations {label} \u2014 Player Height vs Weight",
            fontsize=15,
        )
        ax.xaxis.set_major_locator(ticker.MultipleLocator(10))
        ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
        ax.text(
            0.5,
            -0.02,
            f"{len(df)} players with biometric data",
            transform=ax.transAxes,
            ha="center",
            fontsize=10,
            color="gray",
        )

    anim = FuncAnimation(
        fig,
        draw_frame,
        frames=len(round_data),
        interval=500,
        repeat=True,
    )
    anim.save(str(output_path), writer=PillowWriter(fps=2))
    plt.close(fig)
    logger.info("Saved animation to %s", output_path)


def _load_t1_monthly_data() -> tuple[list[str], list[tuple[str, pd.DataFrame]]]:
    """Load T1-vs-T1 appearances, classify positions, group by month.

    Returns (months, round_data) where round_data is [(label, merged_df), ...].
    """
    apps_df = pd.read_csv(ALL_INTERNATIONAL_PATH, dtype=str)
    bio_df = load_player_biometrics()

    t1_mask = apps_df["home_team"].isin(T1_TEAMS) & apps_df["away_team"].isin(T1_TEAMS)
    apps_df = apps_df[t1_mask].copy()
    logger.info("T1-vs-T1 appearances: %d", len(apps_df))

    apps_df["month"] = apps_df["date"].str[:7]
    months = sorted(apps_df["month"].unique())

    round_data: list[tuple[str, pd.DataFrame]] = []
    for month in months:
        month_apps = apps_df[apps_df["month"] == month]
        players = month_apps[["player_name", "team"]].drop_duplicates()

        pos_map = {}
        for _, row in month_apps.iterrows():
            name = row["player_name"]
            if name not in pos_map:
                pos_map[name] = classify_position_group(
                    row.get("position", ""), row.get("shirt_number", "")
                )
        players = players.copy()
        players["position_group"] = players["player_name"].map(pos_map)

        merged = merge_players(players, bio_df)
        d = dt.date.fromisoformat(month + "-01")
        label = d.strftime("%b %Y")
        round_data.append((label, merged))
        logger.info("%s: %d players with biometrics", label, len(merged))

    return months, round_data


def create_t1_animation(output_path: Path, split_position: bool = False) -> None:
    """Create an animated GIF of T1-vs-T1 matches, one frame per month."""
    months, round_data = _load_t1_monthly_data()

    # Compute fixed axis limits
    all_merged = pd.concat([d for _, d in round_data], ignore_index=True)
    weight_min = all_merged["weight_kg"].min() - 5
    weight_max = all_merged["weight_kg"].max() + 5
    height_min = all_merged["height_cm"].min() - 3
    height_max = all_merged["height_cm"].max() + 3

    TRAIL_LENGTH = 40

    if split_position:
        fwd_trail = [_median_cog(d[d["position_group"] == "Forward"]) for _, d in round_data]
        back_trail = [_median_cog(d[d["position_group"] == "Back"]) for _, d in round_data]
    else:
        combined_trail = [_median_cog(d) for _, d in round_data]

    fig, ax = plt.subplots(figsize=(10, 8))
    fig.subplots_adjust(top=0.90, bottom=0.12)

    def draw_frame(frame_idx: int) -> None:
        ax.clear()
        label, df = round_data[frame_idx]

        if len(df) >= 2:
            sns.kdeplot(
                data=df,
                x="weight_kg",
                y="height_cm",
                fill=True,
                cmap="YlOrRd",
                levels=15,
                alpha=0.7,
                ax=ax,
            )

        ax.scatter(
            df["weight_kg"],
            df["height_cm"],
            color="black",
            s=15,
            alpha=0.6,
            zorder=5,
            edgecolors="white",
            linewidths=0.5,
        )

        if split_position:
            fw = fwd_trail[frame_idx]
            bk = back_trail[frame_idx]
            _draw_trail_and_marker(
                ax, fwd_trail, frame_idx, TRAIL_LENGTH,
                color="red",
                label_text=f"Fwd {fw[0]:.0f}kg, {fw[1]:.0f}cm",
                y_offset=-18,
            )
            _draw_trail_and_marker(
                ax, back_trail, frame_idx, TRAIL_LENGTH,
                color="green",
                label_text=f"Back {bk[0]:.0f}kg, {bk[1]:.0f}cm",
                y_offset=12,
            )
        else:
            cog = combined_trail[frame_idx]
            _draw_trail_and_marker(
                ax, combined_trail, frame_idx, TRAIL_LENGTH,
                color="blue",
                label_text=f"{cog[0]:.0f}kg, {cog[1]:.0f}cm",
            )

        ax.set_xlim(weight_min, weight_max)
        ax.set_ylim(height_min, height_max)
        ax.set_xlabel("Weight (kg)", fontsize=13)
        ax.set_ylabel("Height (cm)", fontsize=13)
        ax.set_title(
            f"T1 International Rugby {label} \u2014 Player Height vs Weight",
            fontsize=15,
        )
        ax.xaxis.set_major_locator(ticker.MultipleLocator(10))
        ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
        ax.text(
            0.5,
            -0.02,
            f"{len(df)} players with biometric data",
            transform=ax.transAxes,
            ha="center",
            fontsize=10,
            color="gray",
        )

    anim = FuncAnimation(
        fig,
        draw_frame,
        frames=len(round_data),
        interval=333,
        repeat=True,
    )
    anim.save(str(output_path), writer=PillowWriter(fps=3))
    plt.close(fig)
    logger.info("Saved T1 animation to %s (%d frames)", output_path, len(round_data))


def create_t1_trend_charts(output_dir: Path) -> None:
    """Create static monthly line charts for median height and weight (fwd vs back)."""
    months, round_data = _load_t1_monthly_data()

    dates = [dt.date.fromisoformat(m + "-01") for m in months]

    fwd_height, fwd_weight = [], []
    back_height, back_weight = [], []
    for _, df in round_data:
        fwd = df[df["position_group"] == "Forward"]
        bck = df[df["position_group"] == "Back"]
        fwd_height.append(fwd["height_cm"].median() if len(fwd) else float("nan"))
        fwd_weight.append(fwd["weight_kg"].median() if len(fwd) else float("nan"))
        back_height.append(bck["height_cm"].median() if len(bck) else float("nan"))
        back_weight.append(bck["weight_kg"].median() if len(bck) else float("nan"))

    # --- Median Height chart ---
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(dates, fwd_height, color="red", linewidth=1.5, alpha=0.4, label="_nolegend_")
    ax.plot(dates, back_height, color="green", linewidth=1.5, alpha=0.4, label="_nolegend_")

    # Rolling average (window of 5 months) for smoother trend
    fwd_h_series = pd.Series(fwd_height).rolling(5, min_periods=1, center=True).mean()
    back_h_series = pd.Series(back_height).rolling(5, min_periods=1, center=True).mean()
    ax.plot(dates, fwd_h_series, color="red", linewidth=2.5, label="Forwards")
    ax.plot(dates, back_h_series, color="green", linewidth=2.5, label="Backs")

    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Median Height (cm)", fontsize=12)
    ax.set_title("T1 International Rugby \u2014 Median Player Height Over Time", fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)

    height_path = output_dir / "t1_median_height_trend.png"
    fig.tight_layout()
    fig.savefig(height_path, dpi=150)
    plt.close(fig)
    logger.info("Saved height trend to %s", height_path)

    # --- Median Weight chart ---
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(dates, fwd_weight, color="red", linewidth=1.5, alpha=0.4, label="_nolegend_")
    ax.plot(dates, back_weight, color="green", linewidth=1.5, alpha=0.4, label="_nolegend_")

    fwd_w_series = pd.Series(fwd_weight).rolling(5, min_periods=1, center=True).mean()
    back_w_series = pd.Series(back_weight).rolling(5, min_periods=1, center=True).mean()
    ax.plot(dates, fwd_w_series, color="red", linewidth=2.5, label="Forwards")
    ax.plot(dates, back_w_series, color="green", linewidth=2.5, label="Backs")

    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Median Weight (kg)", fontsize=12)
    ax.set_title("T1 International Rugby \u2014 Median Player Weight Over Time", fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)

    weight_path = output_dir / "t1_median_weight_trend.png"
    fig.tight_layout()
    fig.savefig(weight_path, dpi=150)
    plt.close(fig)
    logger.info("Saved weight trend to %s", weight_path)

    return height_path, weight_path


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Six Nations height/weight heatmaps")
    parser.add_argument("--animate", action="store_true", help="Generate animated GIF")
    parser.add_argument(
        "--t1", action="store_true",
        help="Animate all T1-vs-T1 international matches (monthly frames, 3 fps)",
    )
    parser.add_argument(
        "--trends", action="store_true",
        help="Generate static monthly trend line charts (median height & weight, fwd vs back)",
    )
    parser.add_argument(
        "--split-position", action="store_true",
        help="Split centre of gravity into forwards and backs",
    )
    args = parser.parse_args()

    if args.trends:
        height_path, weight_path = create_t1_trend_charts(OUTPUT_DIR)
        print(f"Done. Charts saved to:\n  {height_path}\n  {weight_path}")
    elif args.t1:
        output_path = OUTPUT_DIR / "t1_international_height_weight_animated.gif"
        create_t1_animation(output_path, split_position=args.split_position)
        print(f"Done. Animation saved to {output_path}")
    elif args.animate:
        output_path = OUTPUT_DIR / "six_nations_height_weight_animated.gif"
        create_animation(output_path, split_position=args.split_position)
        print(f"Done. Animation saved to {output_path}")
    else:
        apps_df = load_appearances()
        bio_df = load_player_biometrics()
        mask = (apps_df["season"] == "2024-25") & (
            apps_df["date"].isin(["2025-01-31", "2025-02-01"])
        )
        round1 = apps_df.loc[mask, ["player_name", "team"]].drop_duplicates()
        merged = merge_players(round1, bio_df)

        output_path = OUTPUT_DIR / "six_nations_2025_r1_height_weight_heatmap.png"
        create_heatmap(merged, output_path)
        print(f"Done. Heatmap saved to {output_path}")


if __name__ == "__main__":
    main()
