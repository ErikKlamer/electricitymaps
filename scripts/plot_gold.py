"""Plot the Gold time series as PNG charts for the README.

    poetry run python scripts/plot_gold.py [location]   # default: the S3 data lake -> docs/*.png

Daily values are aggregated to full calendar months; 5 years of daily data is too noisy to read.
"""

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import polars as pl  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

from emaps_etl import reader  # noqa: E402
from emaps_etl.config import get_settings  # noqa: E402

OUTPUT = Path("docs")
NOTE = "Surrogate data (demo) until 3 Oct 2026, real Electricity Maps data after. Full months only."

# Chart chrome (light surface).
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"

# Energy sources grouped into series, each with a fixed categorical color (bottom to top).
SOURCE_GROUPS = {
    "Nuclear": (["nuclear"], "#2a78d6"),
    "Hydro": (["hydro", "hydro_storage_discharge"], "#eb6834"),
    "Wind": (["wind"], "#1baf7a"),
    "Solar": (["solar"], "#eda100"),
    "Gas": (["gas"], "#e87ba4"),
    "Coal & oil": (["coal", "oil"], "#008300"),
    "Other": (["biomass", "battery_storage_discharge", "geothermal", "unknown"], "#4a3aa7"),
}
EXPORT_COLOR, IMPORT_COLOR = "#2a78d6", "#e34948"


def full_months(df: pl.DataFrame) -> pl.DataFrame:
    """Add a `month` column and keep only months for which every day is present."""
    df = df.with_columns(month=pl.col("date_utc").dt.truncate("1mo"))
    complete = (
        df.group_by("month")
        .agg(days=pl.col("date_utc").n_unique())
        .filter(pl.col("days") == pl.col("month").dt.month_end().dt.day())
    )
    return df.join(complete.select("month"), on="month")


def style(ax: plt.Axes) -> None:
    ax.set_facecolor(SURFACE)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(BASELINE)
    ax.tick_params(colors=MUTED, labelsize=9, length=0)


def read_gold(root: str, table: str) -> pl.DataFrame:
    return reader.read_table(reader.table_path(root, "gold", table))


def plot_mix_share(root: str) -> Path:
    daily = full_months(read_gold(root, "daily_relative_mix"))
    group_of = {s: group for group, (sources, _) in SOURCE_GROUPS.items() for s in sources}
    monthly = (
        daily.with_columns(group=pl.col("source").replace_strict(group_of))
        .group_by("month", "group")
        .agg(pl.col("energy_mwh").sum())
        .with_columns(share=pl.col("energy_mwh") / pl.col("energy_mwh").sum().over("month") * 100)
        .pivot("group", index="month", values="share")
        .sort("month")
        .fill_null(0)
    )
    months = monthly["month"].to_list()
    groups = list(SOURCE_GROUPS)

    fig, ax = plt.subplots(figsize=(11, 5.5), facecolor=SURFACE)
    ax.stackplot(
        months,
        [monthly[g] for g in groups],
        colors=[SOURCE_GROUPS[g][1] for g in groups],
        labels=groups,
        edgecolor=SURFACE,
        linewidth=1,
    )
    style(ax)
    ax.set_ylim(0, 100)
    ax.set_xlim(months[0], months[-1])
    ax.set_ylabel("Share of monthly production (%)", color=INK_SECONDARY, fontsize=10)

    # Direct labels at the right edge for the larger bands (the legend covers all of them).
    bottom = 0.0
    for group in groups:
        share = monthly[group][-1]
        if share >= 4:
            ax.text(
                months[-1],
                bottom + share / 2,
                f"  {group} {share:.0f}%",
                va="center",
                fontsize=9,
                color=INK,
            )
        bottom += share

    ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.08),
        ncol=len(groups),
        frameon=False,
        fontsize=9,
        labelcolor=INK_SECONDARY,
    )
    fig.suptitle(
        "France: monthly electricity production mix",
        x=0.06,
        ha="left",
        fontsize=14,
        color=INK,
    )
    ax.set_title(NOTE, loc="left", fontsize=9, color=MUTED)
    fig.subplots_adjust(left=0.06, right=0.88, top=0.86, bottom=0.16)
    return save(fig, "energy_mix_share.png")


def plot_net_exports(root: str) -> Path:
    exports = read_gold(root, "fr_daily_exports").select(
        "date_utc", neighbour=pl.col("to_zone"), name=pl.col("to_zone_name"), net=pl.col("net_mwh")
    )
    imports = read_gold(root, "fr_daily_imports").select(
        "date_utc",
        neighbour=pl.col("from_zone"),
        name=pl.col("from_zone_name"),
        net=-pl.col("net_mwh"),
    )
    monthly = (
        full_months(pl.concat([exports, imports]))
        .group_by("month", "neighbour", "name")
        .agg(net_gwh=pl.col("net").sum() / 1000)
        .sort("month")
    )
    neighbours = (
        monthly.group_by("neighbour", "name")
        .agg(pl.col("net_gwh").abs().sum())
        .sort("net_gwh", descending=True)
        .rows()
    )

    fig, axes = plt.subplots(2, 4, figsize=(13, 6), sharex=True, sharey=True, facecolor=SURFACE)
    for ax, (zone, name, _) in zip(axes.flat, neighbours, strict=False):
        data = monthly.filter(pl.col("neighbour") == zone)
        values = data["net_gwh"].to_list()
        ax.bar(
            data["month"].to_list(),
            values,
            width=24,  # days; leaves a gap between monthly bars
            color=[EXPORT_COLOR if v >= 0 else IMPORT_COLOR for v in values],
        )
        style(ax)
        ax.spines["bottom"].set_visible(False)  # the zero line is the baseline
        ax.axhline(0, color=BASELINE, linewidth=0.8)
        ax.set_title(f"{name} ({zone})", loc="left", fontsize=10, color=INK)

    # The 8th panel explains the encoding.
    legend_ax = axes.flat[-1]
    legend_ax.axis("off")
    legend_ax.legend(
        handles=[
            Patch(color=EXPORT_COLOR, label="Net export from France"),
            Patch(color=IMPORT_COLOR, label="Net import into France"),
        ],
        loc="center",
        frameon=False,
        fontsize=10,
        labelcolor=INK_SECONDARY,
    )

    for ax in axes[:, 0]:
        ax.set_ylabel("Net GWh per month", color=INK_SECONDARY, fontsize=10)
    fig.suptitle(
        "France: monthly net electricity exchange per neighbour",
        x=0.05,
        ha="left",
        fontsize=14,
        color=INK,
    )
    fig.text(0.05, 0.915, NOTE, fontsize=9, color=MUTED)
    fig.subplots_adjust(left=0.07, right=0.98, top=0.85, bottom=0.07, hspace=0.3, wspace=0.08)
    return save(fig, "net_exchange_by_neighbour.png")


def save(fig: plt.Figure, name: str) -> Path:
    OUTPUT.mkdir(exist_ok=True)
    path = OUTPUT / name
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    return path


def main() -> None:
    root = sys.argv[1] if len(sys.argv) > 1 else get_settings().storage_uri
    for path in (plot_mix_share(root), plot_net_exports(root)):
        print(f"Wrote {path}")


if __name__ == "__main__":
    main()
