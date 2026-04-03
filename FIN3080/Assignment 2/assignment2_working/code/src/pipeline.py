from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd

from .data_io import (
    DataPaths,
    discover_data_paths,
    load_fund_monthly,
    load_index_prices,
    load_margin_balance,
    load_representative_prices,
    load_trading_value,
    load_valuation_and_bond,
)


plt.rcParams["font.family"] = "Times New Roman"
plt.rcParams["axes.unicode_minus"] = False


EPISODES = {
    "2015": {"start": "2014-06-19", "end": "2015-06-12"},
    "2021": {"start": "2019-01-04", "end": "2021-02-18"},
    "current": {"start": "2024-09-18", "end": "2026-03-09"},
}

FIXED_CURRENT_FUND_END_MONTH = pd.Period("2026-02", freq="M")
BROKER_CODES = {
    "000166",
    "000686",
    "000712",
    "000728",
    "000750",
    "000776",
    "000783",
    "002500",
    "002670",
    "002673",
    "002736",
    "002797",
    "002926",
    "002939",
    "002945",
    "600030",
    "600061",
    "600095",
    "600109",
    "600155",
    "600369",
    "600621",
    "600837",
    "600864",
    "600906",
    "600909",
    "600918",
    "600958",
    "600999",
    "601059",
    "601066",
    "601099",
    "601108",
    "601136",
    "601162",
    "601198",
    "601211",
    "601236",
    "601375",
    "601377",
    "601456",
    "601555",
    "601688",
    "601696",
    "601788",
    "601878",
    "601881",
    "601901",
    "601990",
    "601995",
}


@dataclass
class LoadedData:
    paths: DataPaths
    trading_value: pd.Series
    index_price: pd.Series
    margin_balance: pd.Series
    fund_monthly: pd.Series
    valuation_bond: pd.DataFrame
    representative_prices: pd.DataFrame


def run_pipeline(project_root: Path) -> None:
    outputs_dir = project_root / "outputs"
    tables_dir = outputs_dir / "tables"
    figures_dir = outputs_dir / "figures"
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    data = load_all_data(project_root)
    summary_df, current_trend_df, metadata = build_outputs(data)

    summary_path = tables_dir / "episode_summary.csv"
    trend_path = tables_dir / "current_episode_trend.csv"
    notes_path = tables_dir / "analysis_notes.txt"
    market_summary_path = tables_dir / "market_indicator_summary.csv"
    valuation_summary_path = tables_dir / "valuation_summary.csv"
    stock_summary_path = tables_dir / "stock_summary.csv"

    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    current_trend_df.to_csv(trend_path, index=False, encoding="utf-8-sig")
    notes_path.write_text(build_notes(summary_df, metadata), encoding="utf-8")
    build_display_tables(summary_df).to_csv(market_summary_path, index=False, encoding="utf-8-sig")
    build_valuation_table(summary_df).to_csv(
        valuation_summary_path, index=False, encoding="utf-8-sig"
    )
    build_stock_table(summary_df).to_csv(stock_summary_path, index=False, encoding="utf-8-sig")

    figure_paths = build_figures(current_trend_df, figures_dir, metadata)
    build_pdf_report(summary_df, current_trend_df, metadata, figure_paths, project_root)

    print(f"Saved summary table to {summary_path}")
    print(f"Saved current trend table to {trend_path}")
    print(f"Saved analysis notes to {notes_path}")
    print(f"Saved market summary table to {market_summary_path}")
    print(f"Saved valuation summary table to {valuation_summary_path}")
    print(f"Saved stock summary table to {stock_summary_path}")
    print("Pipeline finished successfully.")


def load_all_data(project_root: Path) -> LoadedData:
    paths = discover_data_paths(project_root)
    return LoadedData(
        paths=paths,
        trading_value=load_trading_value(paths),
        index_price=load_index_prices(paths),
        margin_balance=load_margin_balance(paths),
        fund_monthly=load_fund_monthly(paths),
        valuation_bond=load_valuation_and_bond(paths),
        representative_prices=load_representative_prices(paths),
    )


def build_outputs(data: LoadedData) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
    current_best_broker = find_best_broker(
        data.representative_prices,
        pd.Timestamp(EPISODES["current"]["start"]),
        pd.Timestamp(EPISODES["current"]["end"]),
    )

    summary_rows: list[dict[str, object]] = []
    for name, info in EPISODES.items():
        start = pd.Timestamp(info["start"])
        end = pd.Timestamp(info["end"])
        summary_rows.append(compute_episode_summary(name, start, end, data))

    summary_df = pd.DataFrame(summary_rows)
    current_trend_df = build_current_trend(data, current_best_broker["code"])

    metadata = {
        "current_best_broker_code": current_best_broker["code"],
        "current_best_broker_return_pct": f"{current_best_broker['return'] * 100:.2f}",
    }
    return summary_df, current_trend_df, metadata


def compute_episode_summary(
    episode_name: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    data: LoadedData,
) -> dict[str, object]:
    trading_start = get_nearest_value(data.trading_value, start)
    trading_end = get_nearest_value(data.trading_value, end)
    trading_interval = data.trading_value.loc[start:end]
    trading_peak_date = trading_interval.idxmax()
    trading_peak_value = trading_interval.max()

    index_start = get_nearest_value(data.index_price, start)
    index_peak = get_nearest_value(data.index_price, trading_peak_date)
    index_end = get_nearest_value(data.index_price, end)

    margin_start = get_nearest_value(data.margin_balance, start)
    margin_end = get_nearest_value(data.margin_balance, end)

    start_month = start.to_period("M")
    end_month = end.to_period("M")
    if episode_name == "current":
        end_month = FIXED_CURRENT_FUND_END_MONTH

    fund_start = get_nearest_month_value(data.fund_monthly, start_month, start_month, end_month)
    fund_end = get_nearest_month_value(data.fund_monthly, end_month, start_month, end_month)

    start_val_row = get_nearest_row(data.valuation_bond, start)
    end_val_row = get_nearest_row(data.valuation_bond, end)
    history_end = pd.Timestamp(year=start.year - 1, month=12, day=31)
    history = data.valuation_bond.loc[data.valuation_bond["date"] <= history_end].copy()

    eastmoney_return = compute_stock_return(data.representative_prices, "300059", start, end)
    hundsun_return = compute_stock_return(data.representative_prices, "600570", start, end)
    best_broker = find_best_broker(data.representative_prices, start, end)

    return {
        "episode": episode_name,
        "start_date": start.date().isoformat(),
        "end_date": end.date().isoformat(),
        "trading_value_ratio": trading_end / trading_start,
        "max_trading_value_ratio": trading_peak_value / trading_start,
        "trading_peak_date": trading_peak_date.date().isoformat(),
        "sse_before_peak_return": index_peak / index_start - 1.0,
        "sse_after_peak_return": index_end / index_peak - 1.0,
        "margin_balance_ratio": margin_end / margin_start,
        "fund_issuance_ratio": fund_end / fund_start,
        "fund_start_month": str(start_month),
        "fund_end_month": str(end_month),
        "erp_start_pct": start_val_row["erp"] * 100.0,
        "erp_end_pct": end_val_row["erp"] * 100.0,
        "erp_start_percentile": empirical_percentile(history["erp"], start_val_row["erp"]) * 100.0,
        "erp_end_percentile": empirical_percentile(history["erp"], end_val_row["erp"]) * 100.0,
        "dividend_spread_start_pct": start_val_row["dividend_spread"] * 100.0,
        "dividend_spread_end_pct": end_val_row["dividend_spread"] * 100.0,
        "dividend_spread_start_percentile": empirical_percentile(
            history["dividend_spread"], start_val_row["dividend_spread"]
        )
        * 100.0,
        "dividend_spread_end_percentile": empirical_percentile(
            history["dividend_spread"], end_val_row["dividend_spread"]
        )
        * 100.0,
        "eastmoney_return": eastmoney_return,
        "hundsun_return": hundsun_return,
        "best_broker_code": best_broker["code"],
        "best_broker_return": best_broker["return"],
    }


def build_current_trend(data: LoadedData, fixed_best_broker_code: str) -> pd.DataFrame:
    start = pd.Timestamp(EPISODES["current"]["start"])
    end = pd.Timestamp(EPISODES["current"]["end"])
    current_dates = data.trading_value.loc[start:end].index

    trading_start = get_nearest_value(data.trading_value, start)
    margin_start = get_nearest_value(data.margin_balance, start)
    fund_start_month = start.to_period("M")
    fund_start = get_nearest_month_value(
        data.fund_monthly, fund_start_month, fund_start_month, FIXED_CURRENT_FUND_END_MONTH
    )
    eastmoney_start = get_stock_price_on_date(data.representative_prices, "300059", start)
    hundsun_start = get_stock_price_on_date(data.representative_prices, "600570", start)
    best_broker_start = get_stock_price_on_date(
        data.representative_prices, fixed_best_broker_code, start
    )

    records: list[dict[str, object]] = []
    valuation_map = data.valuation_bond.set_index("date")

    for date in current_dates:
        effective_month = min(date.to_period("M"), FIXED_CURRENT_FUND_END_MONTH)
        fund_end = get_nearest_month_value(
            data.fund_monthly,
            effective_month,
            fund_start_month,
            FIXED_CURRENT_FUND_END_MONTH,
        )

        record = {
            "date": date.date().isoformat(),
            "trading_value_ratio": get_nearest_value(data.trading_value, date) / trading_start,
            "max_trading_value_ratio": data.trading_value.loc[start:date].max() / trading_start,
            "margin_balance_ratio": get_nearest_value(data.margin_balance, date) / margin_start,
            "fund_issuance_ratio": fund_end / fund_start,
            "erp_pct": get_nearest_row(data.valuation_bond, date)["erp"] * 100.0,
            "dividend_spread_pct": get_nearest_row(data.valuation_bond, date)["dividend_spread"]
            * 100.0,
            "eastmoney_return": get_stock_price_on_date(
                data.representative_prices, "300059", date
            )
            / eastmoney_start
            - 1.0,
            "hundsun_return": get_stock_price_on_date(
                data.representative_prices, "600570", date
            )
            / hundsun_start
            - 1.0,
            "best_broker_return": get_stock_price_on_date(
                data.representative_prices, fixed_best_broker_code, date
            )
            / best_broker_start
            - 1.0,
        }
        records.append(record)

    return pd.DataFrame(records)


def build_figures(
    current_trend_df: pd.DataFrame,
    figures_dir: Path,
    metadata: dict[str, str],
) -> list[Path]:
    trend = current_trend_df.copy()
    trend["date"] = pd.to_datetime(trend["date"])
    figure_paths: list[Path] = []

    fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
    plot_line(axes[0, 0], trend, "trading_value_ratio", "Trading Value Ratio")
    plot_line(axes[0, 1], trend, "max_trading_value_ratio", "Max Trading Value Ratio")
    plot_line(axes[1, 0], trend, "margin_balance_ratio", "Margin Balance Ratio")
    plot_line(axes[1, 1], trend, "fund_issuance_ratio", "Fund Issuance Ratio")
    path1 = figures_dir / "current_trend_market_indicators.png"
    fig.savefig(path1, dpi=200)
    plt.close(fig)
    figure_paths.append(path1)

    fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
    plot_line(axes[0, 0], trend, "erp_pct", "CSI 300 ERP (%)")
    plot_line(axes[0, 1], trend, "dividend_spread_pct", "Dividend Yield - 10Y Yield (%)")
    plot_line(axes[1, 0], trend, "eastmoney_return", "East Money Return")
    plot_line(
        axes[1, 1],
        trend,
        "hundsun_return",
        "Hundsun Return",
    )
    path2 = figures_dir / "current_trend_valuation_and_stocks.png"
    fig.savefig(path2, dpi=200)
    plt.close(fig)
    figure_paths.append(path2)

    fig, ax = plt.subplots(figsize=(12, 4), constrained_layout=True)
    plot_line(
        ax,
        trend,
        "best_broker_return",
        f"Best Brokerage ({metadata['current_best_broker_code']}) Return",
    )
    path3 = figures_dir / "current_trend_best_broker.png"
    fig.savefig(path3, dpi=200)
    plt.close(fig)
    figure_paths.append(path3)

    fig, ax = plt.subplots(figsize=(12, 5), constrained_layout=True)
    ax.plot(trend["date"], trend["eastmoney_return"], label="East Money", linewidth=1.5)
    ax.plot(trend["date"], trend["hundsun_return"], label="Hundsun", linewidth=1.5)
    ax.plot(
        trend["date"],
        trend["best_broker_return"],
        label=f"Best Broker ({metadata['current_best_broker_code']})",
        linewidth=1.5,
    )
    ax.set_title("Representative Stock Returns", fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.legend()
    path4 = figures_dir / "current_trend_stock_comparison.png"
    fig.savefig(path4, dpi=200)
    plt.close(fig)
    figure_paths.append(path4)

    return figure_paths


def build_pdf_report(
    summary_df: pd.DataFrame,
    current_trend_df: pd.DataFrame,
    metadata: dict[str, str],
    figure_paths: list[Path],
    project_root: Path,
) -> None:
    report_dir = project_root.parent / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = report_dir / "assignment2_report_draft.pdf"

    with PdfPages(pdf_path) as pdf:
        fig = plt.figure(figsize=(8.27, 11.69))
        ax = fig.add_axes([0.08, 0.06, 0.84, 0.88])
        ax.axis("off")
        current = summary_df.loc[summary_df["episode"] == "current"].iloc[0]
        lines = [
            "FIN3080 Assignment 2",
            "",
            "Current market bubble assessment: early-to-mid stage.",
            "",
            "Main evidence:",
            f"1. Trading value ratio in the current episode is {current['trading_value_ratio']:.2f},",
            "   above 2021 but well below the 2015 boom.",
            f"2. Margin balance ratio is only {current['margin_balance_ratio']:.2f},",
            "   lower than both historical episodes.",
            f"3. Fund issuance ratio is {current['fund_issuance_ratio']:.2f},",
            "   far below the 2015 and 2021 episodes.",
            f"4. ERP fell from {current['erp_start_pct']:.2f}% to {current['erp_end_pct']:.2f}%,",
            f"   but the end percentile remains {current['erp_end_percentile']:.1f}.",
            "5. East Money, Hundsun and the best broker all rose,",
            "   but their gains are still below the most speculative 2015 pattern.",
            "",
            "Interpretation:",
            "The current episode shows clear speculation and strong turnover,",
            "but leverage, fund issuance and valuation compression are still milder",
            "than in the historical bubbles. This is more consistent with an",
            "early-to-mid stage boom, although the turnover peak in January 2026",
            "suggests some short-term cooling after the most euphoric phase.",
            "",
            "Key assumptions:",
            "1. A-share trading value uses Markettype = 21 in TRD_Cndalym.csv.",
            "2. Fund issuance ratio for the current episode uses 2026-02 as the end month.",
            "3. ERP and dividend spread percentiles use historical data up to the prior year-end.",
            "",
            "Best brokerage stock in the current interval:",
            f"Code: {metadata['current_best_broker_code']}",
            f"Full-interval return: {metadata['current_best_broker_return_pct']}%",
        ]
        ax.text(0, 1, "\n".join(lines), va="top", fontsize=11, linespacing=1.5)
        pdf.savefig(fig)
        plt.close(fig)

        fig = plt.figure(figsize=(11.69, 8.27))
        ax = fig.add_axes([0.04, 0.54, 0.92, 0.38])
        ax.axis("off")
        ax.text(0, 1.05, "Table 1. Market, leverage and issuance indicators", fontsize=12, va="bottom")
        table_df = build_display_tables(summary_df)
        table = ax.table(
            cellText=table_df.values,
            colLabels=table_df.columns,
            loc="center",
            cellLoc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.5)

        ax2 = fig.add_axes([0.04, 0.05, 0.92, 0.38])
        ax2.axis("off")
        ax2.text(
            0,
            1.05,
            "Figure 1. Current episode trends for trading, leverage and fund issuance",
            fontsize=12,
            va="bottom",
        )
        ax2.imshow(plt.imread(figure_paths[0]))
        ax2.axis("off")
        pdf.savefig(fig)
        plt.close(fig)

        fig = plt.figure(figsize=(11.69, 8.27))
        ax = fig.add_axes([0.04, 0.54, 0.92, 0.38])
        ax.axis("off")
        ax.text(0, 1.05, "Table 2. ERP and dividend-spread comparisons", fontsize=12, va="bottom")
        valuation_df = build_valuation_table(summary_df)
        table = ax.table(
            cellText=valuation_df.values,
            colLabels=valuation_df.columns,
            loc="center",
            cellLoc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.5)

        ax2 = fig.add_axes([0.04, 0.05, 0.92, 0.38])
        ax2.axis("off")
        ax2.text(
            0,
            1.05,
            "Figure 2. Current episode valuation and representative-stock trends",
            fontsize=12,
            va="bottom",
        )
        ax2.imshow(plt.imread(figure_paths[1]))
        ax2.axis("off")
        pdf.savefig(fig)
        plt.close(fig)

        fig = plt.figure(figsize=(11.69, 8.27))
        ax = fig.add_axes([0.04, 0.62, 0.92, 0.26])
        ax.axis("off")
        ax.text(0, 1.08, "Table 3. Representative-stock performance", fontsize=12, va="bottom")
        stock_df = build_stock_table(summary_df)
        table = ax.table(
            cellText=stock_df.values,
            colLabels=stock_df.columns,
            loc="center",
            cellLoc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.5)

        ax2 = fig.add_axes([0.04, 0.08, 0.92, 0.42])
        ax2.axis("off")
        ax2.text(
            0,
            1.05,
            f"Figure 3. Current best brokerage stock ({metadata['current_best_broker_code']})",
            fontsize=12,
            va="bottom",
        )
        ax2.imshow(plt.imread(figure_paths[2]))
        ax2.axis("off")
        pdf.savefig(fig)
        plt.close(fig)


def build_notes(summary_df: pd.DataFrame, metadata: dict[str, str]) -> str:
    current = summary_df.loc[summary_df["episode"] == "current"].iloc[0]
    past = summary_df.loc[summary_df["episode"].isin(["2015", "2021"])]

    lower_count = 0
    for column in [
        "trading_value_ratio",
        "max_trading_value_ratio",
        "margin_balance_ratio",
        "fund_issuance_ratio",
    ]:
        if current[column] < past[column].mean():
            lower_count += 1

    stage = "mid-stage"
    if lower_count >= 3:
        stage = "early-to-mid stage"
    if (
        current["trading_value_ratio"] > past["trading_value_ratio"].mean()
        and current["margin_balance_ratio"] > past["margin_balance_ratio"].mean()
    ):
        stage = "late stage"

    lines = [
        "Automatic analysis notes",
        "=======================",
        "",
        f"Current best brokerage stock code: {metadata['current_best_broker_code']}",
        f"Current best brokerage return: {metadata['current_best_broker_return_pct']}%",
        "",
        "Heuristic stage judgement:",
        f"The current episode is tentatively classified as {stage}.",
        "",
        "Reasoning:",
        "1. Trading, leverage, and fund issuance indicators are compared against the two historical episodes.",
        "2. If most speculative indicators remain below the historical averages, the episode is treated as earlier stage.",
        "3. If trading and leverage ratios exceed historical averages together, the episode is treated as late stage.",
    ]
    return "\n".join(lines)


def build_display_tables(summary_df: pd.DataFrame) -> pd.DataFrame:
    table = pd.DataFrame(
        {
            "Episode": summary_df["episode"],
            "Trading Ratio": summary_df["trading_value_ratio"].map(lambda x: f"{x:.2f}"),
            "Max Trading Ratio": summary_df["max_trading_value_ratio"].map(lambda x: f"{x:.2f}"),
            "Peak Date": summary_df["trading_peak_date"],
            "Before Peak Return": summary_df["sse_before_peak_return"].map(lambda x: f"{x*100:.2f}%"),
            "After Peak Return": summary_df["sse_after_peak_return"].map(lambda x: f"{x*100:.2f}%"),
            "Margin Ratio": summary_df["margin_balance_ratio"].map(lambda x: f"{x:.2f}"),
            "Fund Ratio": summary_df["fund_issuance_ratio"].map(lambda x: f"{x:.2f}"),
        }
    )
    return table


def build_valuation_table(summary_df: pd.DataFrame) -> pd.DataFrame:
    table = pd.DataFrame(
        {
            "Episode": summary_df["episode"],
            "ERP Start": summary_df["erp_start_pct"].map(lambda x: f"{x:.2f}%"),
            "ERP End": summary_df["erp_end_pct"].map(lambda x: f"{x:.2f}%"),
            "ERP Start Pctl": summary_df["erp_start_percentile"].map(lambda x: f"{x:.1f}"),
            "ERP End Pctl": summary_df["erp_end_percentile"].map(lambda x: f"{x:.1f}"),
            "Spread Start": summary_df["dividend_spread_start_pct"].map(lambda x: f"{x:.2f}%"),
            "Spread End": summary_df["dividend_spread_end_pct"].map(lambda x: f"{x:.2f}%"),
            "Spread Start Pctl": summary_df["dividend_spread_start_percentile"].map(lambda x: f"{x:.1f}"),
            "Spread End Pctl": summary_df["dividend_spread_end_percentile"].map(lambda x: f"{x:.1f}"),
        }
    )
    return table


def build_stock_table(summary_df: pd.DataFrame) -> pd.DataFrame:
    table = pd.DataFrame(
        {
            "Episode": summary_df["episode"],
            "East Money": summary_df["eastmoney_return"].map(lambda x: f"{x*100:.2f}%"),
            "Hundsun": summary_df["hundsun_return"].map(lambda x: f"{x*100:.2f}%"),
            "Best Broker Code": summary_df["best_broker_code"].astype(str).str.zfill(6),
            "Best Broker Return": summary_df["best_broker_return"].map(lambda x: f"{x*100:.2f}%"),
        }
    )
    return table


def plot_line(ax: plt.Axes, df: pd.DataFrame, column: str, title: str) -> None:
    ax.plot(df["date"], df[column], linewidth=1.5)
    ax.set_title(title, fontsize=11)
    ax.grid(True, alpha=0.3)


def empirical_percentile(history: pd.Series, value: float) -> float:
    clean = history.dropna()
    return float((clean <= value).mean())


def get_nearest_value(series: pd.Series, target_date: pd.Timestamp) -> float:
    return float(series.loc[get_nearest_index(series.index, target_date)])


def get_nearest_row(df: pd.DataFrame, target_date: pd.Timestamp) -> pd.Series:
    idx = get_nearest_index(pd.DatetimeIndex(df["date"]), target_date)
    return df.loc[df["date"] == idx].iloc[0]


def get_nearest_index(index: pd.DatetimeIndex, target_date: pd.Timestamp) -> pd.Timestamp:
    position = index.get_indexer([target_date], method="nearest")[0]
    return index[position]


def get_nearest_month_value(
    series: pd.Series,
    target_month: pd.Period,
    start_month: pd.Period,
    end_month: pd.Period,
) -> float:
    valid = series.loc[(series.index >= start_month) & (series.index <= end_month)]
    if target_month in valid.index:
        return float(valid.loc[target_month])

    month_distance = (valid.index.astype(int) - target_month.ordinal).to_numpy()
    best_pos = abs(month_distance).argmin()
    return float(valid.iloc[best_pos])


def compute_stock_return(
    prices: pd.DataFrame, code: str, start: pd.Timestamp, end: pd.Timestamp
) -> float:
    start_price = get_stock_price_on_date(prices, code, start)
    end_price = get_stock_price_on_date(prices, code, end)
    return end_price / start_price - 1.0


def get_stock_price_on_date(prices: pd.DataFrame, code: str, target_date: pd.Timestamp) -> float:
    sub = prices.loc[prices["code"] == code].copy()
    idx = get_nearest_index(pd.DatetimeIndex(sub["date"]), target_date)
    return float(sub.loc[sub["date"] == idx, "Price"].iloc[0])


def find_best_broker(
    prices: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp
) -> dict[str, float | str]:
    best_code = ""
    best_return = float("-inf")
    for code in sorted(BROKER_CODES):
        if code not in set(prices["code"].unique()):
            continue
        value = compute_stock_return(prices, code, start, end)
        if value > best_return:
            best_return = value
            best_code = code
    return {"code": best_code, "return": best_return}
