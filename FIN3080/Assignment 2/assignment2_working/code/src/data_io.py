from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class DataPaths:
    daily_fund_shares: Path
    csi300_pe_dividend: Path
    daily_representative_price: Path
    market_trading_value: Path
    index_price_files: list[Path]
    margin_balance_files: list[Path]
    bond_10y_yield: Path


def discover_data_paths(project_root: Path) -> DataPaths:
    workspace_root = project_root.parents[1]
    working_root = project_root.parent

    index_price_files = sorted(workspace_root.rglob("IDX_Idxtrd.csv"))
    margin_balance_files = sorted(workspace_root.rglob("CHN_Stkmt_ddetails*.csv"))
    market_matches = list(workspace_root.rglob("TRD_Cndalym.csv"))

    if not market_matches:
        raise FileNotFoundError("TRD_Cndalym.csv was not found under the workspace root.")

    bond_file = working_root / "data" / "BND_TreasYield.csv"
    if not bond_file.exists():
        raise FileNotFoundError(
            "BND_TreasYield.csv was not found under assignment2_working/data."
        )

    return DataPaths(
        daily_fund_shares=workspace_root / "daily fund shares (1).dta",
        csi300_pe_dividend=workspace_root / "CSI300 PE and dividend yield (1).dta",
        daily_representative_price=workspace_root / "daily representative price (1).dta",
        market_trading_value=market_matches[0],
        index_price_files=index_price_files,
        margin_balance_files=margin_balance_files,
        bond_10y_yield=bond_file,
    )


def read_csv_auto(path: Path, usecols: list[str] | None = None) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "gbk", "gb18030"):
        try:
            return pd.read_csv(path, encoding=encoding, usecols=usecols)
        except Exception as exc:  # pragma: no cover - fallback path
            last_error = exc
    raise RuntimeError(f"Unable to read CSV file {path}") from last_error


def load_trading_value(paths: DataPaths) -> pd.Series:
    df = read_csv_auto(paths.market_trading_value)
    df["Markettype"] = df["Markettype"].astype(str)
    df = df[df["Markettype"] == "21"].copy()
    df["date"] = pd.to_datetime(df["Trddt"])
    df["trading_value"] = pd.to_numeric(df["Cnvaltrdtl"], errors="coerce")
    series = (
        df[["date", "trading_value"]]
        .dropna()
        .drop_duplicates(subset=["date"])
        .sort_values("date")
        .set_index("date")["trading_value"]
    )
    return series


def load_index_prices(paths: DataPaths) -> pd.Series:
    frames: list[pd.DataFrame] = []
    for path in paths.index_price_files:
        df = read_csv_auto(path)
        df["date"] = pd.to_datetime(df["Idxtrd01"])
        df["index_close"] = pd.to_numeric(df["Idxtrd05"], errors="coerce")
        frames.append(df[["date", "index_close"]])

    merged = (
        pd.concat(frames, ignore_index=True)
        .dropna()
        .drop_duplicates(subset=["date"], keep="last")
        .sort_values("date")
        .set_index("date")["index_close"]
    )
    return merged


def load_margin_balance(paths: DataPaths) -> pd.Series:
    aggregated: dict[pd.Timestamp, float] = {}

    for path in paths.margin_balance_files:
        df = read_csv_auto(path, usecols=["Mtdate", "Mtbalance"])
        df["date"] = pd.to_datetime(df["Mtdate"])
        df["Mtbalance"] = pd.to_numeric(df["Mtbalance"], errors="coerce")
        grouped = df.groupby("date", as_index=False)["Mtbalance"].sum()
        for row in grouped.itertuples(index=False):
            aggregated[row.date] = aggregated.get(row.date, 0.0) + float(row.Mtbalance)

    series = pd.Series(aggregated, name="margin_balance").sort_index()
    return series


def load_fund_monthly(paths: DataPaths) -> pd.Series:
    df = pd.read_stata(paths.daily_fund_shares)
    df["date"] = pd.to_datetime(df["Date"])
    df["Scale"] = pd.to_numeric(df["Scale"], errors="coerce")
    df["month"] = df["date"].dt.to_period("M")
    series = df.groupby("month")["Scale"].sum().sort_index()
    series.name = "fund_shares"
    return series


def load_valuation_and_bond(paths: DataPaths) -> pd.DataFrame:
    valuation = pd.read_stata(paths.csi300_pe_dividend)
    valuation["date"] = pd.to_datetime(valuation["Date"])
    valuation["CSI300_PE"] = pd.to_numeric(valuation["CSI300_PE"], errors="coerce")
    valuation["CSI300_DividendYield"] = pd.to_numeric(
        valuation["CSI300_DividendYield"], errors="coerce"
    )
    valuation = valuation[["date", "CSI300_PE", "CSI300_DividendYield"]]

    bond = read_csv_auto(paths.bond_10y_yield)
    bond["date"] = pd.to_datetime(bond["Trddt"])
    bond["Cvtype"] = pd.to_numeric(bond["Cvtype"], errors="coerce")
    bond["Yeartomatu"] = pd.to_numeric(bond["Yeartomatu"], errors="coerce")
    bond["Yield"] = pd.to_numeric(bond["Yield"], errors="coerce")
    bond = bond[(bond["Cvtype"] == 1) & (bond["Yeartomatu"] == 10)].copy()
    bond = bond[["date", "Yield"]].rename(columns={"Yield": "bond_yield_pct"})

    merged = valuation.merge(bond, on="date", how="inner").sort_values("date")
    merged["erp"] = 1.0 / merged["CSI300_PE"] - merged["bond_yield_pct"] / 100.0
    merged["dividend_spread"] = (
        merged["CSI300_DividendYield"] - merged["bond_yield_pct"]
    ) / 100.0
    return merged


def load_representative_prices(paths: DataPaths) -> pd.DataFrame:
    df = pd.read_stata(paths.daily_representative_price)
    df["date"] = pd.to_datetime(df["Date"])
    df["code"] = df["stk_code"].astype(str).str.zfill(6)
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df = df[["date", "code", "Price"]].dropna().sort_values(["code", "date"])
    return df
