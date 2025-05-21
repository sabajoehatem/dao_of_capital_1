###############################################################
# Siegfried Portfolio – personal implementation skeleton
# -------------------------------------------------------------
# A minimal, end‑to‑end pipeline that:
# 1. Downloads fundamentals & prices
# 2. Computes ROIC and Faustmann ratio (FR)
# 3. Screens for Spitznagel‑style "Siegfrieds"
# 4. Re‑balances your personal portfolio once a month
#
# How to use:
#   $ pip install openbb sec-api yfinance pandas numpy schedule sqlalchemy
#   $ python siegfried_portfolio.py  # dry‑run prints target trades
#
# Next steps:
#   • Wire the `Broker` class to your real broker’s API (IBKR, Alpaca, etc.)
#   • Replace the naive universe loader with a proper index file (Russell 3000,
#     global equities…) or dynamic screener.
#   • Harden error handling & add unit tests.
###############################################################

from __future__ import annotations

import os
import sys
import time
import json
import schedule
from datetime import datetime, date
from functools import lru_cache
from typing import List, Dict, Any

import pandas as pd
import numpy as np
import yfinance as yf

from openbb import obb                # ↳  open‑source multi‑source fundamentals
from sec_api import XbrlApi           # ↳  raw XBRL fallback if OpenBB gaps

# ---------------------------------------------------------------------------
# 0  –  Config (tweak here or load from .env / json yaml)
# ---------------------------------------------------------------------------
CFG = {
    "TAX_RATE": 0.21,              # effective corporate tax rate
    "FR_THRESHOLD": 0.75,          # Faustmann ratio upper bound (≈ cheapest 25 %)
    "ROIC_THRESHOLD": 1.0,         # >100 % ROIC
    "POSITION_MAX_PCT": 0.15,      # max 15 % of portfolio per position
    "PORTFOLIO_EQUITY": 100_000,   # starting capital (USD)
    "UNIVERSE_FILE": "sp500_tickers.csv",  # universe list
}

# ---------------------------------------------------------------------------
# 1  –  Data layer
# ---------------------------------------------------------------------------

@lru_cache(maxsize=None)
def _load_openbb_income(tkr: str) -> pd.Series:
    return obb.equity.fundamental.income(tkr, period="A").latest()

@lru_cache(maxsize=None)
def _load_openbb_balance(tkr: str) -> pd.Series:
    return obb.equity.fundamental.balance(tkr, period="A").latest()

@lru_cache(maxsize=None)
def _load_openbb_cash(tkr: str) -> pd.Series:
    return obb.equity.fundamental.cash(tkr, period="A").latest()


def get_market_cap(tkr: str) -> float:
    """YFinance single call – slower but reliable for US tickers."""
    try:
        return yf.Ticker(tkr).info.get("marketCap", np.nan)
    except Exception:
        return np.nan


# ---------------------------------------------------------------------------
# 2  –  Factor maths
# ---------------------------------------------------------------------------

def compute_roic(inc: pd.Series, bal: pd.Series, tax_rate: float = CFG["TAX_RATE"]) -> float:
    nopat = inc.get("OperatingIncome", np.nan) * (1 - tax_rate)
    invested_cap = bal.get("TotalDebt", np.nan) + bal.get("TotalEquity", np.nan) - bal.get("CashAndEquivalents", 0)
    return float(nopat / invested_cap) if invested_cap else np.nan


def compute_faustmann_ratio(mkt_cap: float, bal: pd.Series) -> float:
    net_worth = bal.get("TotalAssets", np.nan) - bal.get("TotalLiabilities", np.nan)
    return float(mkt_cap / net_worth) if net_worth else np.nan


# ---------------------------------------------------------------------------
# 3  –  Screener
# ---------------------------------------------------------------------------

class SiegfriedScreener:
    def __init__(self, universe: List[str]):
        self.universe = list(set(universe))

    def _evaluate_ticker(self, tkr: str) -> Dict[str, Any] | None:
        try:
            inc, bal, cfs = _load_openbb_income(tkr), _load_openbb_balance(tkr), _load_openbb_cash(tkr)
            roic_val = compute_roic(inc, bal)
            if roic_val is None or np.isnan(roic_val):
                return None
            fr_val = compute_faustmann_ratio(get_market_cap(tkr), bal)
            if np.isnan(fr_val):
                return None
            if roic_val > CFG["ROIC_THRESHOLD"] and fr_val < CFG["FR_THRESHOLD"]:
                return {"ticker": tkr, "roic": roic_val, "faustmann": fr_val}
        except Exception as exc:
            print(f"Problem with {tkr}: {exc}", file=sys.stderr)
        return None

    def run(self) -> pd.DataFrame:
        rows = [self._evaluate_ticker(t) for t in self.universe]
        df = pd.DataFrame([r for r in rows if r])
        return df.sort_values("faustmann") if not df.empty else df

# ---------------------------------------------------------------------------
# 4  –  Portfolio logic (toy example)
# ---------------------------------------------------------------------------

class Portfolio:
    def __init__(self, equity: float):
        self.cash = equity
        self.positions: Dict[str, Dict[str, float]] = {}  # ticker→ {shares, cost_basis}

    def target_equal_weight(self, picks: pd.DataFrame):
        if picks.empty:
            print("No qualifying Siegfrieds today.")
            return
        n = len(picks)
        alloc_per = min(CFG["POSITION_MAX_PCT"], 1.0 / n)
        target_dollars = alloc_per * (self.cash + self.market_value())
        orders = []
        for _, row in picks.iterrows():
            price = yf.Ticker(row.ticker).history(period="1d").iloc[-1].Close
            qty_target = target_dollars // price
            held = self.positions.get(row.ticker, {}).get("shares", 0)
            delta = qty_target - held
            if delta != 0:
                orders.append({"ticker": row.ticker, "qty": int(delta), "price": price})
        print("Suggested orders:")
        for o in orders:
            sign = "BUY" if o["qty"] > 0 else "SELL"
            print(f"  {sign:<4} {abs(o['qty']):>5}  {o['ticker']} @ ~{o['price']:.2f}")
        # TODO: integrate with Broker.execute()

    def market_value(self) -> float:
        mv = 0.0
        for tkr, pos in self.positions.items():
            price = yf.Ticker(tkr).history(period="1d").iloc[-1].Close
            mv += pos["shares"] * price
        return mv


# ---------------------------------------------------------------------------
# 5  –  Execution stub (extend for real trades)
# ---------------------------------------------------------------------------

class Broker:
    @staticmethod
    def execute(order: Dict[str, Any]):
        """Placeholder: print order instead of hitting live broker API."""
        print("EXECUTE:", json.dumps(order))


# ---------------------------------------------------------------------------
# 6  –  Orchestration (monthly rebalance)
# ---------------------------------------------------------------------------

def load_universe() -> List[str]:
    if not os.path.exists(CFG["UNIVERSE_FILE"]):
        raise FileNotFoundError("Universe file not found; supply a CSV with 'symbol' column")
    return pd.read_csv(CFG["UNIVERSE_FILE"]).symbol.str.upper().tolist()


def rebalance_job():
    print("\n====== Rebalance run", datetime.utcnow().isoformat(), "UTC ======")
    screener = SiegfriedScreener(load_universe())
    picks = screener.run()
    print(f"Qualified tickers this run: {len(picks)}")
    if not picks.empty:
        port.target_equal_weight(picks)
        # persist picks and portfolio snapshot to disk/db
        picks.to_csv("latest_picks.csv", index=False)


# ---------------------------------------------------------------------------
# 7  –  Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = Portfolio(CFG["PORTFOLIO_EQUITY"])

    # run immediately, then every 30 days
    rebalance_job()
    schedule.every(30).days.do(rebalance_job)

    while True:
        schedule.run_pending()
        time.sleep(60 * 30)  # wake up twice an hour
