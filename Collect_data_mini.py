import datetime
import pandas as pd
import yfinance as yf
from openbb import obb


''' this code will attempt to collect the financial information of the largest compnay in the sp500 and do some basic math with it, to be expanded later over a larger range'''


def get_sp500_tickers() -> list[str]:
    """Scrape the live list of S&P 500 tickers from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df = tables[0]
    return df.Symbol.str.replace(".", "-", regex=False).tolist()

def get_market_cap(ticker: str) -> float:
    """Fetch current market cap from Yahoo Finance (yfinance)."""
    info = yf.Ticker(ticker).info
    return info.get("marketCap", 0)

def find_index_label(series: pd.Series, keywords: list[str]) -> str:
    """Find the first index label containing all keywords (case-insensitive)."""
    for label in series.index:
        low = label.lower()
        if all(k in low for k in keywords):
            return label
    # Dump what you have, then error
    print(f"[DEBUG fields for {series.name}]:", series.index.tolist())
    raise KeyError(f"No field matching {keywords} in {series.name}")

def load_financials(ticker: str):
    """
    Pull the latest annual Income, Balance & Cash-Flow statements.
    Returns three pandas Series.
    """
    inc_df = obb.equity.fundamental.income(ticker,  period="quarter").to_df()
    bal_df = obb.equity.fundamental.balance(ticker, period="quarter").to_df()
    cfs_df = obb.equity.fundamental.cash(ticker,    period="quarter").to_df()


    latest_inc = inc_df.tail(1).squeeze()
    latest_bal = bal_df.tail(1).squeeze()
    latest_cfs = cfs_df.tail(1).squeeze()
    return latest_inc, latest_bal, latest_cfs

def compute_roic(inc: pd.Series, bal: pd.Series, tax_rate: float = 0.21) -> float:
    """NOPAT / Invested Capital."""
    op_label   = find_index_label(inc, ["operating", "income"])
    debt_label = find_index_label(bal, ["total", "debt"])
    eq_label   = find_index_label(bal, ["total", "equity"])
    cash_label = find_index_label(bal, ["cash", "equivalents"])

    nopat        = inc[op_label] * (1 - tax_rate)
    invested_cap = bal[debt_label] + bal[eq_label] - bal[cash_label]
    return nopat / invested_cap

def compute_faustmann(mkt_cap: float, bal: pd.Series) -> float:
    """MarketCap / (TotalAssets − TotalLiabilities)."""
    assets_label = find_index_label(bal, ["total", "asset"])
    liab_label   = find_index_label(bal, ["total", "liabil"])  # catches both singular/plural

    net_worth = bal[assets_label] - bal[liab_label]
    return mkt_cap / net_worth

if __name__ == "__main__":
    # 1) Get all S&P 500 tickers
    tickers = get_sp500_tickers()

    # 2) Fetch each ticker’s cap, pick the max
    caps = {t: get_market_cap(t) for t in tickers}
    largest = max(caps, key=caps.get)
    print("Largest S&P 500 company by market-cap:", largest)
    print("Market-cap:", caps[largest])

    # 3) Pull its financials
    inc, bal, cfs = load_financials(largest)

    # 4) Print raw data for inspection
    print("\n=== Latest Annual Income Statement ===")
    print(inc.to_string(), "\n")
    print("=== Latest Annual Balance Sheet ===")
    print(bal.to_string(), "\n")
    print("=== Latest Annual Cash-Flow Statement ===")
    print(cfs.to_string(), "\n")

    # 5) Compute your metrics
    roic = compute_roic(inc, bal)
    fr   = compute_faustmann(caps[largest], bal)

    print(f"\nComputed ROIC: {roic:.2f}")
    print(f"Faustmann Ratio: {fr:.2f}")
