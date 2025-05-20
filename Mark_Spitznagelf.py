import datetime
import pandas as pd
import yfinance as yf
from openbb import obb           # OpenBB SDK for fundamentals

def get_sp500_tickers():
    """Scrape live S&P 500 tickers from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df     = tables[0]
    return df.Symbol.str.replace(".", "-", regex=False).tolist()

def get_market_cap(ticker, as_of_date):
    """Fetch (approximate) current market cap via yfinance."""
    info = yf.Ticker(ticker).info
    return info.get("marketCap", 0)

def find_index_label(series: pd.Series, keywords: list[str]) -> str:
    """
    Find the first index in a Series whose lowercase name contains
    all keywords. E.g. ["total","liabil"] will match "TotalLiabilities".
    """
    for label in series.index:
        low = label.lower()
        if all(k in low for k in keywords):
            return label
    # debug: print what you actually have
    print(f"[DEBUG] available fields for series '{series.name}':\n", series.index.tolist())
    raise KeyError(f"No field matching {keywords} in series '{series.name}'")

def load_financials(ticker: str):
    """
    Pull annual income, balance & cash-flow statements for `ticker`,
    convert to DataFrames, then take the latest row as a Series.
    """
    inc_df = obb.equity.fundamental.income(ticker, period="annual").to_df()
    bal_df = obb.equity.fundamental.balance(ticker, period="annual").to_df()
    cfs_df = obb.equity.fundamental.cash(ticker, period="annual").to_df()

    latest_inc = inc_df.tail(1).squeeze()
    latest_bal = bal_df.tail(1).squeeze()
    latest_cfs = cfs_df.tail(1).squeeze()

    return latest_inc, latest_bal, latest_cfs

def compute_roic(inc: pd.Series, bal: pd.Series, tax_rate: float = 0.21) -> float:
    """
    NOPAT / Invested Capital
      NOPAT = OperatingIncome × (1 − tax_rate)
      InvestedCap = TotalDebt + TotalEquity − CashAndEquivalents
    """
    op_inc_label = find_index_label(inc, ["operating", "income"])
    debt_label   = find_index_label(bal, ["total", "debt"])
    eq_label     = find_index_label(bal, ["total", "equity"])
    cash_label   = find_index_label(bal, ["cash"])

    nopat        = inc[op_inc_label] * (1 - tax_rate)
    invested_cap = bal[debt_label] + bal[eq_label] - bal[cash_label]
    return nopat / invested_cap

def compute_faustmann(mkt_cap: float, bal: pd.Series) -> float:
    """
    Faustmann ratio = MarketCap / (TotalAssets − TotalLiabilities)
    """
    assets_label = find_index_label(bal, ["total", "asset"])
    # use "liabil" to catch both singular & plural
    liab_label   = find_index_label(bal, ["total", "liabil"])

    net_worth = bal[assets_label] - bal[liab_label]
    return mkt_cap / net_worth

def screen_universe(as_of_date: datetime.date) -> pd.DataFrame:
    universe = get_sp500_tickers()
    picks    = []

    for tkr in universe:
        try:
            inc, bal, cfs = load_financials(tkr)
        except Exception as e:
            print(f"[WARN] couldn’t load fundamentals for {tkr}: {e}")
            continue

        try:
            roic = compute_roic(inc, bal)
            fr   = compute_faustmann(get_market_cap(tkr, as_of_date), bal)
        except KeyError as ke:
            print(f"[WARN] skipping {tkr} — missing field: {ke}")
            continue

        if roic > 1.0 and fr < 0.7:
            picks.append({"ticker": tkr, "roic": roic, "fr": fr})

    df = pd.DataFrame(picks, columns=["ticker", "roic", "fr"])
    return df.sort_values("fr", ignore_index=True)

if __name__ == "__main__":
    today = datetime.date.today()
    df    = screen_universe(today)
    print(df)
