from datetime import datetime
from calendar import monthrange

TICKERS = ["TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR"]
MIN_BID = 0.50
MIN_OPEN_INTEREST = 100
MIN_VOLUME = 10
MAX_SPREAD_PCT_OF_MID = 0.25
RISK_FREE_RATE = 0.045
HV_LOOKBACK_DAYS = 30
TRADING_DAYS_PER_YEAR = 252
DATA_SOURCE = "yfinance"
SCRIPT_VERSION = "2026-03-19.1"
STALE_QUOTE_SECONDS = 15 * 60

today = datetime.today().date()
year = today.year
month = today.month + 3
if month > 12:
    month -= 12
    year += 1
_, last_day = monthrange(year, month)
MAX_EXPIRATION = f"{year}-{month:02d}-{last_day:02d}"
