import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging
from rs_data import cfg, read_json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent / 'rs_ranking.log'),
        logging.StreamHandler()
    ]
)

DIR = Path(__file__).parent
DATA_DIR = DIR / 'data'
OUTPUT_DIR = DIR / 'output'
OUTPUT_DIR.mkdir(exist_ok=True)

PRICE_DATA_FILE = DATA_DIR / 'price_data.json'
TICKER_INFO_FILE = DIR / 'data_persist' / 'ticker_info.json'
MIN_PERCENTILE = cfg("MIN_PERCENTILE") or 80
REFERENCE_TICKER = cfg("REFERENCE_TICKER") or "SPY"
TRADING_DAYS_PER_MONTH = cfg("TRADING_DAYS_PER_MONTH") or 20
MIN_TICKERS_PER_INDUSTRY = cfg("MIN_TICKERS_PER_INDUSTRY") or 2

TITLE_RANK = "Rank"
TITLE_TICKER = "Ticker"
TITLE_TICKERS = "Tickers"
TITLE_SECTOR = "Sector"
TITLE_INDUSTRY = "Industry"
TITLE_UNIVERSE = "Universe"
TITLE_PERCENTILE = "Percentile"
TITLE_1M = "1 Month Ago"
TITLE_3M = "3 Months Ago"
TITLE_6M = "6 Months Ago"
TITLE_RS = "Relative Strength"
TITLE_PRICE = "Latest Price"

def relative_strength(closes: pd.Series, ref_closes: pd.Series):
    """Calculate relative strength for a stock against reference."""
    try:
        rs_stock = strength(closes)
        rs_ref = strength(ref_closes)
        rs = (1 + rs_stock) / (1 + rs_ref) * 100
        return round(rs, 2)
    except Exception as e:
        logging.error(f"Error calculating RS: {e}")
        return 0

def strength(closes: pd.Series):
    """Calculate weighted strength over four quarters."""
    try:
        quarters = [quarters_perf(closes, n) for n in range(1, 5)]
        return 0.4 * quarters[0] + 0.2 * quarters[1] + 0.2 * quarters[2] + 0.2 * quarters[3]
    except Exception as e:
        logging.error(f"Error in strength calculation: {e}")
        return 0

def quarters_perf(closes: pd.Series, n):
    """Calculate performance for a given quarter."""
    try:
        if len(closes) < int(252/4):  # Minimum 63 days for 1 quarter
            logging.warning(f"Insufficient data for {n} quarters")
            return 0
        length = min(len(closes), n * int(252/4))
        prices = closes.tail(length)
        pct_chg = prices.pct_change().dropna()
        perf_cum = (pct_chg + 1).cumprod() - 1
        return perf_cum.tail(1).item()
    except Exception as e:
        logging.error(f"Error in quarters_perf: {e}")
        return 0

def generate_tradingview_csv(percentile_values, first_rs_values):
    """Generate CSV for TradingView with RS percentiles."""
    lines = []
    trading_days = 0
    yesterday = date.today() - timedelta(days=1)
    for percentile in sorted(percentile_values, reverse=True):
        rs_value = first_rs_values.get(percentile, 0)
        for _ in range(5):
            trading_date = yesterday - timedelta(days=trading_days)
            date_str = trading_date.strftime("%Y%m%dT")
            lines.append(f"{date_str},0,1000,0,{rs_value},0\n")
            trading_days += 1
    return ''.join(reversed(lines))

def process_batch(tickers_batch, ref_closes):
    """Process a batch of tickers to calculate relative strength and include latest price."""
    relative_strengths = []
    json_data = read_json(PRICE_DATA_FILE)
    ticker_info = read_json(TICKER_INFO_FILE)
    
    for ticker in tickers_batch:
        try:
            if ticker not in json_data or not json_data[ticker]["candles"]:
                continue
            candles = json_data[ticker]["candles"]
            closes = pd.Series([candle["close"] for candle in candles])
            if len(closes) < 6 * TRADING_DAYS_PER_MONTH:
                continue
            rs = relative_strength(closes, ref_closes)
            if rs >= 590:
                logging.warning(f"Skipping {ticker}: RS {rs} too high, likely faulty data")
                continue
            rs1m = relative_strength(closes.head(-TRADING_DAYS_PER_MONTH), ref_closes.head(-TRADING_DAYS_PER_MONTH))
            rs3m = relative_strength(closes.head(-3 * TRADING_DAYS_PER_MONTH), ref_closes.head(-3 * TRADING_DAYS_PER_MONTH))
            rs6m = relative_strength(closes.head(-6 * TRADING_DAYS_PER_MONTH), ref_closes.head(-6 * TRADING_DAYS_PER_MONTH))
            latest_price = candles[-1]["close"] if candles else 0
            info = ticker_info.get(ticker, {})
            relative_strengths.append((
                0, ticker, 
                info.get("info", {}).get("sector", json_data[ticker]["sector"]),
                info.get("info", {}).get("industry", json_data[ticker]["industry"]),
                json_data[ticker]["universe"], 
                rs, 100, rs1m, rs3m, rs6m, latest_price
            ))
        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
    return relative_strengths

def rankings():
    """Generate stock and industry rankings based on relative strength."""
    json_data = read_json(PRICE_DATA_FILE)
    if not json_data or REFERENCE_TICKER not in json_data:
        logging.error(f"Price data or reference ticker {REFERENCE_TICKER} not found")
        return []
    
    ref_closes = pd.Series([candle["close"] for candle in json_data[REFERENCE_TICKER]["candles"]])
    tickers = [t for t in json_data if t != REFERENCE_TICKER]
    relative_strengths = []
    industries = {}
    stock_rs = {}

    batch_size = cfg("BATCH_SIZE") or 20
    max_workers = cfg("MAX_WORKERS") or 10  # Increased for performance

    # Process tickers in batches
    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(process_batch, batch, ref_closes): batch for batch in batches}
        for future in tqdm(as_completed(future_to_batch), total=len(batches), desc="Processing ticker batches"):
            batch_results = future.result()
            relative_strengths.extend(batch_results)
            for _, ticker, sector, industry, universe, rs, _, rs1m, rs3m, rs6m, _ in batch_results:
                if industry not in industries:
                    industries[industry] = {
                        "info": (0, industry, sector, 0, 99, 1, 3, 6),
                        TITLE_RS: [], TITLE_1M: [], TITLE_3M: [], TITLE_6M: [], TITLE_TICKERS: []
                    }
                industries[industry][TITLE_RS].append(rs)
                industries[industry][TITLE_1M].append(rs1m)
                industries[industry][TITLE_3M].append(rs3m)
                industries[industry][TITLE_6M].append(rs6m)
                industries[industry][TITLE_TICKERS].append(ticker)
                stock_rs[ticker] = rs

    dfs = []
    if relative_strengths:
        df = pd.DataFrame(relative_strengths, columns=[
            TITLE_RANK, TITLE_TICKER, TITLE_SECTOR, TITLE_INDUSTRY, TITLE_UNIVERSE, 
            TITLE_RS, TITLE_PERCENTILE, TITLE_1M, TITLE_3M, TITLE_6M, TITLE_PRICE
        ])
        df[TITLE_PERCENTILE] = pd.qcut(df[TITLE_RS], 100, labels=False, duplicates="drop")
        df[TITLE_1M] = pd.qcut(df[TITLE_1M], 100, labels=False, duplicates="drop")
        df[TITLE_3M] = pd.qcut(df[TITLE_3M], 100, labels=False, duplicates="drop")
        df[TITLE_6M] = pd.qcut(df[TITLE_6M], 100, labels=False, duplicates="drop")
        df = df.sort_values(TITLE_RS, ascending=False)
        df[TITLE_RANK] = range(1, len(df) + 1)
        df = df[df[TITLE_PERCENTILE] >= MIN_PERCENTILE]

        percentile_values = [98, 89, 69, 49, 29, 9, 1]
        first_rs_values = {p: df[df[TITLE_PERCENTILE] == p][TITLE_RS].iloc[0] for p in percentile_values if p in df[TITLE_PERCENTILE].values}
        with open(OUTPUT_DIR / "RSRATING.csv", "w") as csv_file:
            csv_file.write(generate_tradingview_csv(percentile_values, first_rs_values))
        df.to_csv(OUTPUT_DIR / 'rs_stocks.csv', index=False)
        dfs.append(df)

    # Process industries
    filtered_industries = [i for i in industries.values() if len(i[TITLE_TICKERS]) >= MIN_TICKERS_PER_INDUSTRY]
    if filtered_industries:
        df_industries = pd.DataFrame(
            [i["info"] for i in filtered_industries],
            columns=[TITLE_RANK, TITLE_INDUSTRY, TITLE_SECTOR, TITLE_RS, TITLE_PERCENTILE, TITLE_1M, TITLE_3M, TITLE_6M]
        )
        df_industries[TITLE_RS] = df_industries[TITLE_INDUSTRY].apply(
            lambda x: round(sum(industries[x][TITLE_RS]) / len(industries[x][TITLE_RS]), 2) if industries[x][TITLE_RS] else 0
        )
        df_industries[TITLE_1M] = df_industries[TITLE_INDUSTRY].apply(
            lambda x: round(sum(industries[x][TITLE_1M]) / len(industries[x][TITLE_1M]), 2) if industries[x][TITLE_1M] else 0
        )
        df_industries[TITLE_3M] = df_industries[TITLE_INDUSTRY].apply(
            lambda x: round(sum(industries[x][TITLE_3M]) / len(industries[x][TITLE_3M]), 2) if industries[x][TITLE_3M] else 0
        )
        df_industries[TITLE_6M] = df_industries[TITLE_INDUSTRY].apply(
            lambda x: round(sum(industries[x][TITLE_6M]) / len(industries[x][TITLE_6M]), 2) if industries[x][TITLE_6M] else 0
        )
        df_industries[TITLE_TICKERS] = df_industries[TITLE_INDUSTRY].apply(
            lambda x: ",".join(sorted(industries[x][TITLE_TICKERS], key=lambda t: stock_rs.get(t, 0), reverse=True))
        )
        df_industries[TITLE_PERCENTILE] = pd.qcut(df_industries[TITLE_RS], 100, labels=False, duplicates="drop")
        df_industries[TITLE_1M] = pd.qcut(df_industries[TITLE_1M], 100, labels=False, duplicates="drop")
        df_industries[TITLE_3M] = pd.qcut(df_industries[TITLE_3M], 100, labels=False, duplicates="drop")
        df_industries[TITLE_6M] = pd.qcut(df_industries[TITLE_6M], 100, labels=False, duplicates="drop")
        df_industries = df_industries.sort_values(TITLE_RS, ascending=False)
        df_industries[TITLE_RANK] = range(1, len(df_industries) + 1)
        df_industries.to_csv(OUTPUT_DIR / 'rs_industries.csv', index=False)
        dfs.append(df_industries)

    return dfs

def main(skipEnter=False):
    """Generate and save relative strength rankings."""
    try:
        ranks = rankings()
        if ranks:
            print(ranks[0])
            print("***\nYour 'rs_stocks.csv' is in the output folder.\n***")
        if not skipEnter and cfg("EXIT_WAIT_FOR_ENTER"):
            input("Press Enter key to exit...")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
