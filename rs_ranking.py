import sys
import pandas as pd
import numpy as np
import json
import os
from datetime import date, timedelta
import yaml
from rs_data import cfg, read_json
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

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

pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)

try:
    with open(DIR / 'config.yaml', 'r') as stream:
        config = yaml.safe_load(stream)
except FileNotFoundError:
    config = None
    logging.error("config.yaml not found")
except yaml.YAMLError as exc:
    logging.error(f"YAML error: {exc}")

PRICE_DATA = DATA_DIR / "price_history.json"
MIN_PERCENTILE = cfg("MIN_PERCENTILE") or 80
POS_COUNT_TARGET = cfg("POSITIONS_COUNT_TARGET") or 100
REFERENCE_TICKER = cfg("REFERENCE_TICKER") or "SPY"
ALL_STOCKS = cfg("USE_ALL_LISTED_STOCKS") or False
TICKER_INFO_FILE = DIR / "data_persist" / "ticker_info.json"
TICKER_INFO_DICT = read_json(TICKER_INFO_FILE)

TITLE_RANK = "Rank"
TITLE_TICKER = "Ticker"
TITLE_TICKERS = "Tickers"
TITLE_SECTOR = "Sector"
TITLE_INDUSTRY = "Industry"
TITLE_UNIVERSE = "Universe" if not ALL_STOCKS else "Exchange"
TITLE_PERCENTILE = "Percentile"
TITLE_1M = "1 Month Ago"
TITLE_3M = "3 Months Ago"
TITLE_6M = "6 Months Ago"
TITLE_RS = "Relative Strength"

OUTPUT_DIR = DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

def relative_strength(closes: pd.Series, closes_ref: pd.Series):
    try:
        rs_stock = strength(closes)
        rs_ref = strength(closes_ref)
        rs = (1 + rs_stock) / (1 + rs_ref) * 100
        return round(rs, 2)
    except Exception as e:
        logging.error(f"Error calculating RS: {e}")
        return 0

def strength(closes: pd.Series):
    try:
        quarters = [quarters_perf(closes, n) for n in range(1, 5)]
        return 0.4 * quarters[0] + 0.2 * quarters[1] + 0.2 * quarters[2] + 0.2 * quarters[3]
    except Exception as e:
        logging.error(f"Error in strength calculation: {e}")
        return 0

def quarters_perf(closes: pd.Series, n):
    try:
        if len(closes) < int(252/4):  # Minimum 63 days for 1 quarter
            logging.warning(f"Insufficient data for {n} quarters, using 0")
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
    lines = []
    trading_days = 0
    yesterday = date.today() - timedelta(days=1)
    for percentile in sorted(percentile_values, reverse=True):
        rs_value = first_rs_values.get(percentile, 0)
        for _ in range(5):
            trading_date = yesterday - timedelta(days=trading_days)
            date_str = trading_date.strftime("%Y%m%dT")
            csv_row = f"{date_str},0,1000,0,{rs_value},0\n"
            lines.append(csv_row)
            trading_days += 1
    return ''.join(reversed(lines))

def process_batch(tickers_batch, ref_closes, trading_days_per_month=cfg("TRADING_DAYS_PER_MONTH") or 20):
    relative_strengths = []
    for ticker in tickers_batch:
        try:
            json_data = read_json(PRICE_DATA)
            if ticker not in json_data or REFERENCE_TICKER not in json_data:
                continue
            closes = pd.Series([candle["close"] for candle in json_data[ticker]["candles"]])
            industry = TICKER_INFO_DICT.get(ticker, {}).get("info", {}).get("industry", json_data[ticker]["industry"])
            sector = TICKER_INFO_DICT.get(ticker, {}).get("info", {}).get("sector", json_data[ticker]["sector"])
            if len(closes) >= 6 * trading_days_per_month:
                if (not cfg("SP500") and json_data[ticker]["universe"] == "S&P 500") or \
                   (not cfg("SP400") and json_data[ticker]["universe"] == "S&P 400") or \
                   (not cfg("SP600") and json_data[ticker]["universe"] == "S&P 600") or \
                   (not cfg("NQ100") and json_data[ticker]["universe"] == "Nasdaq 100"):
                    continue
                rs = relative_strength(closes, ref_closes)
                if rs >= 590:
                    logging.warning(f"Skipping {ticker}: RS {rs} too high, likely faulty data")
                    continue
                rs1m = relative_strength(closes.head(-trading_days_per_month), ref_closes.head(-trading_days_per_month))
                rs3m = relative_strength(closes.head(-3 * trading_days_per_month), ref_closes.head(-3 * trading_days_per_month))
                rs6m = relative_strength(closes.head(-6 * trading_days_per_month), ref_closes.head(-6 * trading_days_per_month))
                relative_strengths.append((0, ticker, sector, industry, json_data[ticker]["universe"], rs, 100, rs1m, rs3m, rs6m))
        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
    return relative_strengths

def rankings():
    json_data = read_json(PRICE_DATA)
    if not json_data or REFERENCE_TICKER not in json_data:
        logging.error(f"Price data or reference ticker {REFERENCE_TICKER} not found")
        return []
    ref_closes = pd.Series([candle["close"] for candle in json_data[REFERENCE_TICKER]["candles"]])
    tickers = [t for t in json_data if t != REFERENCE_TICKER]
    relative_strengths = []
    industries = {}
    ind_ranks = []
    stock_rs = {}

    batch_size = cfg("BATCH_SIZE") or 50
    max_workers = cfg("MAX_WORKERS") or 2
    min_tickers_per_industry = cfg("MIN_TICKERS_PER_INDUSTRY") or 2

    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(process_batch, batch, ref_closes): batch for batch in batches}
        for future in tqdm(as_completed(future_to_batch), total=len(batches), desc="Processing ticker batches"):
            batch_results = future.result()
            relative_strengths.extend(batch_results)
            for _, ticker, sector, industry, universe, rs, _, rs1m, rs3m, rs6m in batch_results:
                if industry not in industries:
                    industries[industry] = {
                        "info": (0, industry, sector, 0, 99, 1, 3, 6),
                        TITLE_RS: [],
                        TITLE_1M: [],
                        TITLE_3M: [],
                        TITLE_6M: [],
                        TITLE_TICKERS: []
                    }
                    ind_ranks.append(len(ind_ranks) + 1)
                industries[industry][TITLE_RS].append(rs)
                industries[industry][TITLE_1M].append(rs1m)
                industries[industry][TITLE_3M].append(rs3m)
                industries[industry][TITLE_6M].append(rs6m)
                industries[industry][TITLE_TICKERS].append(ticker)
                stock_rs[ticker] = rs

    dfs = []
    suffix = ''

    if relative_strengths:
        df = pd.DataFrame(relative_strengths, columns=[TITLE_RANK, TITLE_TICKER, TITLE_SECTOR, TITLE_INDUSTRY, TITLE_UNIVERSE, TITLE_RS, TITLE_PERCENTILE, TITLE_1M, TITLE_3M, TITLE_6M])
        df[TITLE_PERCENTILE] = pd.qcut(df[TITLE_RS], 100, labels=False, duplicates="drop")
        df[TITLE_1M] = pd.qcut(df[TITLE_1M], 100, labels=False, duplicates="drop")
        df[TITLE_3M] = pd.qcut(df[TITLE_3M], 100, labels=False, duplicates="drop")
        df[TITLE_6M] = pd.qcut(df[TITLE_6M], 100, labels=False, duplicates="drop")
        df = df.sort_values([TITLE_RS], ascending=False)
        df[TITLE_RANK] = range(1, len(df) + 1)
        out_tickers_count = sum(df[TITLE_PERCENTILE] >= MIN_PERCENTILE)
        df = df.head(out_tickers_count)

        percentile_values = [98, 89, 69, 49, 29, 9, 1]
        first_rs_values = {p: df[df[TITLE_PERCENTILE] == p][TITLE_RS].iloc[0] for p in percentile_values if p in df[TITLE_PERCENTILE].values}
        tradingview_csv_content = generate_tradingview_csv(percentile_values, first_rs_values)
        with open(OUTPUT_DIR / "RSRATING.csv", "w") as csv_file:
            csv_file.write(tradingview_csv_content)
        df.to_csv(OUTPUT_DIR / f'rs_stocks{suffix}.csv', index=False)
        dfs.append(df)

    def get_rs_average(industries, industry, column):
        rs = sum(industries[industry][column]) / len(industries[industry][column]) if industries[industry][column] else 0
        return round(rs, 2)

    def get_tickers(industries, industry):
        return ",".join(sorted(industries[industry][TITLE_TICKERS], key=lambda t: stock_rs.get(t, 0), reverse=True))

    filtered_industries = [i for i in industries.values() if len(i[TITLE_TICKERS]) >= min_tickers_per_industry]
    if filtered_industries:
        df_industries = pd.DataFrame(
            [i["info"] for i in filtered_industries],
            columns=[TITLE_RANK, TITLE_INDUSTRY, TITLE_SECTOR, TITLE_RS, TITLE_PERCENTILE, TITLE_1M, TITLE_3M, TITLE_6M]
        )
        df_industries[TITLE_RS] = df_industries.apply(lambda row: get_rs_average(industries, row[TITLE_INDUSTRY], TITLE_RS), axis=1)
        df_industries[TITLE_1M] = df_industries.apply(lambda row: get_rs_average(industries, row[TITLE_INDUSTRY], TITLE_1M), axis=1)
        df_industries[TITLE_3M] = df_industries.apply(lambda row: get_rs_average(industries, row[TITLE_INDUSTRY], TITLE_3M), axis=1)
        df_industries[TITLE_6M] = df_industries.apply(lambda row: get_rs_average(industries, row[TITLE_INDUSTRY], TITLE_6M), axis=1)
        df_industries[TITLE_PERCENTILE] = pd.qcut(df_industries[TITLE_RS], 100, labels=False, duplicates="drop")
        df_industries[TITLE_1M] = pd.qcut(df_industries[TITLE_1M], 100, labels=False, duplicates="drop")
        df_industries[TITLE_3M] = pd.qcut(df_industries[TITLE_3M], 100, labels=False, duplicates="drop")
        df_industries[TITLE_6M] = pd.qcut(df_industries[TITLE_6M], 100, labels=False, duplicates="drop")
        df_industries[TITLE_TICKERS] = df_industries.apply(lambda row: get_tickers(industries, row[TITLE_INDUSTRY]), axis=1)
        df_industries = df_industries.sort_values([TITLE_RS], ascending=False)
        df_industries[TITLE_RANK] = range(1, len(df_industries) + 1)
        df_industries.to_csv(OUTPUT_DIR / f'rs_industries{suffix}.csv', index=False)
        dfs.append(df_industries)

    return dfs

def main(skipEnter=False):
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
