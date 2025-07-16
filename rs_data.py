#!/usr/bin/env python
import requests
import json
import time
import bs4 as bs
import datetime as dt
import os
import yfinance as yf
import pandas as pd
import re
from ftplib import FTP
from io import StringIO
from time import sleep
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import yaml
from pathlib import Path
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent / 'rs_data.log'),
        logging.StreamHandler()
    ]
)

DIR = Path(__file__).parent
DATA_DIR = DIR / 'data'
TMP_DIR = DIR / 'tmp'

DATA_DIR.mkdir(exist_ok=True)
TMP_DIR.mkdir(exist_ok=True)

# Load configuration
try:
    with open(DIR / 'config_private.yaml', 'r') as stream:
        private_config = yaml.safe_load(stream)
except FileNotFoundError:
    private_config = None
    logging.error("config_private.yaml not found")
except yaml.YAMLError as exc:
    logging.error(f"YAML error in config_private.yaml: {exc}")
    private_config = None

try:
    with open(DIR / 'config.yaml', 'r') as stream:
        config = yaml.safe_load(stream)
except FileNotFoundError:
    config = None
    logging.error("config.yaml not found")
except yaml.YAMLError as exc:
    logging.error(f"YAML error in config.yaml: {exc}")
    config = None

def cfg(key):
    return private_config.get(key) or config.get(key)

def read_json(json_file):
    try:
        with open(json_file, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception as e:
        logging.error(f"Error reading {json_file}: {e}")
        return {}

API_KEY = cfg("API_KEY")
PRICE_DATA_FILE = DATA_DIR / "price_history.json"
REFERENCE_TICKER = cfg("REFERENCE_TICKER") or "SPY"
DATA_SOURCE = cfg("DATA_SOURCE") or "YAHOO"
ALL_STOCKS = cfg("USE_ALL_LISTED_STOCKS") or False
TICKER_INFO_FILE = DIR / "data_persist" / "ticker_info.json"
TICKER_INFO_DICT = read_json(TICKER_INFO_FILE)
REF_TICKER = {"ticker": REFERENCE_TICKER, "sector": "--- Reference ---", "industry": "--- Reference ---", "universe": "--- Reference ---"}
UNKNOWN = "unknown"

def load_failed_symbols_cache(cache_file):
    """Load failed symbols from cache to skip them"""
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                return set(json.load(f))
        except Exception as e:
            logging.error(f"Error loading failed symbols cache: {e}")
            return set()
    return set()

def save_failed_symbols_cache(cache_file, failed_symbols):
    """Save failed symbols to cache"""
    try:
        with open(cache_file, 'w') as f:
            json.dump(list(failed_symbols), f, indent=2)
        logging.info(f"Saved {len(failed_symbols)} failed symbols to {cache_file}")
    except Exception as e:
        logging.error(f"Error saving failed symbols cache: {e}")

def get_securities(url, ticker_pos=1, table_pos=1, sector_offset=1, industry_offset=1, universe="N/A"):
    try:
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        resp.raise_for_status()
        soup = bs.BeautifulSoup(resp.text, 'lxml')
        table = soup.findAll('table', {'class': 'wikitable sortable'})[table_pos-1]
        secs = {}
        for row in table.findAll('tr')[table_pos:]:
            sec = {
                "ticker": row.findAll('td')[ticker_pos-1].text.strip(),
                "sector": row.findAll('td')[ticker_pos-1+sector_offset].text.strip(),
                "industry": row.findAll('td')[ticker_pos-1+sector_offset+industry_offset].text.strip(),
                "universe": universe
            }
            secs[sec["ticker"]] = sec
        with open(TMP_DIR / "tickers.pickle", "wb") as f:
            pickle.dump(secs, f)
        return secs
    except Exception as e:
        logging.error(f"Error fetching securities from {url}: {e}")
        return {}

def exchange_from_symbol(symbol):
    mapping = {"Q": "NASDAQ", "A": "NYSE MKT", "N": "NYSE", "P": "NYSE ARCA", "Z": "BATS", "V": "IEXG"}
    return mapping.get(symbol, "n/a")

def get_tickers_from_nasdaq(tickers, retries=3, initial_delay=5, batch_size=20, max_workers=2):
    """Fetch NASDAQ tickers and enrich with sector/industry data using yfinance in batches."""
    filename = "nasdaqtraded.txt"
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
    ticker_column = 1
    etf_column = 5
    exchange_column = 3
    test_column = 7
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    for attempt in range(retries):
        try:
            logging.info(f"Fetching NASDAQ data from {url} (attempt {attempt + 1}/{retries})")
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            lines = StringIO(response.text)
            securities = {}
            for entry in lines.readlines():
                values = entry.split('|')
                ticker = values[ticker_column].strip()
                if (re.match(r'^[A-Z]+$', ticker) and 
                    values[etf_column] == "N" and 
                    values[test_column] == "N"):
                    sec = {
                        "ticker": ticker,
                        "universe": exchange_from_symbol(values[exchange_column])
                    }
                    securities[ticker] = sec
            lines.close()

            # Batch process to fetch sector/industry
            symbols = list(securities.keys())
            batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
            failed_symbols = set()
            ticker_info = {}

            def process_batch(batch):
                result = {}
                failed = []
                for ticker in batch:
                    try:
                        info = yf.Ticker(ticker).info
                        sector = info.get('sector', 'N/A')
                        industry = info.get('industry', 'N/A')
                        if sector != 'N/A' and industry != 'N/A' and sector != 'Unknown' and industry != 'Unknown':
                            result[ticker] = {"info": {"industry": industry, "sector": sector}}
                        else:
                            failed.append(ticker)
                    except Exception as e:
                        logging.warning(f"Error fetching info for {ticker}: {e}")
                        failed.append(ticker)
                return result, failed

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {executor.submit(process_batch, batch): batch for batch in batches}
                for future in tqdm(as_completed(future_to_batch), total=len(batches), desc="Enriching NASDAQ tickers"):
                    batch_result, batch_failed = future.result()
                    ticker_info.update(batch_result)
                    failed_symbols.update(batch_failed)
                    sleep(2)  # Respect rate limits

            # Merge with initial tickers
            for ticker in securities:
                securities[ticker].update(ticker_info.get(ticker, {"info": {"industry": UNKNOWN, "sector": UNKNOWN}}))
            tickers.update(securities)
            logging.info(f"Processed {len(securities)} NASDAQ tickers, failed {len(failed_symbols)}")
            return tickers

        except Exception as e:
            logging.warning(f"NASDAQ data fetch failed (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                delay = initial_delay * (2 ** attempt) + random.uniform(0, 0.1)
                sleep(delay)
    logging.error("Failed to fetch NASDAQ tickers after retries")
    return tickers

def get_tickers_from_wikipedia(tickers):
    sources = [
        ("NQ100", 'https://en.wikipedia.org/wiki/Nasdaq-100', 2, 3, 1, 1, "Nasdaq 100"),
        ("SP500", 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 1, 1, 3, 1, "S&P 500"),
        ("SP400", 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies', 2, 1, 1, 1, "S&P 400"),
        ("SP600", 'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies', 2, 1, 1, 1, "S&P 600")
    ]
    for key, url, ticker_pos, table_pos, sector_offset, industry_offset, universe in sources:
        if cfg(key):
            tickers.update(get_securities(url, ticker_pos, table_pos, sector_offset, industry_offset, universe))
    return tickers

def get_resolved_securities():
    tickers = {REFERENCE_TICKER: REF_TICKER}
    return get_tickers_from_nasdaq(tickers) if ALL_STOCKS else get_tickers_from_wikipedia(tickers)

def write_to_file(dict_data, file):
    try:
        with open(file, "w", encoding='utf8') as fp:
            json.dump(dict_data, fp, ensure_ascii=False, indent=2)
        logging.info(f"Saved data to {file}")
    except Exception as e:
        logging.error(f"Error writing to {file}: {e}")

def get_yf_data_batch(securities, start_date, end_date, retries=7, initial_delay=5):
    attempt = 0
    rate_limited_tickers = []
    failure_reasons = {}
    tickers_dict = {}
    while attempt < retries:
        try:
            tickers = yf.Tickers([s["ticker"] for s in securities])
            failed_tickers = []
            for sec in securities:
                ticker = sec["ticker"]
                try:
                    df = tickers.tickers.get(ticker).history(start=start_date, end=end_date, auto_adjust=False)
                    if df.empty:
                        logging.warning(f"No data for {ticker}, may be delisted")
                        failed_tickers.append(ticker)
                        failure_reasons[ticker] = "Empty data"
                        continue
                    candles = [
                        {
                            "open": row["Open"],
                            "close": row["Close"],
                            "low": row["Low"],
                            "high": row["High"],
                            "volume": row["Volume"],
                            "datetime": int(row.name.timestamp())
                        } for _, row in df.iterrows()
                    ]
                    ticker_data = {"candles": candles}
                    enrich_ticker_data(ticker_data, sec)
                    tickers_dict[ticker] = ticker_data
                    logging.info(f"Retrieved data for {ticker}")
                except Exception as e:
                    if "404" in str(e):
                        logging.error(f"HTTP 404 for {ticker}")
                        failed_tickers.append(ticker)
                        failure_reasons[ticker] = "HTTP 404"
                    elif "429" in str(e) or "Too Many Requests" in str(e):
                        logging.warning(f"Rate limit for {ticker}")
                        rate_limited_tickers.append(ticker)
                        failed_tickers.append(ticker)
                        failure_reasons[ticker] = "Rate limit (HTTP 429)"
                    else:
                        logging.warning(f"Error for {ticker}: {str(e)}")
                        failed_tickers.append(ticker)
                        failure_reasons[ticker] = f"Other error: {str(e)}"
            return tickers_dict, failed_tickers, rate_limited_tickers, failure_reasons
        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                attempt += 1
                rate_limited_tickers.extend([s["ticker"] for s in securities])
                if attempt < retries:
                    delay = initial_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    logging.warning(f"Rate limit hit, retrying in {delay:.2f}s (attempt {attempt}/{retries})")
                    sleep(delay)
                else:
                    logging.error(f"Failed after {retries} retries: {str(e)}")
                    failure_reasons = {s["ticker"]: f"Batch failure: {str(e)}" for s in securities}
                    return {}, [s["ticker"] for s in securities], rate_limited_tickers, failure_reasons
            else:
                logging.error(f"Non-retryable error: {str(e)}")
                failure_reasons = {s["ticker"]: f"Non-retryable error: {str(e)}" for s in securities}
                return {}, [s["ticker"] for s in securities], rate_limited_tickers, failure_reasons

def enrich_ticker_data(ticker_response, security):
    ticker_response["sector"] = TICKER_INFO_DICT.get(security["ticker"], {}).get("info", {}).get("sector", security["sector"])
    ticker_response["industry"] = TICKER_INFO_DICT.get(security["ticker"], {}).get("info", {}).get("industry", security["industry"])
    ticker_response["universe"] = security["universe"]

def load_ticker_info(ticker, info_dict):
    try:
        info = yf.Ticker(ticker).info
        info_dict[ticker] = {
            "info": {
                "industry": info.get("industry", "n/a"),
                "sector": info.get("sector", "n/a")
            }
        }
    except Exception as e:
        logging.error(f"Error loading info for {ticker}: {e}")
        info_dict[ticker] = {"info": {"industry": "n/a", "sector": "n/a"}}

def load_prices_from_yahoo(securities, batch_size=20, max_workers=2):
    logging.info("*** Loading Stocks from Yahoo Finance ***")
    today = dt.date.today()
    start_date = today - dt.timedelta(days=1*365+183)  # 18 months
    tickers_dict = read_json(PRICE_DATA_FILE)
    failed_tickers = load_failed_symbols_cache(DATA_DIR / "failed_tickers.json")
    failure_reasons_file = DATA_DIR / "failure_reasons.json"

    # Clear failed tickers cache with 20% probability
    if (DATA_DIR / "failed_tickers.json").exists() and random.random() < 0.2:
        failed_tickers = set()
        logging.info("Cleared failed tickers cache")

    # Filter securities
    valid_securities = [
        sec for sec in securities
        if sec["ticker"] not in failed_tickers and sec["ticker"] not in tickers_dict
    ]
    logging.info(f"Processing {len(valid_securities)} new tickers")

    # Process in batches
    batches = [valid_securities[i:i + batch_size] for i in range(0, len(valid_securities), batch_size)]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(get_yf_data_batch, batch, start_date, today): batch for batch in batches}
        for future in tqdm(as_completed(future_to_batch), total=len(batches), desc="Processing batches"):
            batch_result, batch_failed, batch_rate_limited, batch_failure_reasons = future.result()
            tickers_dict.update(batch_result)
            failed_tickers.update(batch_failed)
            failure_reasons = read_json(failure_reasons_file) if failure_reasons_file.exists() else []
            failure_reasons.extend([{"ticker": k, "reason": v} for k, v in batch_failure_reasons.items()])
            write_to_file(failure_reasons, failure_reasons_file)
            if batch_result:
                write_to_file(tickers_dict, PRICE_DATA_FILE)
            sleep(2)

    # Update ticker info for new tickers
    for sec in valid_securities:
        ticker = sec["ticker"]
        if ticker not in TICKER_INFO_DICT:
            load_ticker_info(ticker, TICKER_INFO_DICT)
    write_to_file(TICKER_INFO_DICT, TICKER_INFO_FILE)

    # Save failed tickers
    save_failed_symbols_cache(DATA_DIR / "failed_tickers.json", failed_tickers)
    logging.info(f"Processed {len(tickers_dict)} tickers, failed {len(failed_tickers)}")

    return tickers_dict

def save_data(source, securities, api_key=None, info=None):
    if source == "YAHOO":
        return load_prices_from_yahoo(securities, info=info or {})
    elif source == "TD_AMERITRADE" and api_key:
        logging.error("TD Ameritrade support deprecated; using Yahoo Finance instead")
        return load_prices_from_yahoo(securities, info=info or {})
    else:
        logging.error(f"Unsupported or missing data source: {source}, defaulting to Yahoo Finance")
        return load_prices_from_yahoo(securities, info=info or {})

def main(forceTDA=False, api_key=None):
    securities = get_resolved_securities().values()
    save_data(DATA_SOURCE if not forceTDA else "TD_AMERITRADE", securities, api_key, {"forceTDA": forceTDA})
    write_to_file(TICKER_INFO_DICT, TICKER_INFO_FILE)

if __name__ == "__main__":
    main()
