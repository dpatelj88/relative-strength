#!/usr/bin/env python
import json
import datetime as dt
import os
from pathlib import Path
import logging
import yfinance as yf
from time import sleep
import random
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
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
DATA_DIR.mkdir(exist_ok=True)
PRICE_DATA_FILE = DATA_DIR / 'price_data.json'
TICKER_INFO_FILE = DIR / 'data_persist' / 'ticker_info.json'

# Load configuration
try:
    with open(DIR / 'config_private.yaml', 'r') as stream:
        private_config = yaml.safe_load(stream)
except FileNotFoundError:
    private_config = None
    logging.warning("config_private.yaml not found, using config.yaml if available")
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
    value = private_config.get(key) if private_config else config.get(key) if config else None
    defaults = {
        "REFERENCE_TICKER": "SPY",
        "BATCH_SIZE": 20,
        "MAX_WORKERS": 2,
        "MAX_RETRIES": 7,
        "INITIAL_DELAY": 5,
    }
    return value if value is not None else defaults.get(key)

def read_json(file_path):
    """Read and parse a JSON file."""
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        logging.error(f"JSON file not found: {file_path}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {file_path}: {e}")
        return {}

def write_to_file(dict_data, file):
    """Write dictionary to JSON file."""
    try:
        with open(file, "w", encoding='utf8') as fp:
            json.dump(dict_data, fp, ensure_ascii=False, indent=2)
        logging.info(f"Saved data to {file}")
    except Exception as e:
        logging.error(f"Error writing to {file}: {e}")

def load_failed_symbols_cache(cache_file):
    """Load failed symbols from cache."""
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                return set(json.load(f))
        except Exception as e:
            logging.error(f"Error loading failed symbols cache: {e}")
            return set()
    return set()

def save_failed_symbols_cache(cache_file, failed_symbols):
    """Save failed symbols to cache."""
    try:
        with open(cache_file, 'w') as f:
            json.dump(list(failed_symbols), f, indent=2)
        logging.info(f"Saved {len(failed_symbols)} failed symbols to {cache_file}")
    except Exception as e:
        logging.error(f"Error saving failed symbols cache: {e}")

def get_yf_data_batch(securities, start_date, end_date, retries=cfg("MAX_RETRIES"), initial_delay=cfg("INITIAL_DELAY")):
    """Fetch price data for a batch of securities from Yahoo Finance."""
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
                    ticker_data = {
                        "candles": candles,
                        "sector": sec["sector"],
                        "industry": sec["industry"],
                        "universe": sec.get("universe", "N/A")
                    }
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

def load_prices_from_yahoo(securities, batch_size=cfg("BATCH_SIZE"), max_workers=cfg("MAX_WORKERS")):
    """Load price data for securities from Yahoo Finance."""
    logging.info("*** Loading Stocks from Yahoo Finance ***")
    today = dt.date.today()
    start_date = today - dt.timedelta(days=1*365+183)  # 18 months
    tickers_dict = read_json(PRICE_DATA_FILE)
    failed_tickers = load_failed_symbols_cache(DATA_DIR / "failed_tickers.json")
    failure_reasons_file = DATA_DIR / "failure_reasons.json"

    if (DATA_DIR / "failed_tickers.json").exists() and random.random() < 0.2:
        failed_tickers = set()
        logging.info("Cleared failed tickers cache")

    valid_securities = [sec for sec in securities if sec["ticker"] not in failed_tickers and sec["ticker"] not in tickers_dict]
    logging.info(f"Processing {len(valid_securities)} new tickers")

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
            sleep(cfg("INITIAL_DELAY"))

    save_failed_symbols_cache(DATA_DIR / "failed_tickers.json", failed_tickers)
    logging.info(f"Processed {len(tickers_dict)} tickers, failed {len(failed_tickers)}")
    return tickers_dict

def get_resolved_securities():
    """Load securities from ticker_info.json."""
    ticker_info = read_json(TICKER_INFO_FILE)
    securities = []
    for ticker, data in ticker_info.items():
        securities.append({
            "ticker": ticker,
            "sector": data["info"].get("sector", "N/A"),
            "industry": data["info"].get("industry", "N/A"),
            "universe": data.get("universe", "N/A")
        })
    # Ensure reference ticker is included
    ref_ticker = cfg("REFERENCE_TICKER")
    if ref_ticker not in ticker_info:
        securities.append({
            "ticker": ref_ticker,
            "sector": "N/A",
            "industry": "N/A",
            "universe": "Reference"
        })
    return securities

def main():
    """Main function to load and save price data."""
    securities = get_resolved_securities()
    if not securities:
        logging.error("No securities loaded from ticker_info.json")
        return
    load_prices_from_yahoo(securities)

if __name__ == "__main__":
    main()
