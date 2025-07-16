import pandas as pd
import yfinance as yf
import json
from time import sleep
import requests
from pathlib import Path
from io import StringIO
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('update_stocks.log'),
        logging.StreamHandler()
    ]
)

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

def fetch_nasdaq_data(retries=3, initial_delay=5):
    """Fetch NASDAQ data with fallback URLs and retry logic"""
    urls = [
        "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
    ]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    for url in urls:
        attempt = 0
        while attempt < retries:
            try:
                logging.info(f"Fetching data from {url} (attempt {attempt + 1}/{retries})")
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                df = pd.read_csv(StringIO(response.text), delimiter='|')
                df = df[df['Symbol'].str.contains('File Creation Time') == False]
                logging.info(f"Retrieved {len(df)} symbols from NASDAQ")
                return df
            except requests.exceptions.Timeout as e:
                attempt += 1
                if attempt < retries:
                    delay = initial_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    logging.warning(f"Timeout fetching {url}, retrying in {delay:.2f} seconds (attempt {attempt}/{retries})")
                    sleep(delay)
                else:
                    logging.error(f"Failed to fetch from {url} after {retries} attempts: Timeout")
            except Exception as e:
                logging.error(f"Failed to fetch from {url}: {e}")
                break
        if attempt == retries or url == urls[-1]:
            raise Exception("All NASDAQ URLs failed after retries")
    return None

def get_ticker_info_batch(symbols, retries=7, initial_delay=5):
    """Retrieve ticker information for a batch of symbols with retry logic"""
    attempt = 0
    rate_limited_symbols = []
    failure_reasons = {}
    while attempt < retries:
        try:
            tickers = yf.Tickers(symbols)
            result = {}
            failed_symbols = []
            
            for symbol in symbols:
                try:
                    info = tickers.tickers.get(symbol).info
                    sector = info.get('sector', 'N/A')
                    industry = info.get('industry', 'N/A')
                    
                    if info.get('quoteType', '') == 'ETF':
                        result[symbol] = {"info": {"industry": "ETF", "sector": "ETF"}}
                        logging.info(f"Retrieved: {symbol} - Sector: ETF, Industry: ETF")
                    elif sector and industry and sector != 'N/A' and industry != 'N/A' and sector != 'Unknown' and industry != 'Unknown':
                        result[symbol] = {"info": {"industry": industry, "sector": sector}}
                        logging.info(f"Retrieved: {symbol} - Sector: {sector}, Industry: {industry}")
                    else:
                        logging.debug(f"Skipped: {symbol} - Missing or invalid sector/industry (Sector: {sector}, Industry: {industry})")
                        failed_symbols.append(symbol)
                        failure_reasons[symbol] = "Missing or invalid sector/industry"
                except Exception as e:
                    if "404" in str(e):
                        logging.error(f"HTTP 404 for {symbol}, skipping")
                        failed_symbols.append(symbol)
                        failure_reasons[symbol] = "HTTP 404"
                    elif "429" in str(e) or "Too Many Requests" in str(e):
                        logging.warning(f"Rate limit hit for {symbol}")
                        rate_limited_symbols.append(symbol)
                        failed_symbols.append(symbol)
                        failure_reasons[symbol] = "Rate limit (HTTP 429)"
                    elif isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
                        logging.warning(f"Timeout or connection error for {symbol}, marking for retry")
                        failed_symbols.append(symbol)
                        failure_reasons[symbol] = "Timeout or connection error"
                    else:
                        logging.warning(f"Error for {symbol}: {str(e)}")
                        failed_symbols.append(symbol)
                        failure_reasons[symbol] = f"Other error: {str(e)}"
            
            return result, failed_symbols, rate_limited_symbols, failure_reasons
        
        except Exception as e:
            if isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)) or "429" in str(e) or "Too Many Requests" in str(e):
                attempt += 1
                rate_limited_symbols.extend(symbols)
                if attempt < retries:
                    delay = initial_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    logging.warning(f"Rate limit, timeout, or connection error, retrying in {delay:.2f} seconds (attempt {attempt}/{retries})")
                    sleep(delay)
                else:
                    logging.error(f"Failed after {retries} retries: {str(e)}")
                    failure_reasons = {symbol: f"Batch failure after {retries} retries: {str(e)}" for symbol in symbols}
                    return {}, symbols, rate_limited_symbols, failure_reasons
            else:
                logging.error(f"Non-retryable error: {str(e)}")
                failure_reasons = {symbol: f"Non-retryable batch error: {str(e)}" for symbol in symbols}
                return {}, symbols, rate_limited_symbols, failure_reasons

def process_nasdaq_file(batch_size=10, max_workers=2):
    """Process NASDAQ symbols and update JSON file with sector/industry data"""
    result = {}
    skipped_symbols = []
    failed_symbols = set()
    rate_limited_symbols = set()
    all_failure_reasons = []  # List to store all failure reasons as dictionaries
    
    # Define paths
    script_dir = Path(__file__).parent
    output_file = script_dir / 'ticker_info.json'
    skipped_file = script_dir / 'skipped_symbols.txt'
    failed_cache_file = script_dir / 'failed_symbols.json'
    failure_reasons_file = script_dir / 'failure_reasons.json'
    
    # Load existing data
    if output_file.exists():
        try:
            with open(output_file, 'r') as f:
                result = json.load(f)
            logging.info(f"Loaded existing data with {len(result)} entries")
        except Exception as e:
            logging.error(f"Error loading existing file: {e}")

    # Load failed symbols cache
    failed_symbols = load_failed_symbols_cache(failed_cache_file)
    
    # Clear failed symbols cache with 20% probability
    if failed_cache_file.exists() and random.random() < 0.2:
        failed_symbols = set()
        logging.info("Cleared failed symbols cache for full retry")

    try:
        # Fetch NASDAQ data
        df = fetch_nasdaq_data()
        if df is None:
            raise Exception("Failed to fetch NASDAQ data")

        # Filter out non-standard and test symbols
        pattern = re.compile(r'.*[.\$][A-Z]$|.*\.W$|.*\.R$')
        test_symbol_pattern = re.compile(r'^Z[A-Z]+T$')
        symbols = []
        for _, row in df.iterrows():
            symbol = row['Symbol']
            if isinstance(symbol, str) and pd.notna(symbol):
                if (not pattern.match(symbol) and
                    not test_symbol_pattern.match(symbol) and
                    symbol not in result and
                    symbol not in failed_symbols):
                    symbols.append(symbol)
                else:
                    logging.debug(f"Excluded symbol: {symbol} (non-standard, test, or already processed)")
            else:
                logging.warning(f"Invalid symbol: {symbol} (skipped due to non-string or null value)")
        
        # Pre-validate symbols
        valid_symbols = []
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                ticker.basic_info  # Lightweight check for symbol validity
                valid_symbols.append(symbol)
            except Exception as e:
                if "404" in str(e):
                    logging.error(f"Pre-validation failed for {symbol}: HTTP 404")
                    failed_symbols.add(symbol)
                    all_failure_reasons.append({"symbol": symbol, "reason": "HTTP 404 (pre-validation)"})
                else:
                    valid_symbols.append(symbol)  # Retry transient errors in batch
        symbols = valid_symbols

        # Retry a subset of failed symbols
        if failed_symbols:
            retry_failed = set(random.sample(list(failed_symbols), min(500, len(failed_symbols))))
            symbols.extend(list(retry_failed))
            logging.info(f"Added {len(retry_failed)} previously failed symbols for retry")

        logging.info(f"Processing {len(symbols)} new symbols")

        # Process symbols in batches
        batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        logging.info(f"Created {len(batches)} batches of size {batch_size}")

        def process_batch(batch):
            """Helper function for parallel batch processing"""
            batch_result, batch_failed, batch_rate_limited, batch_failure_reasons = get_ticker_info_batch(batch)
            return batch_result, batch_failed, batch_rate_limited, batch_failure_reasons

        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {executor.submit(process_batch, batch): batch for batch in batches}
            for future in as_completed(future_to_batch):
                batch_result, batch_failed, batch_rate_limited, batch_failure_reasons = future.result()
                result.update(batch_result)
                skipped_symbols.extend(batch_failed)
                failed_symbols.update(batch_failed)
                rate_limited_symbols.update(batch_rate_limited)
                all_failure_reasons.extend([{"symbol": k, "reason": v} for k, v in batch_failure_reasons.items()])
                sleep(2)

        logging.info(f"Encountered {len(rate_limited_symbols)} rate-limited symbols")

        # Save results
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        logging.info(f"Saved results to {output_file}")
        
        # Save skipped symbols
        with open(skipped_file, 'w') as f:
            f.write('\n'.join(skipped_symbols))
        logging.info(f"Saved {len(skipped_symbols)} skipped symbols to {skipped_file}")
        
        # Save failed symbols cache
        save_failed_symbols_cache(failed_cache_file, failed_symbols)
        
        # Save failure reasons as a proper JSON array
        if all_failure_reasons or failure_reasons_file.exists():
            try:
                existing_data = []
                if failure_reasons_file.exists():
                    with open(failure_reasons_file, 'r') as f:
                        existing_data = json.load(f)
                existing_data.extend(all_failure_reasons)
                with open(failure_reasons_file, 'w') as f:
                    json.dump(existing_data, f, indent=2)
                logging.info(f"Saved {len(all_failure_reasons)} failure reasons to {failure_reasons_file}")
            except Exception as e:
                logging.error(f"Error saving failure_reasons.json: {e}")
    
    except Exception as e:
        logging.error(f"Error processing NASDAQ data: {e}")
        raise
    
    logging.info(f"Final dataset contains {len(result)} symbols")
    return result

if __name__ == "__main__":
    process_nasdaq_file()
