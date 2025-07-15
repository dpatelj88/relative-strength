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

def fetch_nasdaq_data():
    """Fetch NASDAQ data with fallback URLs"""
    urls = [
        "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt",
        "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    ]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    for url in urls:
        try:
            logging.info(f"Fetching data from {url}")
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text), delimiter='|')
            # Drop footer rows (e.g., "File Creation Time")
            df = df[df['Symbol'].str.contains('File Creation Time') == False]
            logging.info(f"Retrieved {len(df)} symbols from NASDAQ")
            return df
        except Exception as e:
            logging.error(f"Failed to fetch from {url}: {e}")
            if url == urls[-1]:
                raise Exception("All NASDAQ URLs failed")
    return None

def get_ticker_info_batch(symbols, retries=5, initial_delay=5):
    """Retrieve ticker information for a batch of symbols with retry logic"""
    attempt = 0
    rate_limited_symbols = []
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
                    
                    if sector and industry and sector != 'N/A' and industry != 'N/A' and sector != 'Unknown' and industry != 'Unknown':
                        result[symbol] = {"info": {"industry": industry, "sector": sector}}
                        logging.info(f"Retrieved: {symbol} - Sector: {sector}, Industry: {industry}")
                    else:
                        logging.debug(f"Skipped: {symbol} - Missing or invalid sector/industry (Sector: {sector}, Industry: {industry})")
                        failed_symbols.append(symbol)
                except Exception as e:
                    if "404" in str(e):
                        logging.error(f"HTTP 404 for {symbol}, skipping")
                        failed_symbols.append(symbol)
                    elif "401" in str(e):
                        logging.warning(f"HTTP 401 for {symbol}, retrying")
                        failed_symbols.append(symbol)
                    elif "429" in str(e) or "Too Many Requests" in str(e):
                        logging.warning(f"Rate limit hit for {symbol}")
                        rate_limited_symbols.append(symbol)
                        failed_symbols.append(symbol)
                    else:
                        logging.warning(f"Error for {symbol}: {str(e)}")
                        failed_symbols.append(symbol)
            
            return result, failed_symbols, rate_limited_symbols
        
        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                attempt += 1
                rate_limited_symbols.extend(symbols)
                if attempt < retries:
                    delay = initial_delay * (2 ** attempt) + random.uniform(0, 0.1)  # Exponential backoff
                    logging.warning(f"Rate limit hit, retrying in {delay:.2f} seconds (attempt {attempt}/{retries})")
                    sleep(delay)
                else:
                    logging.error(f"Failed after {retries} retries: {str(e)}")
                    return {}, symbols, rate_limited_symbols
            else:
                logging.error(f"Non-rate-limit error: {str(e)}")
                return {}, symbols, rate_limited_symbols

def process_nasdaq_file(batch_size=20, max_workers=3):
    """Process NASDAQ symbols and update JSON file with sector/industry data"""
    result = {}
    skipped_symbols = []
    failed_symbols = set()
    rate_limited_symbols = set()
    
    # Define paths
    script_dir = Path(__file__).parent
    output_file = script_dir / 'ticker_info.json'
    skipped_file = script_dir / 'skipped_symbols.txt'
    failed_cache_file = script_dir / 'failed_symbols.json'
    
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

    try:
        # Fetch NASDAQ data
        df = fetch_nasdaq_data()
        if df is None:
            raise Exception("Failed to fetch NASDAQ data")

        # Filter out non-standard symbols (warrants, units, preferred stocks)
        pattern = re.compile(r'.*[.\$][A-Z]$|.*\.W$|.*\.R$')
        symbols = []
        for _, row in df.iterrows():
            symbol = row['Symbol']
            # Ensure symbol is a string and not null
            if isinstance(symbol, str) and pd.notna(symbol):
                if (not pattern.match(symbol) and
                    symbol not in result and
                    symbol not in failed_symbols):
                    symbols.append(symbol)
                else:
                    logging.debug(f"Excluded symbol: {symbol} (non-standard or already processed)")
            else:
                logging.warning(f"Invalid symbol: {symbol} (skipped due to non-string or null value)")
        
        logging.info(f"Processing {len(symbols)} new symbols")

        # Process symbols in batches
        batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        logging.info(f"Created {len(batches)} batches of size {batch_size}")

        def process_batch(batch):
            """Helper function for parallel batch processing"""
            batch_result, batch_failed, batch_rate_limited = get_ticker_info_batch(batch)
            return batch_result, batch_failed, batch_rate_limited

        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {executor.submit(process_batch, batch): batch for batch in batches}
            for future in as_completed(future_to_batch):
                batch_result, batch_failed, batch_rate_limited = future.result()
                result.update(batch_result)
                skipped_symbols.extend(batch_failed)
                failed_symbols.update(batch_failed)
                rate_limited_symbols.update(batch_rate_limited)
                sleep(2)  # Delay between batches to avoid rate limits

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
    
    except Exception as e:
        logging.error(f"Error processing NASDAQ data: {e}")
        raise
    
    logging.info(f"Final dataset contains {len(result)} symbols")
    return result

if __name__ == "__main__":
    process_nasdaq_file()
