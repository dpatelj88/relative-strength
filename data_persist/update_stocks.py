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

def get_ticker_info_batch(symbols):
    """Retrieve ticker information for a batch of symbols"""
    result = {}
    failed_symbols = []
    
    try:
        tickers = yf.Tickers(symbols)
        for symbol in symbols:
            try:
                info = tickers.tickers.get(symbol).info
                sector = info.get('sector')
                industry = info.get('industry')
                
                if sector and industry and sector != 'Unknown' and industry != 'Unknown':
                    result[symbol] = {"info": {"industry": industry, "sector": sector}}
                    logging.info(f"Retrieved: {symbol} - {sector}/{industry}")
                else:
                    logging.warning(f"Skipped (missing data): {symbol}")
                    failed_symbols.append(symbol)
            except Exception as e:
                if "404" in str(e):
                    logging.error(f"HTTP 404 for {symbol}, skipping")
                else:
                    logging.warning(f"Error for {symbol}: {e}")
                failed_symbols.append(symbol)
    except Exception as e:
        logging.error(f"Batch error for symbols {symbols}: {e}")
        failed_symbols.extend(symbols)
    
    return result, failed_symbols

def process_nasdaq_file(batch_size=50, max_workers=5):
    """Process NASDAQ symbols and update JSON file with sector/industry data"""
    url = "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    result = {}
    skipped_symbols = []
    failed_symbols = set()
    
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

    # HTTP headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        # Fetch NASDAQ data
        logging.info(f"Fetching data from {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Read data into DataFrame
        df = pd.read_csv(StringIO(response.text), delimiter='|')
        logging.info(f"Retrieved {len(df)} symbols from NASDAQ")

        # Filter out non-standard symbols (warrants, units, preferred stocks)
        pattern = re.compile(r'.*[.\$][A-Z]$|.*\.W$|.*\.R$')
        symbols = [
            row['Symbol'] for _, row in df.iterrows()
            if not pattern.match(row['Symbol']) and
            row['Symbol'] not in result and
            row['Symbol'] not in failed_symbols
        ]
        logging.info(f"Processing {len(symbols)} new symbols")

        # Process symbols in batches
        batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        logging.info(f"Created {len(batches)} batches of size {batch_size}")

        def process_batch(batch):
            """Helper function for parallel batch processing"""
            batch_result, batch_failed = get_ticker_info_batch(batch)
            return batch_result, batch_failed

        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {executor.submit(process_batch, batch): batch for batch in batches}
            for future in as_completed(future_to_batch):
                batch_result, batch_failed = future.result()
                result.update(batch_result)
                skipped_symbols.extend(batch_failed)
                failed_symbols.update(batch_failed)
                sleep(0.5)  # Reduced sleep for batch requests

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
