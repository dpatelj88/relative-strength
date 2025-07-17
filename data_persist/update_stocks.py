import yaml
import os
import requests
import pandas as pd
import logging
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config():
    config_path = os.path.join('data_persist', 'config.yaml')
    defaults = {
        'MAX_WORKERS': 10,
        'INITIAL_DELAY': 0.5,
        'NASDAQ_URL': 'https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt',
        'RETRY_ATTEMPTS': 7  # Default retry attempts
    }
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f) or {}
                return {**defaults, **config}
        except Exception as e:
            logging.error(f"Error loading config.yaml: {e}")
    else:
        logging.warning("config.yaml not found, using default settings")
    return defaults

def fetch_nasdaq_data():
    config = load_config()
    url = config['NASDAQ_URL']
    retries = config.get('RETRY_ATTEMPTS', 7)  # Ensure retries is an integer
    attempt = 1
    while attempt <= retries:
        logging.info(f"Fetching data from {url} (attempt {attempt}/{retries})")
        try:
            response = requests.get(url)
            response.raise_for_status()
            df = pd.read_csv(url, sep='|', skipfooter=1, engine='python')
            logging.info(f"Retrieved {len(df)} symbols from NASDAQ")
            return df
        except Exception as e:
            logging.error(f"Attempt {attempt} failed: {e}")
            attempt += 1
            if attempt > retries:
                raise Exception(f"Failed to fetch NASDAQ data after {retries} attempts")
    return None

def process_symbol(symbol, existing_data):
    # Placeholder for symbol processing (unchanged from original)
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        return {
            symbol: {
                'info': {
                    'sector': info.get('sector', 'Unknown'),
                    'industry': info.get('industry', 'Unknown')
                },
                'universe': 'Reference' if symbol == 'SPY' else 'Stock'
            }
        }
    except Exception as e:
        logging.error(f"Failed to process {symbol}: {e}")
        return None

def process_nasdaq_file():
    config = load_config()
    df = fetch_nasdaq_data()
    if df is None:
        raise Exception("Failed to fetch NASDAQ data")
    
    existing_data = {}
    ticker_info_path = os.path.join('data_persist', 'ticker_info.json')
    if os.path.exists(ticker_info_path):
        with open(ticker_info_path, 'r') as f:
            existing_data = json.load(f)
        logging.info(f"Loaded existing data with {len(existing_data)} entries")
    
    symbols = df['Symbol'].tolist()
    symbols.append('SPY')  # Add SPY as ETF
    failed_symbols = []
    skipped_symbols = []
    
    with ThreadPoolExecutor(max_workers=config['MAX_WORKERS']) as executor:
        results = list(tqdm(executor.map(lambda s: process_symbol(s, existing_data), symbols), total=len(symbols)))
    
    new_data = {}
    for result in results:
        if result:
            new_data.update(result)
        else:
            failed_symbols.append(list(result.keys())[0] if result else symbols[results.index(result)])
    
    new_data.update(existing_data)
    with open(ticker_info_path, 'w') as f:
        json.dump(new_data, f, indent=2)
    logging.info(f"Saved {len(new_data)} symbols to {ticker_info_path}")
    
    with open(os.path.join('data_persist', 'failed_symbols.json'), 'w') as f:
        json.dump(failed_symbols, f)
    logging.info(f"Saved {len(failed_symbols)} failed symbols")
    
    with open(os.path.join('data_persist', 'skipped_symbols.txt'), 'w') as f:
        f.write('\n'.join(skipped_symbols))
    logging.info(f"Saved {len(skipped_symbols)} skipped symbols")

if __name__ == "__main__":
    try:
        process_nasdaq_file()
    except Exception as e:
        logging.error(f"Error processing NASDAQ data: {e}")
        raise
