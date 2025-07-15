# Author: dpatelj88
import pandas as pd
import yfinance as yf
import json
from time import sleep
import requests
from pathlib import Path
from io import StringIO
import re

def get_ticker_info(symbol):
    """Retrieve ticker information with retry logic."""
    tries = 2
    for attempt in range(tries):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            sector = info.get('sector')
            industry = info.get('industry')
            
            if sector and industry and sector != 'Unknown' and industry != 'Unknown':
                return sector, industry
            
            if attempt < tries - 1:
                print(f"Attempt {attempt + 1} failed for {symbol}, retrying in 2s...")
                sleep(2)
            
        except Exception as e:
            if attempt < tries - 1:
                print(f"Error for {symbol} (attempt {attempt + 1}): {e}, retrying in 2s...")
                sleep(2)
            else:
                print(f"Final failure for {symbol}: {e}")
                with open("skipped_symbols.txt", "a") as f:
                    f.write(f"{symbol}: {str(e)}\n")
    
    return None, None

def process_nasdaq_file():
    """Process NASDAQ symbols and update JSON file with sector/industry data."""
    
    # Define paths and URL
    script_dir = Path(__file__).parent
    output_file = script_dir / 'ticker_info.json'
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
    
    # Load existing data
    result = {}
    if output_file.exists():
        try:
            with open(output_file, 'r') as f:
                result = json.load(f)
            print(f"Loaded existing data with {len(result)} entries")
        except Exception as e:
            print(f"Error loading existing file: {e}")

    try:
        # Download and process NASDAQ data
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text), delimiter='|')
        print(f"Retrieved {len(df)} symbols from NASDAQ")

        # Regular expression for valid stock symbols (e.g., 1-5 letters, excluding test symbols)
        pattern = re.compile(r'^[A-Z]{1,5}$')

        # Filter valid symbols and handle non-string values
        symbols = []
        for _, row in df.iterrows():
            symbol = row['Symbol']
            # Check if symbol is a string and matches the pattern
            if isinstance(symbol, str) and pattern.match(symbol) and not symbol.startswith('Z'):
                symbols.append(symbol)
            else:
                print(f"Skipped invalid symbol: {symbol}")
                with open("skipped_symbols.txt", "a") as f:
                    f.write(f"Invalid symbol: {symbol}\n")

        print(f"Filtered to {len(symbols)} valid symbols")

        # Process symbols
        for symbol in symbols:
            if symbol not in result:
                sector, industry = get_ticker_info(symbol)
                
                if sector and industry:
                    result[symbol] = {
                        "info": {
                            "industry": industry,
                            "sector": sector
                        }
                    }
                    print(f"Added: {symbol} - {sector}/{industry}")
                else:
                    print(f"Skipped (missing data after retries): {symbol}")
                
                sleep(0.5)  # Balance rate limits and speed
    
        # Save final dataset
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"Final dataset contains {len(result)} symbols")

    except Exception as e:
        print(f"Error processing NASDAQ data: {e}")
        raise

    return result

if __name__ == "__main__":
    process_nasdaq_file()
