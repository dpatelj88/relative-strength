import pandas as pd
import yfinance as yf
import json
from time import sleep
import requests
import os
from pathlib import Path
from io import StringIO

def get_ticker_info(symbol):
    """Function to retrieve ticker information with retry logic"""
    tries = 3  # Increased retries for robustness
    for attempt in range(tries):
        try:
            ticker = yf.Ticker(symbol)  # Remove session parameter
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
    
    return None, None

def process_nasdaq_file():
    """Process NASDAQ symbols and update JSON file with sector/industry data"""
    
    # Get NASDAQ symbols directly from URL
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
    result = {}
    skipped_symbols = []  # Track skipped symbols
    
    # Define path for output file
    script_dir = Path(__file__).parent
    output_file = script_dir / 'ticker_info.json'
    skipped_file = script_dir / 'skipped_symbols.txt'  # File for skipped symbols
    
    # Load existing data if available
    if output_file.exists():
        try:
            with open(output_file, 'r') as f:
                result = json.load(f)
            print(f"Loaded existing data with {len(result)} entries")
        except Exception as e:
            print(f"Error loading existing file: {e}")

    try:
        # Download and process NASDAQ data
        response = requests.get(url)
        response.raise_for_status()
        
        # Read data into DataFrame
        df = pd.read_csv(StringIO(response.text), delimiter='|')
        print(f"Retrieved {len(df)} symbols from NASDAQ")

        for _, row in df.iterrows():
            symbol = row['Symbol']
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
                    skipped_symbols.append(symbol)
                
                sleep(1.5)  # Slightly longer delay to avoid rate limits
    
        # Save results to JSON file
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        # Save skipped symbols to a separate file
        with open(skipped_file, 'w') as f:
            f.write('\n'.join(skipped_symbols))
        print(f"Saved {len(skipped_symbols)} skipped symbols to {skipped_file}")
    
    except Exception as e:
        print(f"Error processing NASDAQ data: {e}")
        raise
    
    print(f"Final dataset contains {len(result)} symbols")
    return result

if __name__ == "__main__":
    process_nasdaq_file()
