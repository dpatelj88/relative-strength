import rs_data
import rs_ranking
import sys
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent / 'relative_strength.log'),
        logging.StreamHandler()
    ]
)

def main():
    skipEnter = sys.argv[1] == "true" if len(sys.argv) > 1 else False
    forceTDA = sys.argv[2] == "true" if len(sys.argv) > 2 else False
    api_key = sys.argv[3] if len(sys.argv) > 3 else None
    try:
        logging.info("Starting data collection and ranking process")
        rs_data.main(forceTDA=forceTDA, api_key=api_key)
        rs_ranking.main(skipEnter=skipEnter)
        logging.info("Process completed successfully")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
