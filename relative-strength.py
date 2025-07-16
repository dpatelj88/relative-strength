import rs_data
import rs_ranking
import sys
import logging
from pathlib import Path

# Set up logging
LOG_DIR = Path(__file__).parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)
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
    try:
        logging.info("Starting data collection and ranking process")
        rs_data.main()
        rs_ranking.main(skipEnter=skipEnter)
        logging.info("Process completed successfully")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
