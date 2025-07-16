import sys
from rs_data import main as rs_data_main
from rs_ranking import main as rs_ranking_main

def main():
    """Run data collection and ranking process."""
    skipEnter = sys.argv[1] == "true" if len(sys.argv) > 1 else False
    rs_data_main()
    rs_ranking_main(skipEnter=skipEnter)

if __name__ == "__main__":
    main()
