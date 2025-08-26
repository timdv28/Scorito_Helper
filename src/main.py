import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..','..', 'src'))
from features.get_startlist import get_startlist, get_rider_info, upload_to_postgres, load_from_postgres
from features.get_latest_results import get_latest_results
from model.build_team import create_team
from model.price_quality_ratio import pqr
from model.make_day_selection import make_day_selection

def main():
    print('Calculating rider metrics')
    pqr()
    print('Creating team and day selections')
    create_team(scaled=True)
    
    stage_number = 1
    DNFs = []
    
    # make_day_selection(stage_number,DNFs)
    
if __name__ == '__main__':
    main()
    