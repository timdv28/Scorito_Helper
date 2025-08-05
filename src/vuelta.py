import pprint
import pandas as pd
import numpy as np
import sys
import os

# Own created functions
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from features.get_startlist import get_startlist, get_rider_info, get_latest_results, upload_to_postgres

race = 'vuelta'
year = 2025

startlist = get_startlist(race, year)
# spec_points = get_rider_info(startlist)

# new_columns = ['climber_spec','gc_spec','hills_spec','odr_spec','sprint_spec','tt_spec']
# for i,j in zip(new_columns,spec_points.columns):
#     startlist[i] = spec_points[j]
    


# for rider_url in startlist['rider_url']:   
#     sums_and_averages = get_latest_results(startlist,race,year,rider_url)
    
    
# table_name = 'riders_vuelta'
# upload_to_postgres(table_name,startlist)


    
    
    




