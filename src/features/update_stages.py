#from procyclingstats import RaceStartlist, Rider, RiderResults, Stage, Race
import pprint
import pandas as pd
import numpy as np
import os
import psycopg2
from psycopg2 import sql
import psycopg
import sys
import math
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Integer
import warnings
import traceback

sys.path.append(os.path.join(os.path.dirname(__file__), '..','..', 'src'))
from features.get_startlist import get_startlist, get_rider_info, upload_to_postgres, load_from_postgres
from features.get_latest_results import get_latest_results

def update_stages_table():
    table_name = 'stages'
    df = load_from_postgres(table_name,query=None)

    # for stage_url in df['stage_url']:
    #     profile_score = df.loc[df['stage_url']==stage_url]['profile_score'].iloc[0]
        
    profile_bin_edges = [0, 60, 120, 180, 280, 470]
    profile_labels = [1, 2, 3, 4, 5]

    df['profile_difficulty'] = pd.cut(
        df['profile_score'],
        bins=profile_bin_edges,
        labels=profile_labels,
        include_lowest=True
    ) 
    
    profile_bin_edges_2 = [-2, 2, 7, 20]
    profile_labels_2 = [1, 2, 3]

    df['final_km_difficulty'] = pd.cut(
        df['gradient_final_km'],
        bins=profile_bin_edges_2,
        labels=profile_labels_2,
        include_lowest=True
    ) 
    

    upload_to_postgres(table_name,df)

if __name__ == "__main__":
    update_stages_table()