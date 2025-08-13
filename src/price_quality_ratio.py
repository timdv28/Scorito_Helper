from procyclingstats import RaceStartlist, Rider, RiderResults, Stage, Race
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

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from features.get_startlist import get_startlist, get_rider_info, upload_to_postgres, load_from_postgres

def main():
    user = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "admin")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "postgres")
    
    rider_url = 'rider/richard-carapaz'
    
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    query1 = """SELECT rider_url, stages.stage_url, finish_rank, pcs_points, uci_points, stage_scorito_points, jersey_scorito_points, team_scorito_points, profile, profile_score, startlist_quality_score
                FROM stage_results 
                LEFT JOIN stages ON stage_results.stage_id=stages.stage_id
                WHERE rider_url = %(rider_url)s;"""
    df = pd.read_sql_query(query1, con=engine, params={"rider_url": rider_url})
    

    print(df)


    
if __name__ == '__main__':
    main()
