from procyclingstats import RaceStartlist, Rider, RiderResults, Stage, Race, Scraper
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

def main():
    race = Race('race/vuelta-a-espana/2025')
    stage_list = race.stages()
    stage_df = pd.DataFrame.from_dict(stage_list)
    
    additional_info_stages = pd.DataFrame()
    new_cols = ['date','stage_type','profile_score','vertical_meters','gradient_final_km']
    for this_stage in stage_df['stage_url']:
        old_date = stage_df.loc[stage_df['stage_url']==this_stage]['date'].iloc[0]
        new_date = '2025-'+old_date
        
        stage = Stage(this_stage)
         
        stage_type = stage.stage_type()
        profile_score = stage.profile_score()
        vertical_meters = stage.vertical_meters()
        gradient_final_km = stage.gradient_final_km()

        
        new_df_row = pd.DataFrame([[new_date,stage_type,profile_score,vertical_meters,gradient_final_km]],columns = new_cols)
        additional_info_stages = additional_info_stages._append(new_df_row,ignore_index=True)
    for col in new_cols:    
        stage_df[col] = additional_info_stages[col]
    
    profile_bin_edges = [0, 60, 120, 180, 280, 600]
    profile_labels = [1, 2, 3, 4, 5]

    stage_df['profile_difficulty'] = pd.cut(
        stage_df['profile_score'],
        bins=profile_bin_edges,
        labels=profile_labels,
        include_lowest=True
    ) 
    
    profile_bin_edges_2 = [-2, 2, 7, 20]
    profile_labels_2 = [1, 2, 3]

    stage_df['final_km_difficulty'] = pd.cut(
        stage_df['gradient_final_km'],
        bins=profile_bin_edges_2,
        labels=profile_labels_2,
        include_lowest=True
    ) 
    
    file_path = "/mnt/c/Users/timdv/OneDrive/Documenten/Scorito/scorito_vuelta2025_stages.xlsx"
    stage_df.to_excel(file_path)
    
    return

if __name__ == "__main__":
    main()
