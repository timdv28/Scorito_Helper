from procyclingstats import RaceStartlist, Rider, RiderResults, Stage, Race
import pprint
import pandas as pd
import numpy as np
import os
import psycopg2
from psycopg2 import sql
import psycopg
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Integer


def get_startlist(race, year):
    if race == 'tour':
        race_insert = 'tour-de-france'
    elif race == 'giro':
        race_insert = 'giro-d-italia'
    elif race == 'vuelta':
        race_insert = 'vuelta-a-espana'
        
    rs = RaceStartlist(f"race/{race_insert}/{year}/startlist").parse()

    number_of_riders = len(rs['startlist'])
    source_columns = ['rider_name','team_name','rider_url','team_url']
    frame_columns = ['rider_id','rider_name','team_name','rider_url','team_url']
    rider_id = 20250000
    
    startlist = pd.DataFrame(columns=frame_columns)
    for i in range(number_of_riders):
        rider_entry = rs['startlist'][i]
        rider_id += 1 
        li = [rider_id]
        
        for k in source_columns:
            li.append(rider_entry[k])

        frame = pd.DataFrame([li], columns=frame_columns)
        startlist = pd.concat([startlist, frame], ignore_index=True)
    return startlist


def get_rider_info(startlist):
    url_list = startlist['rider_url'].to_list()
    source_columns = ['climber','gc','hills','one_day_races','sprint','time_trial']
    
    spec_points = pd.DataFrame(columns=source_columns)
    
    for u in url_list:
        rider_points = Rider(u).points_per_speciality()  
        df = pd.DataFrame([rider_points])
        
        spec_points = pd.concat([spec_points, df], ignore_index=True)
    
    return spec_points

def get_latest_results(startlist,race,year,rider_url):
    if race == 'tour':
        race_insert = 'tour-de-france'
    elif race == 'giro':
        race_insert = 'giro-d-italia'
    elif race == 'vuelta':
        race_insert = 'vuelta-a-espana'
    
    # Define the weights of different race classes
    class_weights = pd.DataFrame([[1,1,0.8,0.8,0.6,0.6,0.5,0.5]],columns=['2.UWT','1.UWT','2.Pro','1.Pro','2.1','1.1','2.2','1.2'])
    max_startlist_quality_score = 2275
    
    rider = 'richard-carapaz'
    rider_results = RiderResults(f"rider/{rider}/results").results()
    
    pcs_gt_point_sum, pcs_stage_point_sum, uci_gt_point_sum, uci_stage_point_sum, startlist_quality_score_sum = [0] * 5
    stage_count = 0
    pcs_points_profile = {'p0': 0, 
                          'p1': 0,
                          'p2': 0, 
                          'p3': 0,
                          'p4': 0, 
                          'p5': 0}
    
    uci_points_profile = {'p0': 0, 
                          'p1': 0,
                          'p2': 0, 
                          'p3': 0,
                          'p4': 0, 
                          'p5': 0}
    
    # Loop over the last 100 races of the current rider
    for i in range(len(rider_results)): # len(rider_results)
        # Get different parameters of the selected race
        cl = rider_results[i]['class']
        weight = class_weights[cl]
        
        # Scrape Stage info
        stage_url = rider_results[i]['stage_url']
        stage = Stage(stage_url)
        profile = stage.profile_icon()
        profile_score = stage.profile_score()
        stage_type = stage.stage_type()
        is_one_day_race = stage.is_one_day_race()
        startlist_quality_score = stage.race_startlist_quality_score()
        
        # Get Rider results info
        finish_rank = rider_results[i]['rank']
        pcs_points = rider_results[i]['pcs_points']
        uci_points = rider_results[i]['uci_points']
        
        # When 'final_classification' == True, the points awarded are counted towards the GT points
        
        if rider_results[i]['distance'] != None:
            stage_count += 1
            final_classification = False
            pcs_stage_point_sum += pcs_points
            uci_stage_point_sum+= uci_points
            startlist_quality_score_sum += startlist_quality_score 
            
            pcs_points_profile[profile] += pcs_points
            uci_points_profile[profile] += uci_points
            startlist_quality_score_avg = startlist_quality_score_sum/stage_count

        else:
            final_classification = True
            pcs_gt_point_sum += pcs_points
            uci_gt_point_sum+= uci_points
        
        # Sums and averages
        
        
        row = [stage_url,cl,profile,profile_score,stage_type,is_one_day_race,startlist_quality_score,final_classification,finish_rank]
        
    sums_and_averages = [pcs_gt_point_sum, pcs_stage_point_sum, uci_gt_point_sum, uci_stage_point_sum,
                         round(startlist_quality_score_avg,1)]
    print(sums_and_averages)           
    return sums_and_averages

def upload_to_postgres(table_name,df):
    user = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "admin")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "postgres")

    # columns = {
    #     "rider_id": String,
    #     "rider_name": String,
    #     "team_name": Integer,
    #     "rider_url": String,
    #     "team_url": String,
    #     "climber_spec": Integer,
    #     "gc_spec": Integer,
    #     "hills_spec": Integer,
    #     "odr_spec": Integer,
    #     "sprint_spec": Integer,
    #     "tt_spec": Integer,
    # }
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    
    print(f"Created {table_name} with {df.shape[0]} entries")