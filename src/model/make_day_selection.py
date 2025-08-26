from procyclingstats import RaceStartlist, Rider, RiderResults, Stage, Race
import pprint
import pandas as pd
import numpy as np
import os
from psycopg2 import sql
import psycopg
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Integer
from pulp import *

# Own created functions
sys.path.append(os.path.join(os.path.dirname(__file__), '..','..', 'src'))
from features.get_startlist import get_startlist, get_rider_info, upload_to_postgres, load_from_postgres
from features.get_latest_results import get_latest_results
from features.update_stages import update_stages_table
from model.build_team import define_race

def make_day_selection(stage_nr,DNFs):
    user = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "admin")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "postgres")
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    
    query = """ SELECT *
                FROM rider_metrics; """
    all_riders = pd.read_sql_query(query, con=engine)
    
    riders_in_race = pd.read_excel("/mnt/c/Users/timdv/OneDrive/Documenten/Scorito/TEAM_SELECTION_VUELTA25.xlsx",sheet_name='Team Selectie')
    
    riders_in_race = riders_in_race.loc[riders_in_race['in_race']==True].reset_index()
    original_day_selection = pd.read_excel("/mnt/c/Users/timdv/OneDrive/Documenten/Scorito/TEAM_SELECTION_VUELTA25.xlsx",sheet_name='Dag Selecties')
    
    scaled = True
    
    profile_list, gradient_list, gt_df = define_race()
    profile_list = profile_list
    gradient_list = gradient_list
    
    day_selections = make_daily_picks(riders_in_race,stage_nr,profile_list,gradient_list,scaled)

    dict = {}
    day_table = pd.DataFrame()
    
    # Make a nicer table for the stage selections
    
    for i in range(stage_nr-1,gt_df['stage_name'].shape[0]):
        dict['stage_nr'] = i+1
        dict['stage_name'] = gt_df['stage_name'].iloc[i]
        dict['profile_type'] = profile_list[i]
        dict['end_type'] = gradient_list[i]
        
        j = 0
        if (dict['profile_type'] != 'team_tt') and dict['profile_type'] != 'tt':
            dict['stage_type'] = f'{dict['profile_type']}_{dict['end_type']}'
            max_points = riders_in_race[f'{dict['stage_type']}_scaled_point_avg'].max()
            dict['captain'] = riders_in_race.loc[riders_in_race[f'{dict['stage_type']}_scaled_point_avg']==max_points]['short_name'].iloc[0]
        elif dict['profile_type'] == 'team_tt':
            dict['stage_type'] = f'{dict['profile_type']}'
            max_points = riders_in_race[f'team_tt_scaled_point_avg'].max()
            dict['captain'] = riders_in_race.loc[riders_in_race['team_tt_scaled_point_avg']==max_points]['short_name'].iloc[0]
        else:
            dict['stage_type'] = f'{dict['profile_type']}'
            max_points = riders_in_race[f'tt_scaled_point_avg'].max()
            dict['captain'] = riders_in_race.loc[riders_in_race['tt_scaled_point_avg']==max_points]['short_name'].iloc[0]
       
        for r in day_selections[i]:
            j += 1
            dict[f'rider_{j}'] = riders_in_race.iloc[r].loc['short_name']

        cols = list(dict.keys())
        row = list(dict.values())

        df = pd.DataFrame([row],columns=cols)
        day_table = day_table._append(df,ignore_index=True)
    
    
    file_path = "/mnt/c/Users/timdv/OneDrive/Documenten/Scorito/UPDATED_DAY_SELECTIONS_VUELTA25.xlsx"
    with pd.ExcelWriter(file_path) as writer:
        day_table.to_excel(writer,sheet_name=f'Dag Selecties_Etappe_{stage_nr}>')


def make_daily_picks(riders,stage_nr,profile_list,gradient_list,scaled):
    n_stages = 21
    
    # Define optimization problem
    model = LpProblem("Daily_LineUps", LpMaximize)

    # Decision variable: 1 if rider is in etappe selection
    y = LpVariable.dicts("y", [(i, s) for i in riders.index for s in range(stage_nr-1,n_stages)], cat="Binary")  # stage lineups
    
    # Stage constraints: 9 riders per stage
    for s in range(stage_nr-1,n_stages):
        model += lpSum([y[(i, s)] for i in riders.index]) == 9
    
    # Make an updated day selection, adjusting for DNF riders and jersey standings
    if 2 <= stage_nr <= 21:
        last_stage_rankings(riders,stage_nr)
    
    # Make a dataframe with the stage_scores
    stage_scores = build_stage_scores(riders,profile_list, gradient_list, scaled)
    

    # Objective: maximize total score
    model += lpSum([stage_scores.iloc[i, s] * y[(i, s)] 
                         for i in riders.index 
                         for s in range(stage_nr-1,n_stages)])
    
    model.solve(PULP_CBC_CMD(msg=False))
    
    # Extract stage lineups
    stage_lineups = {
        s: [i for i in riders.index if value(y[(i, s)]) == 1]
        for s in range(stage_nr-1,n_stages)
    }
    
    
    return stage_lineups

def build_stage_scores(riders,profile_list,gradient_list,scaled):
    n_stages = len(profile_list)
    stage_scores = pd.DataFrame(index=riders.index, columns=range(n_stages))

    if scaled == True:
        how = 'scaled'
    else:
        how = 'weighted'
    
    for s in range(len(profile_list)):
        prof = profile_list[s]
        grad = gradient_list[s]
        col_name_1 = f"{prof}_{how}_point_avg"
        col_name_4 = f"{prof}_{grad}_{how}_point_avg"
        jersey_potential = riders['jersey_potential']
        
        
        
        if prof == 'team_tt':
            point_avg = riders[col_name_1]
            stage_scores[s] = point_avg * 0.2
        elif prof == 'tt':
            point_avg = riders[col_name_1]
            col_name_2 = f"{prof}_team_point_potential"
            col_name_3 = f"{prof}_specialisation"
            
            team_point_potential = riders[col_name_2]
            spec = riders[col_name_3].fillna(0)
        else:
            point_avg = riders[col_name_4]
            col_name_2 = f"{prof}_team_point_potential"
            col_name_3 = f"{prof}_specialisation"
            team_point_potential = riders[col_name_2]
            spec = riders[col_name_3].fillna(0)
            
        if stage_nr <= 11:  
            j_multiplier = stage_nr
        else:
            j_multiplier = 11
        
        if stage_nr != 1:
            jersey_standing = riders['riders_classification_weight']
            # The final calculation for team and stage lineup selections    
            stage_scores[s] = point_avg * (1 + spec) + team_point_potential + jersey_potential + (j_multiplier-1)*jersey_standing/10
        else:
            stage_scores[s] = point_avg * (1 + spec) + team_point_potential + jersey_potential
            
    return stage_scores

def last_stage_rankings(riders,stage_nr):
    last_stage_url = f'race/vuelta-a-espana/2025/stage-{stage_nr-1}'
    stage = Stage(last_stage_url)
    gc_ranking = stage.gc()
    points_ranking = stage.points()
    kom_ranking = stage.kom()
    youth_ranking = stage.youth()
    
    gc_top = []
    points_top = []
    kom_top = []
    youth_top = [] 
    d = {}
    for i in range(5):
        gc_top.append(gc_ranking[i]['rider_url'])
        points_top.append(points_ranking[i]['rider_url'])
        try:
            kom_top.append(kom_ranking[i]['rider_url'])
        except:
            kom_top.append('No-rider-classified')
        youth_top.append(youth_ranking[i]['rider_url'])
        
    d = {'gc_top': gc_top,
        'points_top': points_top,
        'kom_top': kom_top,
        'youth_top': youth_top}   
    
    top_rankings = pd.DataFrame(d)

    riders['riders_classification_weight'] = 0
    cols = ['gc_top','points_top','kom_top','youth_top']

    for col in cols:
        top_riders = pd.unique(top_rankings[col].values.ravel())
        overlap = set(top_riders) & set(riders['rider_url'])
        if overlap:
            for url in overlap:
                standing = top_rankings.loc[top_rankings[col]==url,col].index[0] + 1
                if col == 'gc_top':
                    weight = 1.2 - (0.2*standing)
                elif col == 'points_top' or col == 'kom_top':
                    weight = 1 - (0.2*standing)
                else:
                    weight = 0.8 - (0.15*standing)
                riders.loc[riders['rider_url']==url,'riders_classification_weight'] += weight
    
    return riders

    
if __name__ == '__main__':
    stage_nr = 4
    DNFs = ['rider/axel-zingle']
    make_day_selection(stage_nr,DNFs)
        