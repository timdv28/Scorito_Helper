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

def create_team(scaled):
    user = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "admin")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "postgres")
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    
    profile_list, gradient_list, gt_df = define_race()
    
    query = """ SELECT *
                FROM rider_metrics; """
    all_riders = pd.read_sql_query(query, con=engine)
    all_riders['scorito_price'] = pd.to_numeric(all_riders['scorito_price'])
    roster, day_selections = the_team(all_riders, profile_list, gradient_list,scaled)
    
    roster['in_race'] = True
    
    dict = {}
    day_table = pd.DataFrame()
    
    # Make a nicer table for the stage selections
    for i in range(gt_df['stage_name'].shape[0]):
        dict['stage_nr'] = i+1
        dict['stage_name'] = gt_df['stage_name'].iloc[i]
        dict['profile_type'] = profile_list[i]
        dict['end_type'] = gradient_list[i]
        
        j = 0
        if (dict['profile_type'] != 'team_tt') and dict['profile_type'] != 'tt':
            dict['stage_type'] = f'{dict['profile_type']}_{dict['end_type']}'
            max_points = roster[f'{dict['stage_type']}_scaled_point_avg'].max()
            dict['captain'] = roster.loc[roster[f'{dict['stage_type']}_scaled_point_avg']==max_points]['short_name'].iloc[0]
        elif dict['profile_type'] == 'team_tt':
            dict['stage_type'] = f'{dict['profile_type']}'
            max_points = roster[f'team_tt_scaled_point_avg'].max()
            dict['captain'] = roster.loc[roster['team_tt_scaled_point_avg']==max_points]['short_name'].iloc[0]
        else:
            dict['stage_type'] = f'{dict['profile_type']}'
            max_points = roster[f'tt_scaled_point_avg'].max()
            dict['captain'] = roster.loc[roster['tt_scaled_point_avg']==max_points]['short_name'].iloc[0]
            
        for r in day_selections[i]:
            j += 1
            dict[f'rider_{j}'] = all_riders.iloc[r,1]

        cols = list(dict.keys())
        row = list(dict.values())

        df = pd.DataFrame([row],columns=cols)
        day_table = day_table._append(df,ignore_index=True)
    
    deploy_frequency = pd.DataFrame(columns=['Rider','Price','Frequency'])
    deploy_frequency['Rider'] = roster['short_name']
    deploy_frequency['Price'] = roster['scorito_price']
    rider_cols = ['rider_1', 'rider_2', 'rider_3', 'rider_4', 'rider_5', 'rider_6', 'rider_7', 'rider_8', 'rider_9']
    # print(deploy_frequency['Rider'].head())
    for rider in deploy_frequency['Rider']:
        freq = day_table[rider_cols].apply(lambda row: rider in row.values, axis=1).sum()
        deploy_frequency.loc[deploy_frequency['Rider'] == rider, 'Frequency'] = freq
    deploy_frequency = deploy_frequency.sort_values(by=['Frequency'],ascending=False)        
    
    upload_to_postgres('vuelta_roster', roster)
    upload_to_postgres('roster_per_etappe', day_table)
    
    roster_cols = ['short_name','team_name','scorito_price','in_race']
    file_path = "/mnt/c/Users/timdv/OneDrive/Documenten/Scorito/TEAM_SELECTION_VUELTA25.xlsx"
    with pd.ExcelWriter(file_path) as writer:
        roster.to_excel(writer,sheet_name='Team Selectie')
        day_table.to_excel(writer,sheet_name='Dag Selecties')
        deploy_frequency.to_excel(writer,sheet_name='Selectie Frequentie')

def define_race():
    import pandas as pd
    cols = ['profile_icon','stage_name','stage_url','date','stage_type','profile_score','vertical_meters',
            'gradient_final_km','profile_difficulty','final_km_difficulty']
    gt_df = pd.read_excel("/mnt/c/Users/timdv/OneDrive/Documenten/Scorito/scorito_vuelta2025_stages.xlsx",usecols=cols) 
    etappe_types = {}
    etappe_types['ITT'] = gt_df.loc[gt_df['stage_type']=='ITT'].shape[0]
    etappe_types['TTT'] = gt_df.loc[gt_df['stage_type']=='ITT'].shape[0]
    
    profile_list = []
    gradient_list = []
    for i in range(gt_df.shape[0]):
        if gt_df['stage_type'].iloc[i] == 'RR':
            c = gt_df['profile_difficulty'].iloc[i]
            g = gt_df['final_km_difficulty'].iloc[i]
            profile_list.append(f'c{c}')
            gradient_list.append(f'g{g}')
        elif gt_df['stage_type'].iloc[i] == 'ITT':
            profile_list.append('tt')
            gradient_list.append('g0')
        else:
            profile_list.append('team_tt')
            gradient_list.append('g0')
    
    gt_normal_etappes = gt_df.loc[gt_df['stage_type']=='RR']
    for i in range(1,7):
        etappe_types[f'c{i}'] = gt_normal_etappes.loc[gt_normal_etappes['profile_difficulty']==i].shape[0]

    return profile_list, gradient_list, gt_df


def the_team(riders, profile_list, gradient_list, scaled):
    budget = 46000000
    n_riders = 20
    n_stages = 21
    max_per_team = 4
    
    # Define optimization problem
    model = LpProblem("FantasyTeam", LpMaximize)

    # Decision variable: 1 if rider is selected
    x = LpVariable.dicts("x", riders.index, cat="Binary")
    # Decision variable: 1 if rider is in etappe selection
    y = LpVariable.dicts("y", [(i, s) for i in riders.index for s in range(n_stages)], cat="Binary")  # stage lineups
    
    # Constraints
    model += (budget - 500000) <= lpSum([riders.loc[i, "scorito_price"] * x[i] for i in riders.index]) <= budget
    model += lpSum([x[i] for i in riders.index]) == n_riders
    for team in riders["team_name"].unique():
        indices = riders.index[riders["team_name"] == team]
        model += lpSum([x[i] for i in indices]) <= 4, f"Max4_{team}"
    
    # Stage constraints: 9 riders per stage
    for s in range(n_stages):
        model += lpSum([y[(i, s)] for i in riders.index]) == 9
        
    # Link y to roster: can only pick if on roster
    for i in riders.index:
        for s in range(n_stages):
            model += y[(i, s)] <= x[i]

    # Make a dataframe with the stage_scores
    stage_scores = build_stage_scores(riders, profile_list, gradient_list,scaled)

    # Objective: maximize total score
    model += lpSum([stage_scores.iloc[i, s] * y[(i, s)] 
                         for i in riders.index 
                         for s in range(n_stages)])
    
    model.solve(PULP_CBC_CMD(msg=False))

    # Extract chosen roster
    chosen_riders = riders.loc[[i for i in riders.index if value(x[i]) == 1]]
    
    # Extract stage lineups
    stage_lineups = {
        s: [i for i in riders.index if value(y[(i, s)]) == 1]
        for s in range(n_stages)
    }
    
    return chosen_riders, stage_lineups
    
def build_stage_scores(riders, profile_list, gradient_list,scaled):
    n_stages = len(profile_list)
    stage_scores = pd.DataFrame(index=riders.index, columns=range(n_stages))
    len(stage_scores)
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
        
        # The final calculation for team and stage lineup selections    
        stage_scores[s] = point_avg * (1 + spec) + team_point_potential + jersey_potential
            
    return stage_scores    

if __name__ == '__main__':
    create_team()