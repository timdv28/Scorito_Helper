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
from sklearn.preprocessing import MinMaxScaler

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
from features.get_startlist import get_startlist, get_rider_info, upload_to_postgres, load_from_postgres

# We want to calculate the price-quality-ratio of a rider based on their results of the past year. 
def pqr():
    user = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "admin")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "postgres")
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    
    query0 = """SELECT rider_url
                FROM riders
                WHERE scorito_price IS NOT NULL"""
    priced_riders = pd.read_sql_query(query0, con=engine)

    rider_metrics(priced_riders, engine, filter_on_gt=False)
    
    return
        
def rider_metrics(priced_riders, engine, filter_on_gt=False):
    metrics_table = pd.DataFrame()
    grand_tours = ['tour-de-france', 'giro-d-italia', 'vuelta-a-espana']
    stage_type_names = ['c1','c2','c3','c4','c5']
    final_km_gradient_types = ['g1','g2','g3','g4']
    prof_types_len = len(stage_type_names) + 1
    grad_types_len = len(final_km_gradient_types) + 1
    for rider_url in priced_riders['rider_url']:
    
        query1 = """SELECT rider_url, stages.stage_url, stages.stage_type, date, stage_scorito_points, jersey_scorito_points, team_scorito_points, profile, profile_score, stages.profile_difficulty, stages.final_km_difficulty, startlist_quality_score
                    FROM stage_results 
                    LEFT JOIN stages ON stage_results.stage_url=stages.stage_url
                    WHERE rider_url = %(rider_url)s AND stage_type = %(stage_type)s;"""  
        rider_in_stages = pd.read_sql_query(query1, con=engine, params={"rider_url": rider_url, "stage_type": 'RR'})
        rider_in_TT = pd.read_sql_query(query1, con=engine, params={"rider_url": rider_url, "stage_type": 'ITT'})
        
        if filter_on_gt==True:
            condition = rider_in_stages['stage_url'].str.contains('|'.join(grand_tours))
            rider_in_stages = rider_in_stages.loc[condition]
            
        query2 = """SELECT short_name, team_name, scorito_price, classification_scorito_points
                    FROM riders 
                    WHERE rider_url = %(rider_url)s;"""
        price_in_df = pd.read_sql_query(query2, con=engine, params={"rider_url": rider_url})

        price = price_in_df['scorito_price'].iloc[0]
        short_name = price_in_df['short_name'].iloc[0]
        team_name = price_in_df['team_name'].iloc[0]
        classification_scorito_points = price_in_df['classification_scorito_points'].iloc[0]
        
        nr_of_stages = rider_in_stages.shape[0]
        metrics_dict = {}
        metrics_dict['rider_url'] = rider_url
        metrics_dict['short_name'] = short_name
        metrics_dict['team_name'] = team_name
        metrics_dict['scorito_price'] = price
        metrics_dict['nr_of_races'] = int(nr_of_stages)
        metrics_dict['jersey_potential'] = classification_scorito_points / metrics_dict['nr_of_races']
        points_list = []
        
        for c in range(1,prof_types_len):
            # First we only look at the profiles
            condition = rider_in_stages['profile_difficulty']==f'{c}'
            metrics_dict[f'c{c}_nr_of_stages'] = int(rider_in_stages.loc[condition]['stage_scorito_points'].shape[0])
            
            # Assign the weights to each race, to make sure we have representative scores
            metrics_dict[f'c{c}_weighted_point_sum'] = 0
            metrics_dict[f'c{c}_scaled_point_sum'] = 0
            if metrics_dict[f'c{c}_nr_of_stages'] != 0:
                # Loop over all the stages in this profile difficulty class
                for j in range(metrics_dict[f'c{c}_nr_of_stages']):
                    weight = rider_in_stages.loc[condition]['startlist_quality_score'].iloc[j]/1000
                    if weight == 0:
                        weight = 1
                    
                    metrics_dict[f'c{c}_weighted_point_sum'] += round(rider_in_stages.loc[condition]['stage_scorito_points'].iloc[j]*weight,2)
                    metrics_dict[f'c{c}_scaled_point_sum'] = metrics_dict[f'c{c}_weighted_point_sum']
                    
                metrics_dict[f'c{c}_weighted_point_avg'] = round(metrics_dict[f'c{c}_weighted_point_sum']/metrics_dict[f'c{c}_nr_of_stages'],2)
                metrics_dict[f'c{c}_scaled_point_avg'] = metrics_dict[f'c{c}_weighted_point_avg']
                points_list.append(metrics_dict[f'c{c}_weighted_point_avg'])
            else:
                metrics_dict[f'c{c}_weighted_point_sum'] = 0
                metrics_dict[f'c{c}_weighted_point_avg'] = 0
                metrics_dict[f'c{c}_scaled_point_sum'] = 0
                metrics_dict[f'c{c}_scaled_point_avg'] = 0
                points_list.append(0)
                
            # Now loop over the final_km categories, to see how well the riders do in the various combinations    
            for g in range(1,grad_types_len):
                condition = (rider_in_stages['profile_difficulty']==f'{c}') & (rider_in_stages['final_km_difficulty']==f'{g}')
                metrics_dict[f'c{c}_g{g}_nr_of_stages'] = int(rider_in_stages.loc[condition]['stage_scorito_points'].shape[0])
                
                # Assign the weights to each race, to make sure we have representative scores
                metrics_dict[f'c{c}_g{g}_weighted_point_sum'] = 0
                metrics_dict[f'c{c}_g{g}_scaled_point_sum'] = 0
                if metrics_dict[f'c{c}_g{g}_nr_of_stages'] != 0:
                    # Loop over all the stages in this profile difficulty class
                    for j in range(metrics_dict[f'c{c}_g{g}_nr_of_stages']):
                        weight = rider_in_stages.loc[condition]['startlist_quality_score'].iloc[j]/1000
                        if weight == 0:
                            weight = 1
                        
                        metrics_dict[f'c{c}_g{g}_weighted_point_sum'] += round(rider_in_stages.loc[condition]['stage_scorito_points'].iloc[j]*weight,2)
                        metrics_dict[f'c{c}_g{g}_scaled_point_sum'] = metrics_dict[f'c{c}_g{g}_weighted_point_sum']
                        
                    metrics_dict[f'c{c}_g{g}_weighted_point_avg'] = round(metrics_dict[f'c{c}_g{g}_weighted_point_sum']/metrics_dict[f'c{c}_g{g}_nr_of_stages'],2)
                    metrics_dict[f'c{c}_g{g}_scaled_point_avg'] = metrics_dict[f'c{c}_g{g}_weighted_point_avg']
                    points_list.append(metrics_dict[f'c{c}_g{g}_weighted_point_avg'])
                else:
                    metrics_dict[f'c{c}_g{g}_weighted_point_sum'] = 0
                    metrics_dict[f'c{c}_g{g}_weighted_point_avg'] = 0
                    metrics_dict[f'c{c}_g{g}_scaled_point_sum'] = 0
                    metrics_dict[f'c{c}_g{g}_scaled_point_avg'] = 0
                    points_list.append(0)

        
        # Assign the weights to each ITT race, to make sure we have representative scores
        metrics_dict['tt_nr_of_stages'] = rider_in_TT['stage_scorito_points'].shape[0]
        metrics_dict['tt_weighted_point_sum'] = 0
        metrics_dict['tt_scaled_point_sum'] = 0
        for k in range(metrics_dict['tt_nr_of_stages']):
            weight = rider_in_TT['startlist_quality_score'].iloc[k]/1000
            if weight == 0:
                weight = 1
            metrics_dict['tt_weighted_point_sum'] += round(rider_in_TT['stage_scorito_points'].iloc[k]*weight,2)
            metrics_dict['tt_scaled_point_sum'] = metrics_dict['tt_weighted_point_sum']
            
        metrics_dict['tt_weighted_point_avg'] = round(metrics_dict['tt_weighted_point_sum']/metrics_dict['tt_nr_of_stages'],2)
        metrics_dict['tt_scaled_point_avg'] = metrics_dict['tt_weighted_point_avg']
        points_list.append(metrics_dict[f'tt_weighted_point_avg'])
        

        # SPECIALISATION METRIC
        metrics_dict['total_weighted_point_avg'] = sum(points_list)
        metrics_dict['total_scaled_point_avg'] = sum(points_list)
        specialisation_list = []
        # If the rider never earned points
        if metrics_dict['total_weighted_point_avg'] == 0:
            for i in range(1,prof_types_len):
                metrics_dict[f'c{i}_specialisation'] = 0
                specialisation_list.append(0)
            metrics_dict[f'tt_specialisation'] = 0
            specialisation_list.append(0)
            metrics_dict[f'specialisation_variance'] = 0
        else: # If the rider has earned points, this part of the loop is taken
            for i in range(1,prof_types_len):
                metrics_dict[f'c{i}_specialisation'] =  round(metrics_dict[f'c{i}_weighted_point_avg']/metrics_dict['total_weighted_point_avg'],2)
                specialisation_list.append(metrics_dict[f'c{i}_specialisation'])
                metrics_dict[f'c{i}_stage_suitability'] = metrics_dict[f'c{i}_scaled_point_avg'] * math.sqrt(metrics_dict[f'c{i}_specialisation'])
                
            metrics_dict[f'tt_specialisation'] = round(metrics_dict[f'tt_weighted_point_avg']/metrics_dict['total_weighted_point_avg'],2)
            specialisation_list.append(metrics_dict[f'tt_weighted_point_avg']/metrics_dict['total_weighted_point_avg'])
            metrics_dict[f'specialisation_variance'] = round(np.var(specialisation_list),4)

            metrics_dict[f"tt_stage_suitability"] = metrics_dict[f'tt_scaled_point_avg'] * math.sqrt(metrics_dict[f'tt_specialisation'])
        
        # Grab all of the columns together and make a new row, which is appended tot the dataframe
        metrics_cols = list(metrics_dict.keys())
        metrics_row = list(metrics_dict.values())

        metrics_df = pd.DataFrame([metrics_row],columns=metrics_cols)
        metrics_table = metrics_table._append(metrics_df)
    
    # Add metric for the team_tt
    teams = metrics_table['team_name'].unique().tolist()
    metrics_table['team_tt_weighted_point_avg'] = None
    metrics_table['c1_team_point_potential'] = None
    metrics_table['c2_team_point_potential'] = None
    metrics_table['c3_team_point_potential'] = None
    metrics_table['c4_team_point_potential'] = None
    metrics_table['c5_team_point_potential'] = None
    metrics_table['tt_team_point_potential'] = None
    
    stage_type_names = ['c1','c2','c3','c4','c5','tt']
    for team in teams:
        # tt_specialisations = metrics_table.loc[metrics_table['team_name']==team]['tt_specialisation']
        tt_wpa_sum = metrics_table.loc[metrics_table['team_name']==team]['tt_weighted_point_avg'].sum()
        nr_of_riders = metrics_table.loc[metrics_table['team_name']==team].shape[0]
        team_tt_weighted_point_avg = tt_wpa_sum / nr_of_riders
        team_tt_scaled_point_avg = team_tt_weighted_point_avg
        
        # Get a team dataframe with all riders within a particular team
        riders_in_team = metrics_table.loc[metrics_table['team_name'] == team]
        for r in range(nr_of_riders):
            # Get the rider_url of the rider you want to address
            rider_url = riders_in_team['rider_url'].iloc[r]
            
            # Add the TTT weight_point_avg to a rider
            metrics_table.loc[metrics_table['rider_url']==rider_url, 'team_tt_weighted_point_avg'] = float(team_tt_weighted_point_avg)
            metrics_table.loc[metrics_table['rider_url']==rider_url, 'team_tt_scaled_point_avg'] = float(team_tt_scaled_point_avg)
            
            # We also want to have the team point potential, where we sum the weighted_point_averages of all other riders in the team to get a conclusion
            
            team_without_current_rider = riders_in_team.loc[riders_in_team['rider_url']!=rider_url]
            for c in stage_type_names:
                team_point_potential = team_without_current_rider[f'{c}_weighted_point_avg'].sum()
                metrics_table.loc[metrics_table['rider_url']==rider_url, f'{c}_team_point_potential'] = team_point_potential
            

    ### MinMax scale the weighted point average columns
    scaler = MinMaxScaler()
    tf_cols_0 = ['jersey_potential']
    tf_cols_1 = ['c1_scaled_point_avg','c2_scaled_point_avg','c3_scaled_point_avg','c4_scaled_point_avg','c5_scaled_point_avg','tt_scaled_point_avg', 'team_tt_scaled_point_avg']
    tf_cols_2 = ['c1_team_point_potential','c2_team_point_potential','c3_team_point_potential','c4_team_point_potential','c5_team_point_potential','tt_team_point_potential']
    tf_cols_3 = ['c1_g1_scaled_point_avg','c1_g2_scaled_point_avg','c1_g3_scaled_point_avg',
                 'c2_g1_scaled_point_avg','c2_g2_scaled_point_avg','c2_g3_scaled_point_avg',
                 'c3_g1_scaled_point_avg','c3_g2_scaled_point_avg','c3_g3_scaled_point_avg',
                 'c4_g1_scaled_point_avg','c4_g2_scaled_point_avg','c4_g3_scaled_point_avg',
                 'c5_g1_scaled_point_avg','c5_g2_scaled_point_avg','c5_g3_scaled_point_avg']
    metrics_table[tf_cols_0] = scaler.fit_transform(metrics_table[tf_cols_0])/2.1
    metrics_table[tf_cols_1] = scaler.fit_transform(metrics_table[tf_cols_1])*10
    metrics_table[tf_cols_2] = scaler.fit_transform(metrics_table[tf_cols_2])
    metrics_table[tf_cols_3] = scaler.fit_transform(metrics_table[tf_cols_3])*10
    
    addition_dict = {}
    addition_table = pd.DataFrame()
    for k in range(metrics_table.shape[0]):
        addition_dict['minmax_scaled_point_avg'] = metrics_table[tf_cols_1].iloc[k].sum()
    
    user = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "admin")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "postgres")
   
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    metrics_table.to_sql(name='rider_metrics', con=engine, if_exists='replace', index=False)
    
def define_race():
    import pandas as pd
    cols = ['profile_icon','stage_name','stage_url','date','stage_type','profile_score','vertical_meters',
            'gradient_final_km','profile_difficulty','gradient_final_5_km','elevation_final_5_km']
    gt_df = pd.read_excel("/mnt/c/Users/timdv/OneDrive/Documenten/Scorito/scorito_vuelta2025_stages.xlsx",usecols=cols) 
    etappe_types = {}
    etappe_types['ITT'] = gt_df.loc[gt_df['stage_type']=='ITT'].shape[0]
    etappe_types['TTT'] = gt_df.loc[gt_df['stage_type']=='ITT'].shape[0]
    
    type_list = []
    for i in range(gt_df.shape[0]):
        if gt_df['stage_type'].iloc[i] == 'RR':
            pd = gt_df['profile_difficulty'].iloc[i]
            type_list.append(f'c{pd}')
        elif gt_df['stage_type'].iloc[i] == 'ITT':
            type_list.append('tt')
        else:
            type_list.append('team_tt')
    
    gt_normal_etappes = gt_df.loc[gt_df['stage_type']=='RR']
    for i in range(1,7):
        etappe_types[f'c{i}'] = gt_normal_etappes.loc[gt_normal_etappes['profile_difficulty']==i].shape[0]

    return etappe_types, type_list, gt_df 

def general_rider_importance(metrics_table,etappe_types):
    specialisations = metrics_table
    types = ['ITT','c1','c2','c3','c4','c5']

    i = 0
    gri_table = pd.DataFrame()
    cols = ['rider_url','gri']
    for rider_url in specialisations['rider_url']:
        rider_row = specialisations.loc[specialisations['rider_url']==rider_url]
        gri = 0
        for c in types:
            type_frequency = etappe_types[c]
            if c == 'ITT':
                gri += rider_row['tt_weighted_point_avg'].iloc[0]*type_frequency
            else:
                gri += rider_row[f'{c}_weighted_point_avg'].iloc[0]*type_frequency
        
        gri_df = pd.DataFrame([[rider_url,round(gri,1)]],columns=cols)
        gri_table = gri_table._append(gri_df,ignore_index=True)
        
    return gri_table

# def team_point_metrics(rider_url,engine):
#     rider_url

if __name__ == '__main__':
    pqr()
