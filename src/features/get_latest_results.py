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

def get_latest_results(startlist,rider_url,stage_df,RIS_df,RIC_df,nr_of_warnings,test=False):
    rider_id = startlist.loc[startlist['rider_url']==rider_url]['rider_id'].iloc[0]
    general_rider_results = RiderResults(f"{rider_url}/results")
    rider_results = general_rider_results.results()
    
    # Aggregation dictionary with all sums and averages to be added to the startlist table
    agg_dict = {'uci_points_p0': 0, 'uci_points_p1': 0,'uci_points_p2': 0, 'uci_points_p3': 0,'uci_points_p4': 0, 'uci_points_p5': 0,
                'pcs_points_p0': 0, 'pcs_points_p1': 0,'pcs_points_p2': 0, 'pcs_points_p3': 0,'pcs_points_p4': 0, 'pcs_points_p5': 0,
                'stage_count': 0, 'startlist_quality_score_avg': 0, 'pcs_gt_point_sum': 0, 'pcs_stage_point_sum': 0,
                'uci_gt_point_sum': 0, 'uci_stage_point_sum': 0, 'startlist_quality_score_sum': 0, 'top_tens_stage': 0,
                'top_tens_end_classification': 0, 'stage_scorito_points': 0, 'classification_scorito_points': 0, 'team_scorito_points': 0}
    
    ### Check if the input for test is true or false. When it is true, we set the size of the loop 
    # to 10 to see if we obtain correct metrics
    if test == False:
        range_size = len(rider_results)
    else:
        range_size = 10
    
    # Loop over the last 100 races (standard) of the current rider if we are not testing the code    
    for i in range(range_size):
        try:
            stage_url = rider_results[i]['stage_url']
            print(stage_url)
            # Check if we are dealing with a single stage or with a classification
            if rider_results[i]['distance'] != None:  # ----- SINGLE STAGE -----
                if (stage_df['stage_url'] == stage_url).any():
                    # No need for a new stage creation. Now we check if we need to update info on this particular rider
                    if ((RIS_df['stage_url'] == stage_url) & (RIS_df['rider_url'] == rider_url)).any():
                        # When the rider is already there, we pull their rank and points from this stage from the existing table
                        agg_dict = pull_existing_stage_results(stage_df,stage_url,rider_results,agg_dict,i)
                    else: # When he is not there yet, we create an entry for him and produce the approproate metrics
                        # Add rider specific stage results
                        api_stage = Stage(stage_url)
                        RIS_df, agg_dict = add_rider_stage_results(rider_results,rider_url,startlist,stage_df,
                                                                            RIS_df,i,api_stage,agg_dict)
                else: 
                    # We need to create a new entry in the stages table
                    stage_df,api_stage = add_stage_entry(i,stage_df,stage_url,rider_results)
                    # Add the rider specific info for this stage as well
                    RIS_df, agg_dict = add_rider_stage_results(rider_results,rider_url,startlist,stage_df,
                                                                        RIS_df,i,api_stage,agg_dict)  
            else: # ----- CLASSIFICATION -----
                # Check if this classification is already there
                race_name = stage_url.split('/')[1] + '-' + stage_url.split('/')[2]
                last_string = stage_url.split('/')[-1]
                condition1 = ((RIC_df['race_name']==race_name) & (RIC_df['rider_url']==rider_url)).any()
                if last_string in ['gc','points','kom','youth']:
                    condition2 = not RIC_df.loc[(RIC_df['race_name']==race_name) & (RIC_df['rider_url']==rider_url), f"{last_string}_final_rank"].isin([-1, None]).any() 
                    if condition1 and condition2:
                        agg_dict = pull_existing_class_results(rider_results,agg_dict,i)
                    else:
                        # add the classification of the rider which is applicable to this iteration
                        RIC_df, agg_dict = add_rider_classification(rider_results,rider_id,rider_url,stage_url,RIC_df,i,agg_dict)
            
            
            if rider_results[i]['class'] in ['2.UWT', '1.UWT', '2.Pro', '1.Pro']:
                # Check if the Stage object already exists. If not, we create it
                try:
                    api_stage
                except:
                    api_stage = Stage(stage_url)
                agg_dict, RIS_df, RIC_df = get_scorito_points(rider_results,rider_url,RIS_df,RIC_df,i,agg_dict,api_stage)
                    
            # Sums and averages row is extended, so it can be added to the row with the correct rider in the main function    
            sums_and_averages = [int(agg_dict['pcs_gt_point_sum']), 
                                int(agg_dict['pcs_stage_point_sum']), 
                                int(agg_dict['uci_gt_point_sum']), 
                                int(agg_dict['uci_stage_point_sum']),
                                int(agg_dict['startlist_quality_score_avg']), 
                                int(agg_dict['top_tens_stage']),
                                int(agg_dict['top_tens_end_classification']),
                                agg_dict['pcs_points_p0'], agg_dict['pcs_points_p1'], agg_dict['pcs_points_p2'], agg_dict['pcs_points_p3'], agg_dict['pcs_points_p4'], agg_dict['pcs_points_p5'],
                                agg_dict['uci_points_p0'], agg_dict['uci_points_p1'], agg_dict['uci_points_p2'], agg_dict['uci_points_p3'], agg_dict['uci_points_p4'], agg_dict['uci_points_p5'],
                                int(agg_dict['stage_scorito_points']), int(agg_dict['classification_scorito_points']), int(agg_dict['team_scorito_points'])]      
        except KeyboardInterrupt:
            print("\nKeyboard interrupt detected — exiting.")
            sys.exit(1)
        except:
            tb = traceback.format_exc()
            warnings.warn(f"Exception converted to warning:\n{tb}", stacklevel=2)
            nr_of_warnings += 1 
            
        
    return sums_and_averages,stage_df,RIS_df,RIC_df

def add_stage_entry(i,stage_df,stage_url,rider_results):
    stage_cl = rider_results[i]['class']
    if math.isnan(stage_df['stage_id'].max()) == False:
        stage_id = stage_df['stage_id'].max() + 1
    else:
        stage_id = 20001
        
    # Use the API to pull the stage information    
    stage = Stage(stage_url)
    profile = stage.profile_icon()
    profile_score = stage.profile_score()
    stage_type = stage.stage_type()            
    is_one_day_race = stage.is_one_day_race()
    startlist_quality_score = stage.race_startlist_quality_score()
    
    # Make a new stage row and append it the the stages dataframe
    new_stage_row = [stage_id,stage_url,stage_cl,profile,profile_score,stage_type,
                is_one_day_race,startlist_quality_score]
    new_stage_row = pd.DataFrame([new_stage_row], columns=stage_df.columns.to_list())
    
    stage_df = stage_df._append(new_stage_row,ignore_index=True)
    api_stage = stage
    return stage_df, api_stage

def pull_existing_stage_results(stage_df,stage_url,rider_results,agg_dict,i):
    finish_rank = rider_results[i]['rank']
    pcs_points = int(rider_results[i]['pcs_points'])
    uci_points = int(rider_results[i]['uci_points'])
    
    # Metrics to be added to the startlist table
    agg_dict['pcs_stage_point_sum'] += pcs_points
    agg_dict['uci_stage_point_sum'] += uci_points 
        
    this_stage = stage_df.loc[stage_df['stage_url']==stage_url]
    agg_dict['startlist_quality_score_sum'] += this_stage['startlist_quality_score'].iloc[0] 
    profile = this_stage['profile'].iloc[0]
    agg_dict[f'pcs_points_{profile}'] += pcs_points
    agg_dict[f'uci_points_{profile}'] += uci_points   
    agg_dict['stage_count'] += 1
        
    if type(finish_rank) == int:
        if int(finish_rank) <= 10:
            agg_dict['top_tens_stage'] += 1
            
    agg_dict['startlist_quality_score_avg'] = agg_dict['startlist_quality_score_sum']/agg_dict['stage_count']
    return agg_dict

def pull_existing_class_results(rider_results,agg_dict,i):
    finish_rank = rider_results[i]['rank']
    pcs_points = int(rider_results[i]['pcs_points'])
    uci_points = int(rider_results[i]['uci_points'])
    
    agg_dict['pcs_gt_point_sum'] += pcs_points
    agg_dict['uci_gt_point_sum'] += uci_points
    
    if type(finish_rank) == int:
            if int(finish_rank) <= 10:
                agg_dict['top_tens_end_classification'] += 1
    return agg_dict

def add_rider_stage_results(rider_results,rider_url,startlist,stage_df,RIS_df,i,stage,agg_dict):
    rider_id = startlist.loc[startlist['rider_url']==rider_url]['rider_id'].iloc[0]
    stage_url = rider_results[i]['stage_url']
    stage_id = stage_df.loc[stage_df['stage_url']==stage_url]['stage_id'].iloc[0]
    
    finish_rank = rider_results[i]['rank']
    pcs_points = int(rider_results[i]['pcs_points'])
    uci_points = int(rider_results[i]['uci_points'])
    multiple_stage_race = 'stage-' in stage_url
    if multiple_stage_race:
        try:
            gc_standing = pd.DataFrame(stage.gc())
            gc_rank = int(gc_standing.loc[gc_standing['rider_url']==rider_url]['rank'].squeeze())
        except:
            gc_rank = -1
        try:
            points_standing = pd.DataFrame(stage.points())
            points_rank = int(points_standing.loc[points_standing['rider_url']==rider_url]['rank'].squeeze())
        except:
            points_rank = -1
        try:
            kom_standing = pd.DataFrame(stage.kom())
            kom_rank = int(kom_standing.loc[kom_standing['rider_url']==rider_url]['rank'].squeeze())
        except:
            kom_rank = -1
        try:
            youth_standing = pd.DataFrame(stage.youth())
            youth_rank = int(youth_standing.loc[youth_standing['rider_url']==rider_url]['rank'].squeeze())
        except:
            youth_rank = -1
    else:
        gc_rank = -1
        points_rank = -1
        kom_rank = -1
        youth_rank = -1
    
    rider_in_stage_row = [rider_id,rider_url,stage_id,stage_url,finish_rank,pcs_points,uci_points,multiple_stage_race,
                        gc_rank,points_rank,kom_rank,youth_rank,0,0,0]
    rider_in_stage_row = pd.DataFrame([rider_in_stage_row], columns=RIS_df.columns.to_list())

    # RIS_df = RIS_df._append(rider_in_stage_row,ignore_index=True)
    RIS_df = pd.concat([RIS_df, rider_in_stage_row], ignore_index=True)
    
    # Metrics to be added to the startlist table
    agg_dict['pcs_stage_point_sum'] += pcs_points
    agg_dict['uci_stage_point_sum'] += uci_points 
        
    this_stage = stage_df.loc[stage_df['stage_url']==stage_url]
    agg_dict['startlist_quality_score_sum'] += this_stage['startlist_quality_score'].iloc[0] 
    profile = this_stage['profile'].iloc[0]
    agg_dict[f'pcs_points_{profile}'] += pcs_points
    agg_dict[f'uci_points_{profile}'] += uci_points   
    agg_dict['stage_count'] += 1
        
    if type(finish_rank) == int:
        if int(finish_rank) <= 10:
            agg_dict['top_tens_stage'] += 1
            
    agg_dict['startlist_quality_score_avg'] = agg_dict['startlist_quality_score_sum']/agg_dict['stage_count']
    return RIS_df, agg_dict

def add_rider_classification(rider_results,rider_id,rider_url,stage_url,RIC_df,i,agg_dict):
    race_name = stage_url.split('/')[1] + '-' + stage_url.split('/')[2]
    race_quality_score = Stage(stage_url).race_startlist_quality_score()
    finish_rank = rider_results[i]['rank']
    stage_cl = rider_results[i]['class']
    pcs_points = rider_results[i]['pcs_points']
    uci_points = rider_results[i]['uci_points']

    last_string = stage_url.split('/')[-1]
    if last_string=='gc' or last_string=='points' or last_string=='kom' or last_string=='youth': # ----- GENERAL CLASSIFICATION -----
        category = last_string
        # Check if the current rider already has a classification in this particular race. If he does, fill in the appropriate column
        if RIC_df.loc[RIC_df['race_name']==race_name]['rider_url'].str.contains(rider_url).any():
            RIC_df.loc[(RIC_df['race_name'] == race_name) & (RIC_df['rider_url'] == rider_url),
                        f'{category}_final_rank'
                        ] = finish_rank            
        else: # If the rider does not have a classification in this race yet, we create a new row
            # If statement for creation of a new race_id
            if math.isnan(RIC_df['race_id'].max()) == False:
                if RIC_df['race_name'].str.contains(race_name).any():
                    race_id = int(RIC_df.loc[RIC_df['race_name']==race_name]['race_id'].iloc[0])
                else:
                    race_id = RIC_df['race_id'].max() + 1
            else:
                race_id = 3001
                
            initiation_list = [rider_id,rider_url,race_id,race_name,stage_cl,race_quality_score,-1,-1,-1,-1,0,0]
            initiation_df = pd.DataFrame([initiation_list],
                                            columns=RIC_df.columns.to_list())
            
            try: # Try to add a finishing rank, but when it fails, pass
                RIC_df = pd.concat([RIC_df, initiation_df], ignore_index=True)
                RIC_df.loc[(RIC_df['race_name'] == race_name) & (RIC_df['rider_url'] == rider_url),
                            f'{category}_final_rank'
                            ] = finish_rank  
            except:
                pass
    agg_dict['pcs_gt_point_sum'] += pcs_points
    agg_dict['uci_gt_point_sum'] += uci_points
    
    if type(finish_rank) == int:
            if int(finish_rank) <= 10:
                agg_dict['top_tens_end_classification'] += 1
                             
    return RIC_df, agg_dict

# 'stage_scorito_points': 0, 'classification_scorito_points': 0, 'team_scorito_points': 0
def get_scorito_points(rider_results,rider_url,RIS_df,RIC_df,i,agg_dict,stage):
    filepath = '/mnt/c/Users/timdv/OneDrive/Documenten/Scorito/scorito_points_table.xlsx'
    finish_rank = rider_results[i]['rank']
    stage_url = rider_results[i]['stage_url']
    
    if rider_results[i]['distance'] != None and (finish_rank not in ['DNF', 'DNS', None]):
        # Get the row in the stage result dataframe corresponding to this stage and rider, so we can use it to find the classification standings
        row_condition = (RIS_df['stage_url']==stage_url) & (RIS_df['rider_url']==rider_url)
        result_row = RIS_df.loc[row_condition]   
        stage_points = pd.read_excel(filepath,sheet_name='stage_points')     
        # Add the points for the stage finish rank
        if 1 <= finish_rank <= 20: 
            stage_points_gained = int(stage_points.loc[stage_points['finish_rank']==finish_rank]['stage'].iloc[0])
            agg_dict['stage_scorito_points'] += stage_points_gained
            RIS_df.loc[row_condition,'stage_scorito_points'] = stage_points_gained
            if finish_rank == 1:
                agg_dict['team_scorito_points'] += 10
                RIS_df.loc[row_condition,'team_scorito_points'] = 10

        # Add the points for the gc, kom, points and youth standing if applicable
        gc_rank = result_row['gc_rank'].iloc[0]
        points_rank = result_row['points_rank'].iloc[0]
        kom_rank = result_row['kom_rank'].iloc[0]
        youth_rank = result_row['youth_rank'].iloc[0]
              
        ### CLASSIFICATION STANDING AFTER THE STAGE ###
        if 1 <= gc_rank <= 5 and gc_rank is not None:
            gc_p = int(stage_points.loc[stage_points['finish_rank']==gc_rank]['gc'].iloc[0])
            agg_dict['stage_scorito_points'] += gc_p
            RIS_df.loc[row_condition,'jersey_scorito_points'] += gc_p
            if gc_rank == 1:
                agg_dict['team_scorito_points'] += 8
                RIS_df.loc[row_condition,'team_scorito_points'] = 8
        if 1 <= points_rank <= 5 and points_rank is not None:
            points_p = int(stage_points.loc[stage_points['finish_rank']==points_rank]['points'].iloc[0])
            agg_dict['stage_scorito_points'] += points_p
            RIS_df.loc[row_condition,'jersey_scorito_points'] += points_p
            if points_rank == 1:
                agg_dict['team_scorito_points'] += 6
                RIS_df.loc[row_condition,'team_scorito_points'] = 6
        if 1 <= kom_rank <= 5 and kom_rank is not None:
            kom_p = int(stage_points.loc[stage_points['finish_rank']==kom_rank]['kom'].iloc[0])
            agg_dict['stage_scorito_points'] += kom_p
            RIS_df.loc[row_condition,'jersey_scorito_points'] += kom_p
            if kom_rank == 1:
                agg_dict['team_scorito_points'] += 6
                RIS_df.loc[row_condition,'team_scorito_points'] = 6
        if 1 <= youth_rank <= 5 and youth_rank is not None:
            youth_p = int(stage_points.loc[stage_points['finish_rank']==youth_rank]['youth'].iloc[0])
            agg_dict['stage_scorito_points'] += youth_p
            RIS_df.loc[row_condition,'jersey_scorito_points'] += youth_p
            if youth_rank == 1:
                agg_dict['team_scorito_points'] += 3
                RIS_df.loc[row_condition,'team_scorito_points'] = 3
            
    elif finish_rank not in ['DNF', 'DNS', None]: # If we are dealing with a final classification
        race_name = stage_url.split('/')[1] + '-' + stage_url.split('/')[2]
        row_condition = (RIC_df['race_name']==race_name) & (RIC_df['rider_url']==rider_url)
        result_row = RIC_df.loc[row_condition]
        category = stage_url.split('/')[-1]
        
        # get the final rank of the classifications of this rider in this race
        gc_final_rank = result_row['gc_final_rank'].iloc[0]
        points_final_rank = result_row['points_final_rank'].iloc[0]
        kom_final_rank = result_row['kom_final_rank'].iloc[0]
        youth_final_rank = result_row['youth_final_rank'].iloc[0]
        
        # Only if there is a classification which earns points, then we will proceed. Otherwise, we skipt the entire step
        if (category == 'gc' and 1 <= gc_final_rank <= 20 and gc_final_rank is not None):
            final_classification_points = pd.read_excel(filepath,sheet_name='final_classification_points')
            final_gc_p = int(final_classification_points.loc[final_classification_points['finish_rank']==gc_final_rank]['gc'].iloc[0])
            agg_dict['classification_scorito_points'] += final_gc_p
            RIC_df.loc[row_condition,'classification_scorito_points'] = final_gc_p
            if gc_final_rank == 1:
                agg_dict['team_scorito_points'] += 24
                RIC_df.loc[row_condition,'team_scorito_points'] = 24
        if (category == 'points' and 1 <= points_final_rank <= 10 and points_final_rank is not None):
            final_classification_points = pd.read_excel(filepath,sheet_name='final_classification_points')
            final_points_p = int(final_classification_points.loc[final_classification_points['finish_rank']==points_final_rank]['points'].iloc[0])
            agg_dict['classification_scorito_points'] += final_points_p
            RIC_df.loc[row_condition,'classification_scorito_points'] = final_points_p
            if points_final_rank == 1:
                agg_dict['team_scorito_points'] += 18
                RIC_df.loc[row_condition,'team_scorito_points'] = 18
        if (category == 'kom' and 1 <= kom_final_rank <= 10 and kom_final_rank is not None):
            final_classification_points = pd.read_excel(filepath,sheet_name='final_classification_points')
            final_kom_p = int(final_classification_points.loc[final_classification_points['finish_rank']==kom_final_rank]['kom'].iloc[0])
            agg_dict['classification_scorito_points'] += final_kom_p
            RIC_df.loc[row_condition,'classification_scorito_points'] = final_kom_p
            if kom_final_rank == 1:
                agg_dict['team_scorito_points'] += 18
                RIC_df.loc[row_condition,'team_scorito_points'] = 18
        if (category == 'youth' and 1 <= youth_final_rank <= 5 and youth_final_rank is not None):
            final_classification_points = pd.read_excel(filepath,sheet_name='final_classification_points')
            final_youth_p = int(final_classification_points.loc[final_classification_points['finish_rank']==youth_final_rank]['youth'].iloc[0])
            agg_dict['classification_scorito_points'] += final_youth_p
            RIC_df.loc[row_condition,'classification_scorito_points'] = final_youth_p
            if youth_final_rank == 1:
                agg_dict['team_scorito_points'] += 9
                RIC_df.loc[row_condition,'team_scorito_points'] = 9
        
    return agg_dict, RIS_df, RIC_df
    