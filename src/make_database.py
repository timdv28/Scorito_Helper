import pprint
import pandas as pd
import numpy as np
import sys
import os
import time
from datetime import datetime

# Own created functions
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from features.get_startlist import get_startlist, get_rider_info, upload_to_postgres, load_from_postgres
from features.get_latest_results import get_latest_results

def main():
    race = 'vuelta'
    year = 2025

    start_t1 = time.time()
    table_size_dict = {}

    startlist = get_startlist(race, year)
    table_size_dict['riders'] = startlist.shape[0]
    # spec_points = get_rider_info(startlist)   

    # # Add the Procyclingstats specialisation points to the startlist dataframe
    # new_columns = ['climber_spec','gc_spec','hills_spec','odr_spec','sprint_spec','tt_spec']
    # for i,j in zip(new_columns,spec_points.columns):
    #     startlist[i] = spec_points[j]

    # # # Define column names for the general startlist table
    column_names = ['pcs_gt_point_sum', 'pcs_stage_point_sum', 'uci_gt_point_sum', 'uci_stage_point_sum','startlist_quality_score_avg','top_tens_stages','top_tens_end_classification']
    pcs_profile_names = ['pcs_p0_points','pcs_p1_points','pcs_p2_points','pcs_p3_points','pcs_p4_points','pcs_p5_points']
    uci_profile_names = ['uci_p0_points','uci_p1_points','uci_p2_points','uci_p3_points','uci_p4_points','uci_p5_points']
    scorito_points_names = ['stage_scorito_points', 'classification_scorito_points', 'team_scorito_points']
    column_names.extend(pcs_profile_names)
    column_names.extend(uci_profile_names)
    column_names.extend(scorito_points_names)
    
    proc_t1 = round(time.time() - start_t1,2)
    print(f'Initial table processing took {proc_t1}s.')
    
    startlist, stage_df, RIS_df, RIC_df, table_size_dict, rider_proc_time = gather_results(startlist,column_names,table_size_dict)
    
    avg_rider_proc_time = round(rider_proc_time/startlist.shape[0],3)
    
    print("""
    
    
    
    -------- Processing Results --------
    """)
    # Upload all data tables to postgres
    upload_to_postgres('riders',startlist)
    upload_to_postgres('stages',stage_df)
    upload_to_postgres('stage_results',RIS_df)
    upload_to_postgres('class_results',RIC_df)
    print('')
    print(f'Average processing time per rider is {avg_rider_proc_time}s')
    print('')
    make_report(startlist,stage_df,RIS_df,RIC_df,table_size_dict)
    
    return

def gather_results(startlist,column_names,table_size_dict):
    
    stage_df,RIS_df,RIC_df,table_size_dict = create_tables(startlist,column_names,table_size_dict,replace=False)
    nr_of_warnings = 0   
    rider_proc_time = 0
    # Get the latest results incrementally per rider    
    for idx, rider_url in startlist['rider_url'].items(): 
        start_t2 = time.time()
        sums_and_averages,stage_df,RIS_df,RIC_df = get_latest_results(startlist,rider_url,stage_df,RIS_df,RIC_df,nr_of_warnings,test=False)
        
        # Add the 'sums_and_averages' columns to the startlist dataframe
        for col, value in zip(column_names, sums_and_averages):
            startlist.at[idx, col] = value
        
        # Print the processing time of this iteration
        proc_t2 = round(time.time() - start_t2,2)
        rider_proc_time += proc_t2 
        print(f'{rider_url}: took {proc_t2}s to process')   
    
    return startlist, stage_df, RIS_df, RIC_df, table_size_dict, rider_proc_time

def create_tables(startlist,column_names,table_size_dict,replace=False):
    # # Add the empty columns to startlist
    for col in column_names:
        startlist[col] = None

    stage_table_columns = ['stage_id','stage_url','stage_class','profile','profile_score','stage_type','is_one_day_race',
                            'startlist_quality_score']
    RIS_table_columns = ['rider_id','rider_url','stage_id','stage_url','finish_rank','pcs_points','uci_points','multiple_stage_race',
                            'gc_rank','points_rank','kom_rank','youth_rank','stage_scorito_points', 'jersey_scorito_points', 'team_scorito_points']
    RIC_table_columns = ['rider_id','rider_url','race_id','race_name','race_class','race_quality_score','gc_final_rank','points_final_rank',
                            'kom_final_rank','youth_final_rank','classification_scorito_points', 'team_scorito_points']

    if replace == True:
        # Initialize 3 dataframes, or load them from postgres
        stage_df = pd.DataFrame(columns=stage_table_columns)
        RIS_df = pd.DataFrame(columns=RIS_table_columns)
        RIC_df = pd.DataFrame(columns=RIC_table_columns)
    elif replace == False:
        try:
            mock_stage_df = load_from_postgres('stages',None)
            if mock_stage_df.columns.to_list() == stage_table_columns:
                stage_df = mock_stage_df
                print('Stage dataframe loaded from postgres')
        except:
            print('Empty stage dataframe created')
            pass

        try:
            mock_RIS_df = load_from_postgres('stage_results',None)    
            if mock_RIS_df.columns.to_list() == RIS_table_columns:
                RIS_df = mock_RIS_df
                print('RIS dataframe loaded from postgres')
        except:
            print('Empty RIS dataframe created')
            
        try:   
            mock_RIC_df = load_from_postgres('class_results',None)    
            if mock_RIC_df.columns.to_list() == RIC_table_columns:
                RIC_df = mock_RIC_df
                print('RIC dataframe loaded from postgres')
        except:
            print('Empty RIC dataframe created')
        
    table_size_dict['stages'] = stage_df.shape[0]
    table_size_dict['stage_results'] = RIS_df.shape[0]
    table_size_dict['class_results'] = RIC_df.shape[0]    
    
    return stage_df,RIS_df,RIC_df,table_size_dict

def make_report(startlist,stage_df,RIS_df,RIC_df,table_size_dict):
    report_cols = ['Table', 'Length', 'Change in Rows','Last Updated']

    report_dict = {}
    report_dict['riders'] = startlist
    report_dict['stages'] = stage_df
    report_dict['stage_results'] = RIS_df
    report_dict['class_results'] = RIC_df
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    new_report = pd.DataFrame(columns=report_cols)
    
    for label, table in zip(list(report_dict.keys()),list(report_dict.items())):
        table_name = label
        
        table_length = report_dict[label].shape[0]
        added_rows = table_length-table_size_dict[label]
        
        update = timestamp
        
        new_row = pd.DataFrame([[table_name,table_length,added_rows,update]],columns=report_cols)
        new_report = new_report._append(new_row,ignore_index=True)

    print(new_report)
    upload_to_postgres('report',new_report)

main()





    
    
    




