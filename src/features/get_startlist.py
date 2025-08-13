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
    frame_columns = ['rider_id','short_name','scorito_price','rider_name','team_name','rider_url','team_url']
    rider_id = 10000
    startlist = pd.DataFrame(columns=frame_columns)
    
    for i in range(number_of_riders):
        rider_entry = rs['startlist'][i]
        rider_url = rider_entry['rider_url']
        rider_id += 1
        
        # Create a column with shortened names (last name and first initial). The last letter is always an initial, so we need to add a dot at the end of the string
        upper_case = rider_entry['rider_name'].isupper()
        upper_case = ''.join([c for c in rider_entry['rider_name'] if c.isupper() or c.isspace()]) + '.'
        upper_case = upper_case.replace('-',' ')
        
        # Dot all extra initials, if there are any
        for i in range(len(upper_case)):
            if upper_case[i].isupper() and upper_case[i+1].isspace():
                if upper_case[i-1].isspace():
                    short_name = upper_case[:i+1] + '.' + upper_case[i+2:]
                else:
                    short_name = upper_case
            
        
        rider_price = get_rider_price(rider_entry,rider_url,short_name)
        
        # Make a list, which is a row of extra info to be added to the dataframe
        li = [rider_id,short_name,rider_price]
        for k in source_columns:
            li.append(rider_entry[k])

        frame = pd.DataFrame([li], columns=frame_columns)
        startlist = pd.concat([startlist, frame], ignore_index=True)
        
    startlist.replace(-1,None,inplace=True)
    return startlist

def get_rider_info(startlist):
    url_list = startlist['rider_url'].to_list()
    source_columns = ['climber','gc','hills','one_day_races','sprint','time_trial']
    
    # Get the general PCS specialisation points
    spec_points = pd.DataFrame(columns=source_columns)
    for u in url_list:
        rider_points = Rider(u).points_per_speciality()  
        df = pd.DataFrame([rider_points])
        spec_points = pd.concat([spec_points, df], ignore_index=True)
    return spec_points

def get_rider_price(rider_entry,rider_url,short_name):
    cols = ['short_name','team_name','rider_url','scorito_price']
    price_table = pd.read_excel("/mnt/c/Users/timdv/OneDrive/Documenten/Scorito/scorito_vuelta2025_price_table.xlsx",usecols=cols)
    # print(price_table.loc[price_table['rider_id']==rider_id]['scorito_price'])
    try:
        price = price_table.loc[price_table['rider_url']==rider_url]['scorito_price'].iloc[0]
    except:
        price = None
        print(f'Warning: Rider {short_name} not found in price_table')
        print(f'Rider entry added in the price table without a price')
        
        new_rider_url = rider_entry['rider_url']
        new_short_name = short_name
        new_team_name = rider_entry['team_name']
        new_price = -1
        print(price_table.columns)
        new_row = pd.DataFrame([[new_short_name, new_team_name, new_rider_url, new_price]], columns=cols)
        new_price_table = price_table._append(new_row, ignore_index=True)
        
        new_price_table.to_excel("/mnt/c/Users/timdv/OneDrive/Documenten/Scorito/scorito_vuelta2025_price_table.xlsx")
        
        pass
    return price

def upload_to_postgres(table_name,df):
    user = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "admin")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "postgres")
    
    # Change -1 values to NaN
    df.replace(-1,None,inplace=True)
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
   
def load_from_postgres(table_name,query=None):
    user = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "admin")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "postgres")
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    df = pd.read_sql_table(table_name, con=engine, schema=query)
    
    return df

if __name__ == '__main__':
    rs = RaceStartlist(f"race/vuelta-a-espana/2025/startlist").parse()
    
    startlist = get_startlist('vuelta', 2025)
    
    
    