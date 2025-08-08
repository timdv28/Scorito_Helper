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

general_rider_results = RiderResults(f"rider/egan-bernal/results")
rider_results = general_rider_results.results()[1:10]

stage_url = rider_results[2]['stage_url']

# general_stage_info = Stage(stage_url).parse().keys()

pprint.pprint(general_rider_results.final_n_km_results())