import pandas as pd
import numpy as np
import wafflescrapers as ws
import os

pwd = os.getcwd()

ws.create_folders()

raw_filepath,scraper_issues = ws.get_bojangles_data()
df1 = ws.clean_bojangles_data(raw_filepath,scraper_issues)

raw_filepath,scraper_issues = ws.get_dunkin_data()
df2 = ws.clean_dunkin_data(raw_filepath,scraper_issues)

grid_filepath = os.path.join(pwd,'grids/usa_grid_v1.csv')
grid = pd.read_csv(grid_filepath,index_col=0)
raw_filepath,scraper_issues = ws.get_wendys_data(grid)
df3 = ws.clean_wendys_data(raw_filepath,scraper_issues)
