import pandas as pd
import numpy as np
import wafflescrapers as ws
import os

pwd = os.getcwd()

# Create data folders if they don't already exist
ws.create_folders()

# Initialize proxy pool
proxy_list_path = os.path.join(pwd,'proxies','proxy_list.txt')
proxypool = ws.ProxyPool(proxy_list_path)

# Scrape and clean data on bojangles locations
raw_filepath,scraper_issues = ws.get_bojangles_data()
df1 = ws.clean_bojangles_data(raw_filepath,scraper_issues)

# Scrape and clean data on dunkin locations
raw_filepath,scraper_issues = ws.get_dunkin_data()
df2 = ws.clean_dunkin_data(raw_filepath,scraper_issues)

# Read in low-resolution grid for wendy's data
grid_filepath = os.path.join(pwd,'grids/usa_grid_v1.csv')
grid = pd.read_csv(grid_filepath,index_col=0)

# Scrape and clean data on wendy's locations
raw_filepath,scraper_issues = ws.get_wendys_data(grid)
df3 = ws.clean_wendys_data(raw_filepath,scraper_issues)
