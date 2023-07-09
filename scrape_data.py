import pandas as pd
import numpy as np
import os
import wafflescrapers as ws

pwd = os.getcwd()

# Create data folders if they don't already exist
ws.create_folders()

# Initialize proxy pool
proxy_list_path = os.path.join(pwd,'proxies','proxy_list.txt')
proxypool = ws.ProxyPool(proxy_list_path)

# Scrape and clean data on bojangles locations
raw_filepath,scraper_issues = ws.scrape_bojangles_data(proxypool)
df1 = ws.clean_bojangles_data(raw_filepath,scraper_issues)

# Read in low-resolution grid for dunkin data
grid_filepath = os.path.join(pwd,'grids/usa_grid_v1.csv')
grid = pd.read_csv(grid_filepath,index_col=0).reset_index()

# Scrape and clean data on dunkin locations
raw_filepath,scraper_issues = ws.scrape_dunkin_data(grid,proxypool)
df2 = ws.clean_dunkin_data(raw_filepath,scraper_issues)

# Scrape and clean data on wendy's locations.
# Use same coarse grid as dunkin.
raw_filepath,scraper_issues = ws.scrape_wendys_data(grid,proxypool)
df3 = ws.clean_wendys_data(raw_filepath,scraper_issues)

# Read in high-resolution grid for McDonald's data
grid_filepath = os.path.join(pwd,'grids/mcdonalds_grid.csv')
grid = pd.read_csv(grid_filepath,index_col=0).reset_index()
grid = grid[grid['num_results'] > 0]

# Scrape and clean data on mcdonalds locations
# (use asynchronous web scraping to speed up process)
raw_filepath,scraper_issues = ws.async_scrape_mcdonalds_data(grid,proxypool)
df4 = ws.clean_mcdonalds_data(raw_filepath,scraper_issues)

# Scrape and clean data on waffle house locations
restaurant_numbers = np.arange(1,2600)
raw_filepath,scraper_issues = ws.async_scrape_wafflehouse_data(restaurant_numbers,proxypool)
df5 = ws.clean_wafflehouse_data(raw_filepath,scraper_issues)
