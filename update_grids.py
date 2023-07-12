import pandas as pd
import numpy as np
import wafflescrapers as ws
import os

pwd = os.getcwd()

# Initialize proxy pool
proxy_list_path = os.path.join(pwd,'proxies','proxy_list.txt')
proxypool = ws.ProxyPool(proxy_list_path)
proxypool.remove_bad_proxies()

# Check whether it's the first day of the month
first_of_month = (pd.to_datetime('today').date().day == 1)

# Update gridpoints with number of mcdonalds locations
max_per_day = 1450 # Maximum number of entries to check per day
grid_filepath = os.path.join(pwd,'grids/mcdonalds_grid.csv')
grid = pd.read_csv(grid_filepath,index_col=0)

if first_of_month: # Reset list of points that to check if first day of month
    grid['checked_this_month'] = False

points_to_check = grid[(grid['checked_this_month']==False)&(grid['num_results']<=0)].iloc[:max_per_day]
points_to_check = ws.update_mcdonalds_grid(points_to_check,proxypool)
grid.loc[points_to_check.index] = points_to_check
grid.to_csv(grid_filepath)
