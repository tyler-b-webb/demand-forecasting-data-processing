#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 15:41:17 2023

@author: epnzv
"""

import pandas as pd

from channel_config import(CHANNEL_ABM_MAP, CURRENT_BANK, EFFECTIVE_DATE)
from channel_sales import (read_channel_sales, read_channel_sales_imputeD1MS,
                           read_forecasts)
from channel_products import (generate_age_trait_RM, merge_MPI_data,
                              merge_performance_data)

# crops are always CORN or SOYBEAN
IMPUTE_D1MS = True

def check_dupes(df, stage):
    """Checks if there are any year/hybrid/abm level duplicates in the data (duplicated rows).
    
    Keyword arguments:
        df -- the dataframe at some level of the aggregation process
        stage -- the stage of processing we're at
    Returns:
        None
    """
    print(stage)
    
    df_index = df[['year', 'hybrid', 'abm']]
    
    print(df_index.duplicated().any())
    
    return None


abm_map = pd.read_csv(CHANNEL_ABM_MAP)

CROP = 'CORN'

if IMPUTE_D1MS == False:
    channel_sales = read_channel_sales(crop=CROP, abm_map=abm_map)
elif IMPUTE_D1MS == True:
    channel_sales = read_channel_sales_imputeD1MS(crop=CROP, abm_map=abm_map)

channel_forecasts = read_forecasts(df=channel_sales, crop=CROP)

channel_age = generate_age_trait_RM(df=channel_forecasts, crop=CROP, RM=True)

channel_MPI = merge_MPI_data(df=channel_age, crop=CROP)

channel_performance = merge_performance_data(df=channel_MPI,
                                             abm_map=abm_map,
                                             crop=CROP)

check_dupes(channel_sales, 'sales')
check_dupes(channel_forecasts, 'forecasts')
check_dupes(channel_age, 'age/trait/rm')
check_dupes(channel_MPI, 'MPI')
check_dupes(channel_performance, 'performance')

channel_performance = channel_performance[channel_performance['year'] > 2016]

if CURRENT_BANK == True:
    channel_performance.to_csv('channel_dfs/channel_24_' + str.lower(CROP) + '_test_' + str(EFFECTIVE_DATE['month']) + 
                               '_' + str(EFFECTIVE_DATE['day']) + '_fcst_10.csv', index=False)
else: 
    channel_performance.to_csv('channel_dfs/channel_no24_' + str.lower(CROP) + '_test_' + str(EFFECTIVE_DATE['month']) + 
                               '_' + str(EFFECTIVE_DATE['day']) + '_fcst_10.csv', index=False)