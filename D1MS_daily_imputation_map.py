#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  7 10:13:58 2023

@author: epnzv
"""


import calendar
import pandas as pd
import datetime as dt

from channel_config import (CHANNEL_ABM_MAP, CHANNEL_DIR, CHANNEL_SALES_YEARS,
                            DATA_DIR)

dfs = []
    
crop='CORN'
abm_map = pd.read_csv(CHANNEL_ABM_MAP)

year_range = [2016, 2017, 2018, 2019, 2020]

for year in year_range:
    print('Reading ' + str(year) + ' sales data...')
        
    dfi_path = DATA_DIR + CHANNEL_DIR + str(year) + '_CH.csv'
        
    # read in an individual year
    dfi = pd.read_csv(dfi_path)
        
    # grab the data for the requested crop
    dfi = dfi[dfi['SPECIE_DESCR'] == crop].reset_index(drop=True)
    dfi['year'] = year
        
    # convert the date to datetime format
    dfi['EFFECTIVE_DATE'] = pd.to_datetime(dfi['EFFECTIVE_DATE'])
        
    dfs.append(dfi)
        
sales = pd.concat(dfs).reset_index(drop=True)
    
    
# grab relevant columns
sales = sales[
        ['year', 'EFFECTIVE_DATE', 'NET_SALES_QTY_TO_DATE', 'NET_SHIPPED_QTY_TO_DATE',
         'ORDER_QTY_TO_DATE', 'REPLANT_QTY_TO_DATE', 'RETURN_QTY_TO_DATE',
         'SHIPPING_FIPS_CODE', 'VARIETY_NAME']]
    
# rename columns
sales = sales.rename(columns={'NET_SALES_QTY_TO_DATE': 'nets_Q',
                              'NET_SHIPPED_QTY_TO_DATE': 'shipped_Q',
                              'ORDER_QTY_TO_DATE': 'order_Q',
                              'REPLANT_QTY_TO_DATE': 'replant_Q',
                              'RETURN_QTY_TO_DATE': 'return_Q',
                              'SHIPPING_FIPS_CODE': 'fips',
                              'VARIETY_NAME': 'hybrid'})
    
sales = sales[sales['fips'].isna()==False].reset_index(drop=True)
sales['fips'] = sales['fips'].astype(int)

# merge with ABM map and drop the fips column
sales = sales.merge(abm_map, on=['fips'], how='left').drop(columns=['fips'])
    
sales_abm_by_date = pd.DataFrame()

sales_abm_level_agg = sales.drop(columns=['year', 'hybrid']).groupby(by=['abm'], as_index=False).sum()

month_range = [9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8]

for year in sales['year'].unique():
    for month in month_range:
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            if month > 8:
                date_mask = dt.datetime(year=year - 1,
                                        month=month,
                                        day=day)
            else:
                date_mask = dt.datetime(year=year,
                                        month=month,
                                        day=day)
            
            single_year = sales[sales['year'] == year].reset_index(drop=True)
            single_year_to_date = single_year[single_year['EFFECTIVE_DATE'] <= date_mask].reset_index(drop=True)
            
            single_year_to_date['month'] = month
            single_year_to_date['day'] = day
            
            single_year_to_date_agg = single_year_to_date.groupby(
                    by=['abm', 'year', 'month', 'day'],
                    as_index=False).sum()
            
            if sales_abm_by_date.empty == True:
                sales_abm_by_date = single_year_to_date_agg.copy()
            else:
                sales_abm_by_date = pd.concat([sales_abm_by_date, single_year_to_date_agg])
                
sales_abm_by_date_no_year = sales_abm_by_date.drop(
        columns=['year']).groupby(['abm', 'month', 'day'], as_index=False).sum()
    
sales_abm_by_date_no_year = sales_abm_by_date_no_year.rename(
        columns={'nets_Q': 'nets_Q_ratio',
                 'shipped_Q': 'shipped_Q_ratio',
                 'order_Q': 'order_Q_ratio',
                 'replant_Q': 'replant_Q_ratio',
                 'return_Q': 'return_Q_ratio'})
    
sales_abm_ratios = sales_abm_by_date_no_year.merge(sales_abm_level_agg, on=['abm'], how='left')

sales_abm_ratios['nets_Q_ratio'] = sales_abm_ratios['nets_Q_ratio'] / sales_abm_ratios['nets_Q']
sales_abm_ratios['shipped_Q_ratio'] = sales_abm_ratios['shipped_Q_ratio'] / sales_abm_ratios['shipped_Q']
sales_abm_ratios['order_Q_ratio'] = sales_abm_ratios['order_Q_ratio'] / sales_abm_ratios['order_Q']
sales_abm_ratios['replant_Q_ratio'] = sales_abm_ratios['replant_Q_ratio'] / sales_abm_ratios['replant_Q']
sales_abm_ratios['return_Q_ratio'] = sales_abm_ratios['return_Q_ratio'] / sales_abm_ratios['return_Q']

sales_abm_ratios = sales_abm_ratios.drop(columns=['nets_Q', 'shipped_Q', 'order_Q', 'replant_Q', 'return_Q'])

sales_abm_ratios.to_csv('channel_abm_date_ratios_corn.csv',index=False)