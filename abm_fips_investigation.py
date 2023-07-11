#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 15:41:17 2023

@author: epnzv
"""

import numpy as np
import pandas as pd

DATA_DIR = '../../NA-soy-pricing/data/'
CHANNEL_DIR = 'channel/'
FARMGATE_DIR = 'from_larry_swift/'

CHANNEL_SALES_YEARS = ['2009', '2010', '2011', '2012', '2013', '2014', '2015', 
                       '2016', '2017', '2018', '2019', '2020']

def read_channel_sales(crop):
    """Reads in Channel sales data.
    
    Keyword Arguments:
        crop -- the crop we want to get the data for
    Returns:
        sales -- the dataframe of the concatenated sales data.
    """
    # set up a df 
    dfs = []
    
    for year in CHANNEL_SALES_YEARS:
        print('Reading ' + year + ' sales data...')
        
        dfi_path = DATA_DIR + CHANNEL_DIR + year + '_CH.csv'
        
        # read in an individual year
        dfi = pd.read_csv(dfi_path)
        
        # grab the data for the requested crop
        dfi = dfi[dfi['SPECIE_DESCR'] == crop].reset_index(drop=True)
        dfi['year'] = year
        
        # convert the date to datetime format
        dfi['EFFECTIVE_DATE'] = pd.to_datetime(dfi['EFFECTIVE_DATE'])
        
        dfs.append(dfi)
        
    sales = pd.concat(dfs).reset_index(drop=True)
    
    # abm mapping piece
    abm_map = sales[['SLS_LVL_2_ID', 'SHIPPING_FIPS_CODE']]
    abm_map = abm_map.rename(columns={'SHIPPING_FIPS_CODE': 'FIPS', 
                              'SLS_LVL_2_ID': 'abm'})
    
    abm_map = abm_map.dropna()
    abm_map = abm_map.drop_duplicates().reset_index(drop=True)
    abm_map['FIPS'] = abm_map['FIPS'].astype(float)
    
    # grab relevant columns
    sales = sales[
            ['year','NET_SALES_QTY_TO_DATE', 'NET_SHIPPED_QTY_TO_DATE',
             'ORDER_QTY_TO_DATE', 'REPLANT_QTY_TO_DATE', 'RETURN_QTY_TO_DATE',
             'SLS_LVL_2_ID', 'VARIETY_NAME']]
    
    # rename columns
    sales = sales.rename(columns={'NET_SALES_QTY_TO_DATE': 'nets_Q',
                                  'NET_SHIPPED_QTY_TO_DATE': 'shipped_Q',
                                  'ORDER_QTY_TO_DATE': 'order_Q',
                                  'REPLANT_QTY_TO_DATE': 'replant_Q',
                                  'RETURN_QTY_TO_DATE': 'return_Q',
                                  'SLS_LVL_2_ID': 'abm',
                                  'VARIETY_NAME': 'hybrid'})
    
    # read in the 2021 and 2022 sales
    sales_21 = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2021_D1MS.csv')
    sales_21['year'] = '2021'
    sales_22 = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2022_D1MS.csv')
    sales_22['year'] = '2022'
    
    # concat the two together
    sales_D1MS = pd.concat([sales_21, sales_22]).reset_index(drop=True)
    
    abm_map_D1MS = sales_D1MS.loc[sales_D1MS['BRAND_FAMILY_DESCR'] == 'CHANNEL',
                                  ['FIPS', 'SLS_LVL_2_ID']].reset_index(drop=True)
    abm_map_D1MS = abm_map_D1MS.rename(columns={'SLS_LVL_2_ID': 'new_id'})
    
    abm_map_D1MS = abm_map_D1MS.dropna()
    abm_map_D1MS = abm_map_D1MS.drop_duplicates().reset_index(drop=True)
    abm_map_D1MS['FIPS'] = abm_map_D1MS['FIPS'].astype(float)
    abm_map_merged = abm_map.merge(abm_map_D1MS, on=['FIPS'])
        
    return sales, abm_map_merged
    
channel_sales, abm_map = read_channel_sales(crop='CORN')

insu_map = pd.read_csv('../../NA-soy-pricing/data/county_fips_zone_Tyler_20230623.csv')
insu_map = insu_map[['fips_code', 'ABM_team_CHANNEL']].drop_duplicates().reset_index(drop=True).rename(
        columns={'fips_code': 'FIPS',
                 'ABM_team_CHANNEL': 'insu_ABM'})

abm_map = abm_map.drop_duplicates().reset_index(drop=True)    

abm_map_w_insu = abm_map.merge(insu_map, on=['FIPS']).drop_duplicates().reset_index(drop=True)

abm_map['num_rows_for_fips'] = 1
for fips_code in abm_map['FIPS'].unique():
    single_fips = abm_map[abm_map['FIPS'] == fips_code]
    len_fips = len(single_fips)
    if len_fips > 1:
        abm_map.loc[abm_map['FIPS'] == fips_code, 'num_rows_for_fips'] = len_fips

abm_map_w_insu['num_rows_for_fips'] = 1
for fips_code in abm_map_w_insu['FIPS'].unique():
    single_fips = abm_map_w_insu[abm_map_w_insu['FIPS'] == fips_code]
    len_fips = len(single_fips)
    if len_fips > 1:
        abm_map_w_insu.loc[abm_map_w_insu['FIPS'] == fips_code, 'num_rows_for_fips'] = len_fips

abm_map.to_csv('merged_channel_abm_map.csv', index=False)

abm_map_w_insu.to_csv('merged_channel_abm_map_insu.csv', index=False)

for num_rows in sorted(abm_map['num_rows_for_fips'].unique()):
    abm_map_count = abm_map[
            abm_map['num_rows_for_fips'] == num_rows].reset_index(drop=True)
    print('Number of counties with ' + str(num_rows) + ' rows: ' + str(len(abm_map_count['FIPS'].unique())))
    print('Percentage of counties with ' + str(num_rows) + ' rows: ' + str(
            np.round(100 * len(abm_map_count['FIPS'].unique()) / len(abm_map['FIPS'].unique()), 2)))
    
    
for num_rows in sorted(abm_map_w_insu['num_rows_for_fips'].unique()):
    abm_map_count = abm_map_w_insu[
            abm_map_w_insu['num_rows_for_fips'] == num_rows].reset_index(drop=True)
    print('Number of counties with ' + str(num_rows) + ' rows: ' + str(len(abm_map_count['FIPS'].unique())))
    print('Percentage of counties with ' + str(num_rows) + ' rows: ' + str(
            np.round(100 * len(abm_map_count['FIPS'].unique()) / len(abm_map_w_insu['FIPS'].unique()), 2)))