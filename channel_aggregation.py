#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 15:41:17 2023

@author: epnzv
"""

import datetime as dt
import pandas as pd

DATA_DIR = '../../NA-soy-pricing/data/'
CHANNEL_DIR = 'channel/'
FARMGATE_DIR = 'from_larry_swift/'

CHANNEL_ABM_MAP = 'channel_abm_fips_map.csv'

CHANNEL_SALES_YEARS = [2009, 2010, 2011, 2012, 2013, 2014, 2015,
                       2016, 2017, 2018, 2019, 2020]

EFFECTIVE_DATE = {'month': 8,
                  'day': 31}

abm_map = pd.read_csv(CHANNEL_ABM_MAP)

# read in all sales data to see if we have any unknown abms now
def read_channel_sales(crop, abm_map):
    """Reads in Channel sales data.
    
    Keyword Arguments:
        crop -- the crop we want to get the data for
    Returns:
        sales -- the dataframe of the concatenated sales data.
    """
    # set up a df 
    dfs = []
    
    for year in CHANNEL_SALES_YEARS:
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
    
    # read in the 2021 and 2022 sales
    print('Reading 2021 sales data...')
    sales_21_raw = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2021_D1MS.csv')
    sales_21 = D1MS_dt_format(df=sales_21_raw)
    sales_21['year'] = 2021
    sales_21['EFFECTIVE_DATE'] = pd.to_datetime(sales_21['EFFECTIVE_DATE'])
    
    print('Reading 2022 sales data...')
    sales_22_raw = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2022_D1MS.csv')
    sales_22 = D1MS_dt_format(df=sales_22_raw)
    sales_22['year'] = 2022
    sales_22['EFFECTIVE_DATE'] = pd.to_datetime(sales_22['EFFECTIVE_DATE'])
    
    # concat the two together
    sales_D1MS = pd.concat([sales_21, sales_22]).reset_index(drop=True)
    
    # grab channel and the relevant crop
    sales_D1MS = sales_D1MS[sales_D1MS['BRAND_FAMILY_DESCR'] == 'CHANNEL'].reset_index(drop=True)
    sales_D1MS = sales_D1MS[sales_D1MS['SPECIE_DESCR'] == crop].reset_index(drop=True)
    
    # grab relevant columns
    sales_D1MS_subset = sales_D1MS[['year', 'EFFECTIVE_DATE', 'FIPS', 'VARIETY_NAME', 'SUM(NET_SALES_QTY_TO_DATE)',
                             'SUM(ORDER_QTY_TO_DATE)', 'SUM(NET_SHIPPED_QTY_TO_DATE)',
                             'SUM(RETURN_QTY_TO_DATE)', 'SUM(REPLANT_QTY_TO_DATE)']]
    
    sales_D1MS_subset = sales_D1MS_subset.rename(columns={'SUM(NET_SALES_QTY_TO_DATE)': 'nets_Q',
                                  'SUM(NET_SHIPPED_QTY_TO_DATE)': 'shipped_Q',
                                  'SUM(ORDER_QTY_TO_DATE)': 'order_Q',
                                  'SUM(REPLANT_QTY_TO_DATE)': 'replant_Q',
                                  'SUM(RETURN_QTY_TO_DATE)': 'return_Q',
                                  'FIPS': 'fips',
                                  'VARIETY_NAME': 'hybrid'})
    
    # remove unknown fips
    sales_D1MS_subset = sales_D1MS_subset[sales_D1MS_subset['fips'] != 'UNKNOWN'].reset_index(drop=True)
    sales_D1MS_subset = sales_D1MS_subset[sales_D1MS_subset['fips'].isna() == False].reset_index(drop=True)
    sales_D1MS_subset['fips'] = sales_D1MS_subset['fips'].astype(int)
    
    sales_all = pd.concat([sales, sales_D1MS_subset]).reset_index(drop=True)
    
    # merge with ABM map and drop the fips column
    sales_all = sales_all.merge(abm_map, on=['fips'], how='left').drop(columns=['fips'])
    
    sales_abm_level = pd.DataFrame()
    
    # create to_date order features and aggregate to abm level
    for year in sales_all['year'].unique():
        if EFFECTIVE_DATE['month'] > 8:
            date_mask = dt.datetime(year=year - 1,
                                    month=EFFECTIVE_DATE['month'],
                                    day=EFFECTIVE_DATE['day'])
        else:
            date_mask = dt.datetime(year=year,
                                    month=EFFECTIVE_DATE['month'],
                                    day=EFFECTIVE_DATE['day'])
        
        single_year = sales_all[sales_all['year'] == year].reset_index(drop=True)
        
        single_year_to_date = single_year[single_year['EFFECTIVE_DATE'] <= date_mask].reset_index(drop=True)
        single_year_to_date_subset = single_year_to_date[['year', 'abm', 'hybrid', 'order_Q']]
        single_year_to_date_subset = single_year_to_date_subset.groupby(by=['year', 'abm', 'hybrid'], as_index=False).sum().rename(
                        columns={'order_Q': 'orders_to_date'})
        
        single_year = single_year.groupby(by=['year', 'abm', 'hybrid'], as_index=False).sum()
        
        single_year_with_to_date = single_year.merge(single_year_to_date_subset,
                                                     on=['year', 'abm', 'hybrid'],
                                                     how='left')
        
        if sales_abm_level.empty == True:
            sales_abm_level = single_year_with_to_date.copy()
        else:
            sales_abm_level = pd.concat([sales_abm_level, single_year_with_to_date])
            
        # create the lagged sales quantities
        sales_w_lag = create_lagged_sales(df=sales_abm_level)
                
    return sales_w_lag


def D1MS_dt_format(df):
    """Changes the format of the effective date for the D1MS data.
    
    Keyword Arguments:
        df -- the D1MS data
    Returns: 
        df_new_format -- the dataframe with a new effective date format
    """
    df_new_dt = df.copy()
    df_new_dt['EFFECTIVE_DATE'] = df_new_dt['EFFECTIVE_DATE'].astype(str)
    
    # pull out the year, month, and day
    df_new_dt['ed_year'] = df_new_dt['EFFECTIVE_DATE'].str[0:4]
    df_new_dt['ed_month'] = df_new_dt['EFFECTIVE_DATE'].str[4:6]
    df_new_dt['ed_day'] = df_new_dt['EFFECTIVE_DATE'].str[6:8]

    df_new_dt['EFFECTIVE_DATE'] = (
            df_new_dt['ed_month'] + '/' +  df_new_dt['ed_month'] + '/' + df_new_dt['ed_year'])
    
    df_new_dt = df_new_dt.drop(columns=['ed_year', 'ed_month', 'ed_day'])
    
    return df_new_dt


def create_lagged_sales(df):
    """Creates lagged sales and merges them back onto the df.
    
    Keyword Arguments:
        df -- the sales dataframe
    Returns: 
        df_w_lag -- the dataframe with lagged sales data
    """
    df_copy_1 = df.copy()
    df_copy_2 = df.copy()
    
    # add to the year values and rename columns
    df_copy_1['year'] = df_copy_1['year'] + 1
    df_copy_2['year'] = df_copy_2['year'] + 2
    
    df_copy_1 = df_copy_1.rename(columns={'nets_Q': 'nets_Q_1',
                                          'shipped_Q': 'shipped_Q_1',
                                          'order_Q': 'order_Q_1',
                                          'orders_to_date': 'orders_to_date_1',
                                          'return_Q': 'return_Q_1',
                                          'replant_Q': 'replant_Q_1'})
    
    df_copy_2 = df_copy_2.rename(columns={'nets_Q': 'nets_Q_2',
                                          'shipped_Q': 'shipped_Q_2',
                                          'order_Q': 'order_Q_2',
                                          'orders_to_date': 'orders_to_date_2',
                                          'return_Q': 'return_Q_2',
                                          'replant_Q': 'replant_Q_2'})
    
    df_w_lag = df.merge(df_copy_1, on=['year', 'abm', 'hybrid'], how='left')
    df_w_lag = df_w_lag.merge(df_copy_2, on=['year', 'abm', 'hybrid'], how='left')
    
    # fill missing lagged features with 0
    df_w_lag = df_w_lag.fillna(0)
    
    return df_w_lag

channel_sales = read_channel_sales(crop='SOYBEAN', abm_map=abm_map)

