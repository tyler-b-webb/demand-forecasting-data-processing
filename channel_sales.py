#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 15:34:14 2023

@author: epnzv
"""

import datetime as dt
import pandas as pd

from channel_config import (CHANNEL_DATE_RATIOS, CHANNEL_DIR, CHANNEL_SALES_YEARS,
                            CORN_CF_DATA, CURRENT_BANK, DATA_DIR, EFFECTIVE_DATE,
                            FORECAST_COLS, FORECAST_CY_COLS, SOYBEAN_CF_DATA)


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


def read_forecasts(df, crop):
    """Reads in the consensus forecast data.
    
    Keyword arguments:
        df -- the dataframe we're merging
        crop -- the crop we're reading the data for
    Returns:
        df_forecasts -- the df with forecasts for the crop and brand we're
        interested in
    """
    if crop == 'CORN':
        forecasts_all_brands = pd.read_excel(DATA_DIR + CORN_CF_DATA)
    elif crop == 'SOYBEAN':
        forecasts_all_brands = pd.read_excel(DATA_DIR + SOYBEAN_CF_DATA)
    
    forecasts_one_brand = forecasts_all_brands[
            forecasts_all_brands['BRAND_GROUP'] == 'CHANNEL'].reset_index(drop=True)
    
    forecasts_selected_cols = forecasts_one_brand[FORECAST_COLS].rename(columns={
            'FORECAST_YEAR': 'year',
            'TEAM_KEY': 'abm',
            'ACRONYM_NAME': 'hybrid'})
    
    forecasts_selected_cols['year'] = forecasts_selected_cols['year'] + 1
    
    forecasts_selected_cols = forecasts_selected_cols.dropna()
    
    forecasts_cy_selected_cols = forecasts_one_brand[FORECAST_CY_COLS].rename(columns={
            'FORECAST_YEAR': 'year',
            'TEAM_KEY': 'abm',
            'ACRONYM_NAME': 'hybrid'})
    
    forecasts_cy_selected_cols = forecasts_cy_selected_cols.dropna()
    
    # grab columns where there are indeed forecasts
    forecasts_selected_cols = forecasts_selected_cols[
            forecasts_selected_cols['TEAM_Y1_FCST_2'] != 0].reset_index(drop=True)
    
    forecasts_selected_cols = forecasts_selected_cols.merge(forecasts_cy_selected_cols,
                                                            on=['year', 'abm', 'hybrid'],
                                                            how='left')
    
    df_forecasts = df.merge(forecasts_selected_cols,
                            on=['year', 'abm', 'hybrid'],
                            how='outer').fillna(0)
    
    return df_forecasts


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
            
            
    # add in the current order bank data
    current_order_bank = pd.read_csv(
            DATA_DIR + CHANNEL_DIR + 'channel_24only_' + str.lower(crop) + 
            '_' + str(EFFECTIVE_DATE['month']) + '_' + str(EFFECTIVE_DATE['day']) + 
            '.csv')
    
    # subset out 
    current_order_bank_subset = current_order_bank[
            ['Year', 'ABM.ID', 'VARIETY', 'PRODUCT_QTY']]
    
    current_order_bank_subset = current_order_bank_subset.rename(
            columns={'Year': 'year',
                     'ABM.ID': 'abm',
                     'VARIETY': 'hybrid',
                     'PRODUCT_QTY': 'orders_to_date'})
    
    current_order_bank_summed = current_order_bank_subset.groupby(
            by=['year', 'abm', 'hybrid'], as_index=False).sum()
    
    # set 0 values for other quantities
    quantities = ['nets_Q', 'shipped_Q', 'order_Q', 'replant_Q', 'return_Q']
    
    for qty in quantities:
        current_order_bank_summed[qty] = 0
        
    # concat onto main df
    sales_abm_level_cy_sales = pd.concat([sales_abm_level, current_order_bank_summed])
    
    # create the lagged sales quantities
    sales_w_lag = create_lagged_sales(df=sales_abm_level_cy_sales)
                
    return sales_w_lag


def read_channel_sales_imputeD1MS(crop, abm_map):
    """Reads in Channel sales data and imputes the D1MS based on a daily
    abm/quantity level map.
    
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
    
    # merge with ABM map and drop the fips column
    sales = sales.merge(abm_map, on=['fips'], how='left').drop(columns=['fips'])
    
    sales_abm_level = pd.DataFrame()
    
    # create to_date order features and aggregate to abm level
    for year in sales['year'].unique():
        if EFFECTIVE_DATE['month'] > 8:
            date_mask = dt.datetime(year=year - 1,
                                    month=EFFECTIVE_DATE['month'],
                                    day=EFFECTIVE_DATE['day'])
        else:
            date_mask = dt.datetime(year=year,
                                    month=EFFECTIVE_DATE['month'],
                                    day=EFFECTIVE_DATE['day'])
        
        single_year = sales[sales['year'] == year].reset_index(drop=True)
        
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
            
            
    # read in the 2021 and 2022 sales
    print('Reading 2021 sales data...')
    sales_21 = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2021_D1MS.csv')
    sales_21['year'] = 2021
    
    print('Reading 2022 sales data...')
    sales_22 = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2022_D1MS.csv')
    sales_22['year'] = 2022
    
    print('Reading 2023 sales data...')
    sales_23 = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2023_D1MS.csv')
    sales_23['year'] = 2023
    
    # concat the three together
    sales_D1MS = pd.concat([sales_21, sales_22]).reset_index(drop=True)
    sales_D1MS = pd.concat([sales_D1MS, sales_23]).reset_index(drop=True)
        
    # read in the date map
    date_ratios = pd.read_csv('channel_abm_date_ratios_' + crop + '.csv')
    
    this_date = date_ratios[date_ratios['month'] == EFFECTIVE_DATE['month']].reset_index(drop=True)
    this_date = this_date[this_date['day'] == EFFECTIVE_DATE['day']].reset_index(drop=True)
    
    # grab the order ratio and the abm columns
    this_date = this_date[['abm', 'order_Q_ratio']]
    
    # grab channel and the relevant crop
    sales_D1MS = sales_D1MS[sales_D1MS['BRAND_FAMILY_DESCR'] == 'CHANNEL'].reset_index(drop=True)
    sales_D1MS = sales_D1MS[sales_D1MS['SPECIE_DESCR'] == crop].reset_index(drop=True)
    
    # grab relevant columns
    sales_D1MS_subset = sales_D1MS[['year', 'FIPS', 'VARIETY_NAME', 'SUM(NET_SALES_QTY_TO_DATE)',
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
    
    # merge the abm map
    sales_D1MS_abm = sales_D1MS_subset.merge(abm_map, on=['fips'], how='left').drop(columns=['fips'])

    # group by year, abm and hybrid 
    sales_D1MS_abm = sales_D1MS_abm.groupby(by=['year', 'abm', 'hybrid'], as_index=False).sum()
    
    # calculate orders to date feature
    sales_D1MS_abm = sales_D1MS_abm.merge(this_date, on=['abm'], how='left')
    sales_D1MS_abm['orders_to_date'] = sales_D1MS_abm['order_Q'] * sales_D1MS_abm['order_Q_ratio']
    
    sales_D1MS_abm = sales_D1MS_abm.drop(columns=['order_Q_ratio'])
    
    sales_all = pd.concat([sales_abm_level, sales_D1MS_abm]).reset_index(drop=True)
    
    # drop unknown ABMs
    sales_all = sales_all[sales_all['abm'] != 'UNK'].reset_index(drop=True)
    sales_all = sales_all[sales_all['abm'].isna() == False].reset_index(drop=True)
        
    # COMPASS DATA
    if crop == 'CORN':
        year_var = 'Year'
    elif crop == 'SOYBEAN':
        year_var = 'MKT_YR'
    
    if CURRENT_BANK == True:  
        # add in the current order bank data
        current_order_bank = pd.read_csv(
                DATA_DIR + CHANNEL_DIR + 'channel_24only_' + str.lower(crop) + 
                '_' + str(EFFECTIVE_DATE['month']) + '_' + str(EFFECTIVE_DATE['day']) + 
                '.csv')
    
        # subset out 
        current_order_bank_subset = current_order_bank[
                [year_var, 'ABM.ID', 'VARIETY', 'PRODUCT_QTY']]
    
        current_order_bank_subset = current_order_bank_subset.rename(
                columns={year_var: 'year',
                         'ABM.ID': 'abm',
                         'VARIETY': 'hybrid',
                         'PRODUCT_QTY': 'orders_to_date'})
    
        current_order_bank_summed = current_order_bank_subset.groupby(
                by=['year', 'abm', 'hybrid'], as_index=False).sum()
    
        # set 0 values for other quantities
        quantities = ['nets_Q', 'shipped_Q', 'order_Q', 'replant_Q', 'return_Q']
    
        for qty in quantities:
            current_order_bank_summed[qty] = 0
        
        # concat onto main df
        sales_abm_level_cy_sales = pd.concat([sales_all, current_order_bank_summed])
    
    else:
        sales_abm_level_cy_sales = sales_all.copy()
        
    # create the lagged sales quantities
    sales_w_lag = create_lagged_sales(df=sales_abm_level_cy_sales)
                
    return sales_w_lag