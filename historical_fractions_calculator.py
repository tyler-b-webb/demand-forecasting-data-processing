#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  8 10:10:08 2022

@author: epnzv
"""

import numpy as np
import pandas as pd
import datetime as dt
from calendar import monthrange 
from aggregation_config import (ABM_TABLE, DATA_DIR, SALES_DIR)


def read_abm_teamkey_file():
    """ Reads in and returns the abm and teamkey data as a dataframe.
    
    Keyword arguments:
        None
    Returns:
        ABM_TEAMKEY -- the dataframe of mapping teamkey to abm 
    """
    
    # read in data
    abm_Teamkey_Address = DATA_DIR + ABM_TABLE
    abm_Teamkey = pd.read_csv(abm_Teamkey_Address)
    
    # rename columns 
    abm_Teamkey = abm_Teamkey.rename(columns = {'Old Area ID':'abm', 'New Area ID':'TEAM_KEY'})
    
    # selcted required columns 
    abm_Teamkey = abm_Teamkey[['abm','TEAM_KEY']]
    return abm_Teamkey


abm_Teamkey = read_abm_teamkey_file()

dfs_path = []
    
# read in the data by year 
for year in range(2012, 2021):
    print("Read ", str(year), " Sales Data")
    dfi_path = DATA_DIR + SALES_DIR + str(year) + '.csv'
    dfi = pd.read_csv(dfi_path)
        
    dfi = dfi[dfi['SPECIE_DESCR'] == 'SOYBEAN'].reset_index(drop=True)
        
    # set a year parameter to be the year 
    dfi['year'] = year
        
    # convert the effective date to a datetime format in order to set the mask 
    dfi['EFFECTIVE_DATE'] = pd.to_datetime(dfi['EFFECTIVE_DATE'])
        
    # set 2020 abms to old format
    if year == 2020:
        dfi = dfi.rename(columns = {'SLS_LVL_2_ID':"TEAM_KEY"})
        dfi = dfi.merge(abm_Teamkey, how = 'left', on = ['TEAM_KEY'])
        dfi = dfi.rename(columns = {'abm':'SLS_LVL_2_ID'})
        dfi = dfi.drop(columns=['TEAM_KEY'])

        
    # add modified dataframe to the list 
    dfs_path.append(dfi)
     
# concate all dataframes  
Sale_2012_2020 = pd.concat(dfs_path).reset_index(drop=True)
    
# rename the columns 
SALES_COLUMN_NAMES = {'year': 'year', 'SPECIE_DESCR': 'crop',
                      'SLS_LVL_2_ID': 'abm', 'VARIETY_NAME': 'Variety_Name',
                      'NET_SALES_QTY_TO_DATE': 'nets_Q',
                      'ORDER_QTY_TO_DATE': 'order_Q',
                      'RETURN_QTY_TO_DATE': 'return_Q',
                      'REPLANT_QTY_TO_DATE': 'replant_Q'}
    
Sale_2012_2020 = Sale_2012_2020.rename(columns = SALES_COLUMN_NAMES)
    
# select the soybean crop 
Sale_2012_2020 = Sale_2012_2020[
        Sale_2012_2020['BRAND_FAMILY_DESCR'] == 'NATIONAL'].reset_index(drop=True)
    
# set year as str
Sale_2012_2020['year'] = Sale_2012_2020['year'].astype(str)
    
# create a clean sales data for monthly calculation
df_clean_sale = Sale_2012_2020.copy()
    
# drop unnecessary columns
SALES_COLUMNS_TO_DROP_Monthly = ['BRAND_FAMILY_DESCR', 
                                 'DEALER_ACCOUNT_CY_BRAND_FAMILY',
                                 'SHIPPING_STATE_CODE', 'SHIPPING_COUNTY',
                                 'SHIPPING_FIPS_CODE', 'SLS_LVL_1_ID', 'CUST_ID',
                                 'ACCT_ID', 'NET_SHIPPED_QTY_TO_DATE', 
                                 'Variety_Name', 'crop']
    
df_clean_sale = df_clean_sale.drop(columns=SALES_COLUMNS_TO_DROP_Monthly)
    
months=[9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8]

df_clean_sale['month'] = pd.DatetimeIndex(df_clean_sale['EFFECTIVE_DATE']).month
df_clean_sale['day'] = pd.DatetimeIndex(df_clean_sale['EFFECTIVE_DATE']).day

df_clean_sale = df_clean_sale[['abm', 'month', 'day', 'order_Q', 'nets_Q',
                               'return_Q', 'replant_Q']]

df_clean_sale = df_clean_sale.dropna().reset_index(drop=True)

fractions_df = pd.DataFrame(columns=['abm', 'month', 'day', 'order_fraction', 
                                     'nets_fraction', 'return_fraction',
                                     'replant_fraction'])

abm_col = []
month_col = []
day_col = []
for abm in df_clean_sale['abm'].unique():
    for month in months:
        for day in range(1, monthrange(2020, month)[1] + 1):
            abm_col.append(abm)
            month_col.append(month)
            day_col.append(day)
            
fractions_df['abm'] = abm_col
fractions_df['month'] = month_col
fractions_df['day'] = day_col


quants = ['order', 'nets', 'return', 'replant']


for quantity in quants:
    percent_done = 0
    for abm in fractions_df['abm'].unique():
        
        feature = quantity + '_Q'
        fraction = quantity + '_fraction'
        
        print(str(percent_done / len(fractions_df)) + ' % done with ' + quantity)
        single_abm = df_clean_sale[df_clean_sale['abm'] == abm].reset_index(drop=True)
        single_abm_total = np.sum(single_abm[feature])
    
        for i in range(0, len(months)):
            this_month = months[i]
            prior_months = months[:i]
        
            quantity_this_month = single_abm[single_abm['month'] == this_month]
            if len(prior_months) != 0:
                quantity_prior_months = single_abm[
                        single_abm['month'].isin(prior_months)]
                prior_quantity_total = np.sum(quantity_prior_months[feature])
            else:
                prior_quantity_total = 0
        
            days_in_month = [*range(1, monthrange(2020, this_month)[1]+1, 1)]
        
            for j in range(0, len(days_in_month)):
                today = days_in_month[j]
                days_before_today = days_in_month[:j]
            
                quantity_today = quantity_this_month[quantity_this_month['day'] == today]
                quantity_before_today = quantity_this_month[
                        quantity_this_month['day'].isin(days_before_today)]
            
                if len(quantity_before_today) != 0:
                    quantity_before_today_total = np.sum(quantity_before_today[feature])
                else:
                    quantity_before_today_total = 0
                
                quantity_today_total = np.sum(quantity_today[feature])
            
                quantity_to_date = prior_quantity_total + quantity_before_today_total + quantity_today_total
            
                fractions_df.loc[
                        (fractions_df['abm'] == abm) & 
                        (fractions_df['month'] == this_month) &
                        (fractions_df['day'] == today), fraction] = (
                            quantity_to_date / single_abm_total)
    
                percent_done += 1
    
fractions_df = fractions_df[fractions_df['abm'] != 'UNK']

imputed_abm_orders = fractions_df[fractions_df['abm'] != '9Z01'].drop(columns=['abm'])

for quantity in quants:
    imputed_abm_orders[quantity + '_fraction'] = imputed_abm_orders[quantity + '_fraction'].astype(float)

imputed_abm_orders = imputed_abm_orders.groupby(by=['month', 'day'], as_index=False).mean()

for quantity in quants:
    fractions_df.loc[fractions_df['abm'] == '9Z01', quantity + '_fraction'] = imputed_abm_orders[
            quantity + '_fraction'].values
    
    fractions_df[quantity + '_fraction'] = fractions_df[quantity + '_fraction'].astype(float)

fractions_df.to_csv('historical_daily_fractions_all_features.csv', index=False)