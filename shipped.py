#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 12 11:52:37 2021

@author: epnzv
"""
import pandas as pd 
from functools import reduce 
import numpy as np
from calendar import monthrange
import datetime as dt
from pandasql import sqldf

from aggregation_config import(ABM_FIPS_MAP, ABM_TABLE, BIG_CF_FILE, BLIZZARD_DIR, CM_DIR,
                               DATA_DIR, H2H_DIR, HISTORICAL_SRP,
                               MONTHLY_FRACTIONS, OLD_2020, SALES_2021, SALES_2022,
                               SALES_2021_W_DATE, SALES_DIR, YIELD_COUNTY_DATA)


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

###### ---------------------- Read Sales Data ------------------------ ######
def Preprocess_2020_sale():
    df2020_path = DATA_DIR + SALES_DIR + OLD_2020
    dfsale_2020 = pd.read_csv(df2020_path)
    dfsale_2020 = dfsale_2020.rename(columns = {'SLS_LVL_2_ID':"TEAM_KEY"})
    dfsale_2020 = dfsale_2020.merge(abm_Teamkey, how = 'left', on = ['TEAM_KEY'])
    dfsale_2020 = dfsale_2020.rename(columns = {'abm':'SLS_LVL_2_ID'})
    
    df2020_path_new = DATA_DIR + SALES_DIR + '2020.csv'
    
    dfsale_2020.to_csv(df2020_path_new, index = False)
    "Finish Proprecssing 2020 sales Data"
  
    
def read_sales_filepath():
    """ Reads in and returns the sales data as a dataframe.
    
    Keyword arguments:
        None
    Returns:
        Sale_2012_2020 -- the dataframe of the yealry sales data from 2012 to 2020
        df_clean_sale -- the dataframe of clean sales data with effective date
    """
    # preprocess sales 2020 sales data to get consistent abm data  
    Preprocess_2020_sale()

    # define a list to store all sales data 
    dfs_path = []
    
    # read in the data by year 
    for year in range(2008, 2021):
        print("Read ", str(year), " Sales Data")
        dfi_path = DATA_DIR + SALES_DIR + str(year) + '.csv'
        dfi = pd.read_csv(dfi_path)
        
        # set a year parameter to be the year 
        dfi['year'] = year
        
        # convert the effective date to a datetime format in order to set the mask 
        dfi['EFFECTIVE_DATE'] = pd.to_datetime(dfi['EFFECTIVE_DATE'])
        
        # add modified dataframe to the list 
        dfs_path.append(dfi)
      
    # concate all dataframes  
    Sale_2012_2020 = pd.concat(dfs_path).reset_index(drop = True)
    
    # rename the columns 
    SALES_COLUMN_NAMES = {'year': 'year', 'SPECIE_DESCR': 'crop',
                      'SLS_LVL_2_ID': 'abm', 'VARIETY_NAME': 'Variety_Name',
                      'NET_SALES_QTY_TO_DATE': 'nets_Q',
                      'ORDER_QTY_TO_DATE': 'order_Q',
                      'RETURN_QTY_TO_DATE': 'return_Q',
                      'REPLANT_QTY_TO_DATE': 'replant_Q',
                      'NET_SHIPPED_QTY_TO_DATE': 'shipped_Q'}
    Sale_2012_2020 = Sale_2012_2020.rename(columns = SALES_COLUMN_NAMES)
    
    # select the soybean crop 
    Sale_2012_2020 = Sale_2012_2020[
            Sale_2012_2020['crop'] == 'SOYBEAN'].reset_index(drop=True)
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
                         'ACCT_ID']
    df_clean_sale = df_clean_sale.drop(columns = SALES_COLUMNS_TO_DROP_Monthly)
    
    # drop unnecessary columns for yearly sales
    SALES_COLUMNS_TO_DROP_Yearly = ['BRAND_FAMILY_DESCR', 'EFFECTIVE_DATE',
                         'DEALER_ACCOUNT_CY_BRAND_FAMILY',
                         'SHIPPING_STATE_CODE', 'SHIPPING_COUNTY',
                         'SHIPPING_FIPS_CODE', 'SLS_LVL_1_ID', 'CUST_ID',
                         'ACCT_ID']
    Sale_2012_2020 = Sale_2012_2020.drop(columns = SALES_COLUMNS_TO_DROP_Yearly)
    
    # reorder the columns
    Sale_2012_2020 = Sale_2012_2020[['year', 'abm', 'Variety_Name', 
                                    'nets_Q', 'order_Q', 'return_Q','replant_Q']]
    
    Sale_2012_2020 = Sale_2012_2020.groupby(by = ['year','Variety_Name','abm'],as_index=False).sum()
    
    
    return Sale_2012_2020, df_clean_sale

Sale_2012_2020, clean_Sale = read_sales_filepath()
