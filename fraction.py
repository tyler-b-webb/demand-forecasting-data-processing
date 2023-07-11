#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 12:50:49 2021

@author: epnzv
"""

import pandas as pd 
from functools import reduce 
import numpy as np
from calendar import monthrange
import datetime as dt
from pandasql import sqldf

from aggregation_config import(ABM_FIPS_MAP, ABM_TABLE, BLIZZARD_DIR, CM_DIR,
                               DATA_DIR, H2H_DIR, HISTORICAL_SRP, OLD_2020,
                               SALES_2021, SALES_DIR, YIELD_COUNTY_DATA)


###### --------------------- Read ABM & Teamkey Map  ------------------- ######
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
                      'REPLANT_QTY_TO_DATE': 'replant_Q'}
    Sale_2012_2020 = Sale_2012_2020.rename(columns = SALES_COLUMN_NAMES)
    
    # select the soybean crop 
    Sale_2012_2020 = Sale_2012_2020[Sale_2012_2020['crop'] == 'SOYBEAN']
    
    # set year as str
    Sale_2012_2020['year'] = Sale_2012_2020['year'].astype(str)
    
    # create a clean sales data for monthly calculation
    df_clean_sale = Sale_2012_2020.copy()
    
    # drop unnecessary columns
    SALES_COLUMNS_TO_DROP_Monthly = ['BRAND_FAMILY_DESCR', 
                         'DEALER_ACCOUNT_CY_BRAND_FAMILY',
                         'SHIPPING_STATE_CODE', 'SHIPPING_COUNTY',
                         'SHIPPING_FIPS_CODE', 'SLS_LVL_1_ID', 'CUST_ID',
                         'ACCT_ID', 'NET_SHIPPED_QTY_TO_DATE']
    df_clean_sale = df_clean_sale.drop(columns = SALES_COLUMNS_TO_DROP_Monthly)
    
    # drop unnecessary columns for yearly sales
    SALES_COLUMNS_TO_DROP_Yearly = ['BRAND_FAMILY_DESCR', 'EFFECTIVE_DATE',
                         'DEALER_ACCOUNT_CY_BRAND_FAMILY',
                         'SHIPPING_STATE_CODE', 'SHIPPING_COUNTY',
                         'SHIPPING_FIPS_CODE', 'SLS_LVL_1_ID', 'CUST_ID',
                         'ACCT_ID', 'NET_SHIPPED_QTY_TO_DATE']
    Sale_2012_2020 = Sale_2012_2020.drop(columns = SALES_COLUMNS_TO_DROP_Yearly)
    
    # reorder the columns
    Sale_2012_2020 = Sale_2012_2020[['year', 'abm', 'Variety_Name', 
                                    'nets_Q', 'order_Q', 'return_Q','replant_Q']]
    
    Sale_2012_2020 = Sale_2012_2020.groupby(by = ['year','Variety_Name','abm'],as_index=False).sum()
    
    # concatenate the 2021 data
    Sale_2012_2021 = merge_2021_sales_data(df=Sale_2012_2020)
    
    return Sale_2012_2020, df_clean_sale


def merge_2021_sales_data(df):
    """Merges the 2021 sales data.
    
    Keyword arguments:
        df -- the dataframe to concat onto
    Returns:
        df_merged
    """
    # read in the 2021 sales data
    sales_2021 = pd.read_csv(DATA_DIR + SALES_2021)
    
    # grab relevant columns
    
    
    # rename the columns
    
    # concatenate with the main dataframe
    
    return None


def create_lagged_sales(df):
    """Creates the "lagged" sales features, namely the sales data for a product
    from the two previous years in a given ABM.
    
    Keyword arguments:
        df -- the dataframe with the cleaned sales data that will be used to 
            create the lagged features
    Returns:
        df_with_lag -- the dataframe with the lagged sales features added
    """
    print('Creating lagged features...')
    
    LAGGED_FEATURES = ['nets_Q_1', 'order_Q_1', 'return_Q_1', 'replant_Q_1',
                   'nets_Q_2', 'order_Q_2', 'return_Q_2']
    
    df = df.rename(columns = {'Variety_Name':'hybrid'})
    df['year'] = df['year'].astype(int)
    sales_all = df.copy()


    # define the selection criteria as strings to use in the sqldf commmand
    # this is principally just for readability
    current_year_q = "select a.year, a.abm, a.hybrid, a.nets_Q, a.order_Q, a.return_Q, a.replant_Q, "
    last_year_q = "b.nets_Q as nets_Q_1, b.order_Q as order_Q_1, b.return_Q as return_Q_1, b.replant_Q as replant_Q_1, "
    two_years_q = "c.nets_Q as nets_Q_2, c.order_Q as order_Q_2, c.return_Q as return_Q_2 from sales_all "
    join_last_year = "a left join sales_all b on a.hybrid = b.hybrid and a.abm = b.abm and a.year = b.year + 1 "
    join_two_years = "left join sales_all c on a.hybrid = c.hybrid and a.abm = c.abm and a.year = c.year + 2"
 
    sales_with_lag = sqldf(current_year_q + last_year_q + two_years_q + 
                           join_last_year + join_two_years)
    
    # impute, replacing the NaNs with zeros
    for feature in LAGGED_FEATURES:
        sales_with_lag[feature] = sales_with_lag[feature].fillna(0)
    
    # convert year to str
    sales_with_lag['year'] = sales_with_lag['year'].astype(str)
    
    # rename hybrid columns 
    sales_with_lag = sales_with_lag.rename(columns = {'hybrid':'Variety_Name'})
    
    # grabs data after the cutoff year
    sales_with_lag = sales_with_lag[sales_with_lag['year'] >= '2012'] 

    # drop sales data for this momennt 
    sales_with_lag = sales_with_lag.drop(columns = ['order_Q']).reset_index(drop = True)
    return sales_with_lag


def create_monthly_sales(Sale_2012_2020_lagged, clean_Sale):
    """ Create the "datemask" to get monthly feature for the netsale data
    
    Keyword arguments:
        Sale_2012_2020 -- the dataframe of the yealry sales data from 2012 to 2020 with lagged features 
        df_clean_sale -- the dataframe of clean sales data with effective date
    Returns:
        Sales_all -- the dataframe of clean sales data with montly and lagged features 
    """
    print('Creating monthly features...')

    clean_Sale = clean_Sale[['EFFECTIVE_DATE', 'year', 'abm', 'Variety_Name', 'order_Q']]
    months = [9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8]
    
    dfs_monthly_netsales = []
    years = ['2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']
    for year in years:
        print('year', year)
        df_year = clean_Sale[clean_Sale['year'] == year].reset_index(drop = True)
        
        df_monthly_total = pd.DataFrame()
        
        df_single_year = Sale_2012_2020_lagged[Sale_2012_2020_lagged['year'] == year].copy().reset_index(drop = True)
        
        df_monthly_total['year'] = df_single_year['year'].tolist()
        df_monthly_total['Variety_Name'] = df_single_year['Variety_Name'].tolist()
        df_monthly_total['abm'] = df_single_year['abm'].tolist()
    
        
        for month in months:
            print("month", month)
            if month > 8:
                date_mask = dt.datetime(year = int(year) - 1, month = month, day = monthrange(int(year) - 1, month)[1])
            
            if month <= 8:
                date_mask = dt.datetime(year = int(year), month = month, day = monthrange(int(year), month)[1])
            
            df_monthly = df_year[df_year['EFFECTIVE_DATE'] <= date_mask].copy().reset_index(drop = True)
            
            df_monthly = df_monthly.groupby(by = ['year','Variety_Name', 'abm'], as_index = False).sum().reset_index(drop = True)
            
            # rename 
            df_monthly = df_monthly.rename(columns = {'order_Q': 'order_Q_month_' + str(month)})
            
            # merge with df_montly_total 
            df_monthly_total = df_monthly_total.merge(df_monthly, on = ['year', 'Variety_Name', 'abm'], how = 'left')
        
        dfs_monthly_netsales.append(df_monthly_total)
     
    Sales_monthly = pd.concat(dfs_monthly_netsales).reset_index(drop = True)    
    Sales_monthly = Sales_monthly.fillna(0)
    
    Sale_all = Sale_2012_2020_lagged.merge(Sales_monthly, on = ['year', 'Variety_Name', 'abm'], how = 'left')
    
    return  Sale_all

Sale_2012_2020, clean_Sale = read_sales_filepath()
Sale_2012_2020_lagged = create_lagged_sales(Sale_2012_2020)
Sale_all = create_monthly_sales(Sale_2012_2020_lagged, clean_Sale)

Sale_all = Sale_all.drop(columns=['nets_Q', 'return_Q', 'return_Q', 'replant_Q',
                                  'nets_Q_1', 'order_Q_1', 'return_Q_1',
                                  'replant_Q_1', 'nets_Q_2', 'order_Q_2', 
                                  'return_Q_2'])
        
Sale_all = Sale_all.rename(columns={'order_Q_month_8': 'total_orders'})

Sale_all = Sale_all.groupby(by=['abm'], as_index=False).sum()

Sale_all['sept_frac'] = Sale_all['order_Q_month_9'] / Sale_all['total_orders']
Sale_all['oct_frac'] = Sale_all['order_Q_month_10'] / Sale_all['total_orders']
Sale_all['nov_frac'] = Sale_all['order_Q_month_11'] / Sale_all['total_orders']
Sale_all['dec_frac'] = Sale_all['order_Q_month_12'] / Sale_all['total_orders']
Sale_all['jan_frac'] = Sale_all['order_Q_month_1'] / Sale_all['total_orders']
Sale_all['feb_frac'] = Sale_all['order_Q_month_2'] / Sale_all['total_orders']
Sale_all['mar_frac'] = Sale_all['order_Q_month_3'] / Sale_all['total_orders']
Sale_all['apr_frac'] = Sale_all['order_Q_month_4'] / Sale_all['total_orders']
Sale_all['may_frac'] = Sale_all['order_Q_month_5'] / Sale_all['total_orders']
Sale_all['jun_frac'] = Sale_all['order_Q_month_6'] / Sale_all['total_orders']
Sale_all['jul_frac'] = Sale_all['order_Q_month_7'] / Sale_all['total_orders']

Sale_all = Sale_all[['abm', 'total_orders', 'sept_frac',
                     'oct_frac', 'nov_frac', 'dec_frac', 'jan_frac', 'feb_frac',
                     'mar_frac', 'apr_frac', 'may_frac', 'jun_frac', 'jul_frac']]

# drop NAs
Sale_all = Sale_all.dropna()
