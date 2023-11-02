#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 09:39:38 2023

@author: epnzv
"""

import datetime as dt
import pandas as pd
from aggregation_config import(ABM_TABLE, DAILY_FRACTIONS, DATA_DIR, ORDER_DATE,
                               SALES_2021, SALES_2022, SALES_DIR)


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
    
    LAGGED_FEATURES = ['order_Q_1', 'order_Q_2', 'order_Q_3']
    
    df = df.rename(columns = {'Variety_Name':'hybrid'})
    df['year'] = df['year'].astype(int)
    sales_all = df.copy()
    # define the selection criteria as strings to use in the sqldf commmand
    # this is principally just for readability
    #current_year_q = "select a.year, a.abm, a.hybrid, a.nets_Q, a.order_Q, a.return_Q, a.replant_Q, "
    #last_year_q = "b.nets_Q as nets_Q_1, b.order_Q as order_Q_1, b.return_Q as return_Q_1, b.replant_Q as replant_Q_1, "
    #two_years_q = "c.nets_Q as nets_Q_2, c.order_Q as order_Q_2, c.return_Q as return_Q_2 from sales_all "
    #join_last_year = "a left join sales_all b on a.hybrid = b.hybrid and a.abm = b.abm and a.year = b.year + 1 "
    #join_two_years = "left join sales_all c on a.hybrid = c.hybrid and a.abm = c.abm and a.year = c.year + 2"
 
    #sales_with_lag = sqldf(current_year_q + last_year_q + two_years_q + 
    #                       join_last_year + join_two_years)
    
    sales_all_lag_1 = sales_all.copy()[['year', 'abm', 'hybrid', 'orders_to_date']]
    sales_all_lag_2 = sales_all.copy()[['year', 'abm', 'hybrid', 'orders_to_date']]
    sales_all_lag_3 = sales_all.copy()[['year', 'abm', 'hybrid', 'orders_to_date']]
    
    # adjust years
    sales_all_lag_1['year'] = sales_all_lag_1['year'] + 1
    sales_all_lag_2['year'] = sales_all_lag_2['year'] + 2
    sales_all_lag_3['year'] = sales_all_lag_3['year'] + 3
    
    sales_all_lag_1 = sales_all_lag_1[
            ['year', 'hybrid', 'abm', 'orders_to_date']].rename(
            columns={'orders_to_date': 'order_Q_1'})
    sales_all_lag_2 = sales_all_lag_2[
            ['year', 'hybrid', 'abm', 'orders_to_date']].rename(
            columns={'orders_to_date': 'order_Q_2'})
    sales_all_lag_3 = sales_all_lag_3[
            ['year', 'hybrid', 'abm', 'orders_to_date']].rename(
            columns={'orders_to_date': 'order_Q_3'})
    
    sales_with_lag = sales_all.merge(sales_all_lag_1, on=['year', 'hybrid', 'abm'], how='left')
    sales_with_lag = sales_with_lag.merge(sales_all_lag_2, on=['year', 'hybrid', 'abm'], how='left')
    sales_with_lag = sales_with_lag.merge(sales_all_lag_3, on=['year', 'hybrid', 'abm'], how='left')
    
    for col in sales_with_lag.columns:
        print(col)
    
    # impute, replacing the NaNs with zeros
    for feature in LAGGED_FEATURES:
        sales_with_lag[feature] = sales_with_lag[feature].fillna(0)
    
    # convert year to str
    sales_with_lag['year'] = sales_with_lag['year'].astype(str)
    
    # rename hybrid columns 
    sales_with_lag = sales_with_lag.rename(columns = {'hybrid':'Variety_Name'})
    
    # grabs data after the cutoff year
    sales_with_lag = sales_with_lag[sales_with_lag['year'] >= '2012'] 

    # drop sales data for this moment 
    #sales_with_lag = sales_with_lag.drop(columns = ['order_Q']).reset_index(drop = True)
    
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
        
        orders_to_date_mask = dt.datetime(year=int(year),
                                          month=ORDER_DATE['month'],
                                          day=ORDER_DATE['day'])
        
        # grab the orders to date
        df_to_date = df_year[
                df_year['EFFECTIVE_DATE'] <= orders_to_date_mask].copy().reset_index(drop=True)
        
        df_to_date = df_to_date.groupby(by=['year', 'Variety_Name', 'abm'],
                                        as_index=False).sum().reset_index(drop=True)
        
        df_to_date = df_to_date.rename(columns={'order_Q': 'orders_to_date'})
                
        dfs_monthly_netsales.append(df_to_date)
     
    Sales_monthly = pd.concat(dfs_monthly_netsales).reset_index(drop = True)    
    Sales_monthly = Sales_monthly.fillna(0)
    
    Sale_all = Sale_2012_2020_lagged.merge(Sales_monthly, on = ['year', 'Variety_Name', 'abm'], how = 'left')
    
    if Sale_all.columns.contains('order_Q_2'):
        print('SALE_ALL')
    # read in the 2021 data
    Sale_all_2021 = merge_2021_sales_data_impute_daily(df=Sale_all)
    
    if Sale_all_2021.columns.contains('order_Q_2'):
        print('SALE_ALL_2021')
    # read in the 2022 data
    Sale_all_2022 = merge_2022_sales_data_impute_daily(df=Sale_all_2021)
    
    if Sale_all_2022.columns.contains('order_Q_2'):
        print('SALE_ALL_2022')
    
    #Sale_all_2022['order_Q'] = Sale_all_2022['order_Q_month_8'].values
    
    # read in the 2023 data
    Sale_all_2023 = merge_2023_D1MS(df=Sale_all_2022)
    
    if Sale_all_2023.columns.contains('order_Q_2'):
        print('SALE_ALL_2023')
    # read in the 
    #Sale_all_2023 = merge_2023_tim_pulled_data(df=Sale_all_2022)
    
    # fill nas
    Sale_all_2023 = Sale_all_2023.fillna(0)
    
    return Sale_all_2023


def merge_2021_sales_data_impute_daily(df):
    """Merges the 2021 sales data.
    
    Keyword arguments:
        df -- the dataframe to concat onto
    Returns:
        df_merged
    """
    # read in the 2021 sales data
    sales_2021 = pd.read_csv(DATA_DIR + SALES_2021)
    
    # grab relevant columns
    sales_2021_subset = sales_2021[['Team', 'VARIETY', 'CY Net Sales',
                                    'Returns', 'Haulbacks', 'Replants', 'Shipped']]
    
    # rename the columns
    sales_2021_subset = sales_2021_subset.rename(columns={'Team': 'TEAM_KEY',
                                                          'VARIETY': 'Variety_Name',
                                                          'CY Net Sales': 'nets_Q',
                                                          'Returns': 'return_Q_ret',
                                                          'Haulbacks': 'return_Q_haul',
                                                          'Replants': 'replant_Q',
                                                          'Shipped': 'order_Q'
                                                          })
    
    # remove any variety names with "Empty"
    sales_2021_subset = sales_2021_subset[
            sales_2021_subset['Variety_Name'] != '(Empty)'].reset_index(drop=True)
        
    sales_2021_subset = sales_2021_subset.merge(abm_Teamkey, on=['TEAM_KEY'],
                                                how='left')
    
    sales_2021_subset = sales_2021_subset.drop(columns=['TEAM_KEY'])
    
    # change order data to float
    sales_2021_subset['order_Q'] = sales_2021_subset['order_Q'].str.replace(',','')
    sales_2021_subset['order_Q'] = sales_2021_subset['order_Q'].astype('float64')
    
    # do the same for the returns/haulbacks and set return_Q to be the sum of the quantities
    sales_2021_subset['return_Q_ret'] = sales_2021_subset['return_Q_ret'].str.replace(',','')
    sales_2021_subset['return_Q_ret'] = sales_2021_subset['return_Q_ret'].astype('float64')
    sales_2021_subset['return_Q_haul'] = sales_2021_subset['return_Q_haul'].str.replace(',','')
    sales_2021_subset['return_Q_haul'] = sales_2021_subset['return_Q_haul'].astype('float64')
    
    # do the same for nets Q and replants
    sales_2021_subset['nets_Q'] = sales_2021_subset['nets_Q'].str.replace(',', '')
    sales_2021_subset['nets_Q'] = sales_2021_subset['nets_Q'].astype('float64')
    sales_2021_subset['replant_Q'] = sales_2021_subset['replant_Q'].str.replace(',', '')
    sales_2021_subset['replant_Q'] = sales_2021_subset['replant_Q'].astype('float64')

    
    sales_2021_subset['return_Q'] = (
            sales_2021_subset['return_Q_haul'] + sales_2021_subset['return_Q_ret'])
    
    sales_2021_subset = sales_2021_subset.drop(columns=['return_Q_haul',
                                                        'return_Q_ret'])

    # group by product and abm
    sales_2021_agg = sales_2021_subset.groupby(
            by=['Variety_Name', 'abm'], as_index=False).sum()
    
    # read in the monthly historical fractions
    daily_fractions = pd.read_csv(DAILY_FRACTIONS)
    
    # grab the day from the daily fractions
    daily_fractions_order_date = daily_fractions[
            (daily_fractions['month'] == ORDER_DATE['month']) &
            (daily_fractions['day'] == ORDER_DATE['day'])].reset_index(drop=True)
    
    daily_fractions_order_date = daily_fractions_order_date.drop(columns=['month', 'day'])
    
    # merge the monthly fractions
    sales_2021_to_date = sales_2021_agg.merge(daily_fractions_order_date,
                                              on=['abm'],
                                              how='left')
    
    # drop NAs
    sales_2021_to_date = sales_2021_to_date.dropna().reset_index(drop=True)
            
    sales_2021_to_date['orders_to_date'] = sales_2021_to_date['order_Q'] * sales_2021_to_date['order_fraction']
    sales_2021_to_date['year'] = 2021
    sales_2021_to_date['year'] = sales_2021_to_date['year'].astype(str)

    #sales_2021_to_date = sales_2021_to_date.drop(columns=['order_Q'])
    """        
    df_w_lag = create_late_lagged_sales(df=sales_2021_to_date,
                                        full_df=df,
                                        year=2021)
    """
    df_w_lag = sales_2021_to_date.copy()
    # concatenate with the main dataframe
    df_merged = pd.concat([df, df_w_lag])
    
    return df_merged


def merge_2022_sales_data_impute_daily(df):
    """Merges the 2022 sales data.
    
    Keyword arguments:
        df -- the dataframe to concat onto
    Returns:
        df_merged
    """
    # read in the 2021 sales data
    sales_2022 = pd.read_csv(DATA_DIR + SALES_2022)
    
    # grab relevant columns
    sales_2022_subset = sales_2022[['Team', 'VARIETY', 'CY Net Sales',
                                    'Returns', 'Haulbacks', 'Replants', 'Shipped']]
    
    # rename the columns
    sales_2022_subset = sales_2022_subset.rename(columns={'Team': 'TEAM_KEY',
                                                          'VARIETY': 'Variety_Name',
                                                          'CY Net Sales': 'nets_Q',
                                                          'Returns': 'return_Q_ret',
                                                          'Haulbacks': 'return_Q_haul',
                                                          'Replants': 'replant_Q',
                                                          'Shipped': 'order_Q'})
    
    # remove any variety names with "Empty"
    sales_2022_subset = sales_2022_subset[
            sales_2022_subset['Variety_Name'] != '(Empty)'].reset_index(drop=True)
        
    sales_2022_subset = sales_2022_subset.merge(abm_Teamkey, on=['TEAM_KEY'],
                                                how='left')
    
    sales_2022_subset = sales_2022_subset.drop(columns=['TEAM_KEY'])
    
    # change order data to float
    sales_2022_subset['order_Q'] = sales_2022_subset['order_Q'].str.replace(',','')
    sales_2022_subset['order_Q'] = sales_2022_subset['order_Q'].astype('float64')
    
    # do the same for the returns/haulbacks and set return_Q to be the sum of the quantities
    sales_2022_subset['return_Q_ret'] = sales_2022_subset['return_Q_ret'].str.replace(',','')
    sales_2022_subset['return_Q_ret'] = sales_2022_subset['return_Q_ret'].astype('float64')
    sales_2022_subset['return_Q_haul'] = sales_2022_subset['return_Q_haul'].str.replace(',','')
    sales_2022_subset['return_Q_haul'] = sales_2022_subset['return_Q_haul'].astype('float64')
    
    # do the same for nets Q and replants
    sales_2022_subset['nets_Q'] = sales_2022_subset['nets_Q'].str.replace(',', '')
    sales_2022_subset['nets_Q'] = sales_2022_subset['nets_Q'].astype('float64')
    sales_2022_subset['replant_Q'] = sales_2022_subset['replant_Q'].str.replace(',', '')
    sales_2022_subset['replant_Q'] = sales_2022_subset['replant_Q'].astype('float64')

    sales_2022_subset['return_Q'] = (
            sales_2022_subset['return_Q_haul'] + sales_2022_subset['return_Q_ret'])
    
    sales_2022_subset = sales_2022_subset.drop(columns=['return_Q_haul',
                                                        'return_Q_ret'])

    # group by product and abm
    sales_2022_agg = sales_2022_subset.groupby(
            by=['Variety_Name', 'abm'], as_index=False).sum()
    
    # read in the monthly historical fractions
    daily_fractions = pd.read_csv(DAILY_FRACTIONS)
    
    # grab the day from the daily fractions
    daily_fractions_order_date = daily_fractions[
            (daily_fractions['month'] == ORDER_DATE['month']) &
            (daily_fractions['day'] == ORDER_DATE['day'])].reset_index(drop=True)
    
    daily_fractions_order_date = daily_fractions_order_date.drop(columns=['month', 'day'])
    
    # merge the monthly fractions
    sales_2022_to_date = sales_2022_agg.merge(daily_fractions_order_date,
                                              on=['abm'],
                                              how='left')
    
    # drop NAs
    sales_2022_to_date = sales_2022_to_date.dropna().reset_index(drop=True)
            
    sales_2022_to_date['orders_to_date'] = sales_2022_to_date['order_Q'] * sales_2022_to_date['order_fraction']
    sales_2022_to_date['year'] = 2022
    sales_2022_to_date['year'] = sales_2022_to_date['year'].astype(str)

    #sales_2022_to_date = sales_2022_to_date.drop(columns=['order_Q'])
    """        
    df_w_lag = create_late_lagged_sales(df=sales_2022_to_date,
                                        full_df=df,
                                        year=2022)
    """
    df_w_lag = sales_2022_to_date.copy()
    
    # concatenate with the main dataframe
    df_merged = pd.concat([df, df_w_lag])
    
    return df_merged


def merge_2023_D1MS(df):
    """
    """
    
    # read in the file
    sales_23 = pd.read_csv(DATA_DIR + 'D1_MS_23_product_location_112822.csv')
    
    # fill nas with 0
    sales_23 = sales_23.fillna(0)
    
    sales_23 = sales_23.rename(columns={'MK_YR': 'year', 'VARIETY_NAME': 'Variety_Name',
                                'SLS_LVL_2_ID': 'TEAM_KEY', 
                                'SUM(NET_SALES_QTY_TO_DATE)': 'nets_Q',
                                'SUM(ORDER_QTY_TO_DATE)': 'order_Q',
                                'SUM(RETURN_QTY_TO_DATE)': 'return_Q',
                                'SUM(REPLANT_QTY_TO_DATE)': 'replant_Q'})
    
    # select national brand corn
    sales_23 = sales_23[sales_23['BRAND_FAMILY_DESCR'] == 'NATIONAL'].reset_index(drop=True)
    sales_23 = sales_23[sales_23['SPECIE_DESCR'] == 'SOYBEAN'].reset_index(drop=True)
    
    sales_23_subset = sales_23[
            ['year', 'Variety_Name', 'TEAM_KEY', 'EFFECTIVE_DATE', 'nets_Q',
             'order_Q', 'return_Q', 'replant_Q']]
    
    # drop all nas
    sales_23_subset = sales_23_subset.dropna().reset_index(drop=True)
    
    # re-adjust to old abm names
    sales_23_subset = sales_23_subset.merge(abm_Teamkey, on=['TEAM_KEY'], how='left')
    sales_23_subset = sales_23_subset.drop(columns=['TEAM_KEY'])
    
    # remove the 'RIB' string from the hybrid name
    sales_23_subset['Variety_Name'] = sales_23_subset['Variety_Name'].str.replace('RIB', '')

    # change the effective date string to make the datetime readable
    sales_23_dates = sales_23_subset['EFFECTIVE_DATE'].astype(str).to_frame()
    
    sales_23_dates['year'] = sales_23_dates['EFFECTIVE_DATE'].str[:4]
    sales_23_dates['month'] = sales_23_dates['EFFECTIVE_DATE'].str[4:6]
    sales_23_dates['day'] = sales_23_dates['EFFECTIVE_DATE'].str[6:8]
    
    sales_23_dates = sales_23_dates.drop(columns=['EFFECTIVE_DATE'])
    
    # create a datetime object out of the effective date
    sales_23_subset['EFFECTIVE_DATE'] = pd.to_datetime(sales_23_dates)
    
    # remove M string from the year
    sales_23_subset['year'] = sales_23_subset['year'].str.replace('M', '')
    
    sales_23_no_date = sales_23_subset.drop(columns=['EFFECTIVE_DATE']).groupby(
            by=['year', 'Variety_Name', 'abm'], as_index=False).sum()
    
    # set the orders to date quantity
    sales_23_no_date['orders_to_date'] = sales_23_no_date['order_Q'].values
    df_w_23 = pd.concat([df, sales_23_no_date])
    
    return df_w_23


def read_sales_filepath():
    """ Reads in and returns the sales data as a dataframe.
    
    Keyword arguments:
        None
    Returns:
        Sale_2012_2020 -- the dataframe of the yealry sales data from 2012 to 2020
        df_clean_sale -- the dataframe of clean sales data with effective date
    """
    # preprocess sales 2020 sales data to get consistent abm data  
    #Preprocess_2020_sale()

    # define a list to store all sales data 
    dfs_path = []
    
    # read in the data by year 
    for year in range(2018, 2021):
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
                                     'ACCT_ID', 'NET_SHIPPED_QTY_TO_DATE']
    
    df_clean_sale = df_clean_sale.drop(columns = SALES_COLUMNS_TO_DROP_Monthly)
    
    # drop unnecessary columns for yearly sales
    SALES_COLUMNS_TO_DROP_Yearly = ['BRAND_FAMILY_DESCR', 'EFFECTIVE_DATE',
                                    'DEALER_ACCOUNT_CY_BRAND_FAMILY',
                                    'SHIPPING_STATE_CODE', 'SHIPPING_COUNTY',
                                    'SHIPPING_FIPS_CODE', 'SLS_LVL_1_ID', 'CUST_ID',
                                    'ACCT_ID', 'NET_SHIPPED_QTY_TO_DATE']
    
    Sale_2012_2020 = Sale_2012_2020.drop(columns=SALES_COLUMNS_TO_DROP_Yearly)
    
    # reorder the columns
    Sale_2012_2020 = Sale_2012_2020[['year', 'abm', 'Variety_Name', 
                                    'nets_Q', 'order_Q', 'return_Q','replant_Q']]
    
    Sale_2012_2020 = Sale_2012_2020.groupby(by=['year', 'Variety_Name', 'abm'],
                                            as_index=False).sum()
    
    return Sale_2012_2020, df_clean_sale
