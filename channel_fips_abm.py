#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 13:25:48 2023

@author: epnzv
"""
import numpy as np
import pandas as pd

DATA_DIR = '../../NA-soy-pricing/data/'
CHANNEL_DIR = 'channel/'

CHANNEL_SALES_YEARS = ['2016', '2017', '2018', '2019', '2020']

mars_fips_abm = pd.read_csv(DATA_DIR + CHANNEL_DIR + 'sales_org_fips_list.csv')
mars_fips_abm = mars_fips_abm[['Year', 'ABM.ID', 'FIPS']].drop_duplicates().reset_index(drop=True)
mars_fips_abm = mars_fips_abm.rename(columns={'Year': 'year',
                                              'ABM.ID': 'abm',
                                              'FIPS': 'fips'})

for abm in mars_fips_abm['abm'].unique():
    single_abm = mars_fips_abm[mars_fips_abm['abm'] == abm]
    
    most_recent_year = np.max(single_abm['year'].unique())
    
mars_fips_abm_22 = mars_fips_abm[mars_fips_abm['year'] == 2022].reset_index(drop=True)
mars_fips_abm_22['num_abm_in_fips'] = 1

i = 0
j=0
for fips in mars_fips_abm_22['fips'].unique():
    j+=1 
    single_fips_abm = mars_fips_abm_22[mars_fips_abm_22['fips'] == fips].reset_index(drop=True)
    if len(single_fips_abm['abm'].unique()) > 1:
        i+=1
        
        # log the number of ABMs associated with the FIPS for easy subsetting
        mars_fips_abm_22.loc[
                mars_fips_abm_22['fips'] == fips, 'num_abm_in_fips'] = len(single_fips_abm['abm'].unique())
                
        
sales_22 = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2022_D1MS.csv')

sales_22_subset = sales_22.loc[sales_22['BRAND_FAMILY_DESCR'] == 'CHANNEL',
                        ['FIPS', 'SLS_LVL_2_ID', 'SUM(NET_SALES_QTY_TO_DATE)']].reset_index(drop=True)

sales_22_subset = sales_22_subset.rename(columns={'FIPS': 'fips',
                                                  'SLS_LVL_2_ID': 'abm',
                                                  'SUM(NET_SALES_QTY_TO_DATE)': 'nets_Q'})

sales_22_subset = sales_22_subset.groupby(by=['fips', 'abm'], as_index=False).sum()

# subset out the offending fips/abm pairs and their counterparts
multi_abms = mars_fips_abm_22[
        mars_fips_abm_22['num_abm_in_fips'] > 1].reset_index(drop=True).drop(columns=['num_abm_in_fips'])

single_abm = mars_fips_abm_22[
        mars_fips_abm_22['num_abm_in_fips'] == 1].reset_index(drop=True).drop(columns=['num_abm_in_fips'])

multi_abms_w_sales = multi_abms.merge(sales_22_subset, on=['fips', 'abm'], how='left').fillna(0)

multi_abms_distinct = pd.DataFrame()

# grab the highest selling abm for each fips code. if 0, just assign the first one, randomly I suppose
for fips in multi_abms_w_sales['fips'].unique():
    single_fips = multi_abms_w_sales[multi_abms_w_sales['fips'] == fips]
    single_fips_highest_sale = max(single_fips['nets_Q'])
    if single_fips_highest_sale > 0:
        single_fips_distinct = single_fips[
                single_fips['nets_Q'] == single_fips_highest_sale].reset_index(drop=True).drop(columns=['nets_Q'])
    else:
        single_fips_distinct = single_fips.head(1).reset_index(drop=True).drop(columns=['nets_Q'])
    
    if multi_abms_distinct.empty:
        multi_abms_distinct = single_fips_distinct
    else:
        multi_abms_distinct = pd.concat([multi_abms_distinct, single_fips_distinct]).reset_index(drop=True)
        
distinct_fips_abm = pd.concat([single_abm, multi_abms_distinct]).reset_index(drop=True).drop(columns=['year'])

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
    sales_21['year'] = '2021'
    sales_21['EFFECTIVE_DATE'] = pd.to_datetime(sales_21['EFFECTIVE_DATE'])
    
    print('Reading 2022 sales data...')
    sales_22_raw = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2022_D1MS.csv')
    sales_22 = D1MS_dt_format(df=sales_22_raw)
    sales_22['year'] = '2022'
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
    
    # merge with ABM map
    sales_all = sales_all.merge(abm_map, on=['fips'], how='left')
    
    # check for any additional missing abms and basically repeat using the 2021/2022 D1MS data
    missing_abms = sales_all[sales_all['abm'].isna()]
    
    missing_abms = missing_abms[['fips']]
    
    D1MS_map = sales_D1MS[
            ['FIPS', 'SLS_LVL_2_ID', 'SUM(NET_SALES_QTY_TO_DATE)']].groupby(by=['FIPS', 'SLS_LVL_2_ID'], as_index=False).sum()
    

    D1MS_map_unique = pd.DataFrame()
    
    for fips in D1MS_map['FIPS'].unique():
        single_fips = D1MS_map[D1MS_map['FIPS'] == fips].reset_index(drop=True)
        if len(single_fips) == 1:
            if D1MS_map_unique.empty:
                D1MS_map_unique = single_fips[['FIPS', 'SLS_LVL_2_ID']].reset_index(drop=True)
            else:
                D1MS_map_unique = pd.concat(
                        [D1MS_map_unique, single_fips[['FIPS', 'SLS_LVL_2_ID']]]).reset_index(drop=True)
                
        else:
            single_fips_highest_sale = max(single_fips['SUM(NET_SALES_QTY_TO_DATE)'])
            
            if single_fips_highest_sale > 0:
                single_fips_distinct = single_fips[
                        single_fips['SUM(NET_SALES_QTY_TO_DATE)'] == single_fips_highest_sale].reset_index(drop=True).drop(columns=['SUM(NET_SALES_QTY_TO_DATE)'])
            else:
                single_fips_distinct = single_fips.head(1).reset_index(drop=True).drop(columns=['SUM(NET_SALES_QTY_TO_DATE)'])
            
            if D1MS_map_unique.empty:
                D1MS_map_unique = single_fips_distinct[['FIPS', 'SLS_LVL_2_ID']].reset_index(drop=True)
            else:
                D1MS_map_unique = pd.concat(
                        [D1MS_map_unique, single_fips_distinct[['FIPS', 'SLS_LVL_2_ID']]]).reset_index(drop=True)
                
    
    D1MS_map_unique = D1MS_map_unique.rename(columns={'FIPS': 'fips',
                                                      'SLS_LVL_2_ID': 'abm'})
    
    D1MS_map_unique = D1MS_map_unique[D1MS_map_unique['fips'] != 'UNKNOWN'].reset_index(drop=True)
    D1MS_map_unique = D1MS_map_unique[D1MS_map_unique['fips'].isna() == False].reset_index(drop=True)
    D1MS_map_unique['fips'] = D1MS_map_unique['fips'].astype(int)
    
    missing_abms_w_abm = missing_abms.merge(D1MS_map_unique, on='fips', how='left')
    
    abm_map_amended = pd.concat([abm_map, missing_abms_w_abm]).drop_duplicates()
    abm_map_amended = abm_map_amended.dropna().reset_index(drop=True)
            
    return sales_all, sales, sales_D1MS, abm_map_amended

def D1MS_dt_format(df):
    """Changes the format of the effective date for the D1MS data.
    
    Keyword Argumends:
        df -- the D1MS data
    Returns: 
        df_new_format
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

sales_all, sales_CDW, sales_D1MS, abm_map_amend = read_channel_sales(crop='CORN', abm_map=distinct_fips_abm)

abm_map_amend.to_csv('channel_abm_fips_map.csv', index=False)