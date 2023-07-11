#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  1 14:52:41 2021

@author: epnzv
"""

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
    sales_2021_subset = sales_2021[['Team', 'VARIETY', 'CY Net Sales',
                                    'Returns', 'Haulbacks', 'Replants', 'Orders']]
    
    # rename the columns
    sales_2021_subset = sales_2021_subset.rename(columns={'Team': 'TEAM_KEY',
                                                          'VARIETY': 'Variety_Name',
                                                          'CY Net Sales': 'nets_Q',
                                                          'Returns': 'return_Q_ret',
                                                          'Haulbacks': 'return_Q_haul',
                                                          'Replants': 'replant_Q',
                                                          'Orders': 'order_Q'})
    
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
    monthly_fractions = pd.read_csv(MONTHLY_FRACTIONS)
    
    # merge the monthly fractions
    sales_2021_monthly = sales_2021_agg.merge(monthly_fractions,
                                              on=['abm'],
                                              how='left')
    
    # drop NAs
    sales_2021_monthly = sales_2021_monthly.dropna().reset_index(drop=True)
        
    # create the monthly order features
    months = [9, 10, 11, 12, 1,2, 3, 4, 5, 6, 7]
    for i in months:
        sales_2021_monthly['order_Q_month_' + str(i)] = (
                sales_2021_monthly['order_Q'] * sales_2021_monthly['frac_' + str(i)])
        sales_2021_monthly = sales_2021_monthly.drop(columns=['frac_' + str(i)])
    
    sales_2021_monthly['order_Q_month_8'] = sales_2021_monthly['order_Q'].values
    sales_2021_monthly['year'] = 2021
    sales_2021_monthly['year'] = sales_2021_monthly['year'].astype(str)

    sales_2021_monthly = sales_2021_monthly.drop(columns=['order_Q'])
    
    # concatenate with the main dataframe
    df_merged = pd.concat([df, sales_2021_monthly])
    
    return df_merged
