#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 14:45:12 2021

@author: epnzv
"""

import pandas as pd

ABM_TABLE = 'ABM_Table.csv'

DATA_DIR = '../../NA-soy-pricing/data/'

D1MS_FILE = 'nov1_21_order_bank.csv'

SALES_DIR = 'sales_data/'

d1 = pd.read_csv(DATA_DIR + D1MS_FILE)
abm_table = pd.read_csv(DATA_DIR + ABM_TABLE)
abm_table = abm_table[['Old Area ID', 'New Area ID']]

d1_subset = d1[['BRAND_FAMILY_DESCR', 'SPECIE_DESCR', 'FIPS', 'SLS_LVL_2_ID']]

# grab asgrow soybeans
soybean_counties = d1_subset[
        d1_subset['SPECIE_DESCR'] == 'SOYBEAN'].reset_index(drop=True)
asgrow_counties = soybean_counties[
        soybean_counties['BRAND_FAMILY_DESCR'] == 'NATIONAL'].reset_index(drop=True)

# grab the counties and the abm
asgrow_counties = asgrow_counties[
        ['FIPS', 'SLS_LVL_2_ID']].drop_duplicates().reset_index(drop=True)

# rename and merge the ABM table
asgrow_counties = asgrow_counties.rename(columns={'SLS_LVL_2_ID': 'New Area ID'})
asgrow_counties = asgrow_counties.merge(abm_table, on=['New Area ID'], how='left')

asgrow_counties_2021 = asgrow_counties.copy()
asgrow_counties_2021['year'] = 2021

asgrow_counties_2022 = asgrow_counties.copy()
asgrow_counties_2022['year'] = 2022

dfs_path = []
for year in range(2008, 2021):
    print("Read ", str(year), " Sales Data")
    dfi_path = DATA_DIR + SALES_DIR + str(year) + '.csv'
    dfi = pd.read_csv(dfi_path)
    
    dfi = dfi[dfi['SPECIE_DESCR'] == 'SOYBEAN'].reset_index(drop=True)
    dfi = dfi[dfi['BRAND_FAMILY_DESCR'] == 'NATIONAL'].reset_index(drop=True)
    
    # set a year parameter to be the year 
    dfi['year'] = year
    
    dfi = dfi[['year', 'SHIPPING_FIPS_CODE', 'SLS_LVL_2_ID']]
    dfi = dfi.rename(columns={'SHIPPING_FIPS_CODE': 'FIPS',
                              'SLS_LVL_2_ID': 'Old Area ID'})
    dfi = dfi.merge(abm_table, on=['Old Area ID'], how='left')
        
    # add modified dataframe to the list 
    dfs_path.append(dfi)
dfs_path.append(asgrow_counties_2021)
dfs_path.append(asgrow_counties_2022)
sales = pd.concat(dfs_path).reset_index(drop=True)

sales = sales.drop_duplicates().reset_index(drop=True)

sales.to_csv('abm_years_08_to_22.csv',index=False)
