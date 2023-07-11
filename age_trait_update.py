#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 10:08:21 2022

@author: epnzv
"""

import pandas as pd

# read in the old age_trait map
Age_Trait = pd.read_csv('Age_Trait_Wrong.csv')
#Age_Trait['year'] = Age_Trait['year'].astype(dtype='str',copy=False)

hybrids_first_age = pd.DataFrame(columns=['hybrid', 'first_year', 'first_age'])
hybrids_first_age['hybrid'] = Age_Trait['hybrid'].unique()

for hybrid in Age_Trait['hybrid'].unique():
    single_hybrid = Age_Trait[Age_Trait['hybrid'] == hybrid].copy()
    
    first_year = single_hybrid.iloc[0].year
    first_age = single_hybrid.iloc[0].age
    
    if first_age == 0:
        first_age = 1
    
    hybrids_first_age.loc[hybrids_first_age['hybrid'] == hybrid, 'first_year'] = first_year
    hybrids_first_age.loc[hybrids_first_age['hybrid'] == hybrid, 'first_age'] = first_age

Age_Trait_firsts = Age_Trait.merge(hybrids_first_age, on=['hybrid'], how='left')

Age_Trait_firsts['year'] = Age_Trait_firsts['year'].astype(int)
Age_Trait_firsts['first_year'] = Age_Trait_firsts['first_year'].astype(int)

Age_Trait_firsts['age'] = Age_Trait_firsts['first_age'] + (
        Age_Trait_firsts['year'] - Age_Trait_firsts['first_year'])

Age_Trait_fixed = Age_Trait_firsts.drop(columns=['first_age', 'first_year'])
Age_Trait_fixed = Age_Trait_fixed.rename(columns={'hybrid': 'Variety_Name'})
Age_Trait_fixed.to_csv('Age_Trait_23_fixed.csv', index=False)

"""
# read in the 23 sales
sales = pd.read_csv('../../NA-soy-pricing/data/D1_MS_23_product_location_100422.csv')

sales = sales.rename(columns={'MK_YR': 'year', 'VARIETY_NAME': 'Variety_Name',
                        'SLS_LVL_2_ID': 'TEAM_KEY', 
                        'SUM(NET_SALES_QTY_TO_DATE)': 'nets_Q',
                        'SUM(ORDER_QTY_TO_DATE)': 'order_Q',
                        'SUM(RETURN_QTY_TO_DATE)': 'return_Q',
                        'SUM(REPLANT_QTY_TO_DATE)': 'replant_Q'})
    
# select national brand soybeans
sales = sales[sales['BRAND_FAMILY_DESCR'] == 'NATIONAL'].reset_index(drop=True)
sales = sales[sales['SPECIE_DESCR'] == 'SOYBEAN'].reset_index(drop=True)

sales_varieties = sales['Variety_Name'].to_frame()

sales_varieties = sales_varieties.drop_duplicates().reset_index(drop=True)
sales_varieties['year'] = 2023

newest_sales_year = pd.DataFrame(columns=['year', 'Variety_Name', 'age', 'trait'])

for variety in Age_Trait['Variety_Name'].unique():
    single_variety = Age_Trait[Age_Trait['Variety_Name'] == variety]
    max_year = single_variety['year'].max()
    single_variety_recent_year = single_variety[single_variety['year'] == max_year]

    if newest_sales_year.empty == True:
        newest_sales_year = single_variety_recent_year.reset_index(drop=True)
    else:
        newest_sales_year = pd.concat([newest_sales_year, single_variety_recent_year]).reset_index(drop=True)
        
# merge with 23 sales and calculate new ages
newest_sales_year = newest_sales_year.rename(columns={'year': 'most_recent_year'})

sales_varieties = sales_varieties.merge(newest_sales_year,
                                        on=['Variety_Name'],
                                        how='left')

sales_varieties['year_diff'] = sales_varieties['year'] - sales_varieties['most_recent_year']

sales_varieties['age'] = sales_varieties['age'] + sales_varieties['year_diff']

age_trait_23 = sales_varieties[['Variety_Name', 'age', 'trait', 'year']]

age_trait_updated = pd.concat([Age_Trait, age_trait_23])

age_trait_updated.to_csv('Age_Trait_updated_full_2023.csv', index=False)
"""