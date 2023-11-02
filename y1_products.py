#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 13:03:40 2023

@author: epnzv
"""


import pandas as pd


def get_RM(df):
    """Obtains the RM of a product from its name.
    
    Keyword arguments:
        df -- the dataframe with product names
    Returns:
        df_w_rm -- the dataframe with RM merged
        digits -- the dataframe with product names and their RMs
    """
    hybrids = df['Variety_Name']
    
    digits = hybrids.str.findall('[0-9]+')
    
    for i in range(len(digits)):
        if len(digits[i]) > 0:
            digits[i] = digits[i][0]
        else:
            digits[i] = '00'
    
    # turn the digits series into a df
    digits = digits.to_frame(name='digits')
    
    # set the RM to be 0 and then 
    digits['RM'] = 0
    digits['Variety_Name'] = hybrids.values
    
    digits = digits.drop_duplicates().reset_index(drop=True)
    for i in range(len(digits)):
        if int(digits.loc[i, 'digits'][:2]) <= 0:
            digits.loc[i, 'RM'] = -0.1
        elif (int(digits.loc[i, 'digits'][:2]) > 0  and int(digits.loc[i, 'digits'][:2]) < 5):
            digits.loc[i, 'RM'] = 0
        elif (int(digits.loc[i, 'digits'][:2]) >= 5 and int(digits.loc[i, 'digits'][:2]) < 10):
            digits.loc[i, 'RM'] = 0.5
        elif (int(digits.loc[i, 'digits'][:2]) >= 10 and int(digits.loc[i, 'digits'][:2]) < 15):
            digits.loc[i, 'RM'] = 1.0
        elif (int(digits.loc[i, 'digits'][:2]) >= 15 and int(digits.loc[i, 'digits'][:2]) < 20):
            digits.loc[i, 'RM'] = 1.5
        elif (int(digits.loc[i, 'digits'][:2]) >= 20 and int(digits.loc[i, 'digits'][:2]) < 25):
            digits.loc[i, 'RM'] = 2.0
        elif (int(digits.loc[i, 'digits'][:2]) >= 25 and int(digits.loc[i, 'digits'][:2]) < 30):
            digits.loc[i, 'RM'] = 2.5
        elif (int(digits.loc[i, 'digits'][:2]) >= 30 and int(digits.loc[i, 'digits'][:2]) < 35):
            digits.loc[i, 'RM'] = 3.0
        elif (int(digits.loc[i, 'digits'][:2]) >= 35 and int(digits.loc[i, 'digits'][:2]) < 40):
            digits.loc[i, 'RM'] = 3.5
        elif (int(digits.loc[i, 'digits'][:2]) >= 40 and int(digits.loc[i, 'digits'][:2]) < 45):
            digits.loc[i, 'RM'] = 4.0
        elif (int(digits.loc[i, 'digits'][:2]) >= 45 and int(digits.loc[i, 'digits'][:2]) < 50):
            digits.loc[i, 'RM'] = 4.5
        elif (int(digits.loc[i, 'digits'][:2]) >= 50 and int(digits.loc[i, 'digits'][:2]) < 55):
            digits.loc[i, 'RM'] = 5.0
        elif (int(digits.loc[i, 'digits'][:2]) >= 55 and int(digits.loc[i, 'digits'][:2]) < 60):
            digits.loc[i, 'RM'] = 5.5
        elif (int(digits.loc[i, 'digits'][:2]) >= 60 and int(digits.loc[i, 'digits'][:2]) < 65):
            digits.loc[i, 'RM'] = 6.0
        elif (int(digits.loc[i, 'digits'][:2]) >= 65 and int(digits.loc[i, 'digits'][:2]) < 70):
            digits.loc[i, 'RM'] = 6.5
        elif (int(digits.loc[i, 'digits'][:2]) >= 70 and int(digits.loc[i, 'digits'][:2]) < 75):
            digits.loc[i, 'RM'] = 7.0
        elif (int(digits.loc[i, 'digits'][:2]) >= 75 and int(digits.loc[i, 'digits'][:2]) < 80):
            digits.loc[i, 'RM'] = 7.5
        elif (int(digits.loc[i, 'digits'][:2]) >= 80 and int(digits.loc[i, 'digits'][:2]) < 85):
            digits.loc[i, 'RM'] = 8.0
        elif (int(digits.loc[i, 'digits'][:2]) >= 85 and int(digits.loc[i, 'digits'][:2]) < 90):
            digits.loc[i, 'RM'] = 8.5
            
    df_w_RM = df.merge(digits.drop(columns=['digits']),
                       on=['Variety_Name'],
                       how='left')
    
    return df_w_RM, digits


def read_age_trait():
    """Reads in the age/trait map.
    
    Keyword arguments:
        None
    Returns:
        Age_trait -- the age/trait map
    """
    Age_Trait = pd.read_csv('Age_Trait_23_fixed.csv')
    Age_Trait['year'] = Age_Trait['year'].astype(dtype='str',copy=False)
    
    return Age_Trait
