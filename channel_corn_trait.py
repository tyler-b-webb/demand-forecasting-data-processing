#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 21 12:36:30 2023

@author: epnzv
"""

import pandas as pd

corn_forecasts = pd.read_excel('../NA-soy-pricing/data/FY23_Corn_022023.xlsx')

channel_hybrids = corn_forecasts[corn_forecasts['BRAND_GROUP'] == 'CHANNEL'].reset_index(drop=True)

channel_trait_names = channel_hybrids[['ACRONYM_NAME', 'TRAIT_NAME']]

channel_trait_names['hybrid_stripped'] = channel_trait_names['ACRONYM_NAME'].str.replace('\d+', '')

channel_trait_names['hybrid_stripped'] = channel_trait_names['hybrid_stripped'].str.replace('-', '')

channel_trait_stripped = channel_trait_names[['hybrid_stripped', 'TRAIT_NAME']].drop_duplicates().reset_index(drop=True)

channel_trait_stripped = channel_trait_stripped[channel_trait_stripped['TRAIT_NAME'] !=  '-'].reset_index(drop=True)

for hybrid_strip in channel_trait_stripped['hybrid_stripped'].unique():
    single_hybrid = channel_trait_stripped[channel_trait_stripped['hybrid_stripped'] == hybrid_strip]
    if len(single_hybrid) > 1:
        print(single_hybrid)
        channel_trait_stripped = channel_trait_stripped[
                channel_trait_stripped['hybrid_stripped'] != hybrid_strip].reset_index(drop=True)

#channel_trait_stripped.to_csv('corn_hybrid_trait_map.csv', index=False)