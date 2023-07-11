#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 14:16:15 2022

@author: epnzv
"""

import pandas as pd

from aggregation_config import (CF_2023_FILE, DATA_DIR)

sales_23 = pd.read_csv(DATA_DIR + 'sales_data_from_tim/asgrow_nov7_orders.csv')

cy_23 = pd.read_csv(DATA_DIR + CF_2023_FILE)

cy_23 = cy_23[cy_23['FORECAST_YEAR'] == 2023].reset_index(drop=True)

cy_23_asgrow = cy_23[cy_23['BRAND_GROUP'] == 'ASGROW'].reset_index(drop=True)

cy_23_asgrow = cy_23_asgrow[['TEAM_KEY', 'ACRONYM_NAME', 'TEAM_FCST_QTY_10']].rename(
        columns={'TEAM_KEY' : 'Sales Office', 
                 'ACRONYM_NAME': 'Acronym Name'})

cy_23_asgrow = cy_23_asgrow[cy_23_asgrow['TEAM_FCST_QTY_10'] != 0]

sales_23_w_cy = cy_23_asgrow.merge(sales_23, on=['Sales Office', 'Acronym Name'], how='outer')

sales_23_w_cy[' Total '] = sales_23_w_cy[' Total '].fillna(' 0 ')

sales_23_w_cy = sales_23_w_cy.drop(columns=['TEAM_FCST_QTY_10'])

sales_23_w_cy.to_csv(DATA_DIR + 'sales_data_from_tim/asgrow_nov7_orders_amended.csv')