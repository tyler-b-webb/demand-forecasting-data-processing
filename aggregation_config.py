#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 14:19:38 2021

@author: epnzv
"""

ABM_FIPS_MAP = 'mappingall_processed_updated.csv'

BIG_CF_FILE = 'Soybean_CY_Asgrow_12_29_21.csv'

CF_2022_FILE = 'FY23_01_20_22.csv'

CF_2023_FILE = 'FY23_Soy_101922.csv'

CF_2023_FILE_Y1 = 'FY23_Soy_011923.xlsx'

BLIZZARD_DIR = '../NA-soy-pricing/dataframe_construction_r_r/blizzard/county_data/'

CM_DIR = 'CM_prep/'

# the data directory
DATA_DIR = '../NA-soy-pricing/data/'

H2H_DIR = 'H2H_yield_data/'

HISTORICAL_SRP = 'historical_SRP/'

SCM_DATA_DIR = 'SCM_data/'

SCM_DATA_FILE = 'feb23_22_SCM.csv'

# the abm table
ABM_TABLE = 'ABM_Table.csv'

DAILY_FRACTIONS = 'historical_daily_fractions.csv'

MONTHLY_FRACTIONS = 'historical_monthly_fractions.csv'

SALES_2021 = '2021_hybrid_abm_dealer.csv'

SALES_2022 = '2022_hybrid_abm_dealer.csv'

SALES_2021_W_DATE = 'D1_MS_21_product_location_202110281734.csv'

#SALES_2022 = 'nov1_21_order_bank.csv'

SALES_DIR = 'sales_data/'

OLD_2020 = '2020_old.csv'

YIELD_COUNTY_DATA = 'county_soybean_yield.csv'

IMPUTE_H2H = True

ORDER_DATE = {'month': 1,
              'day': 15}

# the weight to use to get the orders to date for 2021, where we don't have effective dates
ORDER_FRACTION_2021 = 0.23

YEARLY_ABM_FIPS_MAP = 'abm_years_08_to_23.csv'