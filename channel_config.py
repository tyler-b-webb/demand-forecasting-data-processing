#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 15:35:48 2023

@author: epnzv
"""

DATA_DIR = '../NA-soy-pricing/data/'
CHANNEL_DIR = 'channel/'
CORN_H2H_DIR = 'corn_H2H_yield_data/'
FARMGATE_DIR = 'from_larry_swift/'
MPI_DIR = 'MPI_data/'
SOYBEAN_H2H_DIR = 'H2H_yield_data/'

CHANNEL_ABM_MAP = 'channel_abm_fips_map.csv'
CHANNEL_DATE_RATIOS = 'channel_abm_date_ratios.csv'

CORN_CF_DATA = 'FY23_Corn_022023.xlsx'

SOYBEAN_CF_DATA = 'FY23_Soy_011923.xlsx'

CHANNEL_SALES_YEARS = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]

CURRENT_BANK = False

EFFECTIVE_DATE = {'month': 10,
                  'day': 20}

FORECAST_COLS = ['FORECAST_YEAR', 'TEAM_KEY', 'ACRONYM_NAME',
                 'TEAM_Y1_FCST_2']

FORECAST_CY_COLS = ['FORECAST_YEAR', 'TEAM_KEY', 'ACRONYM_NAME',
                    'TEAM_FCST_QTY_10']

CORN_MPI_HIST_COLS = ['names.commercial', 'characteristics.Goss Wilt', 'characteristics.Stalk Strength',
                 'characteristics.Gray Leaf Spot',  'characteristics.Drought Tolerance',
                 'characteristics.Drydown', 'characteristics.Greensnap',
                 'characteristics.Northern Corn Leaf Blight - Race 1',
                 'characteristics.Harvest Appearance', 'characteristics.Plant Height',
                 'characteristics.Anthracnose Stalk Rot', 'characteristics.Stay Green',
                 'characteristics.Seedling Vigor', 'characteristics.Root Strength']

CORN_MPI_NEW_COLS = ['hybrid', 'GOSSS WILT', 'STALK STRENGTH', 'GRAY LEAF SPOT',
                     'DROUGHT TOLERANCE', 'DRYDOWN', 'GREENSNAP', 'NORTHERN CORN LEAF BLIGHT R1',
                     'HARVEST APPEARANCE', 'PLANT HEIGHT', 'ANTHRACNOSE STALK ROT',
                     'STAYGREEN',  'SEEDLING VIGOR', 'ROOT STRENGTH']

CORN_MPI_FEATURE_NAMES = ['hybrid', 'goss_wilt', 'stalk_strength', 'gray_leaf_spot',
                          'drought_tolerance', 'drydown', 'greensnap',
                          'northern_corn_leaf_blight_r1', 'harvest_appearance', 
                          'plant_height', 'athracnose_stalk_rot', 'staygreen',
                          'seedling_vigor', 'root_strength']

PERFORMANCE_COLS = ['yield', 'yield_adv_within_abm_by_trait_brand',
                    'yield_adv_within_abm_outof_trait_brand',
                    'yield_adv_with_abm_brand']

SOYBEAN_MPI_HIST_COLS = ['names.commercial', 'characteristics.No-till Adaptability',
                         'characteristics.Brown Stem Rot', 'characteristics.Plant Type',
                         'characteristics.Standability', 'characteristics.Plant Height',
                         'characteristics.Soybean Cyst Nematode', 'characteristics.Emergence',
                         'characteristics.Iron Deficiency Chlorosis',
                         'characteristics.PRR Field Tolerance', 'characteristics.Pod Color',
                         'characteristics.White Mold']

SOYBEAN_MPI_FEATURE_NAMES = ['hybrid', 'no_till_adaptability', 'brown_stem_rot', 'plant_type',
                             'standability', 'plant_height', 'soybean_cyst_nematode',
                             'emergence', 'iron_deficiency_chlorosis',
                             'prr_field_tolerance', 'pod_color', 'white_mold']