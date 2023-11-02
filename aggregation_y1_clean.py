#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 12:58:49 2023

@author: epnzv
"""

from y1_macro import (clean_commodity, clean_weather,
                      create_commodity_features_merged,
                      create_lagged_CM_features, flatten_monthly_weather,
                      read_commodity_corn_soybean, read_weather_filepath)
from y1_products import (get_RM, read_age_trait)
from y1_sales import (create_lagged_sales, create_monthly_sales,
                      read_sales_filepath)


Sale_2012_2020, clean_Sale = read_sales_filepath()

Sale_2012_2020_monthly = create_monthly_sales(Sale_2012_2020, clean_Sale).reset_index(drop=True)

Sale_all = create_lagged_sales(Sale_2012_2020_monthly)

Sale_all, digies = get_RM(df=Sale_all)

Age_Trait = read_age_trait()

Weather_2012_2020, County_Location, FIPS_abm = read_weather_filepath()

Weather = clean_weather(Weather_2012_2020, County_Location, FIPS_abm) 

Weather_Flattened = flatten_monthly_weather(Weather)

Commodity_Corn, Commodity_Soybean = read_commodity_corn_soybean()

Commodity_Corn_Soybean = clean_commodity(Commodity_Corn, Commodity_Soybean)

CM_Soybean_Corn = create_commodity_features_merged(Commodity_Corn_Soybean)

CM_lagged = create_lagged_CM_features(CM_Soybean_Corn)

