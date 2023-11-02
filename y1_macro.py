#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 14:31:49 2023

@author: epnzv
"""

import pandas as pd

from functools import reduce 

from aggregation_config import(BLIZZARD_DIR, CM_DIR, DATA_DIR, YEARLY_ABM_FIPS_MAP)


def clean_commodity(df_soy, df_corn):
    """Cleans the soy and corn commodity data and combines them into a single
    dataframe.
       Aggregates the commodity data by month, crop, etc.
    
    Keyword arguments:
        df_soy -- the dataframe of soy commodity data
        df_corn -- the dataframe of corn commodity data
    
    Returns:
        corn_soybean -- the combined dataframe appropriately aggregated of 
                        the cleaned corn and soy data
    """
    
    # concatenate two dfs
    corn_soybean = pd.concat([df_soy, df_corn])
    
    # only get those rows that have a real valued Price
    corn_soybean= corn_soybean[
            corn_soybean['Price'].notnull()].reset_index(drop=True)
    
    # only get those rows that have a real Contract Date
    corn_soybean = corn_soybean[
            corn_soybean['Contract Date'].notnull()].reset_index(drop=True)
    
    # create datetime format
    corn_soybean['Contract Date'] = pd.to_datetime(corn_soybean['Contract Date'])
    corn_soybean['Update Date'] = pd.to_datetime(corn_soybean['Update Date'])
    
    # create new features for the day, month, and year for the contract and update 
    corn_soybean['cYr'] = corn_soybean['Contract Date'].dt.year
    corn_soybean['cMn'] = corn_soybean['Contract Date'].dt.month
    corn_soybean['cDt'] = corn_soybean['Contract Date'].dt.day
    
    corn_soybean['uYr'] = corn_soybean['Update Date'].dt.year
    corn_soybean['uMn'] = corn_soybean['Update Date'].dt.month
    corn_soybean['uDt'] = corn_soybean['Update Date'].dt.day
    
    # drop unnecesary columns 
    corn_soybean = corn_soybean.drop(columns=['Contract Date', 'Update Date', 'cDt', 'uDt'])
    
    # get the mean price, grouping by the crop and dates
    corn_soybean = corn_soybean.groupby(
            by=['Crop', 'uYr', 'uMn', 'cYr', 'cMn'], as_index=False).mean()
    
    return corn_soybean 


def clean_weather(df_weather, df_county_locations, df_fips):
    """Adds the FIPS code to the Blizzard data, merging on the latitude and 
    longitude.
       Adds the abm feature to the Blizzard data, merging on the fips and year.
       Aggregate the county-level Blizzard data to the abm level. 
    
    Keyword arguments:
        df_weather -- the dataframe of the Blizzard data
        df_county_locations -- the dataframe with fips, latitude, longtitude
        df_fips -- the dataframe with fips, abm, year 
    Returns:
        Weather_w_abm -- the dataframe aggregated to the abm level
    """   
    Weather = df_weather.copy()
    County_Locations = df_county_locations.copy()
    
    # round latitude and longitude 
    Weather['latitude'] = Weather['latitude'].round(2)
    Weather['longitude'] = Weather['longitude'].round(2)
    
    County_Locations['latitude'] = County_Locations['latitude'].round(2)
    County_Locations['longitude'] = County_Locations['longitude'].round(2)
    
    # merge Weather with County_Locations to add the FIPS feature 
    Weather = Weather.merge(County_Locations, on=['latitude', 'longitude'], how='left')
    
    # merge Weather with FIPS_abm to add the abm feature
    Weather = Weather.merge(df_fips, on = ['fips', 'year'])
    
    # Drop missing value 
    print("Check the fraction of missing value: ", Weather.isna().sum()/Weather.shape[0])
    Weather = Weather.dropna().reset_index(drop = True)
    
    # Drop latitude, longitude, fips 
    dropped_cols = ['latitude', 'longitude', 'fips']
    Weather = Weather.drop(columns = dropped_cols)
    
    # group by the abm, year, and month and take the avg of min/max temperatures
    Weather = Weather.groupby(by=['year', 'month', 'abm'],as_index=False).mean().reset_index(drop=True)
    
    return Weather


def create_commodity_features(df, crop_type):
    """Creates the commodity price features.
    
    Keyword arguments:
        df -- the dataframe of the corn/soy data
        crop_type -- the crop we're creating features for
        
    Returns:
        Commodity_crop -- the dataframe with the newly created commodity features
                          from 1980 to 2022
    """
    # define a list to store all cm dataframes
    dfs_commodity = []
    
    # define the months we will iterate over
    update_months = [1, 2, 3, 4, 5, 6, 7]
    if crop_type == 'soybean':
        contract_months = [7, 8, 9, 11]
    elif crop_type == 'corn':
        contract_months = [7, 9, 12]
        
    # iterate over the contract and update months
    for update_month in update_months:
        for contract_month in contract_months:
            
            # grab the data for the relevant crop 
            df_crop = df[df['Crop'] == crop_type].reset_index(drop = True)
            
            # match the years
            df_commodity = df_crop[
                ((df_crop['uYr'] + 1 == df_crop['cYr']) & 
                 (df_crop['uMn'] == update_month) & 
                 (df_crop['cMn'] == contract_month))].reset_index(drop = True)
            
            # create a timing feature
            df_commodity['timing'] = ('CMprice_' + crop_type + '_' + str(update_month) + 
                      '_' + str(contract_month))
            
            # rename the price feature to include crop name
            df_commodity = df_commodity.rename(columns={'Price': 'CMprice_' + crop_type})
            
            # select relevant columns and rename it 
            df_commodity = df_commodity[['cYr', 'timing', 'CMprice_' + crop_type]]
            df_commodity = df_commodity.rename(columns={'cYr': 'year'})
            dfs_commodity.append(df_commodity)
    
    # concatenate all modified cm dataframes
    Commodity_crop = pd.concat(dfs_commodity)
    
    # reshape the commodity data
    Commodity_crop = Commodity_crop.pivot(index='year', columns='timing', values='CMprice_' + crop_type)
    
    # reset the index to make the year value a column and set all values to be floats
    Commodity_crop = Commodity_crop.reset_index().astype('float64')
    
    Commodity_crop['year'] = Commodity_crop['year'].astype('int32').astype(str)
    
    return Commodity_crop


def create_commodity_features_merged(Commodity_Corn_Soybean):
    """Creates commodity features and merges them.
    
    Keyword arguments: 
        Commodity_Corn_Soybean -- the cleaned commodity data
    Returns:
        CM_Soybean_Corn -- the merged, processed commodity data features
    """
    CM_Soybean = create_commodity_features(Commodity_Corn_Soybean, 'soybean')
    CM_Corn = create_commodity_features(Commodity_Corn_Soybean, 'corn')

    CM_Soybean_Corn = CM_Soybean.merge(CM_Corn, on=['year'])

    return CM_Soybean_Corn


def create_lagged_CM_features(df_cm):
    """Creates the lagged commodity features in the dataframe that can then be
    merged back into the main df. 
    
    Keyword arguments: 
        df_cm -- the dataframe with the flattened commodity data, broken down by
            by month
    Returns:
        df_corn_soy_lag -- the dataframe with the lagged features added
    """
    # Reset_index 
    df = df_cm.set_index('year')
    
    # create lagged feature
    df_lagged = df.shift(periods = 1)
    
    # rename the lagged columns 
    df_lagged = df_lagged.rename(mapper = lambda x: x + "_lag_1", axis = 1)
    
    # reset index 
    df_lagged.reset_index(inplace = True)
    
    # drop unnecessary columns 
    df_lagged = df_lagged.drop(columns = ['year'])
    
    # concatenate two columns 
    df_corn_soy_lag = pd.concat([df_cm, df_lagged], axis = 1, join = 'inner')
    
    # convert year to str
    df_corn_soy_lag['year'] = df_corn_soy_lag['year'].astype(str)
    
    return df_corn_soy_lag


def flatten_monthly_weather(df_weather):
    """Flattens the commodity dataframe in such a way that each month in a year
    gets a column. The number of rows will be the number of years and abms, and 
    the number of columns the number of months * the number of weather features
    + 2 for the year and abm. The data inside is the weather data itself.
    
    Keyword arguments:
        df_weather -- the unflattened dataframe with the weather data
    Returns:
        weather_flattened -- the data flattened as detailed above
    """
    
    # define a list to store all flattened weather data 
    dfs_flattened = []
    df = df_weather.copy()
    
    # get the month range 
    weather_month = df['month'].unique().tolist()
    
    # get weather features 
    weather_features = ['precipitation', 'total_solar_radiation','minimum_temperature', 'maximum_temperature']
    
    for month in weather_month:
        df_month = df[df['month'] == month].reset_index(drop = True)
        
        for feature in weather_features:
            df_month = df_month.rename(columns = {feature: feature + "_" + str(month)})
        
        # drop unnecessary columns
        df_month = df_month.drop(columns=['month']) 
        dfs_flattened.append(df_month)
    
    # merge all dfs together 
    weather_flattened = reduce(lambda df1,df2: pd.merge(df1,df2,how = 'left', on=['year', 'abm']), dfs_flattened)
    
    return weather_flattened


def read_commodity_corn_soybean():
    """Reads in the commodity data for both corn and soybeans.
    
    Keyword arguments:
        None
    Returns:
        Commodity_Corn -- the soybean commodity data to date
        Commodity_Soybean -- the corn commodity data to date
    """
    # read in soybean and commodity data
    Corn_Address = DATA_DIR + CM_DIR + 'corn_to_09262022.csv'
    Soybean_Address = DATA_DIR + CM_DIR + 'soybean_to_09262022.csv'
    
    Commodity_Corn = pd.read_csv(Corn_Address)
    Commodity_Soybean = pd.read_csv(Soybean_Address)
    
    return Commodity_Corn, Commodity_Soybean


def read_weather_filepath():
    """Reads in the Blizzard data and concatenates it into a single dataframe.
       Reads in the county_locations data 
       Reads in the abm_years
    
    Keyword arguments:
        years -- the years we want to read the data for
    Returns:
        Weather_2012_2020 -- the dataframe of all the county level blizzard data
        County_Location - the dataframe of all fips code w.r.t lati and long
        FIPS_abm - the dataframe of all fips w.r.t abm and year 
        
    """
    # define a list to store all weather data 
    dfs_path = []
    
    # read in the data by year
    for i in range(2012, 2024):
        print("Read ", str(i), " Weather Data")
        dfi_path = BLIZZARD_DIR + 'Blizzard_' + str(i) + '.csv'
        dfi = pd.read_csv(dfi_path)
        
        # add the dataframe to the list
        dfs_path.append(dfi)
        
    # concate all dataframes  
    Weather_2012_2020 = pd.concat(dfs_path).reset_index(drop = True)
    
    County_Location_Address = BLIZZARD_DIR + 'county_locations.csv'
    County_Location = pd.read_csv(County_Location_Address)
    
    FIPS_abm_Address = YEARLY_ABM_FIPS_MAP #DATA_DIR + 'abm_years.csv'
    FIPS_abm = pd.read_csv(FIPS_abm_Address)
    FIPS_abm = FIPS_abm[['year', 'fips', 'abm']]
    
    # set year as str
    Weather_2012_2020['year'] = Weather_2012_2020['year'].astype(str)
    FIPS_abm['year'] = FIPS_abm['year'].astype(str)
    
    # set fips to int in FIPS_abm
    FIPS_abm['fips'] = FIPS_abm['fips'].astype(int)
    
    return Weather_2012_2020, County_Location, FIPS_abm
