#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 26 13:58:06 2021

@author: gmtxy
"""

import pandas as pd 
from functools import reduce 
import numpy as np
from calendar import monthrange
import datetime as dt
from pandasql import sqldf

from aggregation_config import(ABM_TABLE, DATA_DIR, OLD_2020, SALES_DIR)


###### --------------------- Read ABM & Teamkey Map  ------------------- ######
def read_abm_teamkey_file():
    """ Reads in and returns the abm and teamkey data as a dataframe.
    
    Keyword arguments:
        None
    Returns:
        ABM_TEAMKEY -- the dataframe of mapping teamkey to abm 
    """
    
    # read in data
    abm_Teamkey_Address = DATA_DIR + ABM_TABLE
    abm_Teamkey = pd.read_csv(abm_Teamkey_Address)
    
    # rename columns 
    abm_Teamkey = abm_Teamkey.rename(columns = {'Old Area ID':'abm', 'New Area ID':'TEAM_KEY'})
    
    # selcted required columns 
    abm_Teamkey = abm_Teamkey[['abm','TEAM_KEY']]
    return abm_Teamkey
abm_Teamkey = read_abm_teamkey_file()

###### ---------------------- Read Sales Data ------------------------ ######
def Preprocess_2020_sale():
    df2020_path = DATA_DIR + SALES_DIR + OLD_2020
    dfsale_2020 = pd.read_csv(df2020_path)
    dfsale_2020 = dfsale_2020.rename(columns = {'SLS_LVL_2_ID':"TEAM_KEY"})
    dfsale_2020 = dfsale_2020.merge(abm_Teamkey, how = 'left', on = ['TEAM_KEY'])
    dfsale_2020 = dfsale_2020.rename(columns = {'abm':'SLS_LVL_2_ID'})
    
    df2020_path_new = DATA_DIR + SALES_DIR + '2020.csv'
    
    dfsale_2020.to_csv(df2020_path_new, index = False)
    "Finish Proprecssing 2020 sales Data"
    
def read_sales_filepath():
    """ Reads in and returns the sales data as a dataframe.
    
    Keyword arguments:
        None
    Returns:
        Sale_2012_2020 -- the dataframe of the yealry sales data from 2012 to 2020
        df_clean_sale -- the dataframe of clean sales data with effective date
    """
    # preprocess sales 2020 sales data to get consistent abm data  
    Preprocess_2020_sale()

    # define a list to store all sales data 
    dfs_path = []
    
    # read in the data by year 
    for year in range(2008, 2021):
        print("Read ", str(year), " Sales Data")
        dfi_path = r'//Users/gmtxy/OneDrive - Bayer/Sales/{year}.csv'.format(year = year)
        dfi = pd.read_csv(dfi_path)
        
        # set a year parameter to be the year 
        dfi['year'] = year
        
        # convert the effective date to a datetime format in order to set the mask 
        dfi['EFFECTIVE_DATE'] = pd.to_datetime(dfi['EFFECTIVE_DATE'])
        
        # add modified dataframe to the list 
        dfs_path.append(dfi)
      
    # concate all dataframes  
    Sale_2012_2020 = pd.concat(dfs_path).reset_index(drop = True)
    
    # rename the columns 
    SALES_COLUMN_NAMES = {'year': 'year', 'SPECIE_DESCR': 'crop',
                      'SLS_LVL_2_ID': 'abm', 'VARIETY_NAME': 'Variety_Name',
                      'NET_SALES_QTY_TO_DATE': 'nets_Q',
                      'ORDER_QTY_TO_DATE': 'order_Q',
                      'RETURN_QTY_TO_DATE': 'return_Q',
                      'REPLANT_QTY_TO_DATE': 'replant_Q'}
    Sale_2012_2020 = Sale_2012_2020.rename(columns = SALES_COLUMN_NAMES)
    
    # select the soybean crop 
    Sale_2012_2020 = Sale_2012_2020[Sale_2012_2020['crop'] == 'SOYBEAN']
    
    # set year as str
    Sale_2012_2020['year'] = Sale_2012_2020['year'].astype(str)
    
    # create a clean sales data for monthly calculation
    df_clean_sale = Sale_2012_2020.copy()
    
    # drop unnecessary columns
    SALES_COLUMNS_TO_DROP_Monthly = ['BRAND_FAMILY_DESCR', 
                         'DEALER_ACCOUNT_CY_BRAND_FAMILY',
                         'SHIPPING_STATE_CODE', 'SHIPPING_COUNTY',
                         'SHIPPING_FIPS_CODE', 'SLS_LVL_1_ID', 'CUST_ID',
                         'ACCT_ID', 'NET_SHIPPED_QTY_TO_DATE']
    df_clean_sale = df_clean_sale.drop(columns = SALES_COLUMNS_TO_DROP_Monthly)
    
    # drop unnecessary columns for yearly sales
    SALES_COLUMNS_TO_DROP_Yearly = ['BRAND_FAMILY_DESCR', 'EFFECTIVE_DATE',
                         'DEALER_ACCOUNT_CY_BRAND_FAMILY',
                         'SHIPPING_STATE_CODE', 'SHIPPING_COUNTY',
                         'SHIPPING_FIPS_CODE', 'SLS_LVL_1_ID', 'CUST_ID',
                         'ACCT_ID', 'NET_SHIPPED_QTY_TO_DATE']
    Sale_2012_2020 = Sale_2012_2020.drop(columns = SALES_COLUMNS_TO_DROP_Yearly)
    
    # reorder the columns
    Sale_2012_2020 = Sale_2012_2020[['year', 'abm', 'Variety_Name', 
                                    'nets_Q', 'order_Q', 'return_Q','replant_Q']]
    
    Sale_2012_2020 = Sale_2012_2020.groupby(by = ['year','Variety_Name','abm'],as_index=False).sum()
    
    return Sale_2012_2020, df_clean_sale

def create_lagged_sales(df):
    """Creates the "lagged" sales features, namely the sales data for a product
    from the two previous years in a given ABM.
    
    Keyword arguments:
        df -- the dataframe with the cleaned sales data that will be used to 
            create the lagged features
    Returns:
        df_with_lag -- the dataframe with the lagged sales features added
    """
    print('Creating lagged features...')
    
    LAGGED_FEATURES = ['nets_Q_1', 'order_Q_1', 'return_Q_1', 'replant_Q_1',
                   'nets_Q_2', 'order_Q_2', 'return_Q_2']
    
    df = df.rename(columns = {'Variety_Name':'hybrid'})
    df['year'] = df['year'].astype(int)
    sales_all = df.copy()


    # define the selection criteria as strings to use in the sqldf commmand
    # this is principally just for readability
    current_year_q = "select a.year, a.abm, a.hybrid, a.nets_Q, a.order_Q, a.return_Q, a.replant_Q, "
    last_year_q = "b.nets_Q as nets_Q_1, b.order_Q as order_Q_1, b.return_Q as return_Q_1, b.replant_Q as replant_Q_1, "
    two_years_q = "c.nets_Q as nets_Q_2, c.order_Q as order_Q_2, c.return_Q as return_Q_2 from sales_all "
    join_last_year = "a left join sales_all b on a.hybrid = b.hybrid and a.abm = b.abm and a.year = b.year + 1 "
    join_two_years = "left join sales_all c on a.hybrid = c.hybrid and a.abm = c.abm and a.year = c.year + 2"
 
    sales_with_lag = sqldf(current_year_q + last_year_q + two_years_q + 
                           join_last_year + join_two_years)
    
    # impute, replacing the NaNs with zeros
    for feature in LAGGED_FEATURES:
        sales_with_lag[feature] = sales_with_lag[feature].fillna(0)
    
    # convert year to str
    sales_with_lag['year'] = sales_with_lag['year'].astype(str)
    
    # rename hybrid columns 
    sales_with_lag = sales_with_lag.rename(columns = {'hybrid':'Variety_Name'})
    
    # grabs data after the cutoff year
    sales_with_lag = sales_with_lag[sales_with_lag['year'] >= '2012'] 

    # drop sales data for this momennt 
    sales_with_lag = sales_with_lag.drop(columns = ['order_Q', 'return_Q',
       'replant_Q']).reset_index(drop = True)
    return sales_with_lag


def create_monthly_sales(Sale_2012_2020_lagged, clean_Sale):
    """ Create the "datemask" to get monthly feature for the netsale data
    
    Keyword arguments:
        Sale_2012_2020 -- the dataframe of the yealry sales data from 2012 to 2020 with lagged features 
        df_clean_sale -- the dataframe of clean sales data with effective date
    Returns:
        Sales_all -- the dataframe of clean sales data with montly and lagged features 
    """
    print('Creating monthly features...')

    clean_Sale = clean_Sale[['EFFECTIVE_DATE', 'year', 'abm', 'Variety_Name', 'order_Q']]
    months = [9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8]
    
    dfs_monthly_netsales = []
    years = ['2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']
    for year in years:
        print('year', year)
        df_year = clean_Sale[clean_Sale['year'] == year].reset_index(drop = True)
        
        df_monthly_total = pd.DataFrame()
        
        df_single_year = Sale_2012_2020_lagged[Sale_2012_2020_lagged['year'] == year].copy().reset_index(drop = True)
        
        df_monthly_total['year'] = df_single_year['year'].tolist()
        df_monthly_total['Variety_Name'] = df_single_year['Variety_Name'].tolist()
        df_monthly_total['abm'] = df_single_year['abm'].tolist()
    
        
        for month in months:
            print("month", month)
            if month > 8:
                date_mask = dt.datetime(year = int(year) - 1, month = month, day = monthrange(int(year) - 1, month)[1])
            
            if month <= 8:
                date_mask = dt.datetime(year = int(year), month = month, day = monthrange(int(year), month)[1])
            
            df_monthly = df_year[df_year['EFFECTIVE_DATE'] <= date_mask].copy().reset_index(drop = True)
            
            df_monthly = df_monthly.groupby(by = ['year','Variety_Name', 'abm'], as_index = False).sum().reset_index(drop = True)
            
            # rename 
            df_monthly = df_monthly.rename(columns = {'order_Q': 'order_Q_month_' + str(month)})
            
            # merge with df_montly_total 
            df_monthly_total = df_monthly_total.merge(df_monthly, on = ['year', 'Variety_Name', 'abm'], how = 'left')
        
        dfs_monthly_netsales.append(df_monthly_total)
     
    Sales_monthly = pd.concat(dfs_monthly_netsales).reset_index(drop = True)    
    Sales_monthly = Sales_monthly.fillna(0)
    
    Sale_all = Sale_2012_2020_lagged.merge(Sales_monthly, on = ['year', 'Variety_Name', 'abm'], how = 'left')
    
    return  Sale_all

Sale_2012_2020, clean_Sale = read_sales_filepath()
Sale_2012_2020_lagged = create_lagged_sales(Sale_2012_2020)
Sale_all = create_monthly_sales(Sale_2012_2020_lagged, clean_Sale)
print("Sale's Structure: ", Sale_all.info())
df_save_path = r'/Users/gmtxy/OneDrive - Bayer/Model_Aggregation_0726/Sale_all.csv'
Sale_all.to_csv(df_save_path, index = False)
print("Check the fraction of missing values in Sales data: ", Sale_all.isna().sum())
print("Sale's shape: ", Sale_all.shape)



###### --------------------- Read Age & Trait Data -------------------- ######
def read_age_trait_filepath():
    """ Reads in and returns the age & trait data as a dataframe.
    
    Keyword arguments:
        None
    Returns:
        Age_Trait -- the dataframe of the age & trait data from 2012 to 2022
    """
    # read in the data
    Age_Trait_Address = r'/Users/gmtxy/OneDrive - Bayer/Model Prep/age_trait_map.csv'
    Age_Trait = pd.read_csv(Age_Trait_Address)
    
    # select required columns 
    AGE_TRAIT_COLUMN_NAMES = {'hybrid': 'Variety_Name'}
    Age_Trait = Age_Trait.rename(columns = AGE_TRAIT_COLUMN_NAMES)
    
    # set year as str
    Age_Trait['year'] = Age_Trait['year'].astype(str)
    return Age_Trait

Age_Trait = read_age_trait_filepath()
print("Age Trait's Structure: ", Age_Trait.info())
df_save_path = r'/Users/gmtxy/OneDrive - Bayer/Model_Aggregation_0726/Age_Trait.csv'
Age_Trait.to_csv(df_save_path, index = False)
print("Check the fraction of missing values in Age & Trait data: ", Age_Trait.isna().sum())
print("Age Trait's shape: ", Age_Trait.shape)

###### -------- Read Weather & County Location & FIPS_abm Data --------- ######
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
    for i in range(2012, 2021):
        print("Read ", str(i), " Weather Data")
        dfi_path = r'/Users/gmtxy/OneDrive - Bayer/Weather/county_data/Blizzard_{year}.csv'.format(year = i)
        dfi = pd.read_csv(dfi_path)
        
        # add the dataframe to the list
        dfs_path.append(dfi)
        
    # concate all dataframes  
    Weather_2012_2020 = pd.concat(dfs_path).reset_index(drop = True)
    
    County_Location_Address = r'/Users/gmtxy/OneDrive - Bayer/Weather/county_data/county_locations.csv'
    County_Location = pd.read_csv(County_Location_Address)
    
    FIPS_abm_Address = r'/Users/gmtxy/OneDrive - Bayer/Weather/county_data/abm_years.csv'
    FIPS_abm = pd.read_csv(FIPS_abm_Address)
    
    # set year as str
    Weather_2012_2020['year'] = Weather_2012_2020['year'].astype(str)
    FIPS_abm['year'] = FIPS_abm['year'].astype(str)
    
    # set fips to int in FIPS_abm
    FIPS_abm['fips'] = FIPS_abm['fips'].astype(int)
    
    return Weather_2012_2020, County_Location, FIPS_abm

Weather_2012_2020, County_Location, FIPS_abm = read_weather_filepath()
print("Weather's Structure: ", Weather_2012_2020.info())
print("County_Location's Structure: ", County_Location.info())
print("FIPS_abm's Structure: ", FIPS_abm.info())


def clean_Weather(df_weather, df_county_locations, df_fips):
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
    Weather = Weather_2012_2020.copy()
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
   
Weather = clean_Weather(Weather_2012_2020, County_Location, FIPS_abm) 
print("Weather's Structure: ", Weather.info())

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

Weather_Flattened = flatten_monthly_weather(Weather)

df_save_path = r'/Users/gmtxy/OneDrive - Bayer/Model_Aggregation_0726/Flattened_Weather_abm_fips.csv'
Weather_Flattened.to_csv(df_save_path, index = False)
print("Flattened Weather's Structure: ", Weather_Flattened.info())
print("Check the fraction of missing value in weather data: ", Weather_Flattened.isnull().sum())
print("Flattened Weather's shape: ", Weather_Flattened.shape)

###### ------------------- Read Commodity Price Data ------------------ ######
def read_commodity_corn_soybean():
    """Reads in the commodity data for both corn and soybeans.
    
    Keyword arguments:
        None
    Returns:
        Commodity_Corn -- the soybean commodity data to date
        Commodity_Soybean -- the corn commodity data to date
    """
    # read in soybean and commodity data
    Corn_Address = r'/Users/gmtxy/OneDrive - Bayer/Commodity Price/corn_to_02152021.csv'
    Soybean_Address = r'/Users/gmtxy/OneDrive - Bayer/Commodity Price/soybean_to_02152021.csv'
    
    Commodity_Corn = pd.read_csv(Corn_Address)
    Commodity_Soybean = pd.read_csv(Soybean_Address)
    
    return Commodity_Corn, Commodity_Soybean

Commodity_Corn, Commodity_Soybean = read_commodity_corn_soybean()

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
    
Commodity_Corn_Soybean = clean_commodity(Commodity_Corn, Commodity_Soybean)

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


CM_Soybean = create_commodity_features(Commodity_Corn_Soybean, 'soybean')
CM_Corn = create_commodity_features(Commodity_Corn_Soybean, 'corn')
# concatenate crops together
CM_Soybean_Corn = CM_Soybean.merge(CM_Corn, on=['year'])

def create_lagged_features(df_cm):
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

CM_lagged = create_lagged_features(CM_Soybean_Corn)
df_save_path = r'/Users/gmtxy/OneDrive - Bayer/Model_Aggregation_0726/CM_Soybean_Corn_Lagged.csv'
CM_lagged.to_csv(df_save_path, index = False)
print("Flattened Commodity_Price's Structure: ", CM_lagged.shape)

###### --------------------- Read Performance Data --------------------- ######
## State_County fips Files 
def read_performance():
    """ Reads in and returns the performance data as a dataframe.
    
    Keyword arguments:
        None
    Returns:
        Peformance_2011_2019 -- the dataframe of the performance data from 2011 to 2019
    """
    
    # create a list to store all H2H data
    dfs_path = []
    for i in range(2011, 2021):
        print("Read ", str(i), "H2H Data")
        dfi_path = r'/Users/gmtxy/OneDrive - Bayer/Performance/Combined_H2H{year}.csv'.format(year = i)
        dfi = pd.read_csv(dfi_path)
        
        # set a year parameter to be the year
        dfi['year'] = i
        dfs_path.append(dfi)
        
    # concatenate all H2H data
    Performance_2011_2019 = pd.concat(dfs_path)
    return Performance_2011_2019
Performance_2011_2019 = read_performance()

def read_state_county_fips():
    """ Reads in and returns the performance data as a dataframe.
    
    Keyword arguments:
        None
    Returns:
        State_fips -- the dataframe of state and fips info
        County_fips -- the dataframe of county and fips info 
    """
    State_fips_Address = r'/Users/gmtxy/OneDrive - Bayer/Performance/state-geocodes-v2018.xlsx'
    County_fips_Address = r'/Users/gmtxy/OneDrive - Bayer/Performance/all-geocodes-v2018.xlsx'
    State_fips = pd.read_excel(State_fips_Address, skiprows = 5)
    County_fips = pd.read_excel(County_fips_Address, skiprows = 4)
    return State_fips, County_fips

State_fips, County_fips = read_state_county_fips()


def clean_state_county(State_fips, County_fips, FIPS_abm):
    """ Reads in and returns the performance data as a dataframe.
    
    Keyword arguments:
        State_fips -- the dataframe of state and fips info
        County_fips -- the dataframe of county and fips info
        FIPS_abm - the dataframe of all fips w.r.t abm and year
    Returns:
        State_County_abm -- the dataframe of state_county, fips and abm info
    """
    
    ## clean state files
    # create a dictionary for state abbrev
    us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
    }
    
    # convert state full name to short abbrev, such as Illinois -> IL 
    state_abbrev = []
    for state in State_fips['Name']:
        if state in us_state_abbrev:
            state_abbrev.append(us_state_abbrev[state])
        else:
            state_abbrev.append("NA")
            
    # create the State variable to store state abbrev
    State_fips['State'] = state_abbrev
    
    # drop missing value
    State_fips = State_fips[State_fips['State'] != "NA"].reset_index()
    
    # select important columns and pad state fips codes: "1" -> "01"
    State_fips = State_fips[['Name','State (FIPS)', 'State']]
    State_fips['State (FIPS)'] = State_fips['State (FIPS)'].astype(str)
    State_fips['State (FIPS)'] = State_fips['State (FIPS)'].str.pad(width=2, side='left', fillchar='0')
    
    
    
    ## clean county files 
    # select important columns
    County_fips = County_fips[[ 'State Code (FIPS)', 'County Code (FIPS)', 'Area Name (including legal/statistical area description)']]
    
    # select information only about county
    Areas = County_fips['Area Name (including legal/statistical area description)']
    County = []
    isCounty = []
    for area in Areas:
        area_list = area.split(" ")
        level = area_list[-1]
        if level == "County":
            isCounty.append("Y")
        else:
            isCounty.append("N")
        area_modified = " ".join(area_list[:-1])
        County.append(area_modified)
    County_fips['County'] = County
    County_fips['isCounty'] = isCounty
    County_fips = County_fips[County_fips['isCounty'] == "Y"]
    
    # pad state and county fips codes and concatenate them together: "1" -> "01 (state) and "1" -> "001" (county) -> "01001"
    County_fips['State Code (FIPS)'] = County_fips['State Code (FIPS)'].astype(str).str.pad(width=2, side='left', fillchar='0')
    County_fips['County Code (FIPS)'] = County_fips['County Code (FIPS)'].astype(str).str.pad(width=3, side='left', fillchar='0')
    County_fips['fips'] = County_fips['State Code (FIPS)'] + County_fips['County Code (FIPS)']
    
    # rename fips columns in order to merge state and county fips dataframe together
    State_fips = State_fips.rename(columns = {'State (FIPS)':'State Code (FIPS)'})
    State_County_fips = State_fips.merge(County_fips, how = "outer", on = ['State Code (FIPS)'])
    State_County_fips = State_County_fips[['Name','State', 'County', 'fips']]
    
    # drop missing values
    State_County_fips = State_County_fips.dropna(how = 'any')

    # get abm level using fips
    FIPS_abm['fips'] = FIPS_abm['fips'].astype(int)
    FIPS_abm['year'] = FIPS_abm['year'].astype(str)
    FIPS_abm['fips'] = FIPS_abm['fips'].astype(str).str.pad(width=5, side='left', fillchar='0')
    State_County_abm = State_County_fips.merge(FIPS_abm, how = "left", on = ['fips'])
    State_County_abm = State_County_abm.dropna(how = 'any')
    
    # rename Columns
    State_County_abm = State_County_abm.rename(columns = {'Name':'state', 'State':'State_abbrev', 'County':'county'})
    State_County_abm = State_County_abm[['state', 'State_abbrev', 'county', 'fips', 'abm']]
    State_County_abm = State_County_abm.drop_duplicates()
    return State_County_abm

State_County_abm = clean_state_county(State_fips, County_fips, FIPS_abm)
df_save_path = r'/Users/gmtxy/OneDrive - Bayer/Model_Aggregation_0726/State_County_abm.csv'
State_County_abm.to_csv(df_save_path, index = False)
# Get abm level using fips

## H2H Files
def Performance_with_yield_adv(df_performance):
    """Creates yield advantage features
    
    Keyword arguments:
        df_performance -- the dataframe of the performance data
    Returns:
        Performance_abm -- the dataframe of the yield advantages
    """
    Performance_abm = df_performance.copy()
    
    # Create a yield advantage feature 
    Performance_abm['yield_adv'] = (Performance_abm['c_yield'] - Performance_abm['o_yield'])/Performance_abm['c_yield']

    Performance_abm = Performance_abm[
            np.isinf(Performance_abm['yield_adv']) == False].reset_index(drop=True)
    
    return Performance_abm
    
def adv_in_trait(df):
    """Aggregates the advantage feature for a given abm within the trait group
    of each product.
    
    Keyword arguments:
        df -- the dataframe of the performance data
    Returns:
        adv_within_trait -- the dataframe with the mean yield advantage
            within the abm and within a product's trait group
    """
    # in_trait: c_trait == o_trait
    in_trait = df[df['c_trait'] == df['o_trait']].reset_index(drop=True)
    
    # selected required columns
    in_trait_abbr = in_trait[['year', 'abm', 'c_hybrid', 'c_trait', 'yield_adv']]
    
    # get the average yield_adv for each abm, year, and product
    adv_within_trait = in_trait_abbr.groupby(by=['year', 'abm', 'c_trait',
                                                 'c_hybrid'], as_index=False).mean()
    adv_within_trait = adv_within_trait.rename(
            columns={'c_trait': 'trait',
                     'c_hybrid': 'hybrid',
                     'yield_adv': 'yield_adv_within_abm_by_trait_brand'})
    
    return adv_within_trait

def adv_outof_trait(df):
    """Aggregates the advantage feature for a given abm outside the trait group
    of each product.
    
    Keyword arguments:
        df -- the dataframe of the performance data
    Returns:
        adv_outside_trait -- the dataframe with the mean yield advantage
            within the abm and outside a product's trait group
    """
    
    # outof_trait: c_trait != o_trait
    outof_trait = df[df['c_trait'] != df['o_trait']].reset_index(drop=True)
    
    # selected required columns
    outof_trait_abbr = outof_trait[['year', 'abm', 'c_hybrid', 'c_trait', 'yield_adv']]
    
    # get the average yield_adv for each abm, year, and product
    adv_outof_trait = outof_trait_abbr.groupby(by=['year', 'abm', 'c_trait', 
                                                   'c_hybrid'], as_index=False).mean()
    adv_outof_trait = adv_outof_trait.rename(
            columns={'c_trait': 'trait',
                     'c_hybrid': 'hybrid',
                     'yield_adv': 'yield_adv_within_abm_outof_trait_brand'})
    
    return adv_outof_trait

def adv_overall(df):
    """Aggregates the advantage feature within a given abm for a product.
    
    Keyword arguments:
        df -- the dataframe  of the performance data
    Returns:
        adv_overall -- the dataframe with the mean yield advantage within the
            abm
    """
    
    # selected required columns
    adv_abbr = df[['year', 'abm', 'c_hybrid', 'c_trait', 'yield_adv']]
    
    # get the average yield_adv for each abm, year, and product
    adv_overall = adv_abbr.groupby(by=['year', 'abm', 'c_trait', 'c_hybrid'],
                                   as_index=False).mean()
    
    adv_overall = adv_overall.rename(
            columns={'c_trait': 'trait',
                     'c_hybrid': 'hybrid',
                     'yield_adv': 'yield_adv_with_abm_brand'})
    
    return adv_overall

def yield_aggregation(df):
    """Aggregates the yield by year/abm/hybrid.
    
    Keyword arguments:
        df -- the dataframe with the yield data
    Returns:
        df_with_yield
    """
    
    # selected required columns
    adv_abbr = df[['year', 'abm', 'c_hybrid', 'c_trait', 'c_yield']]
    
    df_with_yield = adv_abbr.groupby(by=['year', 'abm', 'c_hybrid', 'c_trait'],
                                     as_index=False).mean()
    df_with_yield = df_with_yield.rename(columns={'c_hybrid': 'hybrid',
                                                  'c_trait': 'trait',
                                                  'c_yield': 'yield'})
    return df_with_yield

def merge_advantages(df):
    """Merge yield advantage features, both inside and outside the ABMs.
    
    Keyword arguments:
        df -- the dataframe of the performance data
    Returns:
        adv_df -- the dataframe of the average yield and yield advantages,
            broken down by year, hybrid, trait, and abm. the trait is important
            as we'll use that for imputation
    """
    # aggregate within the abm by trait, out of trait, and overall, mirroring 
    # the df used for the previous years' models
    # TODO find out if we need to aggregate by brand as well
    Performance_yield_adv = df.copy()    
    Performance_in_trait = adv_in_trait(Performance_yield_adv)
    Performance_outof_trait = adv_outof_trait(Performance_yield_adv)
    Performance_overall = adv_overall(Performance_yield_adv)
    yield_overall = yield_aggregation(Performance_yield_adv)
    
    # merge the advantages together
    adv_df = Performance_in_trait.merge(Performance_outof_trait,
                                    on=['year', 'abm', 'trait', 'hybrid'])
    adv_df = adv_df.merge(Performance_overall,
                          on=['year', 'abm', 'trait', 'hybrid'])
    adv_df = adv_df.merge(yield_overall,
                          on=['year', 'abm', 'trait', 'hybrid'])
            
    return adv_df

def clean_performance(df):
    """Modify the performance w.r.t year and trait 
    
    Keyword arguments:
        df -- the dataframe of the performance data
    Returns:
        df -- the modified dataframe
    """
    # we are merging the performance data with the sales data by year + 1
    # ie, merging the sales data for 2019 with the yield data for 2018
    # so we add 1 to the year in the adv_features dataframe
    df['year'] = df['year'] + 1
    
    # replace the 'HT3' name with 'XF'
    df['trait'] = df['trait'].replace('HT3', 'XF')
    
    return df
    

def create_imputation_frames(df):
    """Creates dataframes at certain levels used to impute advantage features
    in the main dataframe.
    
    Keyword arguments:
        df -- the dataframe of advantage features
    Returns:
        product_abm_level -- the dataframe with a product/abm level 
        trait_abm_year_level -- the dataframe with a trait/abm/year level 
        abm_year_level -- the dataframe with an abm/year level 
        year_level -- the dataframe with a year level 
    """
    
    # create a product/abm level aggregation, as year-to-year variation is small
    # rename columns for each level 
    ABM_YEAR_LVL_COLS = {
        'yield_adv_within_abm_by_trait_brand': 'yield_adv_within_abm_by_trait_brand_AY',
        'yield_adv_within_abm_outof_trait_brand': 'yield_adv_within_abm_outof_trait_brand_AY',
        'yield_adv_with_abm_brand': 'yield_adv_with_abm_brand_AY',
        'yield': 'yield_AY'}
    
    PRODUCT_ABM_LVL_COLS = {
        'yield_adv_within_abm_by_trait_brand': 'yield_adv_within_abm_by_trait_brand_PA',
        'yield_adv_within_abm_outof_trait_brand': 'yield_adv_within_abm_outof_trait_brand_PA',
        'yield_adv_with_abm_brand': 'yield_adv_with_abm_brand_PA',
        'yield': 'yield_PA'}
    
    TRAIT_ABM_YEAR_LVL_COLS = {
        'yield_adv_within_abm_by_trait_brand': 'yield_adv_within_abm_by_trait_brand_TAY',
        'yield_adv_within_abm_outof_trait_brand': 'yield_adv_within_abm_outof_trait_brand_TAY',
        'yield_adv_with_abm_brand': 'yield_adv_with_abm_brand_TAY',
        'yield': 'yield_TAY'}
    
    YEAR_LVL_COLS = {
        'yield_adv_within_abm_by_trait_brand': 'yield_adv_within_abm_by_trait_brand_Y',
        'yield_adv_within_abm_outof_trait_brand': 'yield_adv_within_abm_outof_trait_brand_Y',
        'yield_adv_with_abm_brand': 'yield_adv_with_abm_brand_Y',
        'yield': 'yield_Y'}
    df['year'] = df['year'].astype(str)
    
    # create a product/abm level aggregation, as year-to-year variation is small
    product_abm_df = df.drop(columns=['year', 'trait'])
    product_abm_level = product_abm_df.groupby(by=['hybrid', 'abm'],
                                               as_index=False).mean()
    product_abm_level = product_abm_level.rename(columns=PRODUCT_ABM_LVL_COLS)
    
    # create a trait/abm/year level aggregation for if product/abm is unavailable
    trait_abm_year_df = df.drop(columns=['hybrid'])
    trait_abm_year_level = trait_abm_year_df.groupby(by=['trait', 'abm', 'year'],
                                                     as_index=False).mean()
    trait_abm_year_level = trait_abm_year_level.rename(columns=TRAIT_ABM_YEAR_LVL_COLS)
    
    # create an abm/year level aggregation for products without a trait value
    abm_year_df = df.drop(columns=['trait', 'hybrid'])
    abm_year_level = abm_year_df.groupby(by=['abm', 'year'],
                                         as_index=False).mean()
    abm_year_level = abm_year_level.rename(columns=ABM_YEAR_LVL_COLS)
    
    # create a year level aggregation so we have values for when we don't have 
    # data for a given abm in a given year
    year_df = df.drop(columns=['trait', 'hybrid', 'abm'])
    year_level = year_df.groupby(by=['year'], as_index=False).mean()
    year_level = year_level.rename(columns=YEAR_LVL_COLS)
    
    return product_abm_level, trait_abm_year_level, abm_year_level, year_level

def impute_h2h_data(df, product_abm_level, trait_abm_year_level, abm_year_level, year_level):
    """Imputes missing h2h data using the previously aggregated dfs.
    
    Keyword arguments:
        df -- the dataframe with the h2h data merged
        pa_level -- the values aggregated at the product/abm level for use when
            we have missing years
        tay_level -- the values aggregated at the trait/abm/year level for use when 
            we have missing products in a given abm
        ay_level -- the values aggregated at the abm/year level for when we don't have
            trait information for a product in a given abm for a given year
        y_level -- the values aggregated at the year level for when we don't have
            information for an abm for a given year
    Returns:
        df_imputed -- the dataframe with imputed h2h data
    """
    # the idea is to merge the aggregated frames onto the df, then set the actual adv
    # features to be a certain aggregated value based on the logic of what is missing:
    # if we have product data for an abm for some years but not others, use pa_level,
    # if we have the trait data for a given abm/year, tay_level values, etc. it's 
    # an order of preference: pa -> tay -> ay -> y
    H2H_AGG_LEVELS = ['_PA', '_TAY', '_AY', '_Y']
    YIELD_ADV_FEATURES = ['yield_adv_within_abm_by_trait_brand',
                      'yield_adv_within_abm_outof_trait_brand',
                      'yield_adv_with_abm_brand',
                      'yield']
    
    
    product_abm_level = product_abm_level.rename(columns = {'hybrid': 'Variety_Name'})
    df_with_pa = df.merge(product_abm_level, on=['abm', 'Variety_Name'], how='left')
    
    df_with_tay = df_with_pa.merge(trait_abm_year_level, on=['year', 'abm', 'trait'],
                                   how='left')
    
    df_with_ay = df_with_tay.merge(abm_year_level, on=['year', 'abm'], how='left')

    df_with_y = df_with_ay.merge(year_level, on=['year'], how='left')
    
    # the features we'll drop
    agg_features_to_drop = []
    
    # iterate through the levels in the order pa -> tay -> ay -> y
    for level in H2H_AGG_LEVELS:
    # iterate through the features we're going to impute
        for feature in YIELD_ADV_FEATURES:
            # replace missing values with the level of aggregation if it's available
            df_with_y.loc[((df_with_y[feature].isnull() == True) & 
                           (df_with_y[feature + level].isnull() == False)),
        feature] = df_with_y.loc[((df_with_y[feature].isnull() == True) & 
               (df_with_y[feature + level].isnull() == False)), feature + level]
        
            # append the feature + level to drop
            agg_features_to_drop.append(feature + level)
    
    print(agg_features_to_drop)
    df_imputed = df_with_y.drop(columns=agg_features_to_drop)
    
    return df_imputed

###### ---------------- Read Consensus Forecasting Data ---------------- ######

def read_concensus_forecasting():
    """ Reads in and returns the concensus forecasting data as a dataframe.
    
    Keyword arguments:
        None
    Returns:
        CF_2016_2022 -- the dataframe of CF data from 2016 to 2022
    """
    
    # read in data
    CF_2016_2020_Address = r'/Users/gmtxy/OneDrive - Bayer/cf/FY16_20_soybean.csv'
    CF_2021_2022_Address = r'/Users/gmtxy/OneDrive - Bayer/cf/FY22_01_14_21.csv'
    CF_2016_2020 = pd.read_csv(CF_2016_2020_Address)
    CF_2021_2022 = pd.read_csv(CF_2021_2022_Address)
    
    # select required columns 
    selected_columns = ['FORECAST_YEAR', 'CROP_DESCR', 'BRAND_GROUP', 'ACRONYM_NAME',
       'TEAM_KEY', 'TEAM_Y1_FCST_1']
    CF_2016_2020 = CF_2016_2020[selected_columns]
    CF_2021_2022 = CF_2021_2022[selected_columns]
    
    # concatenate two dataframe 
    CF_2016_2022 = pd.concat([CF_2016_2020, CF_2021_2022])
    
    # subset crop_descr  = soybean and brand_group = ASGROW
    CF_2016_2022 = CF_2016_2022[(CF_2016_2022['CROP_DESCR'] == 'SOYBEAN') & 
                                (CF_2016_2022['BRAND_GROUP'] == 'ASGROW')]
    
    # drop unnecessary columns 
    dropped_cols = ['CROP_DESCR','BRAND_GROUP']
    CF_2016_2022 = CF_2016_2022.drop(columns = dropped_cols)
    
    # rename columns in order to merge
    CF_2016_2022 = CF_2016_2022.rename(columns = {'FORECAST_YEAR':'year',
                                                  'ACRONYM_NAME':'Variety_Name'})
    # convert year to str
    CF_2016_2022['year'] = CF_2016_2022['year'].astype(int).astype(str)
    
    # drop missing value 
    CF_2016_2022 = CF_2016_2022.dropna(how = 'any')
    return CF_2016_2022

CF_2016_2022 = read_concensus_forecasting()
print("Concensus Forecasting data's Structure: ", CF_2016_2022.info())
print("Checking the fraction of missing value: ", CF_2016_2022.isna().sum()/CF_2016_2022.shape[0])
df_save_path = r'/Users/gmtxy/OneDrive - Bayer/Model_Aggregation_0726/CF_2016_2022.csv'
CF_2016_2022.to_csv(df_save_path, index = False)

def merge_cf_with_abm(df_cf, df_abm_key):
    """ Reads in and returns the abm and teamkey data as a dataframe.
    
    Keyword arguments:
        df_cf -- the dataframe of cf data
        df_abm_key -- the dataframe of mapping teamkay to abm 
    Returns:
        CF_abm -- the dataframe of CF data at the abm level  
    """
    # merge CF data with abm 
    CF_abm = df_cf.merge(df_abm_key, how = 'left', on = ['TEAM_KEY'])
    
    # drop missing value
    CF_abm = CF_abm.dropna(how = 'any')

    # select required columns
    CF_abm = CF_abm[['year', 'Variety_Name', 'abm', 'TEAM_Y1_FCST_1']]
    
    return CF_abm

CF_abm = merge_cf_with_abm(CF_2016_2022, abm_Teamkey)

###### --------------------------- Read SRP  ------------------------- ######
def read_SRP():
    """Reads in the SRP data.
    
    Keyword arguments:
        None
    Returns: 
        SRP_2011_2020 -- the fully concatenated SRP values from 2011 to 2020
    """
    
    # define a list to store all SRP data
    dfs_path = []
    
    # read in the historical data and concatenate each file from 2011 to 2019
    for year in range(2011, 2020):
        print("Read ", str(year), "SRP Data")
        dfi_path = r'/Users/gmtxy/OneDrive - Bayer/SRP/historical_SRP/{year}_SRP.csv'.format(year = year)
        
        dfi = pd.read_csv(dfi_path)
        
        # set a year parameter to be the year 
        dfi['year'] = year
        dfs_path.append(dfi)
    
    # concatenate all dataframes    
    SRP_2011_2019 = pd.concat(dfs_path).reset_index(drop=True)
    
    # select required columns
    SRP_2011_2019 = SRP_2011_2019[['year', 'VARIETY', 'SRP']]
    
    # rename the columns 
    SRP_2011_2019 = SRP_2011_2019.rename(columns={'VARIETY': 'Variety_Name'})
    
    
    # remove any leading or trailing spaces as well as dollar signs
    SRP_2011_2019['SRP'] = SRP_2011_2019['SRP'].str.strip().str.replace('$','')
    
    # remove any null values
    SRP_2011_2019 = SRP_2011_2019[SRP_2011_2019['SRP'] != '-']
    SRP_2011_2019 = SRP_2011_2019.dropna(how = 'any').reset_index(drop = True)
    
    # drop any duplicates
    SRP_2011_2019 = SRP_2011_2019.drop_duplicates().reset_index(drop = True)
    
    # set year as str, SRP as float
    SRP_2011_2019['year'] = SRP_2011_2019['year'].astype(str)
    SRP_2011_2019['SRP'] = SRP_2011_2019['SRP'].astype(float)
    
    # read 2020 SRP data 
    df2020_path = r'/Users/gmtxy/OneDrive - Bayer/SRP/historical_SRP/2020_SRP.csv'
    SRP_2020 = pd.read_csv(df2020_path)
    # add a year column
    SRP_2020['year'] = '2020'
    # set price as float 
    SRP_2020['Price'] = SRP_2020['Price'].astype(float)
    # rename columns
    SRP_2020 = SRP_2020.rename(columns = {'Product':'Variety_Name', 'Price':'SRP'})
    
    SRP_2011_2020 = pd.concat([SRP_2011_2019, SRP_2020])
    
    return SRP_2011_2020

read_SRP()

def impute_SRP(df):
    """Imputes missing SRP values.
    
    Keyword arguments:
        df -- the dataframe with the sales data
    Returns:
        df_imputed -- the dataframe with imputed SRP values
    """
    # create a dataframe of the real-valued SRP values and grab SRP, trait, and year
    SRP_real = df[df['SRP'].isnull() == False].reset_index(drop=True)
    SRP_real = SRP_real[['year', 'trait', 'SRP']]
    
    # aggregate the SRP_real based on trait/year and year
    SRP_trait_year = SRP_real.groupby(by=['year', 'trait'], as_index=False).mean()
    SRP_year = SRP_real[['year', 'SRP']].groupby(by=['year'], as_index=False).mean()
    
    # merge these two back into the main df
    SRP_trait_year = SRP_trait_year.rename(columns={'SRP': 'SRP_ty'})
    SRP_year = SRP_year.rename(columns={'SRP': 'SRP_y'})
    
    df_imputed = df.merge(SRP_trait_year, on=['year', 'trait'], how='left')
    df_imputed = df_imputed.merge(SRP_year, on=['year'], how='left')
    
    # set missing values to be the aggregated values. trait/year if it is available,
    # just by year if not
    df_imputed.loc[((df_imputed['SRP'].isnull() == True) & 
                    (df_imputed['SRP_ty'].isnull() == False)), 
    'SRP'] = df_imputed.loc[((df_imputed['SRP'].isnull() == True) & 
         (df_imputed['SRP_ty'].isnull() == False)), 'SRP_ty']
    
    df_imputed.loc[((df_imputed['SRP'].isnull() == True) & 
                    (df_imputed['SRP_y'].isnull() == False)), 
    'SRP'] = df_imputed.loc[((df_imputed['SRP'].isnull() == True) & 
         (df_imputed['SRP_y'].isnull() == False)), 'SRP_y']
    
    # drop the aggregated values, leaving just the SRP column with the newly
    # imputed values
    df_imputed = df_imputed.drop(columns=['SRP_ty', 'SRP_y'])

    return df_imputed

###### --------------------------- Read trait map  ------------------------- ######
def read_soybean_trait_map():
    """Reads in the SRP data.
    
    Keyword arguments:
        None
    Returns: 
        trait_map -- the trait_map for encoding 
    """
    Address_trait_map = r'/Users/gmtxy/Downloads/soybean_trait_map_xf.csv'
    trait_map = pd.read_csv(Address_trait_map)
    
    trait_map = trait_map.fillna(0)
    return trait_map

trait_map = read_soybean_trait_map()
SRP_2011_2019 = read_SRP()

###### ----------------------- Merge All Datasets ---------------------- ######
def merge_all():
    """ Reads in and returns the final combined dataframe.
    
    Keyword arguments:
        None
    Returns:
        Sale_HP_trait_weather_CM_Performance_CF -- the dataframe of sales, 
                                                    hot products, trait/age, 
                                                    weather, commoidty price, 
                                                    concensus forecasting data
    """
    # ## Merge Sale with HP
    # print("Step 1: Merge Sale_2012_2019 with Hot Products......")
    # Sale_HP = Sale_2012_2019_lagged.merge(Hot_Products, how = 'left', on = ['year', 'Variety_Name'])
    # #print("Step 1: Sale_HP's structure: ", Sale_HP.info())
    # print("Step 1: Sale_HP's shape: ", Sale_HP.shape)
    # print("..................")
    
    ## Merge Sale_HP with age_trait 
    print("Step 2: Merge Sale_HP with Age_Trait......")
    Sale_HP_trait = Sale_all.merge(Age_Trait, how = 'left', on = ['year', 'Variety_Name'])
    #print("Step 2: Sale_HP_trait's structure: ", Sale_HP_trait.info())
    print("Step 2: Sale_HP_trait's shape: ", Sale_HP_trait.shape)
    print("..................")
    
    
    ## Merge Sale_HP_trait with weather_flattened
    print("Step 3: Merge Sale_HP_trait with Weather_flattened......")
    Sale_HP_trait_weather = Sale_HP_trait.merge(Weather_Flattened, how = 'left', on = ['year', 'abm'])
    #print("Step 3: Sale_HP_trait_weather's structure: ", Sale_HP_trait_weather.info())
    print("Step 3: Sale_HP_trait_weather's shape: ", Sale_HP_trait_weather.shape)
    print("..................")
    
    ## Merge Sale_HP_trait_weather with CM_Soybean_Corn
    print("Step 4: Merge Sale_HP_trait with CM_lagged_soybean_corn......")
    Sale_HP_trait_weather_CM = Sale_HP_trait_weather.merge(CM_lagged, how = 'left', on = ['year'])
    print("Step 4: Sale_HP_trait_weather_CM's shape: ", Sale_HP_trait_weather_CM.shape)
    print("..................")
    
    
    ## Merge Sale_HP_trait_weather_CM with the Performance
    print("Step 5: Merge Sale_HP_trait_weather_CM with Performance......")

    Performance_abm = Performance_2011_2019.merge(State_County_abm, how = 'left', on = ['state', 'county'])
    Performance_yield_adv = Performance_with_yield_adv(Performance_abm)
    Performance_adv = merge_advantages(Performance_yield_adv)
    Performance_adv = clean_performance(Performance_adv)
    df_save_path = r'/Users/gmtxy/OneDrive - Bayer/Model_Aggregation_0726/Performance_adv.csv'
    Performance_adv.to_csv(df_save_path, index = False)
    
    product_abm_level, trait_abm_year_level, abm_year_level, year_level = create_imputation_frames(
                df=Performance_adv)
    # rename hybrid columns
    Performance_adv1 = Performance_adv.rename(columns = {'hybrid': 'Variety_Name'})
    # drop trait columns
    Performance_adv1 = Performance_adv1.drop(columns=['trait'])
    Performance_adv1['year'] = Performance_adv1['year'].astype(str)
    
    Sale_HP_trait_weather_CM_Performance = Sale_HP_trait_weather_CM.merge(Performance_adv1,
                                        on=['year', 'abm', 'Variety_Name'],
                                        how='left')
    # impute the missing value 
    Sale_HP_trait_weather_CM_Performance = impute_h2h_data(Sale_HP_trait_weather_CM_Performance, 
                                                            product_abm_level, trait_abm_year_level,
                                                            abm_year_level, year_level)
    
    # replace any blank trait values with "Conventional"
    Sale_HP_trait_weather_CM_Performance['trait'] = Sale_HP_trait_weather_CM_Performance['trait'].fillna('Conventional')
    print("Step 5: Sale_HP_trait_weather_CM's shape: ", Sale_HP_trait_weather_CM_Performance.shape)
    print("..................")
    
    df_save_path = r'/Users/gmtxy/OneDrive - Bayer/Model_Aggregation_0726/Sale_HP_trait_weather_CM_Performance.csv'
    Sale_HP_trait_weather_CM_Performance.to_csv(df_save_path, index = False)
    
    ## Merge Sale_HP_trait_weather_CM_Performance with Concensus Forecasting
    print("Step 6: Merge Sale_HP_trait_weather_CM with CF......")
    Sale_HP_trait_weather_CM_Performance_CF = Sale_HP_trait_weather_CM_Performance.merge(CF_abm, how = 'left', on = ['year','Variety_Name','abm'])
    print("Step 6: Sale_HP_trait_weather_CM's shape: ", Sale_HP_trait_weather_CM_Performance_CF.shape)
    print("..................")
    
    print("Checking the portion of missing value in the combined dataset: ")
    Sale_HP_trait_weather_CM_Performance_CF.isna().sum()/Sale_HP_trait_weather_CM_Performance_CF.shape[0]
    Sale_HP_trait_weather_CM_Performance_CF['TEAM_Y1_FCST_1'] = Sale_HP_trait_weather_CM_Performance_CF['TEAM_Y1_FCST_1'].fillna(0)
    
    print("Step 7: Merge Sale_HP_trait_weather_CM_CF with SRP......")
    Sale_HP_trait_weather_CM_Performance_CF_SRP = Sale_HP_trait_weather_CM_Performance_CF.merge(SRP_2011_2019, how = 'left', on = ['year', 'Variety_Name'])
    # impute the missing value
    Sale_HP_trait_weather_CM_Performance_CF_SRP = impute_SRP(Sale_HP_trait_weather_CM_Performance_CF_SRP)
    print("Step 7: Sale_HP_trait_weather_CM_SRP's shape: ", Sale_HP_trait_weather_CM_Performance_CF_SRP.shape)
    print("..................")
    
    
    
    print("Saving Files.........")
    df_save_path = r'/Users/gmtxy/OneDrive - Bayer/Model_Aggregation_0726/Sale_HP_trait_weather_CM_Performance_CF_SRP.csv'
    Sale_HP_trait_weather_CM_Performance_CF_SRP.to_csv(df_save_path, index = False)

    return Sale_HP_trait_weather_CM_Performance_CF_SRP

Sale_HP_trait_weather_CM_Performance_CF_SRP = merge_all()

# encoding trait
print("Step 8: Encoding trait")
Final_df = Sale_HP_trait_weather_CM_Performance_CF_SRP.merge(trait_map, how = 'left', on = ['trait']) 
Final_df = Final_df.drop(columns = ['trait'])
df_save_path = r'/Users/gmtxy/OneDrive - Bayer/Model_Aggregation_0726/Final_order_df_2012_2020.csv'
Final_df.to_csv(df_save_path, index = False)
Final_df.isna().sum()

Final_df.isna().sum().sum()


Final_df
