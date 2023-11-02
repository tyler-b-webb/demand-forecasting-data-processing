#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 16:14:47 2023

@author: epnzv
"""

import pandas as pd
import re

from channel_config import (CORN_H2H_DIR, CORN_MPI_HIST_COLS, CORN_MPI_NEW_COLS,
                            CORN_MPI_FEATURE_NAMES, DATA_DIR, MPI_DIR, PERFORMANCE_COLS,
                            SOYBEAN_H2H_DIR, SOYBEAN_MPI_HIST_COLS,
                            SOYBEAN_MPI_FEATURE_NAMES)


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


def corn_trait_processing(df):
    """Does additional processing on corn traits, stuff not covered by just 
    stripping digits and merging on the map.
    
    Keyword arguments:
        df -- the dataframe with hybrid names
    Returns:
        df_w_processed_traits -- the dataframe with the processed traits
    """
    df_w_processed_traits = df.copy()
    
    # now a bunch of assignment statements
    df_w_processed_traits.loc[
            df_w_processed_traits['hybrid'].str.contains('VT2P'), 'trait'] = 'VT2P'
    
    df_w_processed_traits.loc[
            df_w_processed_traits['hybrid'].str.contains('VT3P'), 'trait'] = 'VT3P'   

    df_w_processed_traits.loc[
            df_w_processed_traits['hybrid'].str.contains('VT4P'), 'trait'] = 'VT4P'
    
    # set a RIB trait depending on the hybrid name
    df_w_processed_traits['RIB'] = 0 
    
    df_w_processed_traits.loc[
            df_w_processed_traits['hybrid'].str.contains('RIB'), 'RIB'] = 1
            
    return df_w_processed_traits


def generate_adv(df):
    """Generates yield advantage features from raw performance data.
    
    Keyword arguments:
        df -- the performance data with the abm feature added
    Returns:
        performance_w_adv -- the performance dataframe with all the advantage features
    """
    # generate advantages
    df['yield_adv'] = (
            df['c_yield'] - df['o_yield'])
    
    performance_channel_abm_subset = df[[
            'year', 'abm', 'c_trait', 'o_trait', 'c_hybrid', 'c_yield', 'yield_adv']]
    
    # aggregate advantages accordingly
    performance_channel_yield = yield_aggregation(df=performance_channel_abm_subset)
    
    performance_channel_in_trait = adv_in_trait(df=performance_channel_abm_subset)
    
    performance_channel_out_of_trait = adv_outof_trait(df=performance_channel_abm_subset)
    
    performance_channel_overall = adv_overall(df=performance_channel_abm_subset)
    
    # merge them all together
    performance_w_adv = performance_channel_yield.merge(performance_channel_in_trait,
                                                        on=['year', 'abm', 'trait', 'hybrid'],
                                                        how='left')
    performance_w_adv = performance_w_adv.merge(performance_channel_out_of_trait,
                                                on=['year', 'abm', 'trait', 'hybrid'],
                                                how='left')
    
    performance_w_adv = performance_w_adv.merge(performance_channel_overall,
                                                on=['year', 'abm', 'trait', 'hybrid'],
                                                how='left')

    return performance_w_adv


def generate_age_trait_RM(df, crop, RM=False):
    """
    Generates the age and RM features from a sales dataframe.
    
    Keyword arguments:
        df -- the Channel sales dataframe
        crop -- the crop we're getting data for
        RM -- whether we're calculating RM
    Returns:
        df_age_RM -- the Channel sales dataframe 
    """
    age_RM_map = pd.DataFrame()
    
    # go hybrid by hybrid
    for hybrid in df['hybrid'].unique():
        
        # age generation
        single_hybrid = df.loc[
                df['hybrid'] == hybrid, ['hybrid', 'year']].drop_duplicates().reset_index(drop=True)
        
        single_hybrid['first_year'] = min(single_hybrid['year'].unique()) 
        single_hybrid['age'] = single_hybrid['year'] - single_hybrid['first_year'] + 1
        
        single_hybrid = single_hybrid.drop(columns=['first_year'])
        
        # stripp all characters out of the name
        hybrid_stripped = re.sub("[^0-9]", "", hybrid)
        
        if len(hybrid_stripped) == 0:
            continue
        
        # RM generation
        if RM == True:            
            if crop=='SOYBEAN':
                RM_digits = hybrid_stripped[:2]
                if len(RM_digits) < 2:
                    continue
                if RM_digits != '00':
                    RM_digits_point = RM_digits[0] + '.' + RM_digits[1]
                    single_hybrid['RM'] = float(RM_digits_point)
                else:
                    RM_digits_long = hybrid_stripped[:3]
                    RM_digits_point = RM_digits_long[1] + '.' + RM_digits_long[2]
                    single_hybrid['RM'] = float(RM_digits_point) - 1
    
            elif crop=='CORN':              
                RM_digits = hybrid_stripped[:3]
                #print(hybrid)
                #print(RM_digits)
                single_hybrid['RM'] = int(RM_digits) - 100
            
        # concat the map
        if age_RM_map.empty == True:
            age_RM_map = single_hybrid.copy()
        else:
            age_RM_map = pd.concat([age_RM_map, single_hybrid]).reset_index(drop=True)
    
    df_age_RM = df.merge(age_RM_map, on=['year', 'hybrid'], how='left')
    
    # drop any RMs that are blank (vanishingly few, weird hybrid names)
    df_age_RM = df_age_RM[df_age_RM['RM'].isna() == False]
    
    # merge in the trait map
    if crop == 'SOYBEAN':
        trait_map = pd.read_csv('soybean_hybrid_trait_map.csv')
        
    elif crop == 'CORN':
        trait_map = pd.read_csv('corn_hybrid_trait_map.csv')
        
    
    df_age_RM['hybrid_re'] = df_age_RM['hybrid'].str.replace('\d+', '')
    
    df_age_trait_RM = df_age_RM.merge(trait_map, on=['hybrid_re'], how='left').drop(columns=['hybrid_re'])
    
    # if the crop is corn, some more wrangling has to be done
    if crop == 'CORN':
         df_age_trait_RM = corn_trait_processing(df=df_age_trait_RM)    
    
    df_age_trait_RM['trait'] = df_age_trait_RM['trait'].fillna('CONV')
    
    # merge the trait_decomposition map onto the dfs
    if crop == 'CORN':
        trait_decomp = pd.read_csv('corn_trait_decomp.csv')
    elif crop == 'SOYBEAN':
        trait_decomp = pd.read_csv('soybean_trait_decomp.csv')
        
    df_age_trait_RM = df_age_trait_RM.merge(trait_decomp, on=['trait'], how='left')
    
    return df_age_trait_RM


def germplasm_imputation(df):
    """Imputes based on the numbers (germplasm) for corn.
    
    Keyword arguments:
        df -- the dataframe with the non-imputed adv data
    Returns:
        df_germplasm_imputed -- the dataframe with the germplasm imputed data
    """
    df_germplasm_imputed = df.copy()
    
    # get the germplasm numbers
    df_germplasm_imputed['hybrid_re'] = df_germplasm_imputed['hybrid'].str.replace('\D', '')
    
    
    imputed_cols = ['year', 'abm', 'hybrid_re'] + PERFORMANCE_COLS
    df_germplasm_imputed_subset = df_germplasm_imputed[imputed_cols]
    
    df_germplasm_impute_source = df_germplasm_imputed_subset[['year', 'abm', 'hybrid_re'] + PERFORMANCE_COLS]
    
    # grab stuff with data sources
    df_germplasm_impute_source = df_germplasm_impute_source.dropna().reset_index(drop=True)
    
    df_germplasm_impute_source.groupby(by=['year', 'abm', 'hybrid_re']).mean().reset_index(drop=True)
    
    # rename columns
    for col in PERFORMANCE_COLS:
        df_germplasm_impute_source = df_germplasm_impute_source.rename(columns={col: col + '_germ'})
    
    df_germplasm_imputed = df_germplasm_imputed.merge(df_germplasm_impute_source,
                                                      on=['year', 'abm', 'hybrid_re'],
                                                      how='left')
    
    for col in PERFORMANCE_COLS:
        df_germplasm_imputed.loc[df_germplasm_imputed[col].isna(), col] = df_germplasm_imputed.loc[
                df_germplasm_imputed[col].isna(), col + '_germ']
        
        df_germplasm_imputed = df_germplasm_imputed.drop(columns=[col + '_germ'])
        
    df_germplasm_imputed = df_germplasm_imputed.drop(columns=['hybrid_re'])
    
    return df_germplasm_imputed


def merge_MPI_data(df, crop):
    """Reads in and merges MPI data.
    
    Keyword arguments:
        df -- the "master" dataframe we're merging onto
        crop -- the crop
    Returns:
        df_w_MPI -- the master df with the MPI data merged
    """
    # read in the dataframes
    MPI_hist_data = pd.read_excel(DATA_DIR + MPI_DIR + 'Channel_' + crop.capitalize() + '.xlsx')
    
    # grab the columns
    if crop == 'CORN':
        hist_cols = CORN_MPI_HIST_COLS
        new_cols = CORN_MPI_NEW_COLS
        feature_names = CORN_MPI_FEATURE_NAMES
        
    elif crop == 'SOYBEAN':
        hist_cols = SOYBEAN_MPI_HIST_COLS
        feature_names = SOYBEAN_MPI_FEATURE_NAMES
    
    MPI_hist_data_subset = MPI_hist_data[hist_cols]
    
    
    # rename the features
    MPI_hist_rename = {}
    
    
    for i in range(0, len(hist_cols)):
        hist_orig = hist_cols[i]
        
        feature_name = feature_names[i]
        
        #  log the dict entries
        MPI_hist_rename[hist_orig] = feature_name
    
    # rename the columns
    MPI_hist_data_subset = MPI_hist_data_subset.rename(columns=MPI_hist_rename)
    
    
    # if we have/need new data, put it here
    if crop == 'CORN':
        MPI_new_data = pd.read_csv(DATA_DIR + MPI_DIR + 'CHANNEL_' + crop + '_NEW_PIM.csv')
        MPI_new_data_subset = MPI_new_data[new_cols]
        MPI_new_rename = {}
    
        for i in range(0, len(hist_cols)):
            new_orig = new_cols[i]
            feature_name = feature_names[i]
        
            #  log the dict entries
            MPI_new_rename[new_orig] = feature_name
            
        MPI_new_data_subset = MPI_new_data_subset.rename(columns=MPI_new_rename)

        # concat the dataframes
        MPI_df = pd.concat([MPI_hist_data_subset, MPI_new_data_subset]).reset_index(drop=True)

    else:
        MPI_df = MPI_hist_data_subset

    # drop null hybrids
    MPI_df = MPI_df[MPI_df['hybrid'].isna() == False].reset_index(drop=True)
    
    # drop duplicate hybrids
    MPI_df = MPI_df.drop_duplicates(subset=['hybrid']).reset_index(drop=True)
    
    # merge with the dataframe
    df_w_MPI = df.merge(MPI_df, on=['hybrid'], how='left')

    # fill nas with 0s
    df_w_MPI = df_w_MPI.fillna(0)    
    
    # replace '-' with 0s
    df_w_MPI = df_w_MPI.replace('-', 0)
    
    return df_w_MPI


def merge_performance_data(df, abm_map, crop):
    """Reads in, processes, and merges the H2H data for Channel.
    
    Keyword arguments:
        df -- the "master" dataframe we're merging onto
    Returns:
        df_w_performance -- the master df with the performance data merged
    """
    if crop == 'SOYBEAN':
        h2h_dir = SOYBEAN_H2H_DIR
    elif crop == 'CORN':
        h2h_dir = CORN_H2H_DIR
        
    dfs_path = []    
    for i in range(2011, 2023):
        print("Read ", str(i), "H2H Data")
        dfi_path = DATA_DIR + h2h_dir + 'Combined_H2H'+ str(i) + '.csv'
        dfi = pd.read_csv(dfi_path)
        dfi['year'] = i + 1
        
        # grab just the channel data
        dfi = dfi[dfi['c_brand'] == 'CHANNEL'].reset_index(drop=True)

        dfs_path.append(dfi)
            
    # 2024 data 
    dfi_24 = dfi.copy()
    dfi['year'] = dfi['year'] + 1
    dfs_path.append(dfi_24)
    
    # concatenate all H2H data
    performance_channel = pd.concat(dfs_path)
    
    # merge abm 
    fips_county_map = pd.read_csv('State_County_abm.csv')
    fips_county_map = fips_county_map[['state', 'county', 'fips']]
    
    performance_channel_fips = performance_channel.merge(fips_county_map,
                                                        on=['state', 'county'],
                                                        how='left')
    
    performance_channel_abm = performance_channel_fips.merge(abm_map,
                                                             on=['fips'],
                                                             how='left')
    
    # generate advantages
    performance_w_adv = generate_adv(df=performance_channel_abm)
    
    # merge with main df
    df_w_performance = df.merge(performance_w_adv.drop(columns=['trait']),
                                on=['year', 'abm', 'hybrid'],
                                how='left')
    
    # germplasm imputation
    df_w_performance_imputed = performance_imputation(df=df_w_performance, crop='CORN')
    
    return df_w_performance_imputed


def performance_imputation(df, crop):
    """Imputes the yield adv based on ABM and year
    
    Keyword arguments:
        df -- the dataframe with the yield advantages
        crop -- the crop we're imputing
    Returns:
        df_imputed -- the dataframe with the imputed 
    """    
    if crop=='CORN':
        df_imputed = germplasm_imputation(df=df)
    else:
        df_imputed = df.copy()
    
    df_imputed = df.copy()
    
    non_nan = df_imputed.dropna().reset_index(drop=True)
    
    performance_subset = non_nan[
            ['abm', 'year', 'trait', 'TEAM_Y1_FCST_2'] + PERFORMANCE_COLS]
    
    for col in PERFORMANCE_COLS:
        performance_subset[col + '_weighted'] = (
                performance_subset[col] * performance_subset['TEAM_Y1_FCST_2'])
    
    # group by relevant agg levels
    performance_subset_tay = performance_subset.groupby(by=['year', 'abm', 'trait']).sum()
    performance_subset_ay = performance_subset.groupby(by=['year', 'abm']).sum()
    performance_subset_y = performance_subset.groupby(by=['year']).sum()
    
    # calculate the weighted averages and drop the weighted columns
    for col in PERFORMANCE_COLS:
        performance_subset_tay[col + '_tay'] =  (
                performance_subset_tay[col + '_weighted'] / performance_subset_tay['TEAM_Y1_FCST_2'])
        performance_subset_ay[col + '_ay'] =  (
                performance_subset_ay[col + '_weighted'] / performance_subset_ay['TEAM_Y1_FCST_2'])
        performance_subset_y[col + '_y'] = (
                performance_subset_y[col + '_weighted'] / performance_subset_y['TEAM_Y1_FCST_2'])
        
        performance_subset_tay = performance_subset_tay.drop(columns=[col, col + '_weighted'])
        performance_subset_ay = performance_subset_ay.drop(columns=[col, col + '_weighted'])
        performance_subset_y = performance_subset_y.drop(columns=[col, col + '_weighted'])
    
    performance_subset_tay = performance_subset_tay.drop(columns=['TEAM_Y1_FCST_2'])
    performance_subset_ay = performance_subset_ay.drop(columns=['TEAM_Y1_FCST_2'])
    performance_subset_y = performance_subset_y.drop(columns=['TEAM_Y1_FCST_2'])
    
    df_imputed = df_imputed.merge(performance_subset_tay, on=['year', 'abm', 'trait'], how='left')
    df_imputed = df_imputed.merge(performance_subset_ay, on=['year', 'abm'], how='left')
    df_imputed = df_imputed.merge(performance_subset_y, on=['year'], how='left')
    
    # assignment statements in tay -> ay -> y preferential order
    for col in PERFORMANCE_COLS:
        df_imputed.loc[df_imputed[col].isna(), col] = df_imputed.loc[df_imputed[col].isna(), col + '_tay']
        df_imputed.loc[df_imputed[col].isna(), col] = df_imputed.loc[df_imputed[col].isna(), col + '_ay']
        df_imputed.loc[df_imputed[col].isna(), col] = df_imputed.loc[df_imputed[col].isna(), col + '_y']
        
        df_imputed = df_imputed.drop(columns=[col + '_tay', col + '_ay', col + '_y'])
    
    return df_imputed


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