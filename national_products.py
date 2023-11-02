#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 14:26:32 2023

@author: epnzv
"""

import pandas as pd
import re

from national_config import (ADV_FILE, DATA_DIR, MPI_CORN_HIST_COLS,
                             MPI_SOYBEAN_HIST_COLS_TO_DROP, PERFORMANCE_COLS,
                             SRP_DIR, SRP_YEARS)

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
        trait_map = pd.read_csv('../national_soybean_hybrid_trait_map.csv')
        
    elif crop == 'CORN':
        trait_map = pd.read_csv('../corn_hybrid_trait_map.csv')
        
    
    df_age_RM['hybrid_re'] = df_age_RM['hybrid'].str.replace('\d+', '')
    
    df_age_trait_RM = df_age_RM.merge(trait_map, on=['hybrid_re'], how='left').drop(columns=['hybrid_re'])
    
    # if the crop is corn, some more wrangling has to be done
    if crop == 'CORN':
         df_age_trait_RM = corn_trait_processing(df=df_age_trait_RM)    
    
    df_age_trait_RM['trait'] = df_age_trait_RM['trait'].fillna('CONV')
    
    # merge the trait_decomposition map onto the dfs
    if crop == 'CORN':
        trait_decomp = pd.read_csv('../corn_trait_decomp.csv')
    elif crop == 'SOYBEAN':
        trait_decomp = pd.read_csv('../soybean_trait_decomp.csv')
        
    df_age_trait_RM = df_age_trait_RM.merge(trait_decomp, on=['trait'], how='left')
    
    return df_age_trait_RM, df_age_RM


def impute_SRP(df):
    """Imputes SRP data.
    
    Keyword arguments:
        df -- the dataframe
    Returns:
        df_imputed -- the imputed_dataframe
    """
    df_no_missing = df[df['SRP'].isna() == False].reset_index(drop=True)
    
    df_no_missing_yt = df_no_missing[['year', 'trait', 'SRP']].groupby(
            by=['year', 'trait'], as_index=False).mean()
    
    df_no_missing_yt['SRP'] = df_no_missing_yt['SRP'].astype(int)
    
    df_no_missing_yt = df_no_missing_yt.rename(columns={'SRP': 'SRP_imp'})
    
    df_imputed = df.merge(df_no_missing_yt, on=['year', 'trait'], how='left')
    
    df_imputed.loc[df_imputed['SRP'].isna(), 'SRP'] = df_imputed.loc[
            df_imputed['SRP'].isna(), 'SRP_imp']
    
    df_imputed = df_imputed.drop(columns=['SRP_imp'])
    
    return df_imputed


def merge_MPI_data(df, crop):
    """Reads in and merges MPI data.
    
    Keyword arguments:
        df -- the "master" dataframe we're merging onto
        crop -- the crop
    Returns:
        df_w_MPI -- the master df with the MPI data merged
    """
    
     # read in the dataframes
    if crop == 'SOYBEAN':
        MPI_hist_data = pd.read_csv('processed_soy_mpi_data.csv')
    
    # grab the columns
    if crop == 'CORN':
        hist_cols_to_drop = MPI_CORN_HIST_COLS
        
    elif crop == 'SOYBEAN':
        hist_cols_to_drop = MPI_SOYBEAN_HIST_COLS_TO_DROP
    
    MPI_hist_data_subset = MPI_hist_data.drop(columns=hist_cols_to_drop)
    
    MPI_df = MPI_hist_data_subset.copy()

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
    # generate advantages
    performance_w_adv = pd.read_csv(DATA_DIR + ADV_FILE)
    
    # merge with abm_map
    performance_w_adv = performance_w_adv.rename(columns={'ABM': 'abm',
                                                          'c_yield': 'yield',
                                                          'VARIETY': 'hybrid',
                                                          'YEAR': 'year'})
    
    performance_w_adv = performance_w_adv.merge(abm_map, on=['abm'], how='left')
    
    performance_w_adv = performance_w_adv.drop(
            columns=['abm', 'experiment_stage_name']).rename(columns={'TEAM_KEY': 'abm'})
    
    df_w_performance = df.merge(performance_w_adv,
                                on=['year', 'abm', 'hybrid'],
                                how='left')
    
    # germplasm imputation
    df_w_performance_imputed = performance_imputation(df=df_w_performance, crop='CORN')
    
    return df_w_performance_imputed


def merge_price_data(df, crop):
    """Merges the SRP data.
    
    Keyword arguments:
        df -- the dataframe we're merging onto
        crop -- the crop we're pulling data for
    Returns:
        df_price -- the dataframe with the price data
    """
    SRP_df = pd.DataFrame()
    
    for year in SRP_YEARS:
        if year < 2020:
            single_year_SRP = pd.read_csv(DATA_DIR + SRP_DIR + str(year) + '_SRP.csv')
            single_year_SRP = single_year_SRP[['VARIETY', 'SRP']]
            single_year_SRP = single_year_SRP.rename(columns={'VARIETY': 'hybrid'})
            
            # turn the SRP into an int
            single_year_SRP['SRP'] = single_year_SRP['SRP'].str.strip().str.replace('$','')
            single_year_SRP = single_year_SRP[single_year_SRP['SRP'] != '-'].reset_index(drop=True)

            single_year_SRP['SRP'] = single_year_SRP['SRP'].astype(float)
            
        elif year == 2020:
            single_year_SRP = pd.read_csv(DATA_DIR + SRP_DIR + str(year) + '_SRP.csv')
            single_year_SRP = single_year_SRP.rename(columns={'Product': 'hybrid',
                                                              'Price': 'SRP'})
            single_year_SRP['SRP'] = single_year_SRP['SRP'].astype(float)
        
        elif year > 2020:
            single_year_SRP = pd.read_csv(DATA_DIR + SRP_DIR + str(year) + '_product_SRP.csv')
            single_year_SRP = single_year_SRP[['Product Name', 'Srp']]
            single_year_SRP = single_year_SRP.rename(columns={'Product Name': 'hybrid',
                                                              'Srp': 'SRP'})
            single_year_SRP['SRP'] = single_year_SRP['SRP'].astype(float)
    
        single_year_SRP['year'] = year
        single_year_SRP = single_year_SRP.groupby(by=['year', 'hybrid'], as_index=False).mean()
        
        if SRP_df.empty == True:
            SRP_df = single_year_SRP.copy()
        else:
            SRP_df = pd.concat([SRP_df, single_year_SRP]).reset_index(drop=True)
            
    df_w_SRP = df.merge(SRP_df, on=['year', 'hybrid'], how='left')
    
    # impute SRP
    df_w_SRP_imputed = impute_SRP(df=df_w_SRP)
            
    return df_w_SRP_imputed


def performance_imputation(df, crop):
    """Imputes the yield adv based on ABM and year
    
    Keyword arguments:
        df -- the dataframe with the yield advantages
        crop -- the crop we're imputing
    Returns:
        df_imputed -- the dataframe with the imputed 
    """    
    if crop=='CORN':
        df_imputed = df.copy() #germplasm_imputation(df=df)
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