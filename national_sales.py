#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  6 15:15:51 2023

@author: epnzv
"""

import datetime as dt
import pandas as pd

from national_config import (ABM_DATE_RATIOS, CHANNEL_DIR, CORN_CF_DATA, CURRENT_BANK,
                             CY_FORECASTS, DATA_DIR, EFFECTIVE_DATE, FORECAST_COLS, FORECAST_CY_COLS,
                             NATIONAL_SALES_YEARS, RCI_DIR, RCI_NUMERICAL_VALUES,
                             RCI_PY_RENAME, RCI_PY1_RENAME, RCI_PY2_RENAME,
                             RCI_PY_SUBSET_COLUMNS, RCI_PY1_SUBSET_COLUMNS,
                             RCI_PY2_SUBSET_COLUMNS, RCI_RENAME, SALES_DIR,
                             SOYBEAN_CF_DATA, SOYBEAN_CF_DATA_NEW_SCHEMA)


def create_lagged_sales(df):
    """Creates lagged sales and merges them back onto the df.
    
    Keyword Arguments:
        df -- the sales dataframe
    Returns: 
        df_w_lag -- the dataframe with lagged sales data
    """
    df_copy_1 = df.copy()
    df_copy_2 = df.copy()
    
    # add to the year values and rename columns
    df_copy_1['year'] = df_copy_1['year'] + 1
    df_copy_2['year'] = df_copy_2['year'] + 2
    
    df_copy_1 = df_copy_1.rename(columns={'nets_Q': 'nets_Q_1',
                                          'shipped_Q': 'shipped_Q_1',
                                          'order_Q': 'order_Q_1',
                                          'orders_to_date': 'orders_to_date_1',
                                          'return_Q': 'return_Q_1',
                                          'replant_Q': 'replant_Q_1'})
    
    df_copy_2 = df_copy_2.rename(columns={'nets_Q': 'nets_Q_2',
                                          'shipped_Q': 'shipped_Q_2',
                                          'order_Q': 'order_Q_2',
                                          'orders_to_date': 'orders_to_date_2',
                                          'return_Q': 'return_Q_2',
                                          'replant_Q': 'replant_Q_2'})
    
    df_w_lag = df.merge(df_copy_1, on=['year', 'abm', 'hybrid'], how='left')
    df_w_lag = df_w_lag.merge(df_copy_2, on=['year', 'abm', 'hybrid'], how='left')
    
    # fill missing lagged features with 0
    df_w_lag = df_w_lag.fillna(0)
    
    return df_w_lag


def D1MS_dt_format(df):
    """Changes the format of the effective date for the D1MS data.
    
    Keyword Arguments:
        df -- the D1MS data
    Returns: 
        df_new_format -- the dataframe with a new effective date format
    """
    df_new_dt = df.copy()
    df_new_dt['EFFECTIVE_DATE'] = df_new_dt['EFFECTIVE_DATE'].astype(str)
    
    # pull out the year, month, and day
    df_new_dt['ed_year'] = df_new_dt['EFFECTIVE_DATE'].str[0:4]
    df_new_dt['ed_month'] = df_new_dt['EFFECTIVE_DATE'].str[4:6]
    df_new_dt['ed_day'] = df_new_dt['EFFECTIVE_DATE'].str[6:8]

    df_new_dt['EFFECTIVE_DATE'] = (
            df_new_dt['ed_month'] + '/' +  df_new_dt['ed_month'] + '/' + df_new_dt['ed_year'])
    
    df_new_dt = df_new_dt.drop(columns=['ed_year', 'ed_month', 'ed_day'])
    
    return df_new_dt


def read_forecasts(df, crop):
    """Reads in the consensus forecast data.
    
    Keyword arguments:
        df -- the dataframe we're merging
        crop -- the crop we're reading the data for
    Returns:
        df_forecasts -- the df with forecasts for the crop and brand we're
        interested in
    """
    if crop == 'CORN':
        forecasts_all_brands = pd.read_excel(DATA_DIR + CORN_CF_DATA)
    elif crop == 'SOYBEAN':
        forecasts_all_brands = pd.read_excel(DATA_DIR + SOYBEAN_CF_DATA)
    
    forecasts_one_brand = forecasts_all_brands[
            (forecasts_all_brands['BRAND_GROUP'] == 'ASGROW') |
            (forecasts_all_brands['BRAND_GROUP'] == 'NATIONAL')].reset_index(drop=True)
    
    forecasts_selected_cols = forecasts_one_brand[FORECAST_COLS].rename(columns={
            'FORECAST_YEAR': 'year',
            'TEAM_KEY': 'abm',
            'ACRONYM_NAME': 'hybrid'})
    
    forecasts_selected_cols['year'] = forecasts_selected_cols['year'] + 1
    
    forecasts_selected_cols = forecasts_selected_cols.dropna()
    
    # grab columns where there are indeed forecasts
    forecasts_selected_cols = forecasts_selected_cols[
            forecasts_selected_cols['TEAM_Y1_FCST_2'] != 0].reset_index(drop=True)
    
    if CY_FORECASTS == True:
        forecasts_cy_selected_cols = forecasts_one_brand[FORECAST_CY_COLS].rename(columns={
                'FORECAST_YEAR': 'year',
                'TEAM_KEY': 'abm',
                'ACRONYM_NAME': 'hybrid'})
    
        forecasts_cy_selected_cols = forecasts_cy_selected_cols.dropna()

        
        # read in 2024 columns (new schema)
        forecasts_cy_new_schema = pd.read_csv(DATA_DIR + SOYBEAN_CF_DATA_NEW_SCHEMA)
             
        forecasts_cy_new_schema_one_brand = forecasts_cy_new_schema[
                forecasts_cy_new_schema['BRAND_GROUP'] == 'ASGROW'].reset_index(drop=True)
        
        forecasts_cy_new_schema_selected_cols = forecasts_cy_new_schema_one_brand[
                FORECAST_CY_COLS].rename(columns={
                'FORECAST_YEAR': 'year',
                'TEAM_KEY': 'abm',
                'ACRONYM_NAME': 'hybrid'})
    
        # concat
        forecasts_cy_selected_cols = pd.concat([forecasts_cy_selected_cols,
                                                forecasts_cy_new_schema_selected_cols])
        
        forecasts_selected_cols = forecasts_selected_cols.merge(forecasts_cy_selected_cols,
                                                                on=['year', 'abm', 'hybrid'],
                                                                how='outer')
        
        forecasts_selected_cols = forecasts_selected_cols.fillna(0)
    
    df_forecasts = df.merge(forecasts_selected_cols,
                            on=['year', 'abm', 'hybrid'],
                            how='outer').fillna(0)
    
    # drop any rows with a 0 order to date AND 0 forecasts
    df_forecasts = df_forecasts[(df_forecasts['orders_to_date'] != 0) |
                                (df_forecasts['TEAM_FCST_QTY_10'] != 0)].reset_index(drop=True)
    
    return df_forecasts, forecasts_cy_new_schema_selected_cols, forecasts_cy_selected_cols, forecasts_selected_cols


def read_national_sales_imputeRCI(crop, abm_map):
    """Reads in Channel sales data and imputes the D1MS based on a daily
    abm/quantity level map.
    
    Keyword Arguments:
        crop -- the crop we want to get the data for
    Returns:
        sales -- the dataframe of the concatenated sales data.
    """
    # set up a df 
    dfs = []
    
    for year in NATIONAL_SALES_YEARS:
        print('Reading ' + str(year) + ' sales data...')
        
        dfi_path = DATA_DIR + SALES_DIR + str(year) + '.csv'
        
        # read in an individual year
        dfi = pd.read_csv(dfi_path)
        
        # grab the data for the requested crop
        dfi = dfi[dfi['SPECIE_DESCR'] == crop].reset_index(drop=True)
        dfi['year'] = year
        
        # convert the date to datetime format
        dfi['EFFECTIVE_DATE'] = pd.to_datetime(dfi['EFFECTIVE_DATE'])
        
        if year !=  2020:
            dfi = dfi.rename(columns={'SLS_LVL_2_ID': 'abm'})
            dfi = dfi.merge(abm_map, on=['abm'], how='left')
            dfi = dfi.drop(columns=['abm'])
        else:
            dfi = dfi.rename(columns={'SLS_LVL_2_ID': 'TEAM_KEY'})
        
        dfs.append(dfi)
        
    sales = pd.concat(dfs).reset_index(drop=True)
    
    # grab relevant columns
    sales = sales[
            ['year', 'EFFECTIVE_DATE', 'NET_SALES_QTY_TO_DATE', 'NET_SHIPPED_QTY_TO_DATE',
             'ORDER_QTY_TO_DATE', 'REPLANT_QTY_TO_DATE', 'RETURN_QTY_TO_DATE',
             'TEAM_KEY', 'VARIETY_NAME']]
    
    # rename columns
    sales = sales.rename(columns={'NET_SALES_QTY_TO_DATE': 'nets_Q',
                                  'NET_SHIPPED_QTY_TO_DATE': 'shipped_Q',
                                  'ORDER_QTY_TO_DATE': 'order_Q',
                                  'REPLANT_QTY_TO_DATE': 'replant_Q',
                                  'RETURN_QTY_TO_DATE': 'return_Q',
                                  'TEAM_KEY': 'abm',
                                  'VARIETY_NAME': 'hybrid'})
            
    sales_abm_level = pd.DataFrame()
    
    # create to_date order features and aggregate to abm level
    for year in sales['year'].unique():
        if EFFECTIVE_DATE['month'] > 8:
            date_mask = dt.datetime(year=year - 1,
                                    month=EFFECTIVE_DATE['month'],
                                    day=EFFECTIVE_DATE['day'])
        else:
            date_mask = dt.datetime(year=year,
                                    month=EFFECTIVE_DATE['month'],
                                    day=EFFECTIVE_DATE['day'])
        
        single_year = sales[sales['year'] == year].reset_index(drop=True)
        
        single_year_to_date = single_year[single_year['EFFECTIVE_DATE'] <= date_mask].reset_index(drop=True)
        single_year_to_date_subset = single_year_to_date[['year', 'abm', 'hybrid', 'order_Q']]
        single_year_to_date_subset = single_year_to_date_subset.groupby(by=['year', 'abm', 'hybrid'], as_index=False).sum().rename(
                        columns={'order_Q': 'orders_to_date'})
        
        single_year = single_year.groupby(by=['year', 'abm', 'hybrid'], as_index=False).sum()
        
        single_year_with_to_date = single_year.merge(single_year_to_date_subset,
                                                     on=['year', 'abm', 'hybrid'],
                                                     how='left')
        
        if sales_abm_level.empty == True:
            sales_abm_level = single_year_with_to_date.copy()
        else:
            sales_abm_level = pd.concat([sales_abm_level, single_year_with_to_date])
            
            
    # read in the 2021 and 2022 sales
    print('Reading 2021 sales data...')
    sales_21 = pd.read_csv(RCI_DIR + '2021_' + crop + '_RCI.csv')
    sales_21['year'] = 2021
    
    print('Reading 2022 sales data...')
    sales_22 = pd.read_csv(RCI_DIR + '2022_' + crop + '_RCI.csv')
    sales_22['year'] = 2022
    
    print('Reading 2023 sales data...')
    sales_23 = pd.read_csv(RCI_DIR + '2023_' + crop + '_RCI.csv')
    sales_23['year'] = 2023
    
    # rename and subset columns
    sales_21 = sales_21.rename(columns=RCI_RENAME)
    sales_22 = sales_22.rename(columns=RCI_RENAME)
    sales_23 = sales_23.rename(columns=RCI_RENAME)
    
    sales_21 = sales_21[RCI_PY2_SUBSET_COLUMNS].rename(columns=RCI_PY2_RENAME)
    sales_22 = sales_22[RCI_PY1_SUBSET_COLUMNS].rename(columns=RCI_PY1_RENAME)
    sales_23 = sales_23[RCI_PY_SUBSET_COLUMNS].rename(columns=RCI_PY_RENAME)
    
    # concat the three together
    sales_RCI = pd.concat([sales_21, sales_22]).reset_index(drop=True)
    sales_RCI = pd.concat([sales_RCI, sales_23]).reset_index(drop=True)
        
    # set values to be numerical
    for value in RCI_NUMERICAL_VALUES:
        sales_RCI[value] = sales_RCI[value].str.replace(',','').astype(int)
    
    # read in the date map
    date_ratios = pd.read_csv(ABM_DATE_RATIOS)
    date_ratios = date_ratios[['abm', 'month', 'day', 'orders_ratio']]
    
    this_date = date_ratios[date_ratios['month'] == EFFECTIVE_DATE['month']].reset_index(drop=True)
    this_date = this_date[this_date['day'] == EFFECTIVE_DATE['day']].reset_index(drop=True)
    
    # grab the order ratio and the abm columns
    this_date = this_date[['abm', 'orders_ratio']]
    
    # grab channel and the relevant crop
    sales_RCI_subset = sales_RCI[
            (sales_RCI['BRAND_FAMILY_DESCR'] == 'NATIONAL') | 
            (sales_RCI['BRAND_FAMILY_DESCR'] == 'DISTRIBUTION ALLOY')].reset_index(drop=True).drop(columns=['BRAND_FAMILY_DESCR'])

    sales_RCI_abm = sales_RCI_subset.rename(columns={'SLS_LVL_2_ID': 'abm',
                                                     'VARIETY_NAME': 'hybrid'})
        
    # group by year, abm and hybrid 
    sales_RCI_abm = sales_RCI_abm.groupby(by=['year', 'abm', 'hybrid'], as_index=False).sum()
        
    # calculate orders to date feature
    sales_RCI_abm = sales_RCI_abm.merge(this_date, on=['abm'], how='left')
    sales_RCI_abm['orders_to_date'] = sales_RCI_abm['order_Q'] * sales_RCI_abm['orders_ratio']
    sales_RCI_abm['orders_to_date'] = sales_RCI_abm['orders_to_date'].astype(int)
    
    sales_RCI_abm = sales_RCI_abm.drop(columns=['orders_ratio'])
    
    sales_all = pd.concat([sales_abm_level, sales_RCI_abm]).reset_index(drop=True)
    
    # drop unknown ABMs
    sales_all = sales_all[sales_all['abm'] != 'UNK'].reset_index(drop=True)
    sales_all = sales_all[sales_all['abm'].isna() == False].reset_index(drop=True)
        
    
    if CURRENT_BANK == True:  
        # add in the current order bank data
        current_order_bank = pd.read_csv(RCI_DIR + '2024_' + crop + '_RCI_' +
                str(EFFECTIVE_DATE['month']) + '_' + str(EFFECTIVE_DATE['day']) + '.csv')
                
        current_order_bank['year'] = 2024
        
        # drop NAs
        current_order_bank = current_order_bank.dropna()
        
        # rename stuff
        current_order_bank = current_order_bank.rename(columns=RCI_RENAME)
        
        # grab NATIONAL brand
        current_order_bank = current_order_bank[
                (current_order_bank['BRAND_FAMILY_DESCR'] == 'NATIONAL') |
                (current_order_bank['BRAND_FAMILY_DESCR'] == 'DISTRIBUTION ALLOY')].reset_index(drop=True)
        
        # subset out 
        current_order_bank_subset = current_order_bank[
                ['year', 'SLS_LVL_2_ID', 'VARIETY_NAME', 'CY_Order Qty']]
        
    
        current_order_bank_subset = current_order_bank_subset.rename(
                columns={'year': 'year',
                         'SLS_LVL_2_ID': 'abm',
                         'VARIETY_NAME': 'hybrid',
                         'CY_Order Qty': 'orders_to_date'})
    
        # turn orders to date into an int
        current_order_bank_subset['orders_to_date'] = current_order_bank_subset[
                'orders_to_date'].str.strip().str.replace(',','').astype(int)
    
        current_order_bank_summed = current_order_bank_subset.groupby(
                by=['year', 'abm', 'hybrid'], as_index=False).sum()
    
        # set 0 values for other quantities
        quantities = ['nets_Q', 'shipped_Q', 'order_Q', 'replant_Q', 'return_Q']
    
        for qty in quantities:
            current_order_bank_summed[qty] = 0
        
        # concat onto main df
        sales_abm_level_cy_sales = pd.concat([sales_all, current_order_bank_summed])
    
    else:
        sales_abm_level_cy_sales = sales_all.copy()
        
    # create the lagged sales quantities
    sales_w_lag = create_lagged_sales(df=sales_abm_level_cy_sales)
                
    return sales_w_lag