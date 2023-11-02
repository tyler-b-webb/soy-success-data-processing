#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 12:45:52 2023

@author: epnzv
"""

import numpy as np
import pandas as pd
from national_config import (CHANNEL_DIR, DATA_DIR, SALES_DIR)

dfs = []

lil_years = [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]

for year in lil_years:
    print('Reading ' + str(year) + ' sales data...')
        
    dfi_path = DATA_DIR + SALES_DIR + str(year) + '.csv'
        
    # read in an individual year
    dfi = pd.read_csv(dfi_path)
        
    # grab the data for the requested crop
    dfi = dfi[dfi['SPECIE_DESCR'] == 'SOYBEAN'].reset_index(drop=True)
    dfi['year'] = year
        
    # convert the date to datetime format
    dfi['EFFECTIVE_DATE'] = pd.to_datetime(dfi['EFFECTIVE_DATE'])
        
    dfs.append(dfi)
        
sales = pd.concat(dfs).reset_index(drop=True)

sales = sales[['NET_SALES_QTY_TO_DATE', 'SHIPPING_FIPS_CODE', 'SLS_LVL_2_ID']]

sales = sales.rename(columns={'NET_SALES_QTY_TO_DATE': 'nets_Q',
                              'SHIPPING_FIPS_CODE': 'fips',
                              'SLS_LVL_2_ID': 'abm'})

sales = sales[sales['nets_Q'] != 0].reset_index(drop=True)

sales = sales.groupby(by=['abm', 'fips'], as_index=False).sum().reset_index(drop=True)

sales_unique = pd.DataFrame()

for fips in sales['fips'].unique():
    single_fips = sales[sales['fips'] == fips]
    if len(single_fips) > 1:
        max_sales = np.max(single_fips['nets_Q'])
        single_fips = single_fips[single_fips['nets_Q'] == max_sales].reset_index(drop=True)
    
    if sales_unique.empty == True:
        sales_unique = single_fips.copy()
    else:
        sales_unique = pd.concat([sales_unique, single_fips])

sales_unique = sales_unique.drop_duplicates().reset_index(drop=True)

print('Reading 2021 sales data...')
sales_21 = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2021_D1MS.csv')
sales_21['year'] = 2021
    
print('Reading 2022 sales data...')
sales_22 = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2022_D1MS.csv')
sales_22['year'] = 2022
    
print('Reading 2023 sales data...')
sales_23 = pd.read_csv(DATA_DIR + CHANNEL_DIR + '2023_D1MS.csv')
sales_23['year'] = 2023
    
# concat the three together
sales_D1MS = pd.concat([sales_21, sales_22]).reset_index(drop=True)
sales_D1MS = pd.concat([sales_D1MS, sales_23]).reset_index(drop=True)
            
# grab channel and the relevant crop
sales_D1MS = sales_D1MS[sales_D1MS['BRAND_FAMILY_DESCR'] == 'NATIONAL'].reset_index(drop=True)
sales_D1MS = sales_D1MS[sales_D1MS['SPECIE_DESCR'] == 'SOYBEAN'].reset_index(drop=True)

# grab relevant columns
sales_D1MS = sales_D1MS[['SUM(NET_SALES_QTY_TO_DATE)',  'FIPS', 'SLS_LVL_2_ID']]

sales_D1MS = sales_D1MS.rename(columns={'SUM(NET_SALES_QTY_TO_DATE)': 'nets_Q',
                                        'FIPS': 'fips',
                                        'SLS_LVL_2_ID': 'TEAM_KEY'})

sales_D1MS['fips'] = pd.to_numeric(sales_D1MS['fips'])

sales_D1MS = sales_D1MS.dropna().reset_index(drop=True)
sales_D1MS = sales_D1MS[sales_D1MS['nets_Q'] != 0].reset_index(drop=True)

sales_D1MS = sales_D1MS.groupby(by=['TEAM_KEY', 'fips'], as_index=False).sum().reset_index(drop=True)

sales_D1MS_unique = pd.DataFrame()

for fips in sales_D1MS['fips'].unique():
    single_fips = sales_D1MS[sales_D1MS['fips'] == fips]
    if len(single_fips) > 1:
        max_sales = np.max(single_fips['nets_Q'])
        single_fips = single_fips[single_fips['nets_Q'] == max_sales].reset_index(drop=True)
        
    single_fips = single_fips.drop(columns=['nets_Q'])
    if sales_D1MS_unique.empty == True:
        sales_D1MS_unique = single_fips.copy()
    else:
        sales_D1MS_unique = pd.concat([sales_D1MS_unique, single_fips])
        
sales_D1MS_unique = sales_D1MS_unique.drop_duplicates().reset_index(drop=True)
        
sales_D1MS = sales_D1MS.drop(columns=['nets_Q'])
keys_abm_fips = sales_unique.merge(sales_D1MS_unique, on=['fips'])

keys_abm_fips = sales.merge(sales_D1MS, on=['fips'])

keys_abm = keys_abm_fips.drop(columns=['fips']).groupby(
        by=['TEAM_KEY', 'abm'], as_index=False).sum().reset_index(drop=True)
keys_abm = keys_abm.sort_values(by=['TEAM_KEY'])

keys_abm_unique = pd.DataFrame()

for team_key in keys_abm['TEAM_KEY'].unique():
    single_key = keys_abm[keys_abm['TEAM_KEY'] == team_key]
    if len(single_key) > 1:
        max_sales = np.max(single_key['nets_Q'])
        single_key = single_key[single_key['nets_Q'] == max_sales].reset_index(drop=True)
        
    single_key = single_key.drop(columns=['nets_Q'])
    if keys_abm_unique.empty == True:
        keys_abm_unique = single_key.copy()
    else:
        keys_abm_unique = pd.concat([keys_abm_unique, single_key]).reset_index(drop=True)

keys_abm_unique.to_csv('keys_abm.csv', index=False)