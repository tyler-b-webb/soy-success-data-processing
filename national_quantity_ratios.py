#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 14:56:25 2023

@author: epnzv
"""

import calendar
import datetime as dt
import pandas as pd
from national_config import(ABM_TABLE, DATA_DIR, NATIONAL_SALES_YEARS, SALES_DIR)

dfs = []

def read_abm_teamkey_file():
    """ Reads in and returns the abm and teamkey data as a dataframe.
    
    Keyword arguments:
        None
    Returns:
        ABM_TEAMKEY -- the dataframe of mapping teamkey to abm 
    """
    
    # read in data
    abm_Teamkey_Address = ABM_TABLE
    abm_Teamkey = pd.read_csv(abm_Teamkey_Address)
        
    return abm_Teamkey

abm_map = read_abm_teamkey_file()
    
for year in NATIONAL_SALES_YEARS:
    print('Reading ' + str(year) + ' sales data...')
        
    dfi_path = DATA_DIR + SALES_DIR + str(year) + '.csv'
        
    # read in an individual year
    dfi = pd.read_csv(dfi_path)
        
    # grab the data for the requested crop
    dfi = dfi[dfi['SPECIE_DESCR'] == 'SOYBEAN'].reset_index(drop=True)
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

sales['month'] = pd.DatetimeIndex(sales['EFFECTIVE_DATE']).month
sales['day'] = pd.DatetimeIndex(sales['EFFECTIVE_DATE']).day

# subset out columns
sales = sales[sales['BRAND_FAMILY_DESCR'] == 'NATIONAL'].reset_index(drop=True)
sales_subset = sales[['year', 'EFFECTIVE_DATE', 'TEAM_KEY', 'NET_SALES_QTY_TO_DATE',
                      'NET_SHIPPED_QTY_TO_DATE', 'ORDER_QTY_TO_DATE',
                      'REPLANT_QTY_TO_DATE', 'RETURN_QTY_TO_DATE']]

sales_abm = sales_subset.drop(columns=['year', 'EFFECTIVE_DATE']).groupby(
        by=['TEAM_KEY'], as_index=False).sum().reset_index(drop=True)

sales_abm = sales_abm.rename(columns={'NET_SALES_QTY_TO_DATE': 'nets_Q_total',
                                      'NET_SHIPPED_QTY_TO_DATE': 'shipped_total',
                                      'ORDER_QTY_TO_DATE': 'orders_total',
                                      'REPLANT_QTY_TO_DATE': 'replants_total',
                                      'RETURN_QTY_TO_DATE': 'returns_total'})

months = [9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8]

daily_ratios = pd.DataFrame()

for year in NATIONAL_SALES_YEARS:
    single_year = sales_subset[sales_subset['year'] == year].reset_index(drop=True)
    for month in months:
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            if month > 8:
                date_mask = dt.datetime(year=year - 1,
                                        month=month,
                                        day=day)
            else:
                date_mask = dt.datetime(year=year,
                                        month=month,
                                        day=day)
                
            single_year_to_date = single_year[
                    single_year['EFFECTIVE_DATE'] <= date_mask].reset_index(drop=True)
            
            single_year_grouped = single_year_to_date.drop(columns=['EFFECTIVE_DATE', 'year']).groupby(
                    by=['TEAM_KEY'], as_index=False).sum()
            
            single_year_grouped = single_year_grouped.rename(
                    columns={'NET_SALES_QTY_TO_DATE': 'nets_Q_to_date',
                             'NET_SHIPPED_QTY_TO_DATE': 'shipped_to_date',
                             'ORDER_QTY_TO_DATE': 'orders_to_date',
                             'REPLANT_QTY_TO_DATE': 'replants_to_date',
                             'RETURN_QTY_TO_DATE': 'returns_to_date'})
            
            single_year_grouped['month'] = month
            single_year_grouped['day'] = day
    
            if daily_ratios.empty == True:
                daily_ratios = single_year_grouped.copy()
            else:
                daily_ratios = pd.concat(
                        [daily_ratios, single_year_grouped]).reset_index(drop=True)

daily_ratios = daily_ratios.groupby(by=['TEAM_KEY', 'month', 'day'], as_index=False).sum().reset_index(drop=True)

daily_ratios = daily_ratios.merge(sales_abm, on=['TEAM_KEY'], how='left')

qtys = ['nets_Q', 'shipped', 'orders', 'replants', 'returns']

for qty in qtys:
    daily_ratios[qty + '_ratio'] = daily_ratios[qty + '_to_date'] / daily_ratios[qty + '_total']
    daily_ratios = daily_ratios.drop(columns=[qty + '_to_date', qty + '_total'])

daily_ratios = daily_ratios.rename(columns={'TEAM_KEY': 'abm'})    

daily_ratios = daily_ratios.dropna().reset_index(drop=True)
daily_ratios.to_csv('daily_ratios.csv', index=False)
    