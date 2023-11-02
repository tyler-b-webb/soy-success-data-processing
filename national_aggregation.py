#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 16:44:59 2023

@author: epnzv
"""

import pandas as pd

from national_config import (ABM_TABLE, CY_FORECASTS, EFFECTIVE_DATE)
from national_products import(generate_age_trait_RM, merge_MPI_data,
                              merge_performance_data, merge_price_data)
from national_sales import (read_national_sales_imputeRCI, read_forecasts)

CROP = 'SOYBEAN'

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

national_sales = read_national_sales_imputeRCI(crop=CROP, abm_map=abm_map)

national_forecast, cy_forecasts_nu_schema, cy_forecasts, all_forecasts = read_forecasts(df=national_sales, crop=CROP)

national_age, national_stripped = generate_age_trait_RM(df=national_forecast, crop=CROP, RM=True)

national_MPI = merge_MPI_data(df=national_age, crop=CROP)

national_price = merge_price_data(df=national_MPI, crop=CROP)

national_performance = merge_performance_data(df=national_price,
                                              abm_map=abm_map,
                                              crop=CROP)


national_performance = national_performance.fillna(0)

national_performance = national_performance[national_performance['year'] > 2016].reset_index(drop=True)

if CY_FORECASTS == False:
    national_performance.to_csv('national_alloy_'+ CROP + '_' + str(EFFECTIVE_DATE['month']) + '_' +
                                str(EFFECTIVE_DATE['day']) + '.csv', index=False)
else:
    national_performance.to_csv('national_alloy_'+ CROP + '_' + str(EFFECTIVE_DATE['month']) + '_' +
                                str(EFFECTIVE_DATE['day']) + '_CYfix_inclusive.csv', index=False)