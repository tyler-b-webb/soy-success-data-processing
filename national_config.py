#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  9 09:50:45 2023

@author: epnzv
"""

# the abm table
ABM_TABLE = 'keys_abm.csv'

ADV_FILE = 'soybean_trustworthy_yield.csv'

DATA_DIR = '../../NA-soy-pricing/data/'

CHANNEL_DIR = 'channel/'

CORN_CF_DATA = 'FY23_Corn_022023.xlsx'

ABM_DATE_RATIOS = 'daily_ratios.csv'

RCI_DIR = 'RCI_Data/'

SALES_DIR = 'sales_data/'

SOYBEAN_CF_DATA = 'FY23_Soy_102323.xlsx'

SOYBEAN_CF_DATA_NEW_SCHEMA = 'FY24_Soybean_CF_new_schema_10_25.csv'

SRP_DIR = 'historical_SRP/'

CURRENT_BANK = True

CY_FORECASTS = True

EFFECTIVE_DATE = {'month': 10,
                  'day': 25}

FORECAST_COLS = ['FORECAST_YEAR', 'TEAM_KEY', 'ACRONYM_NAME',
                 'TEAM_Y1_FCST_2']

FORECAST_CY_COLS = ['FORECAST_YEAR', 'TEAM_KEY', 'ACRONYM_NAME',
                    'TEAM_FCST_QTY_10']

MPI_CORN_HIST_COLS = ['names.commercial', 'characteristics.Goss Wilt', 'characteristics.Stalk Strength',
                 'characteristics.Gray Leaf Spot',  'characteristics.Drought Tolerance',
                 'characteristics.Drydown', 'characteristics.Greensnap',
                 'characteristics.Northern Corn Leaf Blight - Race 1',
                 'characteristics.Harvest Appearance', 'characteristics.Plant Height',
                 'characteristics.Anthracnose Stalk Rot', 'characteristics.Stay Green',
                 'characteristics.Seedling Vigor', 'characteristics.Root Strength']

MPI_CORN_NEW_COLS = ['hybrid', 'GOSSS WILT', 'STALK STRENGTH', 'GRAY LEAF SPOT',
                     'DROUGHT TOLERANCE', 'DRYDOWN', 'GREENSNAP', 'NORTHERN CORN LEAF BLIGHT R1',
                     'HARVEST APPEARANCE', 'PLANT HEIGHT', 'ANTHRACNOSE STALK ROT',
                     'STAYGREEN',  'SEEDLING VIGOR', 'ROOT STRENGTH']

MPI_CORN_FEATURE_NAMES = ['hybrid', 'goss_wilt', 'stalk_strength', 'gray_leaf_spot',
                          'drought_tolerance', 'drydown', 'greensnap',
                          'northern_corn_leaf_blight_r1', 'harvest_appearance', 
                          'plant_height', 'athracnose_stalk_rot', 'staygreen',
                          'seedling_vigor', 'root_strength']

MPI_SOYBEAN_HIST_COLS = ['hybrid', 'launch_year', 'char_prod_iron_chlorosis',
                         'char_plant_desc_height_category', 'char_plant_desc_hilum_color',
                         'char_plant_desc_pubescence_color', 'char_prod_no_till_adaptability',
                         'char_plant_desc_canopy', 'char_plant_desc_pod_wall_color']

MPI_SOYBEAN_HIST_COLS_TO_DROP = ['char_mgmt_relative_maturity', 'life_cycle_status',
                                 'trait', 'char_dis_rat_frogeye_leaf_spot',
                                 'char_dis_rat_southerm_stem_canker',
                                 'char_dis_rat_sudden_death_syndrome',
                                 'char_dis_rat_southern_root_knot']

MPI_SOYBEAN_FEATURE_NAMES = ['hybrid', 'no_till_adaptability', 'brown_stem_rot', 'plant_type',
                             'standability', 'plant_height', 'soybean_cyst_nematode',
                             'emergence', 'iron_deficiency_chlorosis',
                             'prr_field_tolerance', 'pod_color', 'white_mold']

NATIONAL_SALES_YEARS = [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]

PERFORMANCE_COLS = ['yield', 'yield_advantage']

RCI_NUMERICAL_VALUES = ['order_Q', 'nets_Q', 'shipped_Q', 'return_Q', 'replant_Q']

RCI_PY_RENAME = {'PY_Order Qty': 'order_Q',
                 'PY Net Sales Qty': 'nets_Q',
                 'PY Shipped': 'shipped_Q',
                 'PY_Return Qty': 'return_Q', 
                 'PY_Replant Qty (Harmonized)': 'replant_Q'}

RCI_PY1_RENAME = {'(PY-1)_Order Qty': 'order_Q',
                  '(PY-1)_Net Sales Qty': 'nets_Q',
                  '(PY-1)_Shipped': 'shipped_Q',
                  '(PY-1)_Return Qty': 'return_Q', 
                  '(PY-1)_Replant Qty (Harmonized)': 'replant_Q'}

RCI_PY2_RENAME = {'(PY-2)_Order Qty': 'order_Q',
                  '(PY-2)_Net Sales Qty': 'nets_Q',
                  '(PY-2)_Shipped': 'shipped_Q',
                  '(PY-2)_Return Qty': 'return_Q', 
                  '(PY-2)_Replant Qty (Harmonized)': 'replant_Q'}

RCI_PY_SUBSET_COLUMNS =['year', 'BRAND_FAMILY_DESCR', 'SLS_LVL_2_ID', 'VARIETY_NAME',
                        'PY_Order Qty', 'PY Net Sales Qty', 'PY Shipped',
                        'PY_Return Qty', 'PY_Replant Qty (Harmonized)']

RCI_PY1_SUBSET_COLUMNS = ['year', 'BRAND_FAMILY_DESCR', 'SLS_LVL_2_ID', 'VARIETY_NAME',
                          '(PY-1)_Order Qty', '(PY-1)_Net Sales Qty',
                          '(PY-1)_Shipped', '(PY-1)_Return Qty', '(PY-1)_Replant Qty (Harmonized)']

RCI_PY2_SUBSET_COLUMNS = ['year', 'BRAND_FAMILY_DESCR', 'SLS_LVL_2_ID', 'VARIETY_NAME',
                          '(PY-2)_Order Qty', '(PY-2)_Net Sales Qty',
                          '(PY-2)_Shipped', '(PY-2)_Return Qty', '(PY-2)_Replant Qty (Harmonized)']

RCI_RENAME = {'Column A': 'BRAND_FAMILY_DESCR',
              'Column B': 'SLS_LVL_2_ID',
              'Column C': 'VARIETY_NAME'}

SRP_YEARS = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021,
             2022, 2023, 2024]