import pandas as pd
import os
import numpy as np
import jieba
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings("ignore")

def jieba_score_processing(dataframe):
    """
    :param dataframe: DataFrame waiting to change into panel table
    :return: DataFrame prepared for panel table merge
    """
    # First check b_info_issuercode's format is list
    dataframe['b_info_issuercode'] = dataframe['b_info_issuercode'].apply(lambda x: eval(x) if type(x) == str else x)
    
    # Explode b_info_issuercode and calculate each issuercode's jieba score sum value
    dataframe = dataframe.explode('b_info_issuercode').reset_index(drop=True)
    dataframe = dataframe.groupby('b_info_issuercode').sum().reset_index()
    
    # Construct each issuercode's related to two rows. One is called 'jieba_score_sum', the other is 'default'.
    # 'jieba_score_sum' is each issuercode's total jieba score in this month, i.e. sum of all the news' scores related to this issuercode
    # 'default' is the list whether this issuercode has default dates in this month. If not, empty. If has, all the default dates in a list.
    dataframe['variable'] = [['jieba_score_sum','default']] * dataframe.shape[0]
    dataframe = dataframe.explode('variable').reset_index(drop=True)
    
    # Empty the wrong value in each issuer's default rows to prepare for filling with true default dates value
    dataframe.loc[dataframe[dataframe['variable'] == 'default'].index, 'jieba_score_div_lenissuer_wordnum'] = np.nan
    
    return dataframe

def bond_profile_processing(bond_profile):
    """
    :param bond_profile: bond_profile DataFrame that waiting to filter default issercodes' information
    :return: bond_default DataFrame listing all issuercodes' all default dates; default issuercodes list; aggregated DataFrame of issuercodes with default dates in list
    """
    # Filter out issuercodes with default records
    bond_default = bond_profile.dropna(subset=['b_default_date']).reset_index(drop=True)
    bond_default['b_default_date'] = bond_default['b_default_date'].apply(lambda x: str(int(x)))
    default_issuercode = list(bond_default['b_info_issuercode'].unique())
    bond_default_aggregate = bond_default.groupby(['b_info_issuercode','b_info_issuer']).agg({'b_default_date':lambda x: x.tolist()}).reset_index()
    
    return bond_default, default_issuercode, bond_default_aggregate

def panel_table_processing(jiebascore, bond_default, default_issuercode, bond_default_aggregate, filename, bond_profile):
    """
    :param jiebascore: jieba score DataFrame after calculation
    :param bond_default: bond_default DataFrame listing all issuercodes' all default dates
    :param default_issuercode: default issuercodes list
    :param bond_default_aggregate: aggregated DataFrame of issuercodes with default dates in list
    :return: This month's panel table in issuercode level
    """
    # Merge jieba score with default information together
    panel_table = jiebascore.merge(bond_default_aggregate, on=['b_info_issuercode'], how='left')
    panel_table.loc[panel_table[(panel_table['variable'] == 'default') & (~panel_table['b_default_date'].isna())].index, 'jieba_score_div_lenissuer_wordnum'] = panel_table.loc[panel_table[(panel_table['variable'] == 'default') & (~panel_table['b_default_date'].isna())].index, ].apply(lambda x: [d for d in list(bond_default[bond_default['b_info_issuercode'] == x['b_info_issuercode']]['b_default_date']) if d[:6] == filename[:6]] if x['b_info_issuercode'] in default_issuercode else np.nan, axis=1)
    panel_table.loc[panel_table[(panel_table['variable'] == 'default') & (~panel_table['b_default_date'].isna())].index, 'jieba_score_div_lenissuer_wordnum'] = panel_table.loc[panel_table[(panel_table['variable'] == 'default') & (~panel_table['b_default_date'].isna())].index, 'jieba_score_div_lenissuer_wordnum'].apply(lambda x: x if type(x) != list else (np.nan if len(x) == 0 else x))
    panel_table['default_label'] = panel_table['b_info_issuercode'].apply(lambda x: 1 if x in default_issuercode else 0)
    
    # Filter out used columns
    panel_table = panel_table[['variable','b_info_issuercode','default_label','jieba_score_div_lenissuer_wordnum']]
    panel_table.rename(columns={'jieba_score_div_lenissuer_wordnum':filename[:6]}, inplace=True)
    
    # Merge b_info_issuercode's issuer name
    panel_table = panel_table.merge(bond_profile[bond_profile['b_info_issuercode'].isin(list(panel_table['b_info_issuercode'].unique()))].reset_index(drop=True)[['b_info_issuercode','b_info_issuer']], on=['b_info_issuercode'], how='left')
    panel_table = panel_table[['variable','b_info_issuercode','b_info_issuer','default_label',filename[:6]]]
    
    # Make sure the data has no duplicates
    panel_table = panel_table.drop_duplicates(subset=['variable','b_info_issuercode']).reset_index(drop=True)
    
    return panel_table
