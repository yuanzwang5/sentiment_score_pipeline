import pandas as pd
import os
import numpy as np

def bond_related_processing(dataframe, s_info_windcode, b_info_issuercode, config):
    """
    :param dataframe: Input DataFrame that need to construct two columns, 's_info_windcode' and 'b_info_issuercode'
    :param s_info_windcode: s_info_windcode in 4 groups of bond
    :param b_info_issuercode: b_info_issuercode in 4 groups of bond related issuers
    :return: DataFrame that include two columns, 's_info_windcode' and 'b_info_issuercode' including in those 4 bond groups ('Enterprise Bond', 'Commercial Paper', 'Corporate Bond', 'Medium Term Note')
    """
    # Double check publishDate is in string format and change Windcodes from string to list
    dataframe.publishDate = dataframe.publishDate.apply(lambda x: str(x))
    dataframe.Windcodes = dataframe.Windcodes.apply(lambda x: eval(x))
    
    # Drop rows that Windcodes == []
    dataframe = dataframe.drop(dataframe[dataframe.Windcodes.apply(lambda x: len(x) == 0)].index).reset_index(drop=True)
    
    # Split the ':' inside Windcodes
    dataframe.Windcodes = dataframe.Windcodes.apply(lambda x: [x[i].split(':') for i in range(len(x)) if len(x) > 0])
    dataframe.Windcodes = dataframe.Windcodes.apply(lambda x: [item for sublist in x for item in sublist])
    
    # Filter the 's_info_windcode' and 'b_info_issuercode' in those 4 bond groups
    dataframe['s_info_windcode'] = dataframe.Windcodes.apply(lambda x: list(set(x) & set(s_info_windcode)))
    dataframe['b_info_issuercode'] = dataframe.Windcodes.apply(lambda x: list(set(x) & set(b_info_issuercode)))
    
    # Filter out rows that contain 's_info_windcode' or 'b_info_issuercode'
    dataframe = dataframe[dataframe.apply(lambda row: len(row['s_info_windcode'] + row['b_info_issuercode']) > 0, axis=1)].reset_index(drop=True)
    
    # Save processed data
    publishdate = dataframe.publishDate[0]
    save_file_name = str(publishdate) + '_listfilter_bond.csv'
    if not os.path.exists(config.bond_related_output_dir):
        os.makedirs(config.bond_related_output_dir)
    dataframe.to_csv(os.path.join(config.bond_related_output_dir, save_file_name), index=False, encoding='utf_8_sig')
    
    return dataframe
