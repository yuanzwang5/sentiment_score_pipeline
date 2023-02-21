import sys
sys.path.insert(0, '/mnt/utnfs/data/sentiment_score_pipeline/python_code/v0.1.0/generate_panel_table')
import pandas as pd
import os
from panel_table_config import Panel_table_config
import panel_table_processing
import warnings

warnings.filterwarnings("ignore")


def generate_panel_table(data_date=None):
    panel_table_config = Panel_table_config(data_date=data_date)
    
    # ----- Load Data in Use ----- #
    ####################################################################################################################
    jieba_score_input_path = panel_table_config.jieba_score_input_dir
    bond_profile_input_path = panel_table_config.bond_profile_input_dir
    panel_table_output_path = panel_table_config.panel_table_output_dir
    
    # Load in bond_profile.pkl
    bond_profile = pd.read_pickle(os.path.join(bond_profile_input_path, 'bond_profile.pkl'))[['b_info_issuercode','b_info_issuer','b_default_date']].drop_duplicates().reset_index(drop=True)
    
    # Load in jieba score file
    filename = list(os.listdir(jieba_score_input_path))[0]
    df_jiebascore = pd.read_csv(os.path.join(jieba_score_input_path, filename))[['b_info_issuercode','jieba_score_div_lenissuer_wordnum']]
    
    # ----- Data Cleaning Processing ----- #
    ####################################################################################################################
    
    # Calculate each issuercode's this month's total jieba score
    df_jiebascore = panel_table_processing.jieba_score_processing(df_jiebascore)
    
    # Get default related issuercodes list
    bond_default,default_issuercode,bond_default_aggregate = panel_table_processing.bond_profile_processing(bond_profile)
    
    # ----- Export ----- #
    ####################################################################################################################
    
    # Get this month's panel table
    panel_table = panel_table_processing.panel_table_processing(df_jiebascore, bond_default, default_issuercode, bond_default_aggregate, filename, bond_profile)
    
    # Save processed data
    save_file_name = filename[:6] + '_panel_table.csv'
    if not os.path.exists(panel_table_output_path):
        os.makedirs(panel_table_output_path)
    panel_table.to_csv(os.path.join(panel_table_output_path, save_file_name), index=False, encoding='utf_8_sig')
    
if __name__ == '__main__':
    generate_panel_table()
