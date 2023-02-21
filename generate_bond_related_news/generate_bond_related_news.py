import sys
sys.path.insert(0, '/mnt/utnfs/data/sentiment_score_pipeline/python_code/v0.1.0/generate_bond_related_news')
import pandas as pd
import os
from joblib import Parallel, delayed
from bond_related_news_config import Bond_related_news_config
import bond_related_news_processing
import warnings

warnings.filterwarnings("ignore")


def generate_bond_related_news(data_date=None):
    bond_related_news_config = Bond_related_news_config(data_date=data_date)
    
    # ----- Load Data in Use ----- #
    ####################################################################################################################
    second_filter_input_path = bond_related_news_config.second_filter_input_dir
    bond_profile_input_path = bond_related_news_config.bond_profile_input_dir
    
    # Load in bond_profile.pkl
    bond_profile = pd.read_pickle(os.path.join(bond_profile_input_path, 'bond_profile.pkl'))
    
    # Load in second filter dataframe into a list
    second_filter = []
    for file in os.listdir(second_filter_input_path):
        date_secondfilter = pd.read_csv(os.path.join(second_filter_input_path, file))
        if date_secondfilter.empty:
            continue
        else:
            second_filter.append(date_secondfilter)
    
    # ----- Data Cleaning Processing ----- #
    ####################################################################################################################
    # First get 's_info_windcode' and 'b_info_issuercode' list in those 4 bond groups ('Enterprise Bond', 'Commercial Paper', 'Corporate Bond', 'Medium Term Note')
    s_info_windcode = list(bond_profile.s_info_windcode.unique())
    b_info_issuercode = list(bond_profile.b_info_issuercode.unique())

    # ----- Export ----- #
    ####################################################################################################################    
    # Get Bond related news inside those 4 bond groups and save them in csv files
    Parallel(n_jobs=20)(delayed(bond_related_news_processing.bond_related_processing)(frame, s_info_windcode, b_info_issuercode, bond_related_news_config) for frame in second_filter)

if __name__ == '__main__':
    generate_bond_related_news()
