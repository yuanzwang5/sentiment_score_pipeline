import sys
sys.path.insert(0, '/mnt/utnfs/data/sentiment_score_pipeline/python_code/v0.1.0/generate_default_labels')
import pandas as pd
import os
from joblib import Parallel, delayed
from default_labels_config import Default_labels_config
import default_labels_processing
import warnings

warnings.filterwarnings("ignore")

'''
Default labels logistic:
    1. If news's s_info_windcode is not empty, find their related b_info_issuercode and then extend them into the original b_info_issuercode list;
        --> This could make each news's b_info_issuercode without empty situation
    2. Judge whether news's b_info_issuercode only have one issuercode or not:
        a) Only 1 issuercode, check the default records backward and forward;
        b) More than 1 issuercode, only check the default records forward; 
    3. Judge whether news's s_info_windcode is empty:
        a) If not empty, we find the earliest default date among these s_info_windcode to be this news's related default date to judge default labels
        b) If empty, we use b_info_issuercode to find all the default dates related to this news to judge default labels
    4. Forward and Backward:
        a) Forward: We now look three time periods ('12_months', '3_months', '1_months') after news's publishdate to check whether there are default records or not;
        b) Backward: We check whether there are default records before news's publishdate or not.
'''

def generate_default_labels(data_date=None):
    default_labels_config = Default_labels_config(data_date=data_date)
    
    # ----- Load Data in Use ----- #
    ####################################################################################################################
    bond_related_input_path = default_labels_config.bond_related_input_dir
    bond_profile_input_path = default_labels_config.bond_profile_input_dir
    
    # Load in bond_profile.pkl
    bond_profile = pd.read_pickle(os.path.join(bond_profile_input_path, 'bond_profile.pkl'))
    
    # Load in bond related news dataframe into a list
    bond_related_news = []
    for file in os.listdir(bond_related_input_path):
        data_bond_related = pd.read_csv(os.path.join(bond_related_input_path, file))
        bond_related_news.append(data_bond_related)
    
    # ----- Data Cleaning Processing ----- #
    ####################################################################################################################
   
    # Set different time periods that we want to consider the default labels
    extendList = ['12_months', '3_months', '1_months'] # We want to label the default records within 12 months, 3 months, 1 months started from news's publishdate, respectively
    backList = ['last_12month'] # We want to label each news's default record before publishdate, no matter how long it is. ('last_12month' here does not mean we just look backward for 12 months, but all previous periods)
    
    # ----- Export ----- #
    ####################################################################################################################
    # Get Bond related news with Default labels and save them in csv files
    Parallel(n_jobs=20)(delayed(default_labels_processing.default_labels_processing)(frame, bond_profile, extendList, backList, default_labels_config) for frame in bond_related_news)

if __name__ == '__main__':
    generate_default_labels()
