import sys
sys.path.insert(0, '/mnt/utnfs/data/sentiment_score_pipeline/python_code/v0.1.0/generate_origin_news')
import pandas as pd
import os
from joblib import Parallel, delayed
from origin_news_config import Origin_news_config
import origin_news_processing 
import warnings

warnings.filterwarnings("ignore")


def generate_origin_news(data_date=None):
    origin_news_config = Origin_news_config(data_date=data_date)
    
    # ----- Load Raw Data ----- #
    ####################################################################################################################
    input_path = origin_news_config.cbond_tables_dir
    origin_news_output_path = origin_news_config.origin_news_output_dir
    split_publishdate_output_path = origin_news_config.split_publishdate_output_dir
    second_filter_output_path = origin_news_config.second_filter_output_dir
    
    financialnews = pd.read_pickle(os.path.join(input_path, 'financialnews.pkl.gz'))
    
    # ----- Data Cleaning Processing ----- #
    ####################################################################################################################
    # First clean the original financialnews data
    financialnews = origin_news_processing.financialnews_preprocess(financialnews)
    
    # Split data by publishdate into a list, each element in the list will be a dataframe
    publishdate_list = list(financialnews.publishDate.unique())
    if not os.path.exists(split_publishdate_output_path):
        os.makedirs(split_publishdate_output_path)
    splitted_financialnews = Parallel(n_jobs=20, backend='threading')(delayed(origin_news_processing.split_publishdate)(financialnews, publish_date, origin_news_config) for publish_date in publishdate_list)
    
    # Complete second filter steps: Drop empty Title and Content; Delete HTML signs ...
    if not os.path.exists(second_filter_output_path):
        os.makedirs(second_filter_output_path)
    Parallel(n_jobs=20)(delayed(origin_news_processing.second_filter)(frame, origin_news_config) for frame in splitted_financialnews)
    
    # ----- Export ----- #
    ####################################################################################################################
    if not os.path.exists(origin_news_output_path):
        os.makedirs(origin_news_output_path)
    
    financialnews.to_pickle(os.path.join(origin_news_output_path, origin_news_config.filtered_newsinfo))

if __name__ == '__main__':
    generate_origin_news()
