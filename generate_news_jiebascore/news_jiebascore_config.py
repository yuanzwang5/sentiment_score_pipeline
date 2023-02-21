import os

class News_jiebascore_config():
    def __init__(self, data_date=None):
        cbond_tables_dir = '/mnt/utnfs/data/sentiment_score_pipeline/data/raw_data/updated_data/cbond_tables'
        if data_date is None:
            data_version = sorted(os.listdir(cbond_tables_dir))[-1]
        else:
            if data_date not in sorted(os.listdir(cbond_tables_dir)):
                raise ValueError('No data at that date')
            data_version = data_date
        
        self.cbond_tables_dir = os.path.join(cbond_tables_dir, data_version)
        
        ### processed data
        processed_date_dir = '/mnt/utnfs/data/sentiment_score_pipeline/data/processed_data'
        processing_version = os.path.abspath(os.path.join(os.getcwd(), os.pardir)).split('/')[-1]
        
        default_labels_input_dir = os.path.join(processed_date_dir, processing_version, 'features_data/single_default_label')
        
        jieba_score_output_dir = os.path.join(processed_date_dir, processing_version, 'features_data/jieba_score')
        
        self.default_labels_input_dir = os.path.join(default_labels_input_dir, data_version)
        self.jieba_score_output_dir = os.path.join(jieba_score_output_dir, data_version)
        
if __name__ == '__main__':
    config = News_jiebascore_config()
