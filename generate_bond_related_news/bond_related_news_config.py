import os

class Bond_related_news_config():
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
        
        second_filter_input_dir = os.path.join(processed_date_dir, processing_version, 'features_data/second_filter')
        bond_profile_input_dir = os.path.join(processed_date_dir, processing_version, 'features_data/bond_profile')
        
        bond_related_output_dir = os.path.join(processed_date_dir, processing_version, 'features_data/bond_related_news')
        
        self.second_filter_input_dir = os.path.join(second_filter_input_dir, data_version)
        self.bond_profile_input_dir = os.path.join(bond_profile_input_dir, data_version)
        self.bond_related_output_dir = os.path.join(bond_related_output_dir, data_version)
        
if __name__ == '__main__':
    config = Bond_related_news_config()
