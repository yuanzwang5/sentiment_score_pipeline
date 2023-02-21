import os
from datetime import date
import datetime

class Model_output_config():
    def __init__(self, data_date=None, version=None):
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
        
        bond_profile_input_dir = os.path.join(processed_date_dir, processing_version, 'features_data/bond_profile')
        panel_table_input_dir = os.path.join(processed_date_dir, processing_version, 'features_data/panel_table')
        
        # Get last month's date to load in panel table
        formatdate = datetime.date(*map(int, data_version.split('-')))
        thismonth_startdate = datetime.date(formatdate.year, formatdate.month, 1)
        lastmonth_date = datetime.date((thismonth_startdate - datetime.timedelta(1)).year, (thismonth_startdate - datetime.timedelta(1)).month, 15).strftime('%Y-%m-%d')
        
        ### model output
        model_input_dir = '/mnt/utnfs/data/sentiment_score_pipeline/model_output'
        model_output_dir = '/mnt/utnfs/data/sentiment_score_pipeline/model_output'
        
        self.bond_profile_input_dir = os.path.join(bond_profile_input_dir, data_version)
        self.panel_table_input_dir = os.path.join(panel_table_input_dir, data_version)
        self.model_input_dir = os.path.join(model_input_dir, lastmonth_date, version)
        self.model_output_dir = os.path.join(model_output_dir, data_version, version)
        
if __name__ == '__main__':
    config = Model_output_config()
