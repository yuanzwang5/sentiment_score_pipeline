import sys
sys.path.insert(0, '/mnt/utnfs/data/sentiment_score_pipeline/python_code/v0.1.0')
from get_data_from_mysql import get_data_from_mysql
from generate_bond_profile_features import generate_bond_profile_features
from generate_origin_news import generate_origin_news
from generate_bond_related_news import generate_bond_related_news
from generate_default_labels import generate_default_labels
from generate_news_jiebascore import generate_news_jiebascore
from generate_panel_table import generate_panel_table
from generate_model_output import generate_model_output

data_date = '2022-02-15'
update_data = True

### update the data from mySQLDB
if update_data == True:
    get_data_from_mysql.get_updated_data_from_db(data_date=data_date)

### Step 1: generate bond profile
generate_bond_profile_features.generate_bond_profile_features(data_date=data_date)
print('-----------------------------------------------')
print('Finish generate bond profile')
print('-----------------------------------------------')

### Step 2: generate original news
generate_origin_news.generate_origin_news(data_date=data_date)
print('-----------------------------------------------')
print('Finish generate original news')
print('-----------------------------------------------')

### Step 3: generate bond related news
generate_bond_related_news.generate_bond_related_news(data_date=data_date)
print('-----------------------------------------------')
print('Finish generate bond related news')
print('-----------------------------------------------')

### Step 4: generate default labels
generate_default_labels.generate_default_labels(data_date=data_date)
print('-----------------------------------------------')
print('Finish generate default labels')
print('-----------------------------------------------')

### Step 5: generate news jieba score
generate_news_jiebascore.generate_news_jiebascore(data_date=data_date)
print('-----------------------------------------------')
print('Finish generate news jieba score')
print('-----------------------------------------------')

### Step 6: generate panel table
generate_panel_table.generate_panel_table(data_date=data_date)
print('-----------------------------------------------')
print('Finish generate panel table')
print('-----------------------------------------------')

### Step 7: generate model output
generate_model_output.generate_model_output(data_date=data_date)
print('-----------------------------------------------')
print('Finish generate model output')
print('-----------------------------------------------')
