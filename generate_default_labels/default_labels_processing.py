import pandas as pd
import os
import numpy as np
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import warnings

warnings.filterwarnings("ignore")

def default_judge(publishDate, periodList, default_date, waytolook):
    """
    :param publishDate: News's publishDate
    :param periodList: time periods that we want to check the default record
    :param default_date: Each news may related to different 's_info_windcode' or 'b_info_issuercode'. We find the first default date's record
    :param waytolook: Look forward or backward (after or before publishdate)
    :return: Default judge label list (1 for Default and -1 for Non-Default)
    """
    # Set an empty list to save default labels
    judge_list = []
    
    # Start to judge different time periods' default records
    for period in periodList:
        # Prepare different time periods' calculation function
        if period.endswith('months'):
            func = relativedelta(months = int(period[:(period.index('_'))]))
        elif period.endswith('days'):
            func = timedelta(days = int(period[:(period.index('_'))]))
        elif period.endswith('weeks'):
            func = timedelta(weeks = int(period[:(period.index('_'))]))
        
        # To judge forward or backward
        if waytolook == 'forward':
            end_time = publishDate + func
            if pd.isnull(default_date):
                judge_list.append(1)
            elif default_date <= publishDate:
                judge_list.append(1)
            elif default_date <= end_time:
                judge_list.append(-1)
            else:
                judge_list.append(1)
        
        elif waytolook == 'backward':
            if pd.isnull(default_date):
                judge_list.append(1)
            elif default_date <= publishDate:
                judge_list.append(-1)
            else:
                judge_list.append(1)
    
    return judge_list

def containNeg(x):
    """
    :param x: Input list with 'last_12month', '12_months', '3_months', '1_months' to judge whether -1 is included in the list
    :return: +/- 1
    """
    if -1 in x:
        res = -1
    else:
        res = 1
    return res

def get_default_frame(df, code, bond_profile, extendlist, backlist):
    """
    :param df: News DataFrame waiting to judge default
    :param code: Which code used to judge default? ('s_info_windcode' or 'b_info_issuercode')
    :param bond_profile: 4 groups bonds information DataFrame
    :param extendlist: Time periods that we want to use to judge default (Looking ahead)
    :param backlist: Time periods that we want to use to judge default (Looking backward)
    :return: DataFrame that labeled with default records forward and backward
    """
    # Copy the column of the code for further use or save
    df[code + '_copy'] = df[code].copy()
    
    # Explode code list into rows for default judgement
    df = df.explode(code).reset_index(drop=True)
    
    # For different code, use different default_date
    if code == 's_info_windcode':
        # if each 's_info_windcode' has many default date, we keep the earliest one
        df = df.merge(bond_profile[[code, 'b_default_date']].sort_values(by=[code, 'b_default_date']).drop_duplicates(keep='first').reset_index(drop=True), on=[code], how='left')
    else:
        # if other codes (e.g. 'b_info_issuercode') have many default date, we consider all default dates
        df = df.merge(bond_profile[[code, 'b_default_date']].drop_duplicates().reset_index(drop=True), on=[code], how='left')
    
    # Change default date and publish date's format to YYYYMMDD
    df['b_default_date'] = pd.to_datetime(df['b_default_date'], format='%Y%m%d')
    df['publishDate'] = pd.to_datetime(df['publishDate'], format='%Y%m%d')
    
    # Looking Forward Default Judgement results
    df['y_label'] = df.apply(lambda x: default_judge(x['publishDate'], extendlist, x['b_default_date'], 'forward'), axis=1)
    
    # Looking Backward Default Judgement results 
    # (!!! Notes: Only when news related to only one 's_info_windcode' or 'b_info_issuercode', we judge backward. If not, we just label backward not default here)
    df['special_label'] = df.apply(lambda x: default_judge(x['publishDate'], backlist, x['b_default_date'], 'backward') if x['single_news'] == 1 else [1]*len(backlist), axis=1)
    
    # Concat Default Judgement results with news data together
    df = pd.concat([df, pd.DataFrame(df['y_label'].tolist(), columns=extendlist)], axis=1)
    df = pd.concat([df, pd.DataFrame(df['special_label'].tolist(), columns=backlist)], axis=1)
    
    # Groupby default judgement together 
    # (Because each news may have different codes. We just judge default one by one, and here we summary the results)
    # (Which means that if the news has many codes and at least one of them has default records within related time period, we label this news -1 (default) in this time period)
    df = df.groupby(['News_ID'])['12_months', '3_months', '1_months', 'last_12month'].agg(lambda x: int(containNeg(list(x)))).reset_index()
    
    return df

def default_labels_processing(dataframe, bond_profile, extendlist, backlist, config):
    '''
    :param dataframe: DataFrame that waiting for label default judgement
    :param bond_profile: Bond information DataFrame used to extend 'b_info_issuercode' using 's_info_windcode'
    :param extendlist: Time periods that we want to use to judge default (Looking ahead)
    :param backlist: Time periods that we want to use to judge default (Looking backward)
    :param config: Files' path
    :return: DataFrame labeled with default after judgement
    '''
    # Use bond_profile to find each news's 's_info_windcode' and then extend the related 'b_info_issuercode' into 'b_info_issuercode' list
    bond_issuercode = bond_profile[['s_info_windcode', 'b_info_issuercode']].drop_duplicates().reset_index(drop=True)
    bond_dict = bond_issuercode.set_index('s_info_windcode')['b_info_issuercode'].to_dict()
    dataframe['b_info_issuercode'] = dataframe.apply(lambda x: list(set(eval(x['b_info_issuercode'])).union(set([bond_dict.get(item, item) for item in eval(x['s_info_windcode'])]))), axis=1)
    
    # Label whether the news only related to one issuercode after extend. 1 for single news, 0 for not single news
    dataframe['single_news'] = dataframe.apply(lambda x: 1 if len(x['b_info_issuercode']) == 1 else 0, axis=1)
    
    # Switch 's_info_windcode' and 'b_info_issuercode' to string to judge whether they are empty and also choose which code to use for the judgement
    dataframe['s_info_windcode'] = dataframe['s_info_windcode'].apply(lambda x: str(x))
    dataframe['b_info_issuercode'] = dataframe['b_info_issuercode'].apply(lambda x: str(x))
    
    # Split the DataFrame into two parts, whether the 's_info_windcode' is empty or not
    overall_dataframe = [] # waiting to concat the DataFrame after filtering
    # If 's_info_windcode' is not empty, we use it to judge default label
    windcode_notempty = dataframe[dataframe['s_info_windcode'] != '[]'].reset_index(drop=True)
    if windcode_notempty.empty:
        pass
    else:
        windcode_notempty = get_default_frame(windcode_notempty, 's_info_windcode', bond_profile, extendlist, backlist)
        overall_dataframe.append(windcode_notempty)
    # If 's_info_windcode' is empty, we use 'b_info_issuercode' to judge default label
    windcode_empty = dataframe[(dataframe['s_info_windcode'] == '[]') & (dataframe['b_info_issuercode'] != '[]')].reset_index(drop=True)
    if windcode_empty.empty:
        pass
    else:
        windcode_empty = get_default_frame(windcode_empty, 'b_info_issuercode', bond_profile, extendlist, backlist)
        overall_dataframe.append(windcode_empty)
    result = pd.concat(overall_dataframe).reset_index(drop=True)
    result = result[['News_ID', 'last_12month', '12_months', '3_months', '1_months']]
    finaloutput = pd.merge(dataframe, result, on=['News_ID'], how='left')
    
    # Save processed data
    publishdate = finaloutput.publishDate[0]
    save_file_name = str(publishdate) + '_listfilter_bond.csv'
    if not os.path.exists(config.default_labels_output_dir):
        os.makedirs(config.default_labels_output_dir)
    finaloutput.to_csv(os.path.join(config.default_labels_output_dir, save_file_name), index=False, encoding='utf_8_sig')
    
    return finaloutput
