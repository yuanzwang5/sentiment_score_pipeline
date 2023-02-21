import pandas as pd
import numpy as np
import os
import re
import warnings

warnings.filterwarnings("ignore")

def split_labels(string):
    """
    :param string: label in string or Nonetype
    :return: list of labels
    """
    # Judge whether input label is string or not
    if type(string) != str:
        output_string = []
    else:
        if '|' in string:
            output_string = string.split('|')
        else:
            output_string = [string]
    return output_string

def financialnews_preprocess(financialnews):
    """
    :param financialnews: Raw Financialnews DataFrame
    :return: filtered financialnews table
    """
    # Filter no empty publishdate, title, content
    financialnews = financialnews[(financialnews.publishdate.notnull()) & (financialnews.title.notnull()) & (financialnews.content.notnull())].reset_index(drop=True) # Another way: financialnews = financialnews.dropna(subset=['publishdate', 'title', 'content']).reset_index(drop=True)
    
    # Split publishdate from YYYY-MM-DD HH:MM:SS to YYYYMMDD and HH:MM:SS
    financialnews['publish_date'] = financialnews.publishdate.apply(lambda x: str(x)[:10].replace('-',''))
    financialnews['publish_time'] = financialnews.publishdate.apply(lambda x: str(x)[11:19])
    
    # Split '|' in labels into list. If it's empty, fill it with []
    split_label_list = ['windcodes', 'source', 'sections', 'areacodes', 'industrycodes', 'mktsentiments', 'newslevels']
    for label in split_label_list:
        financialnews[label] = financialnews[label].apply(lambda x: split_labels(x))
    
    # Construct News_ID label (maybe not use later, just unite)
    financialnews = financialnews.reset_index()
    financialnews['index'] = financialnews['index'] + 1
    financialnews['News_ID'] = financialnews.apply(lambda x: x['publish_date'][:6]+str(x['index']).zfill(6)+x['publish_date']+x['publish_time'][:2], axis=1)
    
    # Filter out final use columns and rename columns
    financialnews = financialnews[['News_ID','publish_date','publish_time','title','content'] + split_label_list]
    financialnews.columns = ['News_ID','publishDate','publishTime','Title','Content','Windcodes','Source','Sections','Areacodes','Industrycodes','Mktsentiments','Newslevels']
    
    return financialnews

def split_publishdate(financialnews, publishdate, config):
    """
    :param financialnews: DataFrame waiting to split
    :param publishdate: News publishdate to split DataFrame
    :param config: setting path to save files
    :return: each publishdate's DataFrame
    """
    eachdate_financialnews = financialnews[financialnews.publishDate == publishdate].reset_index(drop=True)
    save_file_name = str(publishdate) + '.csv'
    eachdate_financialnews.to_csv(os.path.join(config.split_publishdate_output_dir, save_file_name), index=False, encoding='utf_8_sig')
    
    return eachdate_financialnews

def HtmlFormat_Clear(content):
    """
    :param content: content waiting to delete HTML signs
    :return: content without HTML signs
    """
    dr = re.compile(r'<[^<]*?>', re.S)
    content = dr.sub('', content)
    content = re.sub('\n', '', content)
    content = re.sub('\s', '', content)
    content = re.sub('&nbsp;', '', content)
    
    return content

def second_filter(dataframe, config):
    """
    :param dataframe: DataFrame waiting to drop duplicates 
        --> For news filtered out special signs such as HTML and so on, we just keep the one whose publishtime is at first. Drop others.
    :param config: setting path to save files
    :return: DataFrame without repeated news
    """
    # Drop rows whose Title or Content are not in string format
    dataframe['judge_drop'] = dataframe.apply(lambda x: 1 if ((type(x['Title']) == str) and (type(x['Content']) == str)) else 0, axis=1)
    dataframe = dataframe[dataframe.judge_drop == 1].reset_index(drop=True)
    
    # Remove HTML signs in Title and Content
    dataframe.Title = dataframe.Title.apply(lambda x: HtmlFormat_Clear(HtmlFormat_Clear(x)))
    dataframe.Content = dataframe.Content.apply(lambda x: HtmlFormat_Clear(HtmlFormat_Clear(x)))
    dataframe.rename(columns={'Title':'FormatTitle', 'Content':'FormatContent'}, inplace=True)
    
    # Build up a TempContent without any punctuations
    dataframe['TempContent'] = dataframe.FormatContent.apply(lambda x: ''.join(re.findall(r'[\u4e00-\u9fa5]', x)))
    
    # Keep the first news when some of them are same in TempContent
    dataframe = dataframe.sort_values(by=['TempContent','publishTime'], ascending=(True, True)).reset_index(drop=True)
    dataframe = dataframe.drop_duplicates(subset=['TempContent'], keep='first').reset_index(drop=True)
    
    # Drop news with empty Title or Contents less than 10 words
    dataframe['judge_drop'] = dataframe.apply(lambda x: 1 if ((len(x['FormatTitle']) > 0) and (len(x['TempContent']) > 10)) else 0, axis=1)
    dataframe = dataframe[dataframe.judge_drop == 1].reset_index(drop=True)
    
    # Drop not useful columns
    dataframe = dataframe.drop(['TempContent','judge_drop'], axis=1)
    
    # Save processed data
    publishdate = dataframe.publishDate[0]
    save_file_name = str(publishdate) + '_secondfilter.csv'
    dataframe.to_csv(os.path.join(config.second_filter_output_dir, save_file_name), index=False, encoding='utf_8_sig')
    
    return dataframe
    
