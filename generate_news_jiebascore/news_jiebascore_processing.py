import pandas as pd
import jieba
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()
import warnings

warnings.filterwarnings("ignore")

def stopwordslist(filepath):
    """
    :param filepath: Path that save stopwords file
    :return: stopwords list
    """
    stopwords = [line.strip() for line in open(filepath, 'r', encoding='UTF-8').readlines()]
    return stopwords

def seg_sentence(sentence, filepath):
    """
    :param sentence: The string waiting to segregate
    :param filepath: File path of stopword list
    :return: List of words for each sentence without words in stopword list
    """
    sentence_seged = jieba.cut(sentence.strip(), cut_all=False)
    stopwords = stopwordslist(filepath)
    outstr = ''
    for word in sentence_seged:
        if word not in stopwords:
            if word != '\t':
                outstr += word
                outstr += " "
    return outstr


def jieba_score(title, content, onepointlist, twopointlist, threepointlist, fourpointlist, fivepointlist, filepath):
    """
    :param title: Each News's FormatTitle
    :param content: Each News's FormatContent
    :param onepointlist ~ fivepointlist: Five words list to give 1 ~ 5 points to calculate news's score related to default
    :param filepath: File path of stopword list
    :return: Each news's jieba score that we construct and each sentence's word number
    """
    # First combine FormatTitle and FormatContent together to complete jieba cut
    combine = title + content
    
    # Start to segregate sentence and summary words number
    segcombine = seg_sentence(combine, filepath)
    seglist = segcombine.split(' ')
    segcount = pd.value_counts(seglist).reset_index()
    segcount.columns = ['Word', 'Num']
    
    # Calculate each pointlist's total points
    fivepoint = segcount[segcount['Word'].isin(fivepointlist)]['Num'].sum() * 5
    fourpoint = segcount[segcount['Word'].isin(fourpointlist)]['Num'].sum() * 4
    threepoint = segcount[segcount['Word'].isin(threepointlist)]['Num'].sum() * 3
    twopoint = segcount[segcount['Word'].isin(twopointlist)]['Num'].sum() * 2
    onepoint = segcount[segcount['Word'].isin(onepointlist)]['Num'].sum() * 1
    totalpoint = fivepoint + fourpoint + threepoint + twopoint + onepoint
    
    # Summary each sentence's word number
    totalwordnum = segcount['Num'].sum()
    
    return [totalpoint, totalwordnum]

def special_words(title, speciallist):
    """
    :param title: News's FormatTitle
    :param speciallist: Special words list to judge whether we drop the news or not
    :return: Label value. 1 means FormatTitle has word in special words list, 0 means not
    """
    for item in speciallist:
        if item in title:
            result = 1
            break
        else:
            result = 0
    return result


def news_jiebascore_processing(dataframe, onepointlist, twopointlist, threepointlist, fourpointlist, fivepointlist, filepath_jieba, filepath_stopwords, specialwords, newslength):
    """
    :param dataframe: DataFrame that waiting to filter and calculate jieba score
    :param onepointlist ~ fivepointlist: Five words list to give 1 ~ 5 points to calculate news's score related to default
    :param filepath_jieba: File path of jieba dictionary list
    :param filepath_stopwords: File path of stopword list
    :param specialwords: If news's title include these words, we drop this news
    :param newslength: News limited length. If words number of news larger than this number, we drop this news
    :return: DataFrame that calculated jieba score with different considerations
    """
    # Drop News that only have default records before news' publishdate and no default records after publishdate ('last_12month' = -1 and '12_months' = 1)
    dataframe = dataframe.drop(index=list(dataframe[(dataframe['last_12month'] == -1) & (dataframe['12_months'] == 1)].index)).reset_index(drop=True)
    if dataframe.empty:
        pass
    else:
        # Drop News that have default records within 1 month after publishdate (Why do this? --> News within 1 month that has default records does not have predictability at all)
        dataframe = dataframe.drop(index=list(dataframe[dataframe['1_months'] == -1].index)).reset_index(drop=True)
        if dataframe.empty:
            pass
        else:
            # Judge whether FormatTitle include words in special list
            dataframe['contains_words'] = dataframe.apply(lambda x: special_words(x['FormatTitle'], specialwords), axis=1)
            
            # Load in jieba dictionary used
            jieba.load_userdict(filepath_jieba)
            
            # Calculate News's jieba default score
            dataframe['jieba_score'] = dataframe.apply(lambda x: jieba_score(x['FormatTitle'], x['FormatContent'], onepointlist, twopointlist, threepointlist, fourpointlist, fivepointlist, filepath_stopwords)[0], axis=1)
            
            # Divided jieba default score by b_info_issuercode number
            dataframe['b_info_issuercode'] = dataframe['b_info_issuercode'].apply(lambda x: eval(x) if type(x) == str else x)
            dataframe['jieba_score_div_lenissuer'] = dataframe.apply(lambda x: x['jieba_score']/len(x['b_info_issuercode']) if len(x['b_info_issuercode']) != 0 else x['jieba_score'], axis=1)
            
            # Normalized jieba default score
            dataframe['jieba_score_div_lenissuer_normalized'] = scaler.fit_transform(dataframe['jieba_score_div_lenissuer'].values.reshape(-1,1))
            
            # Get each News's total word number
            dataframe['words_num'] = dataframe.apply(lambda x: jieba_score(x['FormatTitle'], x['FormatContent'], onepointlist, twopointlist, threepointlist, fourpointlist, fivepointlist, filepath_stopwords)[1], axis=1)
            
            # Generate another version jieba default score that divided by news's total word number
            dataframe['jieba_score_div_lenissuer_wordnum'] = dataframe['jieba_score_div_lenissuer']/dataframe['words_num']
            
            # Judge the news's words number, whether it largerthan our limit
            dataframe['larger_than_len'] = dataframe['words_num'].apply(lambda x: 1 if x > newslength else 0)
            
            # Drop news that with special words in FormatTitle and news content larger than limit number's length
            dataframe = dataframe[(dataframe['contains_words'] == 0) & (dataframe['larger_than_len'] == 0)].reset_index(drop=True)
            
            dataframe = dataframe.drop(['contains_words','larger_than_len'], axis=1)
    
    return dataframe
