import sys
sys.path.insert(0, '/mnt/utnfs/data/sentiment_score_pipeline/python_code/v0.1.0/generate_news_jiebascore')
import pandas as pd
import os
from joblib import Parallel, delayed
from news_jiebascore_config import News_jiebascore_config
import news_jiebascore_processing
import warnings

warnings.filterwarnings("ignore")


def generate_news_jiebascore(data_date=None):
    news_jiebascore_config = News_jiebascore_config(data_date=data_date)
    
    # ----- Load Data in Use ----- #
    ####################################################################################################################
    default_labels_input_path = news_jiebascore_config.default_labels_input_dir
    jieba_dict_input_path = "/mnt/utnfs/data/sentiment_score_pipeline/data/raw_data/non_updated_data/jieba_dict/default_words.txt"
    stopwords_input_path = "/mnt/utnfs/data/sentiment_score_pipeline/data/raw_data/non_updated_data/jieba_dict/stopwordlist.txt"
    specialwords_input_path = "/mnt/utnfs/data/sentiment_score_pipeline/data/raw_data/non_updated_data/special_words.csv"
    jieba_score_output_path = news_jiebascore_config.jieba_score_output_dir
    
    # Load in news dataframe with default labels
    default_labeled_news = []
    for file in os.listdir(default_labels_input_path):
        data_default_label = pd.read_csv(os.path.join(default_labels_input_path, file))
        default_labeled_news.append(data_default_label)
    
    # ----- Data Cleaning Processing ----- #
    ####################################################################################################################
    
    # Set words list with different points to give
    fivepointlist = ['违约','无力偿还','未偿还','尚未','未能','未','负债','质押','逾期','破产','重组','清偿','退市','踩雷','暴雷']
    fourpointlist = ['查封','清算','违法','摘牌','处罚','违规','涉嫌','违纪','违反','诉讼','冻结','仲裁','起诉','警告','纠纷']
    threepointlist = ['负面','下调','降级','调降','最低','停牌','不确定性','偿债','风险警示','债务危机','兑付风险','风险','危机','警示','警惕','预警','偏离']
    twopointlist = ['缺口','减弱','较弱','难以','不足','下行','困难','加剧','紧张','压力','下滑','减值','未达','缓慢','异常','严重','下跌','缺陷','否定','恶化','惨淡','艰难','不佳','下降','亏损','差','低','减少','延期','降低','波动','递延','损失','有限','低于','跌幅','跌','腰斩','低迷','缺乏','较低','回落','大跌','困境','不利','收窄','暴跌']
    onepointlist = ['出售','减持','变更','回售','暂停','披露','终止','变动','异动','筹措','抛售','质疑','受限','虚假','审议','收购','审计','影响','辞职','拍卖','转让','波及','风波','观察','冲击']
    specialwords = list(pd.read_csv(specialwords_input_path)['special_list'])
    
    news_jiebascore_filtered = Parallel(n_jobs=20)(delayed(news_jiebascore_processing.news_jiebascore_processing)(frame, onepointlist, twopointlist, threepointlist, fourpointlist, fivepointlist, jieba_dict_input_path, stopwords_input_path, specialwords, 2000) for frame in default_labeled_news)
    
    # ----- Export ----- #
    ####################################################################################################################
    
    # Double check whether the DataFrame in the list is empty or not
    waitingoutput = []
    for df in news_jiebascore_filtered:
        if df.empty:
            continue
        else:
            if df.shape[0] == 0:
                continue
            else:
                waitingoutput.append(df)
    
    # Concat the DataFrame in the list into a whole DataFrame table
    df_jiebascore = pd.concat(waitingoutput).reset_index(drop=True)
    
    # Save processed data
    newsmonth = str(df_jiebascore['publishDate'][0])[:6]
    save_file_name = newsmonth + '_jieba_score.csv'
    if not os.path.exists(jieba_score_output_path):
        os.makedirs(jieba_score_output_path)
    df_jiebascore.to_csv(os.path.join(jieba_score_output_path, save_file_name), index=False, encoding='utf_8_sig')

if __name__ == '__main__':
    generate_news_jiebascore()
