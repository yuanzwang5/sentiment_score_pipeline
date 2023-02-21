import sys
sys.path.insert(0, '/mnt/utnfs/data/sentiment_score_pipeline/python_code/v0.1.0/generate_model_output')
import pandas as pd
import numpy as np
import os
from model_output_config import Model_output_config
import model_output_processing
import warnings

warnings.filterwarnings("ignore")


def generate_model_output(data_date=None, version=None):
    model_output_config = Model_output_config(data_date=data_date, version=version)
    
    # ----- Load Data in Use ----- #
    ####################################################################################################################
    bond_profile_input_path = model_output_config.bond_profile_input_dir
    panel_table_input_path = model_output_config.panel_table_input_dir
    lastmonth_model_input_path = model_output_config.model_input_dir
    model_output_path = model_output_config.model_output_dir
    
    # Load in bond_profile.pkl
    bond_profile = pd.read_pickle(os.path.join(bond_profile_input_path, 'bond_profile.pkl'))[['b_info_issuercode','b_default_date','b_info_issuer']]

    # Load in Last month's panel table and this month's panel table
    lastmonth_date = model_output_processing.get_lastmonth_date(model_output_processing.get_lastmonth_date(data_date))
    lastmonth_filename = lastmonth_date[:4] + lastmonth_date[5:7] + '_panel_table.csv'
    lastmonth_paneltable = pd.read_csv(os.path.join(lastmonth_model_input_path, lastmonth_filename))
    thismonth_date = model_output_processing.get_lastmonth_date(data_date)
    thismonth_filename = thismonth_date[:4] + thismonth_date[5:7] + '_panel_table.csv'
    thismonth_paneltable = pd.read_csv(os.path.join(panel_table_input_path, thismonth_filename))

    # Set the kind of score which to use
    case = 'jieba_score_sum'
    # Set plot label and color list
    monthlabel = ['January','February','March','April','May','June','July','August','September','October','November','December']
    colorlist = ["red", "blue", "yellow", "gray", "green", "purple", "orange", "steelblue", "pink", "brown", "palegreen", "blueviolet"]
    ten_color = ["brown", "yellow", "gray", "pink", "green", "purple", "orange", "steelblue", "blue", "red"]
    
    # ----- Data Cleaning Processing ----- #
    ####################################################################################################################
    
    # Merge previous Panel table and this month's panel table together
    df_paneltable = model_output_processing.merge_thismonth_df(lastmonth_paneltable, thismonth_paneltable)
    monthlist = df_paneltable.columns[list(df_paneltable.columns).index('201901'):]
    # Set labels for split groups
    labels = np.linspace(1, 10, 10).tolist()
    
    # Prepare for default issuers' list
    default_table = model_output_processing.default_filter(bond_profile)
    
    # Transform panel table in score to panel table in groups
    df_newpanel,df_score_use = model_output_processing.trans_table_ingroup(df_paneltable, case, default_table, labels, monthlist)
    
    # Summary panel table in monthly
    df_totalmonth,df_summarymonth = model_output_processing.summary_table_monthly(df_newpanel, default_table, labels, monthlist)
    
    # Summary Group 10's issuercode's number and number of issuercode that have default records within 1 year
    df_group10_summary = model_output_processing.group_ten_summary(monthlist, df_newpanel, df_totalmonth)
    
    # ----- Export ----- #
    ####################################################################################################################
        
    # Save updated panel table as model output
    save_file_name = thismonth_filename[:6] + '_panel_table.csv'
    if not os.path.exists(model_output_path):
        os.makedirs(model_output_path)
    df_paneltable.to_csv(os.path.join(model_output_path, save_file_name), index=False, encoding='utf_8_sig')
    
    # Set different folders to save results
    new_folder = [case+'_defaultrate_plot_annual', case+'_defaultrate_plot_month', case+'_defaultrate_plot_annualmonth', case+'_group_timeseries', case+'_default_distribution', case+'_missing_distribution']
    for folder in new_folder:
        if not os.path.exists(os.path.join(model_output_path, case, folder)):
            os.makedirs(os.path.join(model_output_path, case, folder))
    
    df_newpanel.to_csv(os.path.join(model_output_path, case, 'performance_'+case+'.csv'), index=False, encoding='utf_8_sig')
    df_totalmonth.to_csv(os.path.join(model_output_path, case, 'performance_summary_monthly_'+case+'.csv'), index=False, encoding='utf_8_sig')
    df_group10_summary.to_csv(os.path.join(model_output_path, case, 'group10_summary_'+case+'.csv'), index=False, encoding='utf_8_sig')
    
    # Plot annually default rate
    df_plotdata = model_output_processing.default_rate_annual_plot(df_totalmonth, labels, case, model_output_path)
    
    # Plot monthly default rate
    model_output_processing.default_rate_month_plot(df_totalmonth, df_summarymonth, labels, case, model_output_path)
    
    # Plot each year's each month's each group's default rate plot
    yearlist = model_output_processing.annual_monthly_plot(df_totalmonth, df_plotdata, labels, monthlabel, colorlist, case, model_output_path)
    
    # Plot 10 group's time series score plot
    model_output_processing.group_time_series_plot(labels, ten_color, monthlist, df_newpanel, df_score_use, case, model_output_path)
    
    # Plot annual Default Distribution Plots (For each month issuers who have news)
    model_output_processing.annual_default_distribution_plot(yearlist, monthlist, df_newpanel, labels, df_score_use, default_table, case, model_output_path)
    
    # Plot the summary of each issuercode's empty score ratio
    model_output_processing.issuer_emptyscore_ratio(df_score_use, default_table, monthlist, case, model_output_path)
    
if __name__ == '__main__':
    generate_model_output()
