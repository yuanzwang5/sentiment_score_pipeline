import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import os

def get_lastmonth_date(thismonthdate):
    """
    :param thismonthdate: this month's date in format YYYY-MM-DD
    :return: last month's date in format YYYY-MM-DD
    """
    
    formatdate = datetime.date(*map(int, thismonthdate.split('-')))
    thismonth_startdate = datetime.date(formatdate.year, formatdate.month, 1)
    lastmonth_date = datetime.date((thismonth_startdate - datetime.timedelta(1)).year, (thismonth_startdate - datetime.timedelta(1)).month, 15).strftime('%Y-%m-%d')
    
    return lastmonth_date

def merge_thismonth_df(previous_df, thismonth_df):
    """
    :param previous_df: previous DataFrame of panel table
    :param thismonth_df: thismonth's DataFrame of panel table
    """
    # Merge previous DataFrame and this month's DataFrame together
    df_thismonth = previous_df.merge(thismonth_df, on=['variable','b_info_issuercode','b_info_issuer'], how='outer')
    # Make sure no error on merge DataFrame
    df_thismonth = df_thismonth.drop_duplicates(subset=['variable','b_info_issuercode','b_info_issuer']).reset_index(drop=True)
    
    # Redefined default_label_column
    df_thismonth['default_label_x'] = df_thismonth['default_label_y'].fillna(0)
    df_thismonth['default_label_y'] = df_thismonth['default_label_y'].fillna(0)
    df_thismonth['default_label'] = df_thismonth['default_label_x'] + df_thismonth['default_label_y']
    df_thismonth['default_label'] = df_thismonth['default_label'].apply(lambda x: 1 if x == 2 else x)
    df_thismonth = df_thismonth.drop(['default_label_x','default_label_y'], axis=1)
    cols = df_thismonth.columns.tolist()
    cols.insert(3, cols.pop(cols.index('default_label')))
    df_thismonth = df_thismonth[cols]
    
    return df_thismonth

def default_filter(bond_profile):
    """
    :param bond_profile: bond_profile.pkl DataFrame
    :return: default records with all issuercodes, issuers, default date and default month
    """
    default_table = bond_profile.dropna(subset=['b_default_date']).reset_index(drop=True)
    default_table = default_table.drop_duplicates().reset_index(drop=True)
    default_table['b_default_date'] = default_table['b_default_date'].apply(lambda x: str(int(x)))
    default_table['default_month'] = default_table['b_default_date'].apply(lambda x: str(int(x))[:6])
    return default_table

def trans_table_ingroup(previous_panel, case, default_table, labels, monthlist):
    """
    :param score_panel: panel table with jieba_score_sum and default date
    :param case: the score that we choose to use. (e.g. jieba_score_sum)
    :param default_table: default issuercodes' information DataFrame
    :param labels: group labels
    :param monthlist: months we need to summary
    :return: panel table labeled by different groups
    """
    
    # Set new panel DataFrame prepare for merge and filter score rows to split groups
    df_newpanel = previous_panel[previous_panel.columns[:4]][previous_panel.variable == case].sort_values(by=['b_info_issuercode'], ascending=True).reset_index(drop=True)
    df_score_use = previous_panel[previous_panel.variable == case].reset_index(drop=True)
    
    # Start to transform the table in score to table in groups
    ### If issuercode has default records, we give it new score -1. 
    ### If the score is None, we keep it not in any groups. 
    ### If it has score without default records, we rank score and give it group num from 1 to 10    
    for month in monthlist:
        
        # Get list of issuercode who has default records in or before this month
        temp_defaultlist = list(default_table[default_table.default_month <= month].b_info_issuercode.unique())
        # Get this month's previous score from panel table
        df_score = df_score_use[['b_info_issuercode',month]]
    
        # If issuercode has default records, give it new score -1
        tempdf_defaultpart = df_score[df_score.b_info_issuercode.isin(temp_defaultlist)].reset_index(drop=True)
        tempdf_defaultpart[month] = -1
    
        # For issuercodes do not have default records, judge whether the score is nan
        tempdf_nodefaultpart = df_score[~df_score.b_info_issuercode.isin(temp_defaultlist)].reset_index(drop=True)
        tempdf_nodefault_nanpart = tempdf_nodefaultpart[tempdf_nodefaultpart[month].isna()].reset_index(drop=True) # NaN part
        tempdf_nodefault_valuepart = tempdf_nodefaultpart[~tempdf_nodefaultpart[month].isna()].reset_index(drop=True) # With score part
        # For issuercodes have score part, rank score and split into 10 groups
        tempdf_nodefault_valuepart[month] = pd.cut(tempdf_nodefault_valuepart[month].rank(), 10, labels = labels, include_lowest=True)
    
        # Concat together three part of dataframe and merge with the previous panel to get the new panel table
        df_output = pd.concat([tempdf_defaultpart, tempdf_nodefault_nanpart, tempdf_nodefault_valuepart]).sort_values(by=['b_info_issuercode'], ascending=True).reset_index(drop=True)
        df_newpanel = df_newpanel.merge(df_output, on=['b_info_issuercode'], how='left')
        df_newpanel = df_newpanel.drop_duplicates(subset=['b_info_issuercode']).reset_index(drop=True)
        
        del df_score,tempdf_defaultpart,tempdf_nodefaultpart,tempdf_nodefault_nanpart,tempdf_nodefault_valuepart,df_output
    
    return df_newpanel,df_score_use

def summary_table_monthly(df_newpanel, default_table, labels, monthlist):
    """
    :param df_newpanel: panel table in groups label
    :param default_table: default issuercodes' information DataFrame
    :param labels: group labels
    :param monthlist: months we need to summary
    :return: Monthly summary of groups information
    """
    eachmonth_dflist = []
    for month in monthlist:        
        # Set list to save new dataframe's each row value
        eachmonth = [month]
    
        # Get this month's score situation dataframe
        month_score = df_newpanel[['b_info_issuercode','b_info_issuer', month]]
    
        # Number and list of issuercodes who have default in this month or previous months
        havedefaultpart = month_score[month_score[month] == -1].reset_index(drop=True)
        eachmonth.append(havedefaultpart.shape[0])
        eachmonth.append(list(havedefaultpart[['b_info_issuercode','b_info_issuer']].apply(tuple, axis=1)))
    
        # Get default issuers who have default records from this month to 1 year later
        default_with1y = list(default_table[(default_table.default_month >= month) & (default_table.default_month <= str(int(month)+100))].b_info_issuercode.unique())
        
        # Number of issuercodes who do not have news this month and list of them who have default records within 1 year starts from this month
        nonewspart = month_score[month_score[month].isna()].reset_index(drop=True)
        eachmonth.append(nonewspart.shape[0])
        nonews_withindefault1y = list(nonewspart[nonewspart.b_info_issuercode.isin(default_with1y)][['b_info_issuercode','b_info_issuer']].apply(tuple, axis=1))
        eachmonth.append(len(nonews_withindefault1y))
        eachmonth.append(nonews_withindefault1y)
    
        # Number of issuercodes who have news and each group's default issuercode number and company list
        havenewspart = month_score[month_score[month].isin(labels)].reset_index(drop=True)
        eachmonth.append(havenewspart.shape[0])
        for label in labels:
            # Get each group's data part
            eachgroup = havenewspart[havenewspart[month] == label].reset_index(drop=True)
            # Get each group's issuercodes who have default records within 1 year
            eachgroup_default = list(eachgroup[eachgroup.b_info_issuercode.isin(default_with1y)][['b_info_issuercode','b_info_issuer']].apply(tuple, axis=1))
        
            eachmonth.append(eachgroup.shape[0])
            eachmonth.append(len(eachgroup_default))
            eachmonth.append(eachgroup_default)
        
        # Random filter 10% of sample to test the Number of issuercodes who have news and how many issuercodes in them have default records
        random_havenews = havenewspart.sample((havenewspart.shape[0])//10)
        eachmonth.append(random_havenews.shape[0])
        random_havenews_default = list(random_havenews[random_havenews.b_info_issuercode.isin(default_with1y)][['b_info_issuercode','b_info_issuer']].apply(tuple, axis=1))
        eachmonth.append(len(random_havenews_default))
        eachmonth.append(random_havenews_default)
        
        # Save as dataframe
        df_eachmonth = pd.DataFrame(eachmonth).T
        eachmonth_dflist.append(df_eachmonth)
    df_totalmonth = pd.concat(eachmonth_dflist).reset_index(drop=True)
    df_totalmonth.columns = ['Month','Num_havedefault_thisorprevious','listofleft_default','Num_nonews',
                             'Num_nonews_havedefault','listofleft_nonews_default',
                             'Num_havenews',
                             'Num_group1','Num_group1_havedefault','list_default_group1',
                             'Num_group2','Num_group2_havedefault','list_default_group2',
                             'Num_group3','Num_group3_havedefault','list_default_group3',
                             'Num_group4','Num_group4_havedefault','list_default_group4',
                             'Num_group5','Num_group5_havedefault','list_default_group5',
                             'Num_group6','Num_group6_havedefault','list_default_group6',
                             'Num_group7','Num_group7_havedefault','list_default_group7',
                             'Num_group8','Num_group8_havedefault','list_default_group8',
                             'Num_group9','Num_group9_havedefault','list_default_group9',
                             'Num_group10','Num_group10_havedefault','list_default_group10',
                             'Random10','Random10_havedefault','list_default_random10']

    # Summary each month's accuracy (Group10, 10+9, 10+9+8)
    df_summarymonth = df_totalmonth[['Month', 'Num_group1_havedefault', 'Num_group2_havedefault',
                                     'Num_group3_havedefault','Num_group4_havedefault','Num_group5_havedefault',
                                     'Num_group6_havedefault','Num_group7_havedefault','Num_group8_havedefault',
                                     'Num_group9_havedefault','Num_group10_havedefault']]
    
    df_summarymonth['Total_default'] = df_summarymonth.iloc[:,1:11].sum(axis=1)
    df_summarymonth['Group10_default'] = df_summarymonth.iloc[:,10:11].sum(axis=1)
    df_summarymonth['Group10_9_default'] = df_summarymonth.iloc[:,9:11].sum(axis=1)
    df_summarymonth['Group10_8_default'] = df_summarymonth.iloc[:,8:11].sum(axis=1)

    df_summarymonth['Group10_capturerate'] = df_summarymonth['Group10_default']/df_summarymonth['Total_default']
    df_summarymonth['Group10_9_capturerate'] = df_summarymonth['Group10_9_default']/df_summarymonth['Total_default']
    df_summarymonth['Group10_8_capturerate'] = df_summarymonth['Group10_8_default']/df_summarymonth['Total_default']
    
    df_summarymonth['Group10_capturerate_table'] = df_summarymonth.apply(lambda x: str(int(x['Group10_default']))+'/'+str(int(x['Total_default']))+' ({:.2%})'.format(x['Group10_capturerate']), axis=1)
    df_summarymonth['Group10_9_capturerate_table'] = df_summarymonth.apply(lambda x: str(int(x['Group10_9_default']))+'/'+str(int(x['Total_default']))+' ({:.2%})'.format(x['Group10_9_capturerate']), axis=1)
    df_summarymonth['Group10_8_capturerate_table'] = df_summarymonth.apply(lambda x: str(int(x['Group10_8_default']))+'/'+str(int(x['Total_default']))+' ({:.2%})'.format(x['Group10_8_capturerate']), axis=1)
    df_summarymonth = df_summarymonth[['Month','Group10_capturerate','Group10_9_capturerate','Group10_8_capturerate','Group10_capturerate_table','Group10_9_capturerate_table','Group10_8_capturerate_table']]

    df_totalmonth = df_totalmonth.merge(df_summarymonth[['Month','Group10_capturerate_table','Group10_9_capturerate_table','Group10_8_capturerate_table']], on='Month', how='left')
    
    return df_totalmonth,df_summarymonth

def to_percent(temp, position):
    """
    :function use to change format of number show in the plots
    """
    return '%1.1f'%(temp) + '%'

def default_rate_annual_plot(df_totalmonth, labels, case, output_path):
    """
    :param df_totalmonth: Monthly summary of groups information
    :param labels: group labels
    :param case: the score that we choose to use. (e.g. jieba_score_sum)
    :param output_path: path to save plots
    :return: annually data for later plot use
    """
    # Generate year label
    df_totalmonth['Year'] = df_totalmonth.Month.apply(lambda x: x[:4])
    
    # Generate plot data
    df_plotdata = df_totalmonth[['Year','Num_group1', 'Num_group1_havedefault',
                                 'Num_group2', 'Num_group2_havedefault',
                                 'Num_group3', 'Num_group3_havedefault',
                                 'Num_group4', 'Num_group4_havedefault',
                                 'Num_group5', 'Num_group5_havedefault',
                                 'Num_group6', 'Num_group6_havedefault',
                                 'Num_group7', 'Num_group7_havedefault',
                                 'Num_group8', 'Num_group8_havedefault',
                                 'Num_group9', 'Num_group9_havedefault',
                                 'Num_group10', 'Num_group10_havedefault']]
    df_plotdata = df_plotdata.groupby(['Year']).sum()
    
    # Calculate each group's default rate
    columnname = []
    for label in labels:
        df_plotdata['Rate_group'+str(int(label))] = df_plotdata['Num_group'+str(int(label))+'_havedefault']/df_plotdata['Num_group'+str(int(label))]
        columnname.append('Rate_group'+str(int(label)))
    df_plotdata = df_plotdata[columnname].T

    # Start to plot 10 groups' default rate by annually
    for year in df_plotdata.columns:
        plt.rcParams['figure.figsize'] = (30.0, 20.0)
        plot_y = np.array(df_plotdata[year])*100
        plt.plot(plot_y, color='r', linewidth=5, marker='o', markerfacecolor='blue', markersize=15)
        plt.xticks(np.arange(0,10,1), df_plotdata.index, fontsize=20)
        plt.gca().yaxis.set_major_formatter(FuncFormatter(to_percent))
        plt.yticks(fontsize=20)
        plt.title(year+' 10 Groups Default Rate', fontsize=20)
        plt.xlabel('Group', fontsize=20)
        plt.ylabel('Default Rate', fontsize=20)
        for a, b in zip(np.arange(0,10,1), plot_y):
            plt.text(a, b, str(round(b, 2))+'%', ha='right', va='top', fontsize=20)
        plt.grid(axis='y')
        plt.savefig(os.path.join(output_path, case, case+'_defaultrate_plot_annual/'+year+'_'+case+'_defaultrate_plot.png'), bbox_inches='tight', dpi=200, pad_inches=0.0)
        plt.show()
        plt.close()
    return df_plotdata

def default_rate_month_plot(df_totalmonth, df_summarymonth, labels, case, output_path):
    """
    :param df_totalmonth: Monthly summary of groups information
    :param df_summarymonth: Each month's monthly accuracy DataFrame
    :param labels: group labels
    :param case: the score that we choose to use. (e.g. jieba_score_sum)
    :param output_path: path to save plots
    """
    # Generate monthly plot data
    df_plotdata_month = df_totalmonth[['Month', 'Num_havenews',
                                       'Num_group1', 'Num_group1_havedefault',
                                       'Num_group2', 'Num_group2_havedefault',
                                       'Num_group3', 'Num_group3_havedefault',
                                       'Num_group4', 'Num_group4_havedefault',
                                       'Num_group5', 'Num_group5_havedefault',
                                       'Num_group6', 'Num_group6_havedefault',
                                       'Num_group7', 'Num_group7_havedefault',
                                       'Num_group8', 'Num_group8_havedefault',
                                       'Num_group9', 'Num_group9_havedefault',
                                       'Num_group10', 'Num_group10_havedefault',
                                       'Random10','Random10_havedefault']]

    # Calculate each group's default rate
    columnname = []
    for label in labels:
        df_plotdata_month['Rate_group'+str(int(label))] = df_plotdata_month['Num_group'+str(int(label))+'_havedefault']/df_plotdata_month['Num_group'+str(int(label))]
        columnname.append('Rate_group'+str(int(label)))
    df_plotdata_month['Random10_defaultrate'] = df_plotdata_month['Random10_havedefault']/df_plotdata_month['Random10']
    
    # Calculate total default rate
    df_plotdata_month['Num_havedefault'] = df_plotdata_month[['Num_group1_havedefault','Num_group2_havedefault',
                                                         'Num_group3_havedefault','Num_group4_havedefault',
                                                         'Num_group5_havedefault','Num_group6_havedefault',
                                                         'Num_group7_havedefault','Num_group8_havedefault',
                                                         'Num_group9_havedefault','Num_group10_havedefault']].sum(axis=1)
    df_plotdata_month['Total_defaultrate'] = df_plotdata_month['Num_havedefault']/df_plotdata_month['Num_havenews']

    # Prepare for Group 10's capture rate to show on the plot
    df_plotdata_month = df_plotdata_month.merge(df_summarymonth[['Month','Group10_capturerate']], on=['Month'], how='left')
    df_plotdata_month = df_plotdata_month.set_index('Month')
    df_plotdata_month = df_plotdata_month[columnname+['Group10_capturerate','Total_defaultrate','Random10_defaultrate']].T
    
    # Start to plot 10 groups' default rate by monthly
    for month in df_plotdata_month.columns:
        plt.rcParams['figure.figsize'] = (30.0, 20.0)
        plot_y = np.array(df_plotdata_month[month])*100
        plot_y_rate = plot_y[:(len(plot_y)-3)]
        plot_y_capturerate = plot_y[len(plot_y)-3]
        plot_y_totalrate = plot_y[len(plot_y)-2]
        plot_y_randomrate = plot_y[len(plot_y)-1]
        plt.plot(plot_y_rate, color='r', linewidth=5, marker='o', markerfacecolor='blue', markersize=15, label='10 Groups Default Rate')
        plt.xticks(np.arange(0,10,1), df_plotdata_month.index[:(len(df_plotdata_month.index)-3)], fontsize=20)
        plt.gca().yaxis.set_major_formatter(FuncFormatter(to_percent))
        plt.yticks(fontsize=20)
        plt.title(month+' 10 Groups Default Rate', fontsize=20)
        plt.xlabel('Group', fontsize=20)
        plt.ylabel('Default Rate', fontsize=20)
        for a, b in zip(np.arange(0,10,1), plot_y_rate):
            if a != len(np.arange(0,10,1))-1:
                plt.text(a, b, str(round(b, 2))+'%', ha='left', va='top', fontsize=20)
            else:
                plt.text(a, b, str(round(b, 2))+'%'+' ('+str(round(plot_y_capturerate, 2))+'% Capture Rate)', ha='left', va='top', fontsize=20)
        plt.axhline(y=plot_y_totalrate, color='green', linestyle='--', linewidth=3, label='Total Default Rate')
        plt.text(0, plot_y_totalrate, 'Total Default Rate: '+str(round(plot_y_totalrate, 2))+'%', ha='left', va='top', fontsize=20)
        plt.axhline(y=plot_y_randomrate, color='blue', linestyle='--', linewidth=3, label='Random Default Rate')
        plt.text(0, plot_y_randomrate, 'Random Default Rate: '+str(round(plot_y_randomrate, 2))+'%', ha='left', va='top', fontsize=20)
        plt.grid(axis='y')
        plt.legend(fontsize=20)
        plt.savefig(os.path.join(output_path, case, case+'_defaultrate_plot_month/'+month+'_'+case+'_defaultrate_plot.png'), bbox_inches='tight', dpi=200, pad_inches=0.0)
        plt.show()
        plt.close()
        
def group_ten_summary(monthlist, df_newpanel, df_totalmonth):
    """
    :param monthlist: months we need to summary
    :param df_newpanel: each issuercode's score in groups
    :param df_totalmonth: Monthly summary of groups information
    :return: Summary Group 10's issuercode's number and number of issuercode that have default records within 1 year
    """
    group10list = []
    for month in monthlist:
        eachmonthgroup10 = df_newpanel[['b_info_issuercode','b_info_issuer',month]][df_newpanel[month] == 10.0].reset_index(drop=True)
        group10list = group10list + list(eachmonthgroup10[['b_info_issuercode','b_info_issuer']].apply(tuple, axis=1))
    df_group10num = pd.value_counts(group10list).reset_index()
    df_group10num.columns = ['Issuer', 'TotalNum']
    df_group10_default = pd.value_counts(sum(list(df_totalmonth.list_default_group10),[])).reset_index()
    df_group10_default.columns = ['Issuer', 'DefaultNum']
    df_group10_summary = df_group10num.merge(df_group10_default, on='Issuer', how='left')
    df_group10_summary = df_group10_summary.sort_values(by=['DefaultNum','TotalNum'], ascending=(False, False)).reset_index(drop=True)
    
    return df_group10_summary

def annual_monthly_plot(df_totalmonth, df_plotdata, labels, monthlabel, colorlist, case, output_path):
    """
    :param df_totalmonth: Monthly summary of groups information
    :param df_plotdata: Annually plot data
    :param labels: group labels
    :param monthlabel: 12 months' label
    :param colorlist: 12 colors to plot
    :param case: the score that we choose to use. (e.g. jieba_score_sum)
    :param output_path: path to save plots
    :return: yearlist for later use
    """
    # Generate plot data
    df_annualmonth = df_totalmonth[['Year','Month','Num_havenews',
                                    'Num_group1', 'Num_group1_havedefault',
                                    'Num_group2', 'Num_group2_havedefault',
                                    'Num_group3', 'Num_group3_havedefault',
                                    'Num_group4', 'Num_group4_havedefault',
                                    'Num_group5', 'Num_group5_havedefault',
                                    'Num_group6', 'Num_group6_havedefault',
                                    'Num_group7', 'Num_group7_havedefault',
                                    'Num_group8', 'Num_group8_havedefault',
                                    'Num_group9', 'Num_group9_havedefault',
                                    'Num_group10', 'Num_group10_havedefault']]
    # Calculate each group's default rate
    columnname = []
    for label in labels:
        df_annualmonth['Rate_group'+str(int(label))] = df_annualmonth['Num_group'+str(int(label))+'_havedefault']/df_annualmonth['Num_group'+str(int(label))]
        columnname.append('Rate_group'+str(int(label)))
    df_annualmonth['Num_havedefault'] = df_annualmonth[['Num_group1_havedefault','Num_group2_havedefault',
                                                             'Num_group3_havedefault','Num_group4_havedefault',
                                                             'Num_group5_havedefault','Num_group6_havedefault',
                                                             'Num_group7_havedefault','Num_group8_havedefault',
                                                             'Num_group9_havedefault','Num_group10_havedefault']].sum(axis=1)
    df_annualmonth['Total_defaultrate'] = df_annualmonth['Num_havedefault']/df_annualmonth['Num_havenews']
    
    # Get plot data
    yearlist = sorted(list(df_annualmonth.Year.unique()), reverse=False)
    df_plot_annualmonth = df_annualmonth[['Year','Month'] + columnname + ['Total_defaultrate']]
    
    # Start to plot each year's each month's each group's default rate plot
    for year in yearlist:
        # Get each year's dataframe
        eachyear = df_plot_annualmonth[df_plot_annualmonth.Year == year].sort_values(by=['Month'], ascending=True).reset_index(drop=True)
        # Calculate each year's mean default rate without split groups
        annual_defaultrate = np.mean(eachyear.Total_defaultrate)*100
        # Get each group's annual default rate
        group_annual_defaultrate = np.array(df_plotdata[year])*100
    
        # Plot each month's 10 groups' default rate
        plt.rcParams['figure.figsize'] = (30.0, 20.0)
        for i in range(eachyear.shape[0]):
            month_ploty = np.array(eachyear.iloc[i, 2:12])*100
            plt.plot(month_ploty, color=colorlist[i], linewidth=5, marker='o', markerfacecolor=colorlist[i], markersize=10, label=monthlabel[i])
        # Plot each year's total default rate without groups
        plt.axhline(y=annual_defaultrate, color='k', linestyle='--', linewidth=3, label='Total Default Rate')
        plt.text(0, annual_defaultrate, 'Total Default Rate: '+str(round(annual_defaultrate, 2))+'%', ha='left', va='top', fontsize=20)
    
        # Plot each group's annual default rate
        plt.plot(group_annual_defaultrate, color='k', linestyle=':', linewidth=5, label='Group Default Rate')
    
        plt.xticks(np.arange(0,10,1), eachyear.columns[2:12], fontsize=20)
        plt.gca().yaxis.set_major_formatter(FuncFormatter(to_percent))
        plt.yticks(fontsize=20)
        plt.title(year+' 10 Groups Monthly Default Rate', fontsize=20)
        plt.xlabel('Group', fontsize=20)
        plt.ylabel('Default Rate', fontsize=20)
        plt.legend(fontsize=20)
        plt.savefig(os.path.join(output_path, case, case+'_defaultrate_plot_annualmonth/'+year+'_'+case+'_defaultrate_annualmonth.png'), bbox_inches='tight', dpi=200, pad_inches=0.0)
        plt.show()
        plt.close()
    return yearlist

def group_time_series_plot(labels, ten_color, monthlist, df_newpanel, df_score_use, case, output_path):
    """
    :param labels: group labels
    :param ten_color: each group's color
    :param monthlist: months prepare to plot for time series
    :param df_newpanel: panel table in groups label
    :param df_score_use: DataFrame of jieba_score_sum
    :param case: the score that we choose to use. (e.g. jieba_score_sum)
    :param output_path: path to save plots
    """
    label_frame = []
    label_column = []
    for label in labels:
        label_column.append('Rate_group'+str(int(label)))
        month_score = []
        if label == 10:
            quantile10_1 = []
            quantile10_3 = []
        elif label == 9:
            quantile9_1 = []
            quantile9_3 = []
        else:
            pass
        for month in monthlist:
            # Get each month's each group's issuercode list and calculate related average score
            issuercodelist = list(df_newpanel[['b_info_issuercode',month]][df_newpanel[month] == label].b_info_issuercode.unique())
            scorearray = np.array(df_score_use[['b_info_issuercode',month]][df_score_use.b_info_issuercode.isin(issuercodelist)][month]).astype(float)
            month_score.append(np.mean(scorearray))
            if label == 10:
                quantile10_1.append(pd.DataFrame(scorearray).quantile(0.25)[0])
                quantile10_3.append(pd.DataFrame(scorearray).quantile(0.75)[0])
            elif label == 9:
                quantile9_1.append(pd.DataFrame(scorearray).quantile(0.25)[0])
                quantile9_3.append(pd.DataFrame(scorearray).quantile(0.75)[0])
            else:
                pass
        # Get each label's score dataframe
        eachlabel = pd.DataFrame([month_score]).T
        label_frame.append(eachlabel)
    df_groupmean = pd.concat(label_frame, axis=1)
    df_groupmean.columns = label_column
    df_groupmean['Group10_q1'] = quantile10_1
    df_groupmean['Group10_q3'] = quantile10_3
    df_groupmean['Group9_q1'] = quantile9_1
    df_groupmean['Group9_q3'] = quantile9_3
    df_groupmean['Month'] = monthlist
    
    # Start to plot 10 group's time series score plot
    plt.rcParams['figure.figsize'] = (60.0, 40.0)
    for j in range(len(labels)):
        columnname = 'Rate_group'+str(int(labels[j]))
        plot_y = np.array(df_groupmean[columnname])
        if labels[j] not in [9, 10]:
            plt.plot(plot_y, color=ten_color[j], linewidth=5, marker='o', markerfacecolor=ten_color[j], markersize=15, label=columnname)
        else:
            plt.plot(plot_y, color=ten_color[j], linewidth=8, marker='o', markerfacecolor=ten_color[j], markersize=15, label=columnname)
            plt.plot(np.array(df_groupmean['Group'+str(int(labels[j]))+'_q1']), color=ten_color[j], linestyle=':', linewidth=5, marker='o', markerfacecolor=ten_color[j], markersize=15, label='Group10_Q1')
            plt.plot(np.array(df_groupmean['Group'+str(int(labels[j]))+'_q3']), color=ten_color[j], linestyle='--', linewidth=5, marker='o', markerfacecolor=ten_color[j], markersize=15, label='Group10_Q3')        
    plt.xticks(np.arange(0, len(list(df_groupmean['Month'])), 5), [list(df_groupmean['Month'])[k] for k in np.arange(0, len(list(df_groupmean['Month'])), 5)], fontsize=40)
    plt.yticks(fontsize=40)
    plt.title('Monthly 10 Groups Default Score', fontsize=40)
    plt.xlabel('Month', fontsize=40)
    plt.ylabel('Default Score', fontsize=40)
    plt.grid(axis='y')
    plt.legend(loc='upper left', fontsize=30)
    plt.savefig(os.path.join(output_path, case, case+'_group_timeseries/'+case+'_group_timeseries.png'), bbox_inches='tight', dpi=200, pad_inches=0.0)
    plt.show()
    plt.close()

def annual_default_distribution_plot(yearlist, monthlist, df_newpanel, labels, df_score_use, default_table, case, output_path):
    """
    :param yearlist: year to plot
    :param monthlist: month list
    :param df_newpanel: panel table in groups label    
    :param labels: group labels
    :param df_score_use: DataFrame of jieba_score_sum
    :param default_table: Default issuercodes' information DataFrame
    :param case: the score that we choose to use. (e.g. jieba_score_sum)
    :param output_path: path to save plots
    """
    for year in yearlist:
        annual_score_default = []
        annual_score_nondefault = []
        for month in monthlist:
            if month[:4] != year:
                continue
            else:
                # Get this month's score situation dataframe
                month_score = df_newpanel[['b_info_issuercode', month]]
                
                # Get issuercodes who have news, i.e., 10 groups
                havenewspart = month_score[month_score[month].isin(labels)].reset_index(drop=True)
                # Get the issuercodes' score who have news
                newsrelatedscore = df_score_use[['b_info_issuercode',month]][df_score_use.b_info_issuercode.isin(list(havenewspart.b_info_issuercode.unique()))].drop_duplicates(subset=['b_info_issuercode']).reset_index(drop=True)

                # Get default issuers who have default records from this month to 1 year later
                default_with1y = list(default_table[(default_table.default_month >= month) & (default_table.default_month <= str(int(month)+100))].b_info_issuercode.unique())
    
                # Filter default part news score and non-default part news score
                score_defaultpart = list(newsrelatedscore[newsrelatedscore.b_info_issuercode.isin(default_with1y)][month])
                score_nondefaultpart = list(newsrelatedscore[~newsrelatedscore.b_info_issuercode.isin(default_with1y)][month])
                
                annual_score_default = annual_score_default + score_defaultpart
                annual_score_nondefault = annual_score_nondefault + score_nondefaultpart
        
        # Switch list to array in float
        annual_score_default = np.array(annual_score_default).astype(float)
        annual_score_nondefault = np.array(annual_score_nondefault).astype(float)
        
        # Normalized data
        annual_score_default = annual_score_default/np.max(annual_score_default)
        annual_score_nondefault = annual_score_default/np.max(annual_score_nondefault)
        
        # Start to plot the distribution
        fig = plt.figure(figsize=(30,20))
        
        ax1 = fig.add_subplot(111)
        ax1.hist(annual_score_nondefault, rwidth=0.8, bins=50, density=True, stacked=True, color='blue', alpha=0.5, label='Non-Default')
        ax1.set_title(year+' Default Distribution', fontsize=20)
        ax1.set_xlabel('Score', fontsize=20)
        ax1.set_ylabel('Density of Non-Default', fontsize=20)
        plt.tick_params(labelsize=20)
    
        ax2 = ax1.twinx()
        ax2.hist(annual_score_default, rwidth=2, bins=50, density=True, stacked=True, color='red', alpha=0.5, label='Default')
        ax2.set_ylabel('Density of Default', fontsize=20)
        plt.tick_params(labelsize=20)
    
        fig.legend(fontsize=20)
        plt.savefig(os.path.join(output_path, case, case+'_default_distribution/'+case+'_default_distribution.png'), bbox_inches='tight', dpi=200, pad_inches=0.0)
        plt.show()
        plt.close()

def issuer_emptyscore_ratio(df_score_use, default_table, monthlist, case, output_path):
    """
    :param df_score_use: DataFrame of jieba_score_sum
    :param default_table: Default issuercodes' information DataFrame
    :param monthlist: month list
    :param case: the score that we choose to use. (e.g. jieba_score_sum)
    :param output_path: path to save plots
    """    
    # Get score dataframe
    df_score_prepare = df_score_use[list(df_score_use.columns[:4]) + list(monthlist)].drop_duplicates(subset=['b_info_issuercode']).reset_index(drop=True)
    # Get default issuercode list
    default_issuercode = list(default_table.b_info_issuercode.unique())
    
    # Default part missing ratio
    df_score_defaultpart = df_score_prepare[df_score_prepare.b_info_issuercode.isin(default_issuercode)].reset_index(drop=True)
    issuercode_defaultlist = list(df_score_defaultpart.b_info_issuercode.unique())
    default_missingratio = []
    for issuercode in issuercode_defaultlist:
        # Get each issuercode's score array
        default_score_array = np.array(df_score_defaultpart[df_score_defaultpart.b_info_issuercode == issuercode][monthlist]).astype(float)
        # Calculate missing ratio and then input into list
        defaultmissing = len(default_score_array[np.isnan(default_score_array)])/len(default_score_array[0])
        default_missingratio.append(defaultmissing)
    
    # Non-Default part missing ratio
    df_score_nondefaultpart = df_score_prepare[~df_score_prepare.b_info_issuercode.isin(default_issuercode)].reset_index(drop=True)
    issuercode_nondefaultlist = list(df_score_nondefaultpart.b_info_issuercode.unique())
    nondefault_missingratio = []
    for issuercode in issuercode_nondefaultlist:
        # Get each issuercode's score array
        nondefault_score_array = np.array(df_score_nondefaultpart[df_score_nondefaultpart.b_info_issuercode == issuercode][monthlist]).astype(float)
        # Calculate missing ratio and then input into list
        nondefaultmissing = len(nondefault_score_array[np.isnan(nondefault_score_array)])/len(nondefault_score_array[0])
        nondefault_missingratio.append(nondefaultmissing)
    
    # Start to plot two histogram
    plt.rcParams['figure.figsize'] = (30.0, 20.0)
    plt.hist(default_missingratio, rwidth=1, bins=len(monthlist), color='red', label='Default')
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    plt.title("Default Issuers' Missing Ratio Distribution", fontsize=20)
    plt.xlabel('Ratio', fontsize=20)
    plt.ylabel('Number', fontsize=20)
    plt.legend(fontsize=20)
    plt.savefig(os.path.join(output_path, case, case+'_missing_distribution/'+case+'_missing_distribution_default.png'), bbox_inches='tight', dpi=200, pad_inches=0.0)
    plt.show()
    plt.close()
    
    plt.rcParams['figure.figsize'] = (30.0, 20.0)
    plt.hist(nondefault_missingratio, rwidth=1, bins=len(monthlist), color='blue', label='Non-Default')
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    plt.title("Non-Default Issuers' Missing Ratio Distribution", fontsize=20)
    plt.xlabel('Ratio', fontsize=20)
    plt.ylabel('Number', fontsize=20)
    plt.legend(fontsize=20)
    plt.savefig(os.path.join(output_path, case, case+'_missing_distribution/'+case+'_missing_distribution_nondefault.png'), bbox_inches='tight', dpi=200, pad_inches=0.0)
    plt.show()
    plt.close()
