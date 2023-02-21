import sys
sys.path.insert(0, '/mnt/utnfs/data/sentiment_score_pipeline/python_code/v0.1.0/get_data_from_mysql')
from utils import MySqlConn
from mysql_config import MySQL_config
from datetime import date
import datetime
import os, pickle, gzip
from joblib import Parallel, delayed

def get_wds_table(table_name, mysql_config, date):
    '''
    :param table_name: the name of the table selected from the MySQL DB
    :param mysql_config: the config used to get the table
    :param date: the date of getting data from DB to the GPU Server
    :return: save the selected tables into the GPU Server
    '''
    save_home_dir = mysql_config.save_home_dir

    my_sql_loader = MySqlConn(host=mysql_config.host,
                              user=mysql_config.user,
                              pw=mysql_config.pw,
                              port=mysql_config.port,
                              mode=mysql_config.mode)

    if table_name == 'CBONDPRICESNET':
        sel_table = my_sql_loader.sql_to_df("""
                                            SELECT
                                                S_INFO_WINDCODE, TRADE_DT, B_DQ_OPEN, B_DQ_HIGH, B_DQ_LOW, B_DQ_ORIGINCLOSE,
                                                B_DQ_VOLUME, B_DQ_AMOUNT
                                            FROM
                                                wds.CBONDPRICESNET
                                            WHERE B_DQ_VOLUME > 0 AND B_DQ_AMOUNT > 0 AND B_DQ_ORIGINCLOSE > 0
                                        """
                                            )

    elif table_name == 'COMPINTRODUCTION':
        sel_table = my_sql_loader.sql_to_df("""
                                                SELECT
                                                    COMP_ID, COMP_NAME, PROVINCE, REGCAPITAL, CITY, FOUNDDATE, ENDDATE
                                                FROM
                                                    wds.COMPINTRODUCTION
                                                WHERE PROVINCE IS NOT NULL
                                            """
                                            )

    elif table_name == 'CBONDCF':
        sel_table = my_sql_loader.sql_to_df("""
                                        SELECT
                                            S_INFO_WINDCODE, B_INFO_CARRYDATE, B_INFO_ENDDATE, B_INFO_COUPONRATE, 
                                            B_INFO_PAYMENTDATE, B_INFO_PAYMENTINTEREST, B_INFO_PAYMENTPARVALUE, B_INFO_PAYMENTSUM
                                        FROM
                                            wds.CBONDCF
                                    """
                                            )

    elif table_name == 'CBONDACTUALCF':
        sel_table = my_sql_loader.sql_to_df("""
                                            SELECT
                                                B_INFO_WINDCODE, B_INFO_CARRYDATE, B_INFO_ENDDATE, B_INFO_COUPONRATE, 
                                                B_INFO_PAYMENTDATE, B_INFO_PAYMENTINTEREST, B_INFO_PAYMENTPARVALUE, B_INFO_PAYMENTSUM
                                            FROM
                                                wds.CBONDACTUALCF
                                        """
                                            )
    elif table_name == 'CBONDISSUERRATING':
        sel_table = my_sql_loader.sql_to_df(
            """
            SELECT
            S_INFO_COMPCODE, S_INFO_COMPNAME, ANN_DT, B_RATE_STYLE, B_INFO_CREDITRATING,
            B_RATE_RATINGOUTLOOK, B_INFO_CREDITRATINGAGENCY, B_CREDITRATING_CHANGE, B_INFO_ISSUERRATETYPE
            FROM
            wds.CBONDISSUERRATING
            """
        )
    elif table_name == 'CBONDRATING':
        sel_table = my_sql_loader.sql_to_df(
            """
            SELECT
            S_INFO_WINDCODE, ANN_DT, B_RATE_STYLE, B_INFO_CREDITRATING,
            B_INFO_CREDITRATINGAGENCY, B_CREDITRATING_CHANGE
            FROM
            wds.CBONDRATING
            """)
    elif table_name == 'FINANCIALNEWS':
        today_format = datetime.date(*map(int, date.split('-')))
        thismonth_startdate = datetime.date(today_format.year, today_format.month, 1)
        lastmonth_startdate = datetime.date((thismonth_startdate - datetime.timedelta(1)).year, (thismonth_startdate - datetime.timedelta(1)).month, 1).strftime('%Y-%m-%d')
        sel_table = my_sql_loader.sql_to_df(
            """
            SELECT
            *
            FROM
            wds.FINANCIALNEWS
            WHERE (PUBLISHDATE >= '""" + lastmonth_startdate + """ 00:00:00') AND (PUBLISHDATE < '""" + thismonth_startdate.strftime('%Y-%m-%d') + """ 00:00:00')
            """)
    else:
        sel_table = my_sql_loader.get_whole_table('wds', table_name)

    sel_table.columns = sel_table.columns.str.lower()
    save_file_name = table_name.lower()
    save_dir = os.path.join(save_home_dir, date)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    f = gzip.open(os.path.join(save_dir, save_file_name + '.pkl.gz'), 'wb')
    pickle.dump(sel_table, f)
    f.close()


def get_updated_data_from_db(data_date=None):
    '''
    Main function to trigger the update data process
    '''
    if data_date == None:
        today = date.today().strftime('%Y-%m-%d')
    else:
        today = data_date
    # today = date.today().strftime('%Y-%m-%d')
    mysql_config = MySQL_config(data_date=data_date)
    # Parallel(n_jobs=24, verbose=10)(
    #     delayed(get_wds_table)(table_name, mysql_config, today) for table_name in mysql_config.table_list)
    for table_name in mysql_config.table_list:
        get_wds_table(table_name, mysql_config, today)

    print('[x] Get data from DB completed')


def get_data_from_old_db(mysql_config, save_dir, db_name='wind', table_name='default'):
    '''
    :param mysql_config: the config used to get the table
    :param save_dir: the directory to save the output table
    :param db_name: the name of DB
    :param table_name: the name of the table selected from the MySQL DB
    :return: the non-updated table
    '''
    my_sql_loader = MySqlConn(host=mysql_config.host,
                              user=mysql_config.user,
                              pw=mysql_config.pw,
                              port=mysql_config.port,
                              mode=mysql_config.mode)

    sel_table = my_sql_loader.get_whole_table(db_name, table_name)
    sel_table.columns = sel_table.columns.str.lower()
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    f = gzip.open(os.path.join(save_dir, table_name.lower() + '.pkl.gz'), 'wb')
    pickle.dump(sel_table, f)
    f.close()


if __name__ == '__main__':
    get_updated_data_from_db()
    # get_data_from_old_db(mysql_config=MySQL_config(),
    #                      save_dir='/mnt/utnfs/data/issuer_rating_pipeline/data/raw_data/non_update_data')

