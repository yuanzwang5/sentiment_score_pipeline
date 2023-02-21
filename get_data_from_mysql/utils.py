import pandas as pd
import time
import mysql.connector
import math
import numpy as np


class MySqlConn:
    """mode can be 'prod' or 'dev'"""

    def __init__(self, host, user, pw, port, mode,
                 num_dev_sample=100,
                 is_debug=False, is_hide_message=False):
        self.conn = mysql.connector.connect(host=host, user=user, password=pw, port=port, buffered=True)
        self.dict_type_py_to_mysql = {'object': 'VARCHAR(255)',
                                      'int64': 'INT', 'int32': 'INT',
                                      'float64': 'FLOAT(32)',
                                      'datetime64[ns]': 'DATE'}
        self.mode = mode
        self.num_dev_sample = num_dev_sample

        self.is_debug = is_debug
        self.is_hide_message = is_hide_message

    def __manage_conn(method):
        def manage_conn(self, *args, **kwargs):
            self.conn.reconnect(attempts=20, delay=3)
            return_ = method(self, *args, **kwargs)
            self.conn.close()
            return return_

        return manage_conn

    def __time_it(method):
        def time_it(self, *args, **kwargs):
            start_time = time.time()
            result = method(self, *args, **kwargs)
            end_time = time.time()

            minutes = (end_time - start_time) / 60

            if self.is_debug:
                str_args = f"\nargs: {args}, {kwargs}"
            else:
                str_args = ''

            if not self.is_hide_message:
                print(
                    f"\nfunc: {method.__name__}"
                    f"{str_args}"
                    f"\nspent: {minutes:.2f} mins\n"
                )

            return result

        return time_it

    @__manage_conn
    @__time_it
    def sql_to_df(self, sql):
        if self.mode == 'dev':
            print('you are in dev mode!')
            sql = f'{sql} LIMIT {self.num_dev_sample}'
        if self.is_debug:
            print(sql)
        return pd.read_sql(sql, con=self.conn)

    def get_whole_table(self, schema, table):
        return self.sql_to_df(f"SELECT * FROM {schema}.{table}")

    def get_schema_table_list(self, schema):
        cursor = self.conn.cursor()
        cursor.execute(f"USE {schema}")
        return self.sql_to_df("SHOW TABLES")

    def get_date_range(self, schema, table, col):
        cursor = self.conn.cursor()
        cursor.execute(f"USE {schema}")
        return self.sql_to_df(f"SELECT min({col}), max({col}) from {table} ")

    def get_row_count(self, schema, table):
        cursor = self.conn.cursor()
        cursor.execute(f"USE {schema}")
        cursor.execute(f"SELECT COUNT(*) from {table} ")
        count = cursor.fetchall()
        return count[0][0]

    def get_selected_columns(self, schema, table, cols):
        cols_str = ', '.join(cols)
        return self.sql_to_df(f"SELECT {cols_str} FROM {schema}.{table}")

    @staticmethod
    def __get_query_define_col_type(df,
                                    dict_type_py_to_mysql=None,
                                    dict_type_col_to_mysql=None):
        """follow the rule from dict_type_py_to_mysql if do not specific col type in dict_type_col_to_mysql"""

        str_query = ''

        for col, col_type in df.dtypes.items():

            # specify column type
            if dict_type_col_to_mysql and col in dict_type_col_to_mysql:
                str_db_type = dict_type_col_to_mysql[col]
            # base on type conversion dictionary
            else:
                str_db_type = dict_type_py_to_mysql[str(col_type)]

            str_query += f'{col} {str_db_type}, '

        str_query = str_query.strip(', ')

        return str_query

    @__manage_conn
    @__time_it
    def try_create_table(self, df, schema, table,
                         dict_type_py_to_mysql=None,
                         dict_type_col_to_mysql=None):

        if dict_type_py_to_mysql is None:
            dict_type_py_to_mysql = self.dict_type_py_to_mysql

        str_query = self.__get_query_define_col_type(df, dict_type_py_to_mysql, dict_type_col_to_mysql)
        str_query = f'CREATE TABLE IF NOT EXISTS {schema}.{table} ({str_query});'

        if self.is_debug:
            print(str_query)

        self.cursor = self.conn.cursor()
        self.cursor.execute(str_query)

    @__manage_conn
    @__time_it
    def insert_data(self, df, schema, table, chunk_size=5000):

        str_tuple_col = str(tuple(df.columns.tolist())).replace("'", "").replace(',)', ')')
        str_tuple_input = str(tuple(["%s" for col in df])).replace("'", "").replace(',)', ')')

        # py format --> mysql format
        self.__py_dt_to_mysql_dt(df)

        list_df_chunk = self.__split_df(df, chunk_size)

        for df_chunk in list_df_chunk:

            list_tuple_data = list(df_chunk.to_records(index=False))
            list_tuple_data = self.__py_num_to_mysql_num(df_chunk, list_tuple_data)

            str_query = f"INSERT INTO {schema}.{table} {str_tuple_col} VALUES {str_tuple_input}"

            if self.is_debug:
                print(str_query)

            self.cursor = self.conn.cursor()
            self.cursor.executemany(str_query, list_tuple_data)
            self.conn.commit()

            if not self.is_hide_message:
                print(f"{self.cursor.rowcount} row were inserted in {schema}.{table}")

    def try_create_insert_table(self, df, schema, table, chunk_size=5000,
                                dict_type_py_to_mysql=None, dict_type_col_to_mysql=None, ):

        if dict_type_py_to_mysql is None:
            dict_type_py_to_mysql = self.dict_type_py_to_mysql

        self.try_create_table(df, schema, table, dict_type_py_to_mysql, dict_type_col_to_mysql)
        self.insert_data(df, schema, table, chunk_size=chunk_size)

    @__manage_conn
    @__time_it
    def try_drop_table(self, schema, table):
        """DROP TABLE IF EXISTS customers"""

        self.cursor = self.conn.cursor()
        str_query = f"""DROP TABLE IF EXISTS {schema}.{table};"""
        self.cursor.execute(str_query)

    @staticmethod
    def __py_num_to_mysql_num(df, list_tuple_data):
        """
        nan --> None
        int64, int32 --> int
        float64 --> float
        """

        arr_dtypes = df.dtypes.astype(str).values
        list_index_convert_int = [index for index, dtype in enumerate(arr_dtypes) if dtype in ['int64', 'int32']]
        list_index_convert_float = [index for index, dtype in enumerate(arr_dtypes) if dtype in ['float64']]

        list_tuple_tran_data = []

        for tuple_data in list_tuple_data:

            list_tran_data = []
            for index, data in enumerate(tuple_data):
                # mysql do not accept np.nan but accept None"
                if isinstance(data, float) and math.isnan(data):
                    data = None
                elif index in list_index_convert_int:
                    data = int(data)
                elif index in list_index_convert_float:
                    data = float(data)

                list_tran_data.append(data)

            list_tuple_tran_data.append(tuple(list_tran_data))

        return list_tuple_tran_data

    @staticmethod
    def __py_dt_to_mysql_dt(df):
        """python data type to mysql data type e.g. datetime to string"""
        list_col_dt = [col for col, dtype in df.dtypes.astype(str).items() if 'datetime' in dtype]

        df.loc[:, list_col_dt] = df[list_col_dt].astype(str)
        # 'NaT' --> None
        for col in list_col_dt:
            df[col] = np.where(df[col] == 'NaT', None, df[col])

    @staticmethod
    def __split_df(df, chunk_size):
        """if import to large df may have error"""
        list_chunks = list()
        num_chunks = len(df) // chunk_size + 1
        for i in range(num_chunks):
            list_chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
        return list_chunks
