# -*- coding: utf-8 -*-

import logging
import pyodbc
import pandas as pd
import math

from config.settings import LOGGER_NAME
from config.entity_config import MSSQL_DATA_SOURCE_NAME, MSSQL_TYPE_CONNECTION
from config.os_config import OS_ENVIRONMENT



class MSSQLAccess:
    def __init__(self, host="localhost", port=3306, db_name=None, user=None, pwd=None,type_connection=MSSQL_TYPE_CONNECTION):
        self.__logger__ = logging.getLogger('{}.SQLServerAccess'.format(LOGGER_NAME))
        self.__logger__.debug('Initializing')

        self.__host__ = host
        self.__port__ = port
        self.__auth_user__ = user
        self.__auth_pwd__ = pwd
        self.__db_name__ = db_name
        self.__data_source_name__ = MSSQL_DATA_SOURCE_NAME
        self.__type_connection__ = type_connection

    def __connect_using_data_source__(self):
        try:
            con_string = 'DSN={};UID={};PWD={};DATABASE={};'.format(self.__data_source_name__, self.__auth_user__,
                                                                    self.__auth_pwd__, self.__db_name__)
            cnxn = pyodbc.connect(con_string, readonly=True)

            return cnxn
        except pyodbc.Error as e:
            self.__logger__.error('Unable to create connection to MSSQL {}'.format(e))

    def __connect_direct__(self):
        if self.__type_connection__ == "SQLAuthentication":
            try:
                server =self.__host__
                con_string = 'DRIVER={MYSQL ODBC 8.0 ANSI Driver};' + 'SERVER={};PORT=3306;DATABASE={};UID={};PWD={};use_unicode={};charset={};'.format(server,
                                                                                                   self.__db_name__,
                                                                                                   self.__auth_user__,
                                                                                                   self.__auth_pwd__,True,"utf8")

                cnxn = pyodbc.connect(con_string)
                return cnxn
            except pyodbc.Error as e:
                self.__logger__.error('Unable to create connection to MSSQL (SQL Authentication)' + str(e))

        elif self.__type_connection__ == "WindowsAuthentication":
            try:
                server = 'tcp:{}'.format(self.__host__)
                con_string = 'DRIVER={YSQL ODBC 8.0 ANSI Driver};' + 'SERVER={};DATABASE={};TRUSTED_CONECTION={}'.format(server,
                                                                                                   self.__db_name__,
                                                                                                           "YES")
                cnxn = pyodbc.connect(con_string)
                return cnxn
            except pyodbc.Error:
                self.__logger__.error('Unable to create connection to MSSQL (Windows Authentication)')

    def __get_connection__(self):
        """
        Returns a SQLAlchemy connectable(engine/connection) object. Depending the OS_ENVIRIOMENT value, the connection
        is created directly or using a data source (ie: Linux wth FreeTDS data source).
        :return:
        """
        if OS_ENVIRONMENT == 'Linux':
            return self.__connect_using_data_source__()
        elif OS_ENVIRONMENT == 'Windows':
            return self.__connect_direct__()
        else:
            return self.__connect_direct__()

    def __get_cursor__(self):
        """
        Returns a new cursor to database
        :return:
        """
        cnxn = self.__get_connection__()
        return cnxn.cursor()

    def test_connection(self):
        try:
            cursor = self.__get_cursor__()
            cursor.execute("select count(*) from information_schema.tables where table_type = 'BASE TABLE' and table_schema = 'commujer_mit'")
            for row in cursor.fetchall():
                print(row)

            cursor.close()
            self.__logger__.info('Connection Successful')

        except pyodbc.Error:
            self.__logger__.error('Unable to connect to MySQL')

    def select_as_df(self, sql_query):
        try:
            cnxn = self.__get_connection__()
            teste  = cnxn.execute(sql_query)
            # coerce_float=False must be used to avoid float overflow
            try:
                df = pd.read_sql(sql_query, cnxn, coerce_float=False)
            except Exception as e:
                self.__logger__.error('Unable to execute read_sql' + str(e))

            cnxn.cursor().close()

            return df
        except pyodbc.Error as e:
            self.__logger__.error('Unable to execute query' + str(e))

    def insert_staging_with_df(self, df, target_table):
        self.__logger__.info('Start INSERT IN STAGING ')
        cnxn = self.__get_connection__()
        cursor = self.__get_cursor__()
        batch = df.values.tolist()
        c = df.columns.tolist()
        staging_values = ','.join(['?' for _ in batch[0]])
        batch = str(batch).replace("[[", "(").replace("]]", ")").replace("[", "(").replace("]", ")")
        c = str(c).replace("[", "(").replace("]", ")").replace("'", "")
        query = 'INSERT INTO jobsity.{table} {c} VALUES'.format( table=target_table,c=c), batch
        query = str(query).replace("VALUES',", "VALUES").replace('"(',"(").replace('")','').replace("('INSERT", 'INSERT' )

        try:
            cursor.execute(query)
            cursor.execute("COMMIT")
            cnxn.commit()
            cursor.close()
            self.__logger__.info('INSERT in Staging executed with success. Rows:' + str(len(batch)))
        except pyodbc.Error as e:
            cnxn.rollback()
            cursor.close()
            self.__logger__.error('Unable to execute INSERT in Staging  ' + str(e) + "  target_table "  + target_table)


    def truncate_execute(self, sql_query):

        self.__logger__.info('Start Truncate in Staging  ')
        try:
            cnxn_trunc = self.__get_connection__()
            cursor_trunc = self.__get_cursor__()
            cursor_trunc.execute(sql_query)
            cursor_trunc.execute("COMMIT")
            cnxn_trunc.commit()
            cursor_trunc.close()
            return self.__logger__.info('Truncate in Staging executed with sucess')

        except pyodbc.Error as e:
            cursor_trunc.close()
            self.__logger__.error('Unable to execute Truncate in Staging' + str(e) )

    def merge_execute(self, sql_query):

        self.__logger__.info('Start Merge ')
        try:
            cnxn_merge = self.__get_connection__()
            cursor_merge = self.__get_cursor__()
            cursor_merge.execute(sql_query)
            cursor_merge.execute("COMMIT")
            cnxn_merge.commit()
            cursor_merge.close()
            return self.__logger__.info('MERGE executed with sucess')

        except pyodbc.Error as e:
            cursor_merge.close()
            self.__logger__.error('Unable to execute Merge in Staging' + str(e))
