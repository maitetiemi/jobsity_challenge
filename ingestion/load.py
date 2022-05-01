import pandas as pd
import logging
import os.path
import datetime
from config.settings import LOGGER_NAME

from config.entity_config import DB_HOST, \
                                DB_NAME, \
                                DB_PASS, \
                                DB_USER_NAME, \
                                DB_PORT

from config.query_config import MERGE_QUERY_PARAM_COLUMNS_NAME, \
    MERGE_QUERY_PARAM_ID_PRIMARY_KEY, \
    MERGE_QUERY_PARAM_TABLE_NAME, \
    MERGE_QUERY_PARAM_COLUMNS_VALUES, \
    MERGE_QUERY_PARAM_PRIMARYKEY_COLUMN, \
    MERGE_QUERY_FILENAME, \
    MERGE_QUERY_PARAM_STAGING_COL, \
    MERGE_QUERY_PARAM_STAGING_TABLE, \
    MERGE_QUERY_PARAM_SOURCE_VALUES, \
    STAGING_TABLE, \
    STAGING_VALUES,  \
    MERGE_QUERY_PARAM_UPDATE_VALUES,  \
    TRUNCATE_FILE, \
    MERGE_QUERY_STAGING_TABLE, \
    MERGE_QUERY_TABLE,\
    MERGE_QUERY_PRIMARYKEY

from dal.sql_connection import MSSQLAccess


class ClientDataLoad:

    def __init__(self):
        """

        """
        self.__logger__ = logging.getLogger('{}.ClientDataLoad'.format(LOGGER_NAME))
        self.__logger__.debug('Initializing')
        self.__filename__ = MERGE_QUERY_FILENAME
        self.__init_mssql_wrapper__()

    def __init_mssql_wrapper__(self):
        """
        instancia de la conexión
        :return:
        """
        self.mssql_access = MSSQLAccess(host=DB_HOST,
                     port=DB_PORT,
                     db_name=DB_NAME,
                     user=DB_USER_NAME,
                     pwd=DB_PASS)

    def __mssq_read__(self, filename, params):
        sql_file_location = os.path.join('query', filename)
        with open(sql_file_location, 'r') as sqlfile:
            query = sqlfile.read()

        query = query.format(**params)
        return query


    def truncate_staging(self, table):
        filename = TRUNCATE_FILE
        sql_file_location = os.path.join('query', filename)
        try:
            with open(sql_file_location, 'r') as sqlfile:
                query = sqlfile.read()
            query = query.format(**{MERGE_QUERY_STAGING_TABLE: table})
        except Exception as e:
            self.__logger__.error(e)

        return self.mssql_access.truncate_execute(query)

    def insert_staging(self, df, table):
        self.mssql_access.insert_staging_with_df(df=df, target_table=table)
        return df.columns.tolist()

    def merge_staging_with_bd(self, columns_name, staging_table, table_name, primarykey):
        """
        :param sql_query: Merge Query
        :param columns_name: Columnas de la tabla staging
        :param staging_table: Nombre de la tabla Staging
        :param table_name:  Tabla que será hecho el merge
        :return:
        """
        columns = columns_name.copy()
        update_syntaxe = [" table_trips." + sub + " = source." + sub for sub in columns]
        update_syntaxe = ",".join(update_syntaxe)

        filename = self.__filename__
        values_staging = ["source." + sub for sub in columns_name]
        columns_name = str(columns_name).replace("'", "").replace("]", "").replace("[", "")
        values_staging = str(values_staging).replace("'", "").replace("]", "").replace("[", "")

        params = {
            MERGE_QUERY_PARAM_TABLE_NAME: table_name,
            MERGE_QUERY_PARAM_STAGING_COL: columns_name,
            MERGE_QUERY_PARAM_PRIMARYKEY_COLUMN: primarykey,
            MERGE_QUERY_PARAM_COLUMNS_NAME: columns_name,
            MERGE_QUERY_PARAM_STAGING_TABLE: staging_table,
            MERGE_QUERY_PARAM_SOURCE_VALUES: values_staging,
            MERGE_QUERY_PARAM_UPDATE_VALUES: update_syntaxe
        }
        query = self.__mssq_read__(filename=filename,params=params)

        return self.mssql_access.merge_execute(query)

    def load_data(self, df):
        logger = logging.getLogger(self.__logger__.name)

        logger.info('Load Data')
        try:
            self.truncate_staging(MERGE_QUERY_STAGING_TABLE)
            cols = self.insert_staging(df, MERGE_QUERY_STAGING_TABLE)
            result = self.merge_staging_with_bd(cols, MERGE_QUERY_STAGING_TABLE, MERGE_QUERY_TABLE, \
                                      MERGE_QUERY_PRIMARYKEY)
        except Exception as e:
            logger = logging.getLogger(LOGGER_NAME)
            logger.error(e)
            raise e

        return result
