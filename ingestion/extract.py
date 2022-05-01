import os.path
import logging
import pandas as pd
import datetime
from config.settings import LOGGER_NAME
from dal.csv_reader import SimpleCSVReader
from config.entity_config import PATH_RAW_DATA, FIELDS_NAMES
from os import listdir
from os.path import isfile, join

class DataProvider:
    """
        This class encapsulates access to the data through SQL Server database

    """

    def __init__(self,inicial_load):
        """
         """
        self.__initial_load__ = inicial_load
        self.__logger__ = logging.getLogger('{}.RawDataExtract'.format(LOGGER_NAME))
        self.__logger__.debug('Initializing')
        self.__init_csv_reader__(fields_names=PATH_RAW_DATA)

    def __init_csv_reader__(self, fields_names, delimiter=None, encoding=None):
        self.reader = self.csv_reader = SimpleCSVReader(fields_names, delimiter=delimiter, encoding=encoding)

    def get_data(self):
        logger = logging.getLogger(self.__logger__.name)

        logger.info('Getting Data')
        path_file = PATH_RAW_DATA
        ## Params to the reader csv

        params = {"skiprows": 1,
                  "names":FIELDS_NAMES
                  }
        dfs = []
        for f in listdir(path_file):
            archivo = path_file + "\\" + f
            if os.path.exists(archivo):
                dfs.append(self.reader.read(file_location=archivo, delimiter=",", encoding='utf-8', params=params))
            else:
                self.__logger__.error('Unable to read file ' + str(archivo))

        if len(dfs) == 0:
            return pd.DataFrame(dfs)
        else:
            #df_data = pd.DataFrame(dfs, columns =FIELDS_NAMES)
            df_data = pd.concat(dfs, ignore_index=True)

        return df_data
