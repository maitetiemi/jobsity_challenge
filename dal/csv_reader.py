# -*- coding: utf-8 -*-
import logging
import os
import datetime
import csv
import pandas as pd

from config.settings import LOGGER_NAME


class SimpleCSVReader:

    __DEFAULT_ENCODING__ = 'utf8'
    __DEFAULT_DELIMITER__ = ';'

    def __init__(self, fields_names, delimiter=None, encoding=None):
        self.__logger__ = logging.getLogger('{}.SimpleCSVReader'.format(LOGGER_NAME))
        self.__default_delimiter__ = delimiter if delimiter else SimpleCSVReader.__DEFAULT_DELIMITER__
        self.__default_encoding__ = encoding if encoding else SimpleCSVReader.__DEFAULT_ENCODING__
        self.__fields_names__ = fields_names

    def read(self, file_location, delimiter=None, encoding=None, params=None,  provider=None):
        if not file_location:
            raise FileNotFoundError()
        elif not os.path.isfile(file_location):
            self.__logger__.error('FileNotFoundError: {}'.format(file_location))
            raise FileNotFoundError(file_location)
        if not delimiter:
            delimiter = self.__default_delimiter__
        if not encoding:
            encoding = self.__default_encoding__

        input_file = os.path.abspath(file_location)
        self.__logger__.info('Reading file: {}'.format(input_file))

        df = pd.read_csv(input_file,
                         sep=delimiter,
                         encoding=encoding,
                        **params)

        return df
