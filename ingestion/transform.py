import pandas as pd
import logging
import datetime
from datetime import datetime, timedelta
from config.settings import LOGGER_NAME


class DataTransformation:

    def __init__(self):
        """
        This method initialize the instance of this class
        """
        self.__logger__ = logging.getLogger('{}.DataTransformation'.format(LOGGER_NAME))
        self.__logger__.debug('Initializing')

    def transform_data(self, df):
        # Initialization
        logger = logging.getLogger(self.__logger__.name)
        logger.info('Transforming Data: {}')

        df_data = df
        df_data["origin_coord"] = df_data["origin_coord"].str.replace('POINT (', '', regex = False)
        df_data["origin_coord"] = df_data["origin_coord"].str.replace(')', '', regex = False)
        df_data[["origin_coord1","origin_coord2"]] = df_data["origin_coord"].str.split(' ', n=1, expand=True)

        df_data["destination_coord"] = df_data["destination_coord"].str.replace('POINT (', '', regex = False)
        df_data["destination_coord"] = df_data["destination_coord"].str.replace(')', '', regex = False)
        df_data[["destination_coord1", "destination_coord2"]] = df_data["destination_coord"].str.split(' ', expand=True)

        df_data = df_data.drop_duplicates(subset=["region","origin_coord1","origin_coord2","destination_coord1","destination_coord2","datetime"], keep='last')
        df_data = df_data.drop(['origin_coord','destination_coord'], axis = 1)
        df_data = df_data[["region","origin_coord1","origin_coord2","destination_coord1","destination_coord2","datetime",'datasource']]
        df_data["datetime"] = df_data["datetime"].str.strip()
        df_data["datetime"] = pd.to_datetime(df["datetime"], infer_datetime_format=True)

        return df_data
