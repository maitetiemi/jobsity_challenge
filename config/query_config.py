# MERGE QUERY
MERGE_QUERY_FILENAME = 'merge_query.sql'
MERGE_QUERY_PARAM_TABLE_NAME = 'table_name'
MERGE_QUERY_PARAM_ID_PRIMARY_KEY = 'id_row_primarykey'
MERGE_QUERY_PARAM_PRIMARYKEY_COLUMN = 'primarykey'
MERGE_QUERY_PARAM_COLUMNS_NAME = 'col_name'
MERGE_QUERY_PARAM_COLUMNS_VALUES = 'col_values'
MERGE_QUERY_PARAM_STAGING_TABLE = 'staging_table'
MERGE_QUERY_PARAM_STAGING_COL = 'staging_col'
MERGE_QUERY_PARAM_SOURCE_VALUES = 'values_staging'
MERGE_QUERY_PARAM_UPDATE_VALUES = 'cols_and_values'
MERGE_QUERY_PARAM_DEL_CONDICION = 'condition'


# STAGING
STAGING_TABLE = 'staging_table'
STAGING_VALUES = 'staging_values'

TRUNCATE_FILE = "truncate_staging.sql"
MERGE_QUERY_STAGING_TABLE = "table_staging"
MERGE_QUERY_TABLE = "table_trips"

MERGE_QUERY_PRIMARYKEY = "(table_trips.origin_coord1 = source.origin_coord1 " \
                          "and table_trips.origin_coord2 = source.origin_coord2 " \
                          "and table_trips.destination_coord1 = source.destination_coord1 " \
                          "and table_trips.destination_coord2 = source.destination_coord2 " \
                          "and table_trips.datetime = source.datetime" \
                          ")"
