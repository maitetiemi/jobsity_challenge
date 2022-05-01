INSERT INTO jobsity.{table_name}
SELECT * FROM jobsity.{staging_table} AS source
ON DUPLICATE KEY UPDATE {cols_and_values}

