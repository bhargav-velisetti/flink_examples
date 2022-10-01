from pyflink.table import TableEnvironment
from pyflink.table import EnvironmentSettings
import pandas as pd
import numpy as np

settings = EnvironmentSettings.in_batch_mode()
tenv = TableEnvironment.create(settings)

tenv.execute_sql("""CREATE TABLE housing_data (
  longitude STRING,
  latitude STRING,
  housing_median_age STRING,
  total_rooms STRING,
  total_bedrooms STRING,
  population STRING,
  households STRING, 
  m_income STRING,
  m_h_value STRING
)
 with (
     'connector' = 'filesystem',
     'path' = 'sample_data/california_housing_test.csv',
     'format' = 'csv'
    )
""")

# tenv.exe

stable = tenv.sql_query("SELECT * FROM housing_data")  # table

stable2 = tenv.execute_sql("SELECT * FROM housing_data ")  # TableResult

tenv.execute_sql("CREATE TABLE print_table WITH  ('connector' = 'print') like housing_data")

tenv.execute_sql("INSERT INTO print_table SELECT * FROM  housing_data")  # error out since print is not for insert

# stable2.print


# print(str(type(stable)) + '  second one ' + str(type(stable2)))

# print(stable.to_pandas())
