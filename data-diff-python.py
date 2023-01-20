#!/usr/bin/python

"""
To run this file, complete the following steps:

0. Install these packages using pip:

pip install -q data-diff
pip install -q 'data-diff[snowflake]'
pip install pandas
pip install numpy
pip install 'snowflake-connector-python[pandas]'

1.  Update the variables in the EDIT THIS SECTION and save the file.
2.  Add your environmental variables to a .zshrc file or equivalent.

    If you don't know how to set environmental variables, please 
    check out this StackOverflow https://apple.stackexchange.com/a/356455/239984 ,
    reach out to me: https://leoebfolsom.com/ , or ask a techie friend.

3.  Quit your Terminal and open a new one. This will import the variables 
    you just saved in your .zshrc file.
4.  cd into the directory where you've saved this file.
5.  Run this command: python3 data-diff-python.py. 

"""

### EDIT THIS SECTION
##################################################################################################

# Set your database information, including the table name and PR schema.
snowflake_database = 'ANALYTICS'
schema_a = 'ANALYTICS'
schema_b = 'PR_NUM_181'
table_name = 'ORG_ACTIVITY_STREAM'
primary_key = 'ACTIVITY_ID'

# Set your options:

conflicts_only_or_all_columns = "Only Columns With Differences"
# Alternatively, set this to "All Diffed Columns"

matching_primary_keys_only = "Common Primary Keys Only"
# Alternatively, set this to "Common Primary Keys and Non-Matching Primary Keys"

where_clause = "event_timestamp > '2022-11-05' and event_timestamp < '2022-11-07'"
# Set this to whatever you want!

##################################################################################################
### No need to edit anything beyond this section.


# Import packages.
import data_diff
import pandas as pd
import numpy as np
from data_diff import connect_to_table, diff_tables, connect
import snowflake.connector
import os

# Fetch the environmental variables.
snowflake_user = os.getenv('SNOWFLAKE_USER')
snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
snowflake_role = os.getenv('SNOWFLAKE_ROLE')

# Create a Snowflake connection.
conn = snowflake.connector.connect(
                user=snowflake_user,
                password=snowflake_password,
                account=snowflake_account,
                warehouse=snowflake_warehouse
                )
curs=conn.cursor()

# Get the columns from Table A.
query_columns_table_a = f'select column_name from ' \
f'"{snowflake_database}".information_schema.columns ' \
f'where table_schema = \'{schema_a}\' and ' \
f'table_name = \'{table_name}\' order by ordinal_position;'
curs.execute(query_columns_table_a)
show_columns_a = curs.fetch_pandas_all()

# Get the columns from Table B.
query_columns_table_b = f'select column_name from ' \
f'"{snowflake_database}".information_schema.columns ' \
f'where table_schema = \'{schema_b}\' and ' \
f'table_name = \'{table_name}\' order by ordinal_position;'
curs.execute(query_columns_table_b)
show_columns_b = curs.fetch_pandas_all()

# Convert to list and identify common columns.
columns_table_a = show_columns_a["COLUMN_NAME"].to_list()
columns_table_b = show_columns_b["COLUMN_NAME"].to_list()
common_columns = [element for element in columns_table_a if element in columns_table_b] 

# Store unique columns and convert to dataframes.
columns_unique_to_a = [i for i in columns_table_a if i not in columns_table_b]
columns_unique_to_b = [i for i in columns_table_b if i not in columns_table_a]
columns_unique_to_a_df = pd.DataFrame(columns_unique_to_a, columns=['Columns Unique to Table A'])
columns_unique_to_b_df = pd.DataFrame(columns_unique_to_b, columns=['Columns Unique to Table B'])
common_columns_minus_primary_key = common_columns.copy()
common_columns_minus_primary_key.remove(primary_key)
additional_columns_to_diff_input = common_columns_minus_primary_key

SNOWFLAKE_CONN_INFO = {
    "driver": "snowflake",
    "user": snowflake_user,
    "password": snowflake_password,
    "account": snowflake_account,
    "database": snowflake_database,
    "schema": schema_a,
    "warehouse": snowflake_warehouse,
    "role": snowflake_role
}

additional_columns_to_diff = additional_columns_to_diff_input.copy()
table_a_connection = connect_to_table(SNOWFLAKE_CONN_INFO, table_name, primary_key)
table_b_connection = connect_to_table(SNOWFLAKE_CONN_INFO, schema_b+"."+table_name, primary_key) 
diff_output_raw = list(diff_tables(table_a_connection, table_b_connection, algorithm='joindiff', extra_columns=tuple(additional_columns_to_diff), where=where_clause))
diff_output = diff_output_raw.copy()
df_output = pd.DataFrame(diff_output, columns =['a', 'b'])
column_values = pd.DataFrame(df_output['b'].to_list(), columns = [primary_key] + additional_columns_to_diff)
all_columns_values_tall = pd.concat([df_output, column_values], axis=1).\
drop('b', axis=1)
all_columns_values_tall.loc[all_columns_values_tall['a'] == '-', 'a'] = 'Table A'
all_columns_values_tall.loc[all_columns_values_tall['a'] == '+', 'a'] = 'Table B'
all_columns_values_pivot = all_columns_values_tall.\
    pivot(index=primary_key, columns='a',values=[primary_key] + additional_columns_to_diff)
all_columns_values_pivot
all_columns = [primary_key] + additional_columns_to_diff
each_column_twice = [item for item in all_columns for _ in range(2)]
tuples = [(x, 'Table A' if idx % 2 == 0 else 'Table B') for idx, x in enumerate(each_column_twice)]
index = pd.MultiIndex.from_tuples(tuples, names=[None, "a"])
df_with_index = pd.DataFrame(columns=index)
all_columns_values_pivot_multiindex = pd.concat([df_with_index, all_columns_values_pivot])

all_columns_values_pivot_multiindex.columns = [': '.join(i) for i in all_columns_values_pivot_multiindex.columns]
matching_primary_key_rows = all_columns_values_pivot_multiindex.loc[all_columns_values_pivot_multiindex[primary_key+": Table A"] == \
all_columns_values_pivot_multiindex[primary_key+": Table B"]]

if len(columns_unique_to_a) > 0:
    print(columns_unique_to_a_df)

if len(columns_unique_to_b) > 0:
    print(columns_unique_to_b_df)    

# Display missing primary keys
pks_missing_from_table_a = all_columns_values_pivot_multiindex[[primary_key+": Table A"]].isna().sum()[0]   
pks_missing_from_table_b = all_columns_values_pivot_multiindex[[primary_key+": Table B"]].isna().sum()[0]
primary_keys_df = pd.DataFrame([[pks_missing_from_table_a, pks_missing_from_table_b]], columns=['Primary Keys Missing from Table A', 'Primary Keys Missing from Table B'])


# Display columns with conflicts
columns_with_conflicts = []
conflicts_df = pd.DataFrame(columns=['Column', 'Conflicting Rows'])

for i in additional_columns_to_diff:
    conflicts = (matching_primary_key_rows[i+": Table A"] != matching_primary_key_rows[i+": Table B"]) & \
        (matching_primary_key_rows[i+": Table A"].notnull() | matching_primary_key_rows[i+": Table B"].notnull())
    sum_conflicts = sum(conflicts)
    conflicts_df.loc[len(conflicts_df.index)] = [i, sum_conflicts]
    if sum(conflicts) > 0:
        columns_with_conflicts += [i+": Table A", i+": Table B"]
    
    # Ideally, add T/F column to identify conflicts, which will be displayed colorfully conditionally. Or better yet, make the table itself colorful.

if matching_primary_keys_only == "Only Primary Keys That Exist in Both Tables":
    output_base = matching_primary_key_rows
else:
    output_base = all_columns_values_pivot_multiindex
    
if conflicts_only_or_all_columns == "Only Columns With Differences":
    final_output = output_base[output_base.columns.intersection(columns_with_conflicts)]

else:
    final_output = output_base

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 350)
print(primary_keys_df)
print("")
print(conflicts_df)
print("")
print(final_output)
