from dotenv import load_dotenv
import os
import snowflake.connector


load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    # private_key=os.getenv('SNOWFLAKE_PRIVATE_KEY'),
    session_parameters={
        'QUERY_TAG': 'snow-test',
    }
)

conn.cursor().execute("ALTER SESSION SET QUERY_TAG = 'snow-test'")

# Creating a Database, Schema, and Warehouse
# https://docs.snowflake.com/en/user-guide/python-connector-example.html#creating-a-database-schema-and-warehouse
conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse_mg")
conn.cursor().execute("CREATE DATABASE IF NOT EXISTS testdb_mg")
conn.cursor().execute("USE DATABASE testdb_mg")
conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS testschema_mg")


# Using the Database, Schema, and Warehouse
# https://docs.snowflake.com/en/user-guide/python-connector-example.html#using-the-database-schema-and-warehouse
conn.cursor().execute("USE WAREHOUSE tiny_warehouse_mg")
conn.cursor().execute("USE DATABASE testdb_mg")
conn.cursor().execute("USE SCHEMA testdb_mg.testschema_mg")



# Creating Tables and Inserting Data
# https://docs.snowflake.com/en/user-guide/python-connector-example.html#creating-tables-and-inserting-data
conn.cursor().execute(
    "CREATE OR REPLACE TABLE "
    "test_table(col1 integer, col2 string)")

conn.cursor().execute(
    "INSERT INTO test_table(col1, col2) VALUES " + 
    "    (123, 'test string1'), " + 
    "    (456, 'test string2')")


# Loading Data
# https://docs.snowflake.com/en/user-guide/python-connector-example.html#loading-data
'''
Instead of inserting data into tables using individual INSERT commands, 
you can bulk load data from files staged in either an internal or external location.
'''
# Copying Data from an Internal Location
# Putting Data
# conn.cursor().execute("PUT file:///tmp/data/file* @%testtable")
try:
    conn.cursor().execute(
        "CREATE OR REPLACE TABLE "
        "mock_data(id integer, first_name string, last_name string, email string, gender string, ip_address string)")

    conn.cursor().execute("PUT file:///home/softsuave/project/snowflake/tmp/data/MOCK_DATA.csv @%mock_data")
    conn.cursor().execute("COPY INTO mock_data")
except Exception as ex:
    print(ex)

# Copying Data from an External Location
# https://docs.snowflake.com/en/user-guide/python-connector-example.html#copying-data-from-an-external-location
try:
    # Copying Data
    conn.cursor().execute("""
    COPY INTO testtable FROM s3://<s3_bucket>/data/
        STORAGE_INTEGRATION = myint
        FILE_FORMAT=(field_delimiter=',')
    """.format(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY))
except Exception as ex:
    print(ex)


# Querying Data
# https://docs.snowflake.com/en/user-guide/python-connector-example.html#querying-data
'''
With the Snowflake Connector for Python, you can submit:

    1.a synchronous query, which returns control to your application after the query completes.

    2.an asynchronous query, which returns control to your application before the query completes.

After the query has completed, you use the Cursor object to fetch the values in the results. 
By default, the Snowflake Connector for Python converts the values from Snowflake data types to native 
Python data types. (Note that you can choose to return the values as strings and perform the type 
conversions in your application. See Improving Query Performance by Bypassing Data Conversion.)
'''
# Performing a Synchronous Query
# To perform a synchronous query, call the execute() method in the Cursor object. For example:
cur = conn.cursor()
try:
    # we are already mentioned what database, warehouse and schema have to use
    print(cur.execute('select * from mock_data'))
except Exception as ex:
    print(ex)


# Performing an Asynchronous Query
# count = cur.execute_async('select count(*) from table(generator(timeLimit => 25))')
async_query = cur.execute_async('''ALTER TABLE mock_data
                            ADD date_of_birth DATE ;''')

# Checking the Status of a Query
if conn.get_query_status(async_query.get('queryId')).name == 'SUCCESS':
    print('query exceqution is finished')
else:
    print('Query exceqution is not finised')

# To retrieve the results of the query
# Get the results from a query.
async_query = cur.execute_async('''select * from mock_data;''')
cur.get_results_from_sfqid(async_query.get('queryId'))
results = cur.fetchall()
print(f'{results[0]}')


# Retrieving the Snowflake Query ID
# https://docs.snowflake.com/en/user-guide/python-connector-example.html#retrieving-the-snowflake-query-id



print("Finished")