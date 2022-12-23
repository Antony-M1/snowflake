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
