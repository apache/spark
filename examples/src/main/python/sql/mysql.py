#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A simple example demonstrating Spark SQL MySQL integration.
Run with:
  
  ./bin/spark-submit --driver-class-path /path/to/mysql-connector-java-x.x.xx.jar \
    examples/src/main/python/sql/mysql.py \
    --hostname localhost \
    --port 3306 \
    --database your_database \
    --username your_username \
    --password your_password \
    --table employees \
    --driver_path /path/to/mysql-connector-java-x.x.xx.jar

"""


import argparse
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='PySpark MySQL Example')
    parser.add_argument('--hostname', type=str, required=True, help='MySQL server hostname or IP')
    parser.add_argument('--port', type=str, default='3306', help='MySQL port (default: 3306)')
    parser.add_argument('--database', type=str, required=True, help='MySQL database name')
    parser.add_argument('--username', type=str, required=True, help='MySQL username')
    parser.add_argument('--password', type=str, required=True, help='MySQL password')
    parser.add_argument('--table', type=str, required=True, help='MySQL table name')
    parser.add_argument('--driver_path', type=str, required=True, help='Path JDBC driver JAR')

    args = parser.parse_args()

    # Extract arguments
    jdbc_hostname = args.hostname
    jdbc_port = args.port
    jdbc_database = args.database
    jdbc_username = args.username
    jdbc_password = args.password
    table_name = args.table
    jdbc_driver_path = args.driver_path
    
    # Initialize SparkSession with the MySQL JDBC driver
    spark = SparkSession \
        .builder \
        .appName("PySpark MySQL Example") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()

    # JDBC connection properties
    jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"
    connection_properties = {
        "user": jdbc_username,
        "password": jdbc_password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Read data from MySQL table into DataFrame
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", jdbc_username) \
        .option("password", jdbc_password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
    
    # Show the DataFrame content
    df.show()

    # Register the DataFrame as a Temporary View
    df.createOrReplaceTempView(f"{table_name}")

    # Run SQL Queries Using SparkSQL
    # Example 1: Select all records
    query_result = spark.sql(f"SELECT * FROM {table_name}")

    # Display the results
    print(f"Query Result: SELECT * FROM {table_name}")
    query_result.show()

    # Example 2: Filter and select specific records
    # filtered_result = spark.sql(f"SELECT * FROM {table_name} WHERE age > 30")

    # Display the results
    # print(f"Filtered Result: SELECT * FROM {table_name} WHERE age > 30")
    # filtered_result.show()

    # Example 3: Group By and Aggregation
    # grouped_result = spark.sql(f"SELECT department, COUNT(*) as count 
    # FROM {table_name} GROUP BY department")

    # Display the results
    # print(f"Grouped Result: SELECT department, COUNT(*) as count 
    # FROM {table_name} GROUP BY department")
    # grouped_result.show()
    
    # Stop SparkSession
    spark.stop()      

"""
A simple example demonstrating how to create a database/table in Mysql using Python
"""
    
# import mysql.connector
# from mysql.connector import Error

# try:
#     # Connect to MySQL server
#     connection = mysql.connector.connect(
#         host='localhost',  # Replace with your host
#         user='root',       # Replace with your MySQL username
#         password='your_password'  # Replace with your MySQL password
#     )

#     if connection.is_connected():
#         print("Successfully connected to MySQL server")

#         # Create a cursor object
#         cursor = connection.cursor()

#         # Create a new database
#         cursor.execute("CREATE DATABASE IF NOT EXISTS testdb")
#         print("Database 'testdb' created or already exists.")

#         # Connect to the new database
#         connection.database = 'testdb'

#         # Create a new table
#         create_table_query = """
#         CREATE TABLE IF NOT EXISTS employees (
#             id INT AUTO_INCREMENT PRIMARY KEY,
#             name VARCHAR(100),
#             age INT,
#             department VARCHAR(100)
#         )
#         """
#         cursor.execute(create_table_query)
#         print("Table 'employees' created or already exists.")

#         # Insert data into the table
#         insert_data_query = """
#         INSERT INTO employees (name, age, department)
#         VALUES (%s, %s, %s)
#         """
#         employee_data = [
#             ('John Doe', 30, 'Engineering'),
#             ('Jane Smith', 25, 'Marketing'),
#             ('Michael Johnson', 35, 'Sales')
#         ]

#         cursor.executemany(insert_data_query, employee_data)
#         connection.commit()  # Commit the transaction
#         print(f"{cursor.rowcount} records inserted successfully.")

# except Error as e:
#     print(f"Error: {e}")

# finally:
#     if connection.is_connected():
#         cursor.close()
#         connection.close()
#         print("MySQL connection is closed.")