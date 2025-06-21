#!/usr/bin/env python3
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
Test script to reproduce and verify the fix for SPARK-52401.
This script demonstrates the issue where DataFrame.collect() doesn't reflect
table updates after saveAsTable append operations.
"""

import pyspark
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

def test_spark_52401():
    """Test the SPARK-52401 issue and verify the fix."""
    
    # Create Spark session
    spark = pyspark.sql.SparkSession.builder.appName("SPARK-52401-Test").getOrCreate()
    
    # Define schema
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", StringType(), True)
    ])
    
    table_name = "test_table_spark_52401"
    
    try:
        # Create empty table
        spark.createDataFrame([], schema).write.saveAsTable(table_name)
        df = spark.table(table_name)
        
        # Verify initial state
        assert df.count() == 0
        assert df.collect() == []
        
        # Append data to table
        spark.createDataFrame([(1, "foo")], schema).write.mode("append").saveAsTable(table_name)
        
        # This should now work correctly with the fix
        assert df.count() == 1
        assert len(df.collect()) == 1
        assert df.collect()[0]["col1"] == 1
        assert df.collect()[0]["col2"] == "foo"
        
        print("✅ SPARK-52401 fix verification passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise
    finally:
        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.stop()

if __name__ == "__main__":
    test_spark_52401() 