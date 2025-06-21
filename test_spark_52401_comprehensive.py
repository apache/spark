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
Comprehensive test script for SPARK-52401 fix.
Tests multiple scenarios where DataFrame operations should reflect table updates.
"""

import pyspark
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

def test_multiple_appends():
    """Test multiple append operations."""
    spark = pyspark.sql.SparkSession.builder.appName("SPARK-52401-Multiple-Appends").getOrCreate()
    
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", StringType(), True)
    ])
    
    table_name = "test_table_multiple_appends"
    
    try:
        # Create empty table
        spark.createDataFrame([], schema).write.saveAsTable(table_name)
        df = spark.table(table_name)
        
        # Verify initial state
        assert df.count() == 0
        assert df.collect() == []
        
        # First append
        spark.createDataFrame([(1, "foo")], schema).write.mode("append").saveAsTable(table_name)
        assert df.count() == 1
        assert len(df.collect()) == 1
        
        # Second append
        spark.createDataFrame([(2, "bar")], schema).write.mode("append").saveAsTable(table_name)
        assert df.count() == 2
        assert len(df.collect()) == 2
        
        # Third append
        spark.createDataFrame([(3, "baz")], schema).write.mode("append").saveAsTable(table_name)
        assert df.count() == 3
        assert len(df.collect()) == 3
        
        print("✅ Multiple appends test passed!")
        
    except Exception as e:
        print(f"❌ Multiple appends test failed: {e}")
        raise
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.stop()

def test_dataframe_operations():
    """Test various DataFrame operations after table updates."""
    spark = pyspark.sql.SparkSession.builder.appName("SPARK-52401-Operations").getOrCreate()
    
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", StringType(), True)
    ])
    
    table_name = "test_table_operations"
    
    try:
        # Create empty table
        spark.createDataFrame([], schema).write.saveAsTable(table_name)
        df = spark.table(table_name)
        
        # Append data
        spark.createDataFrame([(1, "foo"), (2, "bar")], schema).write.mode("append").saveAsTable(table_name)
        
        # Test various operations
        filter_result = df.filter(df.col1 == 1).collect()
        select_result = df.select("col2").collect()
        group_result = df.groupBy("col2").count().collect()
        
        print(f"Filter operation: {filter_result}")
        print(f"Select operation: {select_result}")
        print(f"Group operation: {group_result}")
        
        # Verify results
        success = (len(filter_result) == 1 and 
                  len(select_result) == 2 and 
                  len(group_result) == 2)
        
        print(f"DataFrame operations test: {'✅ SUCCESS' if success else '❌ FAILURE'}")
        return success
        
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass
        spark.stop()

def test_overwrite_operations():
    """Test overwrite operations."""
    spark = pyspark.sql.SparkSession.builder.appName("SPARK-52401-Overwrite").getOrCreate()
    
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", StringType(), True)
    ])
    
    table_name = "test_table_overwrite"
    
    try:
        # Create table with initial data
        spark.createDataFrame([(1, "foo")], schema).write.saveAsTable(table_name)
        df = spark.table(table_name)
        
        # Verify initial state
        assert df.count() == 1
        assert len(df.collect()) == 1
        
        # Overwrite with new data
        spark.createDataFrame([(2, "bar"), (3, "baz")], schema).write.mode("overwrite").saveAsTable(table_name)
        assert df.count() == 2
        assert len(df.collect()) == 2
        
        print("✅ Overwrite operations test passed!")
        
    except Exception as e:
        print(f"❌ Overwrite operations test failed: {e}")
        raise
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.stop()

def test_mixed_operations():
    """Test mixed append and overwrite operations."""
    spark = pyspark.sql.SparkSession.builder.appName("SPARK-52401-Mixed").getOrCreate()
    
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", StringType(), True)
    ])
    
    table_name = "test_table_mixed"
    
    try:
        # Create empty table
        spark.createDataFrame([], schema).write.saveAsTable(table_name)
        df = spark.table(table_name)
        
        # Verify initial state
        assert df.count() == 0
        assert df.collect() == []
        
        # Append data
        spark.createDataFrame([(1, "foo")], schema).write.mode("append").saveAsTable(table_name)
        assert df.count() == 1
        assert len(df.collect()) == 1
        
        # Overwrite data
        spark.createDataFrame([(2, "bar")], schema).write.mode("overwrite").saveAsTable(table_name)
        assert df.count() == 1
        assert len(df.collect()) == 1
        assert df.collect()[0]["col1"] == 2
        
        # Append more data
        spark.createDataFrame([(3, "baz")], schema).write.mode("append").saveAsTable(table_name)
        assert df.count() == 2
        assert len(df.collect()) == 2
        
        print("✅ Mixed operations test passed!")
        
    except Exception as e:
        print(f"❌ Mixed operations test failed: {e}")
        raise
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.stop()

def main():
    """Run all tests."""
    print("Running comprehensive tests for SPARK-52401 fix...\n")
    
    tests = [
        ("Multiple Appends", test_multiple_appends),
        ("DataFrame Operations", test_dataframe_operations),
        ("Overwrite Operations", test_overwrite_operations),
        ("Mixed Operations", test_mixed_operations)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"Running {test_name} test...")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test failed with exception: {e}")
            results.append((test_name, False))
        print()
    
    # Summary
    print("Test Summary:")
    print("=" * 50)
    passed = 0
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All tests passed! The SPARK-52401 fix is working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. The fix may need further investigation.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 