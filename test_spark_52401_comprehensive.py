#!/usr/bin/env python3
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
        
        # First append
        spark.createDataFrame([(1, "foo")], schema).write.mode("append").saveAsTable(table_name)
        
        # Verify first append
        count1 = df.count()
        collect1 = df.collect()
        print(f"After first append: count()={count1}, collect()={collect1}")
        
        # Second append
        spark.createDataFrame([(2, "bar")], schema).write.mode("append").saveAsTable(table_name)
        
        # Verify both appends
        count2 = df.count()
        collect2 = df.collect()
        print(f"After second append: count()={count2}, collect()={collect2}")
        
        # Verify results
        expected_data = [(1, "foo"), (2, "bar")]
        success = (count2 == 2 and 
                  len(collect2) == 2 and 
                  all(row in collect2 for row in expected_data))
        
        print(f"Multiple appends test: {'✅ SUCCESS' if success else '❌ FAILURE'}")
        return success
        
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass
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
        initial_count = df.count()
        initial_collect = df.collect()
        print(f"Initial state: count()={initial_count}, collect()={initial_collect}")
        
        # Overwrite with new data
        spark.createDataFrame([(2, "bar"), (3, "baz")], schema).write.mode("overwrite").saveAsTable(table_name)
        
        # Verify overwrite
        final_count = df.count()
        final_collect = df.collect()
        print(f"After overwrite: count()={final_count}, collect()={final_collect}")
        
        # Verify results
        expected_data = [(2, "bar"), (3, "baz")]
        success = (final_count == 2 and 
                  len(final_collect) == 2 and 
                  all(row in final_collect for row in expected_data))
        
        print(f"Overwrite test: {'✅ SUCCESS' if success else '❌ FAILURE'}")
        return success
        
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass
        spark.stop()

def main():
    """Run all tests."""
    print("Running comprehensive tests for SPARK-52401 fix...\n")
    
    tests = [
        ("Multiple Appends", test_multiple_appends),
        ("DataFrame Operations", test_dataframe_operations),
        ("Overwrite Operations", test_overwrite_operations)
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