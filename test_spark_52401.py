#!/usr/bin/env python3
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
        
        # Get DataFrame reference to the table
        df = spark.table(table_name)
        
        # Verify initial state
        print("Initial state:")
        print(f"  count(): {df.count()}")
        print(f"  collect(): {df.collect()}")
        
        # Append data to the table
        spark.createDataFrame([(1, "foo")], schema).write.mode("append").saveAsTable(table_name)
        
        # Check if both count() and collect() reflect the update
        print("\nAfter append:")
        print(f"  count(): {df.count()}")
        print(f"  collect(): {df.collect()}")
        
        # The issue is that count() returns 1 but collect() returns []
        # With the fix, both should return the updated data
        count_result = df.count()
        collect_result = df.collect()
        
        print(f"\nResults:")
        print(f"  count() result: {count_result}")
        print(f"  collect() result: {collect_result}")
        print(f"  collect() length: {len(collect_result)}")
        
        # Verify the fix
        if count_result == 1 and len(collect_result) == 1 and collect_result[0] == (1, "foo"):
            print("\n✅ SUCCESS: Both count() and collect() reflect the table update!")
            return True
        else:
            print("\n❌ FAILURE: collect() doesn't reflect the table update!")
            print(f"Expected: count()=1, collect()=[(1, 'foo')]")
            print(f"Actual: count()={count_result}, collect()={collect_result}")
            return False
            
    finally:
        # Clean up
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass
        spark.stop()

if __name__ == "__main__":
    success = test_spark_52401()
    exit(0 if success else 1) 