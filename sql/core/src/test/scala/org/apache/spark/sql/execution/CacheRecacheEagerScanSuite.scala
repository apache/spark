/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.io.File

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite demonstrating the eager file check problem during recache.
 * This shows how data sources like Cobrix that check file existence in buildScan()
 * fail during recache even though the recache doesn't execute the query.
 */
class CacheRecacheEagerScanSuite extends QueryTest with SharedSparkSession {

  test("recache fails with eager file check in buildScan() - mimics Cobrix behavior") {
    withTempPath { dir =>
      withTable("source_table") {
        // Step 1: Create test data files
        val eagerScanPath = new File(dir, "eager_scan_data.txt")
        val data = (1 to 10000).map(_.toString)
        spark.sparkContext.parallelize(data).saveAsTextFile(eagerScanPath.getAbsolutePath)

        // Step 2: Create a regular table
        spark.range(10000).write.saveAsTable("source_table")

        // Step 3: Create a DataFrame that joins the table with the eager-scan data source
        // Use the fully qualified class name instead of short name
        val eagerDf = spark.read
          .format("org.apache.spark.sql.execution.DefaultSource")
          .option("path", eagerScanPath.getAbsolutePath)
          .load()
        eagerDf.createOrReplaceTempView("eager_data")

        val df = spark.sql("""
          SELECT t.id, e.value
          FROM source_table t
          JOIN eager_data e
          ON CAST(e.value AS LONG) = t.id
        """)

        // Step 4: Cache the DataFrame
        df.cache()

        // Step 5: Materialize the cache
        val result1 = df.collect()
        assert(result1.length === 9999)

        // Step 6: Verify cache is loaded
        val cacheRelations = df.queryExecution.withCachedData.collect {
          case i: InMemoryRelation => i
        }
        assert(cacheRelations.nonEmpty, "DataFrame should be cached")
        assert(cacheRelations.head.cacheBuilder.isCachedColumnBuffersLoaded,
          "Cache should be materialized")

        // Step 7: DELETE the eager-scan file
        val deleted = eagerScanPath.listFiles().forall(_.delete()) && eagerScanPath.delete()
        assert(deleted, "Failed to delete eager-scan data directory")

        // Step 8: INSERT into source_table - this triggers recacheByPlan
        // Expected: This should FAIL because recache calls executePlan() which calls buildScan()
        // which eagerly checks if the file exists
        val exception = intercept[IllegalArgumentException] {
          spark.sql("INSERT INTO source_table VALUES (100)")
        }

        // Verify the exception is from our eager check
        assert(exception.getMessage.contains("EagerTableScan: File or directory does not exist"),
          s"Expected eager scan error, got: ${exception.getMessage}")
      }
    }
  }

  test("recache does not fail with eager file check in buildScan() when file is not deleted") {
    withTempPath { dir =>
      withTable("source_table") {
        // Step 1: Create test data files
        val eagerScanPath = new File(dir, "eager_scan_data.txt")
        val data = (1 to 10000).map(_.toString)
        spark.sparkContext.parallelize(data).saveAsTextFile(eagerScanPath.getAbsolutePath)

        // Step 2: Create a regular table
        spark.range(10000).write.saveAsTable("source_table")

        // Step 3: Create a DataFrame that joins the table with the eager-scan data source
        // Use the fully qualified class name instead of short name
        val eagerDf = spark.read
          .format("org.apache.spark.sql.execution.DefaultSource")
          .option("path", eagerScanPath.getAbsolutePath)
          .load()
        eagerDf.createOrReplaceTempView("eager_data")

        val df = spark.sql("""
          SELECT t.id, e.value
          FROM source_table t
          JOIN eager_data e
          ON CAST(e.value AS LONG) = t.id
        """)

        // Step 4: Cache the DataFrame
        df.cache()

        // Step 5: Materialize the cache
        val result1 = df.collect()
        assert(result1.length === 9999)

        // Step 6: Verify cache is loaded
        val cacheRelations = df.queryExecution.withCachedData.collect {
          case i: InMemoryRelation => i
        }
        assert(cacheRelations.nonEmpty, "DataFrame should be cached")
        assert(cacheRelations.head.cacheBuilder.isCachedColumnBuffersLoaded,
          "Cache should be materialized")

        // Step 7: INSERT into source_table - this triggers recacheByPlan
        // This will not fail because the source files still exist
        spark.sql("INSERT INTO source_table VALUES (100)")
      }
    }
  }
}
