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

package org.apache.spark.sql.hive

import java.io.File

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.QueryTest

class HiveTablePerfStatsSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {

  private def setupPartitionedTable(tableName: String, dir: File): Unit = {
    spark.range(5).selectExpr("id", "id as partCol1", "id as partCol2").write
      .partitionBy("partCol1", "partCol2")
      .mode("overwrite")
      .parquet(dir.getAbsolutePath)

    spark.sql(s"""
      |create external table $tableName (id long)
      |partitioned by (partCol1 int, partCol2 int)
      |stored as parquet
      |location "${dir.getAbsolutePath}"""".stripMargin)
    spark.sql(s"msck repair table $tableName")
  }

  test("partitioned pruned table reports only selected files") {
    assert(spark.sqlContext.getConf(HiveUtils.CONVERT_METASTORE_PARQUET.key) == "true")
    withTable("test") {
      withTempDir { dir =>
        setupPartitionedTable("test", dir)
        val df = spark.sql("select * from test")
        assert(df.count() == 5)
        assert(df.inputFiles.length == 5)  // unpruned

        val df2 = spark.sql("select * from test where partCol1 = 3 or partCol2 = 4")
        assert(df2.count() == 2)
        assert(df2.inputFiles.length == 2)  // pruned, so we have less files

        val df3 = spark.sql("select * from test where PARTCOL1 = 3 or partcol2 = 4")
        assert(df3.count() == 2)
        assert(df3.inputFiles.length == 2)

        val df4 = spark.sql("select * from test where partCol1 = 999")
        assert(df4.count() == 0)
        assert(df4.inputFiles.length == 0)
      }
    }
  }

  test("lazy partition pruning reads only necessary partition data") {
    withSQLConf(
        "spark.sql.hive.filesourcePartitionPruning" -> "true",
        "spark.sql.hive.filesourcePartitionFileCacheEnabled" -> "false") {
      withTable("test") {
        withTempDir { dir =>
          setupPartitionedTable("test", dir)
          HiveCatalogMetrics.reset()
          spark.sql("select * from test where partCol1 = 999").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 0)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)

          HiveCatalogMetrics.reset()
          spark.sql("select * from test where partCol1 < 2").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 2)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 2)

          HiveCatalogMetrics.reset()
          spark.sql("select * from test where partCol1 < 3").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 3)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 3)

          // should read all
          HiveCatalogMetrics.reset()
          spark.sql("select * from test").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)

          // read all should not be cached
          HiveCatalogMetrics.reset()
          spark.sql("select * from test").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)

          // cache should be disabled
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)
        }
      }
    }
  }

  test("lazy partition pruning with file status caching enabled") {
    withSQLConf(
        "spark.sql.hive.filesourcePartitionPruning" -> "true",
        "spark.sql.hive.filesourcePartitionFileCacheEnabled" -> "true") {
      withTable("test") {
        withTempDir { dir =>
          setupPartitionedTable("test", dir)
          HiveCatalogMetrics.reset()
          spark.sql("select * from test where partCol1 = 999").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 0)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)

          HiveCatalogMetrics.reset()
          spark.sql("select * from test where partCol1 < 2").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 2)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 2)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)

          HiveCatalogMetrics.reset()
          spark.sql("select * from test where partCol1 < 3").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 3)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 1)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 2)

          HiveCatalogMetrics.reset()
          spark.sql("select * from test").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 2)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 3)

          HiveCatalogMetrics.reset()
          spark.sql("select * from test").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 5)
        }
      }
    }
  }

  test("file status caching respects refresh table and refreshByPath") {
    withSQLConf(
        "spark.sql.hive.filesourcePartitionPruning" -> "true",
        "spark.sql.hive.filesourcePartitionFileCacheEnabled" -> "true") {
      withTable("test") {
        withTempDir { dir =>
          setupPartitionedTable("test", dir)
          HiveCatalogMetrics.reset()
          spark.sql("select * from test").count()
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)

          HiveCatalogMetrics.reset()
          spark.sql("refresh table test")
          spark.sql("select * from test").count()
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)

          spark.catalog.cacheTable("test")
          HiveCatalogMetrics.reset()
          spark.catalog.refreshByPath(dir.getAbsolutePath)
          spark.sql("select * from test").count()
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)
        }
      }
    }
  }

  test("all partitions read and cached when filesource partition pruning is off") {
    withSQLConf("spark.sql.hive.filesourcePartitionPruning" -> "false") {
      withTable("test") {
        withTempDir { dir =>
          setupPartitionedTable("test", dir)

          // We actually query the partitions from hive each time the table is resolved in this
          // mode. This is kind of terrible, but is needed to preserve the legacy behavior
          // of doing plan cache validation based on the entire partition set.
          HiveCatalogMetrics.reset()
          spark.sql("select * from test where partCol1 = 999").count()
          // 5 from table resolution, another 5 from ListingFileCatalog
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 10)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)

          HiveCatalogMetrics.reset()
          spark.sql("select * from test where partCol1 < 2").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)

          HiveCatalogMetrics.reset()
          spark.sql("select * from test").count()
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)
        }
      }
    }
  }
}
