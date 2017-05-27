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
import java.util.concurrent.{Executors, TimeUnit}

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.execution.datasources.FileStatusCache
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class PartitionedTablePerfStatsSuite
  extends QueryTest with TestHiveSingleton with SQLTestUtils with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    FileStatusCache.resetForTesting()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    FileStatusCache.resetForTesting()
  }

  private case class TestSpec(setupTable: (String, File) => Unit, isDatasourceTable: Boolean)

  /**
   * Runs a test against both converted hive and native datasource tables. The test can use the
   * passed TestSpec object for setup and inspecting test parameters.
   */
  private def genericTest(testName: String)(fn: TestSpec => Unit): Unit = {
    test("hive table: " + testName) {
      fn(TestSpec(setupPartitionedHiveTable, false))
    }
    test("datasource table: " + testName) {
      fn(TestSpec(setupPartitionedDatasourceTable, true))
    }
  }

  private def setupPartitionedHiveTable(tableName: String, dir: File): Unit = {
    setupPartitionedHiveTable(tableName, dir, 5)
  }

  private def setupPartitionedHiveTable(
      tableName: String, dir: File, scale: Int, repair: Boolean = true): Unit = {
    spark.range(scale).selectExpr("id as fieldOne", "id as partCol1", "id as partCol2").write
      .partitionBy("partCol1", "partCol2")
      .mode("overwrite")
      .parquet(dir.getAbsolutePath)

    spark.sql(s"""
      |create external table $tableName (fieldOne long)
      |partitioned by (partCol1 int, partCol2 int)
      |stored as parquet
      |location "${dir.toURI}"""".stripMargin)
    if (repair) {
      spark.sql(s"msck repair table $tableName")
    }
  }

  private def setupPartitionedDatasourceTable(tableName: String, dir: File): Unit = {
    setupPartitionedDatasourceTable(tableName, dir, 5)
  }

  private def setupPartitionedDatasourceTable(
      tableName: String, dir: File, scale: Int, repair: Boolean = true): Unit = {
    spark.range(scale).selectExpr("id as fieldOne", "id as partCol1", "id as partCol2").write
      .partitionBy("partCol1", "partCol2")
      .mode("overwrite")
      .parquet(dir.getAbsolutePath)

    spark.sql(s"""
      |create table $tableName (fieldOne long, partCol1 int, partCol2 int)
      |using parquet
      |options (path "${dir.toURI}")
      |partitioned by (partCol1, partCol2)""".stripMargin)
    if (repair) {
      spark.sql(s"msck repair table $tableName")
    }
  }

  genericTest("partitioned pruned table reports only selected files") { spec =>
    assert(spark.sqlContext.getConf(HiveUtils.CONVERT_METASTORE_PARQUET.key) == "true")
    withTable("test") {
      withTempDir { dir =>
        spec.setupTable("test", dir)
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

        val df5 = spark.sql("select * from test where fieldOne = 4")
        assert(df5.count() == 1)
        assert(df5.inputFiles.length == 5)
      }
    }
  }

  genericTest("lazy partition pruning reads only necessary partition data") { spec =>
    withSQLConf(
        SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true",
        SQLConf.HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE.key -> "0") {
      withTable("test") {
        withTempDir { dir =>
          spec.setupTable("test", dir)
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

  genericTest("lazy partition pruning with file status caching enabled") { spec =>
    withSQLConf(
        SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true",
        SQLConf.HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE.key -> "9999999") {
      withTable("test") {
        withTempDir { dir =>
          spec.setupTable("test", dir)
          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test where partCol1 = 999").count() == 0)
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 0)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test where partCol1 < 2").count() == 2)
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 2)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 2)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test where partCol1 < 3").count() == 3)
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 3)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 1)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 2)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test").count() == 5)
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 2)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 3)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test").count() == 5)
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 5)
        }
      }
    }
  }

  genericTest("file status caching respects refresh table and refreshByPath") { spec =>
    withSQLConf(
        SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true",
        SQLConf.HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE.key -> "9999999") {
      withTable("test") {
        withTempDir { dir =>
          spec.setupTable("test", dir)
          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test").count() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)

          HiveCatalogMetrics.reset()
          spark.sql("refresh table test")
          assert(spark.sql("select * from test").count() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)

          spark.catalog.cacheTable("test")
          HiveCatalogMetrics.reset()
          spark.catalog.refreshByPath(dir.getAbsolutePath)
          assert(spark.sql("select * from test").count() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)
        }
      }
    }
  }

  genericTest("file status cache respects size limit") { spec =>
    withSQLConf(
        SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true",
        SQLConf.HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE.key -> "1" /* 1 byte */) {
      withTable("test") {
        withTempDir { dir =>
          spec.setupTable("test", dir)
          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test").count() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)
          assert(spark.sql("select * from test").count() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 10)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)
        }
      }
    }
  }

  test("datasource table: table setup does not scan filesystem") {
    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
      withTable("test") {
        withTempDir { dir =>
          HiveCatalogMetrics.reset()
          setupPartitionedDatasourceTable("test", dir, scale = 10, repair = false)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)
        }
      }
    }
  }

  test("hive table: table setup does not scan filesystem") {
    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
      withTable("test") {
        withTempDir { dir =>
          HiveCatalogMetrics.reset()
          setupPartitionedHiveTable("test", dir, scale = 10, repair = false)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)
          assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 0)
        }
      }
    }
  }

  test("hive table: num hive client calls does not scale with partition count") {
    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
      withTable("test") {
        withTempDir { dir =>
          setupPartitionedHiveTable("test", dir, scale = 100)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test where partCol1 = 1").count() == 1)
          assert(HiveCatalogMetrics.METRIC_HIVE_CLIENT_CALLS.getCount() > 0)
          assert(HiveCatalogMetrics.METRIC_HIVE_CLIENT_CALLS.getCount() < 10)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test").count() == 100)
          assert(HiveCatalogMetrics.METRIC_HIVE_CLIENT_CALLS.getCount() < 10)

          HiveCatalogMetrics.reset()
          assert(spark.sql("show partitions test").count() == 100)
          assert(HiveCatalogMetrics.METRIC_HIVE_CLIENT_CALLS.getCount() < 10)
        }
      }
    }
  }

  test("datasource table: num hive client calls does not scale with partition count") {
    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
      withTable("test") {
        withTempDir { dir =>
          setupPartitionedDatasourceTable("test", dir, scale = 100)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test where partCol1 = 1").count() == 1)
          assert(HiveCatalogMetrics.METRIC_HIVE_CLIENT_CALLS.getCount() > 0)
          assert(HiveCatalogMetrics.METRIC_HIVE_CLIENT_CALLS.getCount() < 10)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test").count() == 100)
          assert(HiveCatalogMetrics.METRIC_HIVE_CLIENT_CALLS.getCount() < 10)

          HiveCatalogMetrics.reset()
          assert(spark.sql("show partitions test").count() == 100)
          assert(HiveCatalogMetrics.METRIC_HIVE_CLIENT_CALLS.getCount() < 10)
        }
      }
    }
  }

  test("hive table: files read and cached when filesource partition management is off") {
    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {
      withTable("test") {
        withTempDir { dir =>
          setupPartitionedHiveTable("test", dir)

          // We actually query the partitions from hive each time the table is resolved in this
          // mode. This is kind of terrible, but is needed to preserve the legacy behavior
          // of doing plan cache validation based on the entire partition set.
          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test where partCol1 = 999").count() == 0)
          // 5 from table resolution, another 5 from InMemoryFileIndex
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 10)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test where partCol1 < 2").count() == 2)
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test").count() == 5)
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 5)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)
        }
      }
    }
  }

  test("datasource table: all partition data cached in memory when partition management is off") {
    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {
      withTable("test") {
        withTempDir { dir =>
          setupPartitionedDatasourceTable("test", dir)
          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test where partCol1 = 999").count() == 0)

          // not using metastore
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 0)

          // reads and caches all the files initially
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 5)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test where partCol1 < 2").count() == 2)
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 0)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)

          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test").count() == 5)
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 0)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 0)
        }
      }
    }
  }

  test("SPARK-18700: table loaded only once even when resolved concurrently") {
    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {
      withTable("test") {
        withTempDir { dir =>
          HiveCatalogMetrics.reset()
          setupPartitionedHiveTable("test", dir, 50)
          // select the table in multi-threads
          val executorPool = Executors.newFixedThreadPool(10)
          (1 to 10).map(threadId => {
            val runnable = new Runnable {
              override def run(): Unit = {
                spark.sql("select * from test where partCol1 = 999").count()
              }
            }
            executorPool.execute(runnable)
            None
          })
          executorPool.shutdown()
          executorPool.awaitTermination(30, TimeUnit.SECONDS)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 50)
          assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == 1)
        }
      }
    }
  }

  test("resolveRelation for a FileFormat DataSource without userSchema scan filesystem only once") {
    withTempDir { dir =>
      import spark.implicits._
      Seq(1).toDF("a").write.mode("overwrite").save(dir.getAbsolutePath)
      HiveCatalogMetrics.reset()
      spark.read.parquet(dir.getAbsolutePath)
      assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 1)
      assert(HiveCatalogMetrics.METRIC_FILE_CACHE_HITS.getCount() == 1)
    }
  }
}
