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

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Test suite to handle metadata cache related.
 */
class HiveMetadataCacheSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  private def setupPartitionedHiveTable(tableName: String, dir: File, scale: Int): Unit = {
    spark.range(scale).selectExpr("id as fieldone", "id as partCol1", "id as partCol2").write
      .partitionBy("partCol1", "partCol2")
      .mode("overwrite")
      .parquet(dir.getAbsolutePath)

    spark.sql(s"""
                 |create external table $tableName (fieldone long)
                 |partitioned by (partCol1 int, partCol2 int)
                 |stored as parquet
                 |location "${dir.getAbsolutePath}"""".stripMargin)
    spark.sql(s"msck repair table $tableName")
  }

  test("SPARK-16337 temporary view refresh") {
    withTempView("view_refresh") {
      withTable("view_table") {
        // Create a Parquet directory
        spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
          .write.saveAsTable("view_table")

        // Read the table in
        spark.table("view_table").filter("id > -1").createOrReplaceTempView("view_refresh")
        assert(sql("select count(*) from view_refresh").first().getLong(0) == 100)

        // Delete a file using the Hadoop file system interface since the path returned by
        // inputFiles is not recognizable by Java IO.
        val p = new Path(spark.table("view_table").inputFiles.head)
        assert(p.getFileSystem(hiveContext.sessionState.newHadoopConf()).delete(p, false))

        // Read it again and now we should see a FileNotFoundException
        val e = intercept[SparkException] {
          sql("select count(*) from view_refresh").first()
        }
        assert(e.getMessage.contains("FileNotFoundException"))
        assert(e.getMessage.contains("REFRESH"))

        // Refresh and we should be able to read it again.
        spark.catalog.refreshTable("view_refresh")
        val newCount = sql("select count(*) from view_refresh").first().getLong(0)
        assert(newCount > 0 && newCount < 100)
      }
    }
  }

  test("SPARK-18700: table loaded only once even when resolved concurrently") {
    withTable("test") {
      withTempDir { dir =>
        HiveCatalogMetrics.reset()
        assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == 0)
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
        // check the cache hit, we use the metric of METRIC_FILES_DISCOVERED and
        // METRIC_PARALLEL_LISTING_JOB_COUNT to check this, while the lock take effect,
        // only one thread can really do the build, so the listing job count is 2, the other
        // one is cache.load func. Also METRIC_FILES_DISCOVERED is $partition_num * 2
        assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == 2)
      }
    }
  }
}
