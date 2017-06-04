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

package org.apache.spark.sql.execution.metric

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.util.Utils

trait BaseWritingDataMetricsSuite extends SparkFunSuite with SQLTestUtils {
  import testImplicits._

  /**
   * Run the given function and return latest execution id.
   *
   * @param func the given function to run.
   */
  def getLatestExecutionId(spark: SparkSession)(func: () => Unit): Long = {
    val previousExecutionIds = spark.sharedState.listener.executionIdToData.keySet
    // Run the given function to trigger query execution.
    func()
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    val executionIds =
      spark.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)
    assert(executionIds.size == 1)
    executionIds.head
  }

  /**
   * Get execution metrics for the given execution id and verify metrics values.
   *
   * @param executionId the given execution id.
   * @param verifyFuncs functions used to verify the values of metrics.
   */
  def verifyWriteDataMetrics(
      spark: SparkSession,
      executionId: Long,
      verifyFuncs: Seq[Int => Boolean]): Unit = {
    val executionData = spark.sharedState.listener.getExecution(executionId).get
    val executedNode = executionData.physicalPlanGraph.nodes.head

    val metricsNames = Seq(
      "number of written files",
      "number of dynamic part",
      "number of output rows")

    val metrics = spark.sharedState.listener.getExecutionMetrics(executionId)

    metricsNames.zip(verifyFuncs).foreach { case (metricsName, verifyFunc) =>
      val sqlMetric = executedNode.metrics.find(_.name == metricsName)
      assert(sqlMetric.isDefined)
      val accumulatorId = sqlMetric.get.accumulatorId
      val metricValue = metrics(accumulatorId).replaceAll(",", "").toInt
      assert(verifyFunc(metricValue))
    }

    // Sanity check.
    val totalNumBytesMetric = executedNode.metrics.find(_.name == "bytes of written output").get
    val totalNumBytes = metrics(totalNumBytesMetric.accumulatorId).replaceAll(",", "").toInt
    assert(totalNumBytes > 0)

    val writingTimeMetric = executedNode.metrics.find(_.name == "average writing time (ms)").get
    val writingTime = metrics(writingTimeMetric.accumulatorId).replaceAll(",", "").toInt
    assert(writingTime >= 0)
  }

  protected def testMetricsNonDynamicPartition(
      spark: SparkSession,
      dataFormat: String,
      tableName: String): Unit = {
    withTable(tableName) {
      val executionId1 = getLatestExecutionId(spark) { () =>
        Seq((1, 2)).toDF("i", "j")
          .write.format(dataFormat).mode("overwrite").saveAsTable(tableName)
      }
      // written 1 file, 1 row, 0 dynamic partition.
      val verifyFuncs1: Seq[Int => Boolean] = Seq(_ == 1, _ == 0, _ == 1)
      verifyWriteDataMetrics(spark, executionId1, verifyFuncs1)

      val executionId2 = getLatestExecutionId(spark) { () =>
        Seq((9, 10), (11, 12)).toDF("i", "j").repartition(2)
          .write.format(dataFormat).insertInto(tableName)
      }
      // written 2 files, 2 rows, 0 dynamic partition.
      val verifyFuncs2: Seq[Int => Boolean] = Seq(_ == 2, _ == 0, _ == 2)
      verifyWriteDataMetrics(spark, executionId2, verifyFuncs2)
    }
  }

  protected def testMetricsDynamicPartition(
      spark: SparkSession,
      provider: String,
      dataFormat: String,
      tableName: String): Unit = {
    withTempPath { dir =>
      spark.sql(
        s"""
           |CREATE TABLE t1(a int, b int)
           |USING $provider
           |PARTITIONED BY(a)
           |LOCATION '${dir.toURI}'
         """.stripMargin)

      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
      assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

      val df = spark.range(start = 0, end = 4, step = 1, numPartitions = 1)
        .selectExpr("id a", "id b")
      sql("SET hive.exec.dynamic.partition.mode=nonstrict")

      val executionId = getLatestExecutionId(spark) { () =>
        df.union(df).repartition(2, $"a")
          .write
          .format(dataFormat)
          .option("maxRecordsPerFile", 2)
          .mode("overwrite")
          .insertInto(tableName)
      }
      assert(Utils.recursiveList(dir).count(_.getName.startsWith("part-")) == 4)
      // written 4 files, 8 rows, 4 dynamic partitions.
      val verifyFuncs: Seq[Int => Boolean] = Seq(_ == 4, _ == 4, _ == 8)
      verifyWriteDataMetrics(spark, executionId, verifyFuncs)
    }
  }
}

class WritingDataMetricsSuite extends SharedSQLContext with BaseWritingDataMetricsSuite {
  test("writing data out metrics") {
    testMetricsNonDynamicPartition(spark, "parquet", "t1")
  }

  test("writing data out metrics: dynamic partition") {
    testMetricsDynamicPartition(spark, "parquet", "parquet", "t1")
  }
}
