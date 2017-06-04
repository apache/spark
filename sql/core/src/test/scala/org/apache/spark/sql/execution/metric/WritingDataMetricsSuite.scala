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

import java.io.File

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.util.Utils

trait BaseWritingDataMetricsSuite extends SparkFunSuite with SQLTestUtils {
  import testImplicits._

  /**
   * Get execution metrics for the SQL execution and verify metrics values.
   *
   * @param metricsValues the expected metric values (numFiles, numPartitions, numOutputRows).
   * @param func the function can produce execution id after running.
   */
  def verifyWriteDataMetrics(
      spark: SparkSession,
      metricsValues: Seq[Int])(func: => Unit): Unit = {
    val previousExecutionIds = spark.sharedState.listener.executionIdToData.keySet
    // Run the given function to trigger query execution.
    func
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    val executionIds =
      spark.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)
    assert(executionIds.size == 1)
    val executionId = executionIds.head

    val executionData = spark.sharedState.listener.getExecution(executionId).get
    val executedNode = executionData.physicalPlanGraph.nodes.head

    val metricsNames = Seq(
      "number of written files",
      "number of dynamic part",
      "number of output rows")

    val metrics = spark.sharedState.listener.getExecutionMetrics(executionId)

    metricsNames.zip(metricsValues).foreach { case (metricsName, expected) =>
      val sqlMetric = executedNode.metrics.find(_.name == metricsName)
      assert(sqlMetric.isDefined)
      val accumulatorId = sqlMetric.get.accumulatorId
      val metricValue = metrics(accumulatorId).replaceAll(",", "").toInt
      assert(metricValue == expected)
    }

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
      // 1 file, 1 row, 0 dynamic partition.
      verifyWriteDataMetrics(spark, Seq(1, 0, 1)) {
        Seq((1, 2)).toDF("i", "j")
          .write.format(dataFormat).mode("overwrite").saveAsTable(tableName)
      }
      val tableLocation =
        new File(spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).location)
      assert(Utils.recursiveList(tableLocation).count(_.getName.startsWith("part-")) == 1)

      // 2 files, 100 rows, 0 dynamic partition.
      verifyWriteDataMetrics(spark, Seq(2, 0, 100)) {
        (0 until 100).map(i => (i, i + 1)).toDF("i", "j").repartition(2)
          .write.format(dataFormat).mode("overwrite").insertInto(tableName)
      }
      assert(Utils.recursiveList(tableLocation).count(_.getName.startsWith("part-")) == 2)
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
           |CREATE TABLE $tableName(a int, b int)
           |USING $provider
           |PARTITIONED BY(a)
           |LOCATION '${dir.toURI}'
         """.stripMargin)
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

      val df = spark.range(start = 0, end = 40, step = 1, numPartitions = 1)
        .selectExpr("id a", "id b")

      // 40 files, 80 rows, 40 dynamic partitions.
      verifyWriteDataMetrics(spark, Seq(40, 40, 80)) {
        df.union(df).repartition(2, $"a")
          .write
          .format(dataFormat)
          .mode("overwrite")
          .insertInto(tableName)
      }
      assert(Utils.recursiveList(dir).count(_.getName.startsWith("part-")) == 40)
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
