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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper, QueryTest, Row}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class FileSourceCountPushDownSuite
    extends QueryTest
    with SharedSparkSession
    with ExplainSuiteHelper {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "")

  Seq(
    (
      "parquet",
      SQLConf.PARQUET_AGGREGATE_PUSHDOWN_ENABLED.key,
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key),
    (
      "orc",
      SQLConf.ORC_AGGREGATE_PUSHDOWN_ENABLED.key,
      SQLConf.ORC_VECTORIZED_READER_ENABLED.key)).foreach {
    case (format, aggregatePushDownKey, vectorizedReaderKey) =>
      test(s"SPARK-60487: $format count on no matching partitions returns zero") {
        withTempPath { dir =>
          spark
            .range(6)
            .selectExpr(
              "id % 2 AS p",
              "CASE WHEN id = 0 THEN CAST(NULL AS BIGINT) ELSE id END AS value")
            .coalesce(1)
            .write
            .partitionBy("p")
            .format(format)
            .save(dir.getCanonicalPath)
          withTempView("tmp") {
            spark.read.format(format).load(dir.getCanonicalPath).createOrReplaceTempView("tmp")
            for (vectorized <- Seq("false", "true"); adaptive <- Seq("false", "true")) {
              withSQLConf(
                aggregatePushDownKey -> "true",
                vectorizedReaderKey -> vectorized,
                SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive) {
                val aggregates = "COUNT(*), COUNT(value), MIN(value), MAX(value)"
                checkAggregation(
                  sql(s"SELECT $aggregates FROM tmp WHERE p = 0"),
                  Seq(Row(3L, 2L, 2L, 4L)))
                checkAggregation(
                  sql(s"SELECT $aggregates FROM tmp WHERE p > 1"),
                  Seq(Row(0L, 0L, null, null)))
                checkAggregation(
                  sql(s"SELECT p, $aggregates FROM tmp WHERE p > 1 GROUP BY p"),
                  Seq.empty[Row])
              }
            }
          }
        }
      }
  }

  private def checkAggregation(df: DataFrame, expected: Seq[Row]): Unit = {
    assert(df.queryExecution.optimizedPlan.exists(_.isInstanceOf[DataSourceV2ScanRelation]))
    checkKeywordsExistsInExplain(
      df,
      "PushedAggregation: [COUNT(*), COUNT(value), MIN(value), MAX(value)]")
    // Check null bits before RowEncoder can interpret a non-nullable COUNT field as a long.
    val rawRows = df.queryExecution.executedPlan.executeCollect()
    assert(rawRows.length == expected.length)
    rawRows.zip(expected).foreach { case (actual, expectedRow) =>
      expectedRow.toSeq.zipWithIndex.foreach { case (value, index) =>
        assert(
          actual.isNullAt(index) == (value == null),
          s"Unexpected nullability at output column $index")
      }
    }
    checkAnswer(df, expected)
  }
}
