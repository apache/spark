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

package org.apache.spark.sql.streaming

import java.io.File

import scala.annotation.tailrec

import org.apache.commons.io.FileUtils

import org.apache.spark.{SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Complete
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{StateStoreKeyRowFormatValidationFailure, StateStoreValueRowFormatValidationFailure}
import org.apache.spark.sql.functions._
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

/**
 * An integrated test for streaming state store format compatibility.
 * For each PR breaks this test, we need to pay attention to the underlying unsafe row format
 * changing. All the checkpoint dirs were generated based on Spark version 2.4.5. If we accept the
 * changes, it means the checkpoint for Structured Streaming will become non-reusable. Please add
 * a new test for the issue, just like the test suite "SPARK-28067 changed the sum decimal unsafe
 * row format".
 */
@SlowSQLTest
class StreamingStateStoreFormatCompatibilitySuite extends StreamTest {
  import testImplicits._

  private def prepareCheckpointDir(testName: String): File = {
    val resourceUri = this.getClass.getResource("/structured-streaming/" +
      s"checkpoint-version-2.4.5-for-compatibility-test-${testName}").toURI
    val checkpointDir = Utils.createTempDir().getCanonicalFile
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)
    checkpointDir
  }

  test("common functions") {
    val inputData = MemoryStream[Int]
    val aggregated =
      inputData.toDF().toDF("value")
      .selectExpr(
        "value",
        "value % 5 AS id",
        "CAST(value AS STRING) as str",
        "CAST(value AS FLOAT) as f",
        "CAST(value AS DOUBLE) as d",
        "CAST(value AS DECIMAL) as dec",
        "value % 3 AS mod",
        "named_struct('key', CAST(value AS STRING), 'value', value) AS s")
      .groupBy($"id")
      .agg(
        avg($"value").as("avg_v"),
        avg($"f").as("avg_f"),
        avg($"d").as("avg_d"),
        avg($"dec").as("avg_dec"),
        count($"value").as("cnt"),
        first($"value").as("first_v"),
        first($"s").as("first_s"),
        last($"value").as("last_v"),
        last($"s").as("last_s"),
        min(struct("value", "str")).as("min_struct"),
        max($"value").as("max_v"),
        sum($"value").as("sum_v"),
        sum($"f").as("sum_f"),
        sum($"d").as("sum_d"),
        // The test for sum decimal broke by SPARK-28067, use separated test for it
        // sum($"dec").as("sum_dec"),
        collect_list($"value").as("col_list"),
        collect_set($"mod").as("col_set"))
      .select("id", "avg_v", "avg_f", "avg_d", "avg_dec", "cnt", "first_v", "first_s.value",
        "last_v", "last_s.value", "min_struct.value", "max_v", "sum_v", "sum_f", "sum_d",
        "col_list", "col_set")

    val checkpointDir = prepareCheckpointDir("common-functions")
    inputData.addData(0 to 9: _*)

    testStream(aggregated, Complete)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
      /*
        Note: The checkpoint was generated using the following input in Spark version 2.4.5
        AddData(inputData, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
        CheckAnswer(
          Row(0, 2.5, 2.5F, 2.5, 2.5000, 2, 0, 0, 5, 5, 0, 5, 5, 5.0, 5.0, Seq(0, 5),
            Seq(0, 2)),
          Row(1, 3.5, 3.5F, 3.5, 3.5000, 2, 1, 1, 6, 6, 1, 6, 7, 7.0, 7.0, Seq(1, 6),
            Seq(0, 1)),
          Row(2, 4.5, 4.5F, 4.5, 4.5000, 2, 2, 2, 7, 7, 2, 7, 9, 9.0, 9.0, Seq(2, 7),
            Seq(1, 2)),
          Row(3, 5.5, 5.5F, 5.5, 5.5000, 2, 3, 3, 8, 8, 3, 8, 11, 11.0, 11.0, Seq(3, 8),
            Seq(0, 2)),
          Row(4, 6.5, 6.5F, 6.5, 6.5000, 2, 4, 4, 9, 9, 4, 9, 13, 13.0, 13.0, Seq(4, 9),
            Seq(0, 1)))
       */
      AddData(inputData, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
      CheckAnswer(
        Row(0, 7.5, 7.5, 7.5, 7.5000, 4, 0, 0, 15, 15, 0, 15, 30, 30.0, 30.0,
          Seq(0, 5, 10, 15), Seq(0, 1, 2)),
        Row(1, 8.5, 8.5, 8.5, 8.5000, 4, 1, 1, 16, 16, 1, 16, 34, 34.0, 34.0,
          Seq(1, 6, 11, 16), Seq(0, 1, 2)),
        Row(2, 9.5, 9.5, 9.5, 9.5000, 4, 2, 2, 17, 17, 2, 17, 38, 38.0, 38.0,
          Seq(2, 7, 12, 17), Seq(0, 1, 2)),
        Row(3, 10.5, 10.5, 10.5, 10.5000, 4, 3, 3, 18, 18, 3, 18, 42, 42.0, 42.0,
          Seq(3, 8, 13, 18), Seq(0, 1, 2)),
        Row(4, 11.5, 11.5, 11.5, 11.5000, 4, 4, 4, 19, 19, 4, 19, 46, 46.0, 46.0,
          Seq(4, 9, 14, 19), Seq(0, 1, 2)))
    )
  }

  test("statistical functions") {
    val inputData = MemoryStream[Long]
    val aggregated =
      inputData.toDF().toDF("value")
        .selectExpr(
          "value",
          "value % 5 AS id",
          "CAST(value AS STRING) as str",
          "CAST(value AS FLOAT) as f",
          "CAST(value AS DOUBLE) as d",
          "CAST(value AS DECIMAL) as dec",
          "value % 3 AS mod")
        .groupBy($"id")
        .agg(
          kurtosis($"d").as("kts"),
          skewness($"d").as("skew"),
          approx_count_distinct($"mod").as("approx_cnt"),
          approx_count_distinct($"f").as("approx_cnt_f"),
          approx_count_distinct($"d").as("approx_cnt_d"),
          approx_count_distinct($"dec").as("approx_cnt_dec"),
          approx_count_distinct($"str").as("approx_cnt_str"),
          stddev_pop($"d").as("stddev_pop"),
          stddev_samp($"d").as("stddev_samp"),
          var_pop($"d").as("var_pop"),
          var_samp($"d").as("var_samp"),
          covar_pop($"value", $"mod").as("covar_pop"),
          covar_samp($"value", $"mod").as("covar_samp"),
          corr($"value", $"mod").as("corr"))
        .select("id", "kts", "skew", "approx_cnt", "approx_cnt_f", "approx_cnt_d",
          "approx_cnt_dec", "approx_cnt_str", "stddev_pop", "stddev_samp", "var_pop", "var_samp",
          "covar_pop", "covar_samp", "corr")

    val checkpointDir = prepareCheckpointDir("statistical-functions")
    inputData.addData(0L to 9L: _*)

    testStream(aggregated, Complete)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
      /*
        Note: The checkpoint was generated using the following input in Spark version 2.4.5
        AddData(inputData, 0L to 9L: _*),
        CheckAnswer(
          Row(0, -2.0, 0.0, 2, 2, 2, 2, 2, 2.5, 3.5355339059327378, 6.25, 12.5, 2.5, 5.0, 1.0),
          Row(1, -2.0, 0.0, 2, 2, 2, 2, 2, 2.5, 3.5355339059327378, 6.25, 12.5, -1.25, -2.5, -1.0),
          Row(2, -2.0, 0.0, 2, 2, 2, 2, 2, 2.5, 3.5355339059327378, 6.25, 12.5, -1.25, -2.5, -1.0),
          Row(3, -2.0, 0.0, 2, 2, 2, 2, 2, 2.5, 3.5355339059327378, 6.25, 12.5, 2.5, 5.0, 1.0),
          Row(4, -2.0, 0.0, 2, 2, 2, 2, 2, 2.5, 3.5355339059327378, 6.25, 12.5, -1.25, -2.5, -1.0))
       */

      AddData(inputData, 10L to 19L: _*),
      CheckAnswer(
        Row(0, -1.36, 0.0, 3, 4, 4, 4, 4, 5.5901699437494745, 6.454972243679028, 31.25,
          41.666666666666664, -0.625, -0.8333333333333334, -0.13483997249264842),
        Row(1, -1.36, 0.0, 3, 4, 4, 4, 4, 5.5901699437494745, 6.454972243679028, 31.25,
          41.666666666666664, 1.25, 1.6666666666666667, 0.31622776601683794),
        Row(2, -1.36, 0.0, 3, 4, 4, 4, 4, 5.5901699437494745, 6.454972243679028, 31.25,
          41.666666666666664, -0.625, -0.8333333333333334, -0.13483997249264842),
        Row(3, -1.36, 0.0, 3, 4, 4, 4, 4, 5.5901699437494745, 6.454972243679028, 31.25,
          41.666666666666664, -0.625, -0.8333333333333334, -0.13483997249264842),
        Row(4, -1.36, 0.0, 3, 4, 4, 4, 4, 5.5901699437494745, 6.454972243679028, 31.25,
          41.666666666666664, 1.25, 1.6666666666666667, 0.31622776601683794))
    )
  }

  test("deduplicate with all columns") {
    val inputData = MemoryStream[Long]
    val result = inputData.toDF().toDF("value")
      .selectExpr(
        "value",
        "value + 10 AS key",
        "CAST(value AS STRING) as topic",
        "value + 100 AS partition",
        "value + 5 AS offset")
      .dropDuplicates()
      .select("key", "value", "topic", "partition", "offset")

    val checkpointDir = prepareCheckpointDir("deduplicate")
    inputData.addData(0L, 1L, 2L, 3L, 4L)

    testStream(result)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
      /*
        Note: The checkpoint was generated using the following input in Spark version 2.4.5
        AddData(inputData, 0L, 1L, 2L, 3L, 4L),
        CheckAnswer(
          Row(10, 0, "0", 100, 5),
          Row(11, 1, "1", 101, 6),
          Row(12, 2, "2", 102, 7),
          Row(13, 3, "3", 103, 8),
          Row(14, 4, "4", 104, 9))
       */
      AddData(inputData, 3L, 4L, 5L, 6L),
      CheckLastBatch(
        Row(15, 5, "5", 105, 10),
        Row(16, 6, "6", 106, 11))
    )
  }

  test("SPARK-28067 changed the sum decimal unsafe row format") {
    val inputData = MemoryStream[Int]
    val aggregated =
      inputData.toDF().toDF("value")
        .selectExpr(
          "value",
          "value % 2 AS id",
          "CAST(value AS DECIMAL) as dec")
        .groupBy($"id")
        .agg(sum($"dec").as("sum_dec"), collect_list($"value").as("col_list"))
        .select("id", "sum_dec", "col_list")

    val checkpointDir = prepareCheckpointDir("sum-decimal")
    inputData.addData(0 to 9: _*)

    testStream(aggregated, Complete)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
      /*
        Note: The checkpoint was generated using the following input in Spark version 2.4.5
        AddData(inputData, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
        CheckAnswer(Row(0, 20, Seq(0, 2, 4, 6, 8)), Row(1, 25, Seq(1, 3, 5, 7, 9)))
       */
      AddData(inputData, 10 to 19: _*),
      ExpectFailure[SparkException] { e =>
        assert(findStateSchemaException(e))
      }
    )
  }

  @tailrec
  private def findStateSchemaException(exc: Throwable): Boolean = {
    exc match {
      case _: SparkUnsupportedOperationException => true
      case _: StateStoreKeyRowFormatValidationFailure => true
      case _: StateStoreValueRowFormatValidationFailure => true
      case e: SparkException if e.getCondition.contains("ROW_FORMAT_VALIDATION_FAILURE") => true
      case e1 if e1.getCause != null => findStateSchemaException(e1.getCause)
      case _ => false
    }
  }
}
