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

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Complete
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.util.Utils

class StreamingAggregationCompatibilitySuite extends StreamTest {
  import testImplicits._

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
        sum($"dec").as("sum_dec"),
        collect_list($"value").as("col_list"),
        collect_set($"mod").as("col_set"))
      .select("id", "avg_v", "avg_f", "avg_d", "avg_dec", "cnt", "first_v", "first_s.value",
        "last_v", "last_s.value", "min_struct.value", "max_v", "sum_v", "sum_f", "sum_d",
        "sum_dec", "col_list", "col_set")

    val resourceUri = this.getClass.getResource("/structured-streaming/" +
      "checkpoint-version-2.4.5-for-compatibility-test-common-functions").toURI
    val checkpointDir = Utils.createTempDir().getCanonicalFile
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

    inputData.addData(0 to 9: _*)

    testStream(aggregated, Complete)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
//       AddData(inputData, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
//       CheckAnswer(
//         Row(0, 2.5, 2.5F, 2.5, 2.5000, 2, 0, 0, 5, 5, 0, 5, 5, 5.0, 5.0, 5, Seq(0, 5),
//           Seq(0, 2)),
//         Row(1, 3.5, 3.5F, 3.5, 3.5000, 2, 1, 1, 6, 6, 1, 6, 7, 7.0, 7.0, 7, Seq(1, 6),
//           Seq(0, 1)),
//         Row(2, 4.5, 4.5F, 4.5, 4.5000, 2, 2, 2, 7, 7, 2, 7, 9, 9.0, 9.0, 9, Seq(2, 7),
//           Seq(1, 2)),
//         Row(3, 5.5, 5.5F, 5.5, 5.5000, 2, 3, 3, 8, 8, 3, 8, 11, 11.0, 11.0, 11, Seq(3, 8),
//           Seq(0, 2)),
//         Row(4, 6.5, 6.5F, 6.5, 6.5000, 2, 4, 4, 9, 9, 4, 9, 13, 13.0, 13.0, 13, Seq(4, 9),
//           Seq(0, 1))),
      AddData(inputData, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
      CheckAnswer(
        Row(0, 7.5, 7.5, 7.5, 7.5000, 4, 0, 0, 15, 15, 0, 15, 30, 30.0, 30.0, 30,
          Seq(0, 5, 10, 15), Seq(0, 1, 2)),
        Row(1, 8.5, 8.5, 8.5, 8.5000, 4, 1, 1, 16, 16, 1, 16, 34, 34.0, 34.0, 34,
          Seq(1, 6, 11, 16), Seq(0, 1, 2)),
        Row(2, 9.5, 9.5, 9.5, 9.5000, 4, 2, 2, 17, 17, 2, 17, 38, 38.0, 38.0, 38,
          Seq(2, 7, 12, 17), Seq(0, 1, 2)),
        Row(3, 10.5, 10.5, 10.5, 10.5000, 4, 3, 3, 18, 18, 3, 18, 42, 42.0, 42.0, 42,
          Seq(3, 8, 13, 18), Seq(0, 1, 2)),
        Row(4, 11.5, 11.5, 11.5, 11.5000, 4, 4, 4, 19, 19, 4, 19, 46, 46.0, 46.0, 46,
          Seq(4, 9, 14, 19), Seq(0, 1, 2)))
    )
  }

  test("SPARK-28067 change the sum decimal unsafe row format") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF().toDF("value")
        .selectExpr(
          "value",
          "value % 2 AS id",
          "CAST(value AS DECIMAL) as dec")
        .groupBy($"id")
        .agg(sum($"dec").as("sum_dec"))
        .select("id", "sum_dec")

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    testStream(aggregated, Complete)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
       AddData(inputData, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
       CheckAnswer(Row(0, 2.5, 2.5F, 2.5, 2.5000, 2, 0, 0, 5, 5, 0, 5, 5, 5.0, 5.0, 5, Seq(0, 5)))
    )
  }
}
