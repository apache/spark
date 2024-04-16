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

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{lit, window}

/**
 * This test ensures that any optimizations done by Spark SQL optimizer are
 * correct for Streaming queries.
 */
class StreamingQueryOptimizationCorrectnessSuite extends StreamTest {
  import testImplicits._

  test("streaming Union with literal produces correct results") {
    val inputStream1 = MemoryStream[Int]
    val ds1 = inputStream1
      .toDS()
      .withColumn("name", lit("ds1"))
      .withColumn("count", $"value")
      .select("name", "count")

    val inputStream2 = MemoryStream[Int]
    val ds2 = inputStream2
      .toDS()
      .withColumn("name", lit("ds2"))
      .withColumn("count", $"value")
      .select("name", "count")

    val result =
      ds1.union(ds2)
        .groupBy("name")
        .count()

    testStream(result, OutputMode.Complete())(
      AddData(inputStream1, 1),
      ProcessAllAvailable(),
      AddData(inputStream2, 1),
      ProcessAllAvailable(),
      CheckNewAnswer(Row("ds1", 1), Row("ds2", 1))
    )
  }

  test("streaming aggregate with literal and watermark after literal column" +
    " produces correct results on query change") {
    withTempDir { dir =>
      val inputStream1 = MemoryStream[Timestamp]
      val ds1 = inputStream1
        .toDS()
        .withColumn("name", lit("ds1"))
        .withColumn("ts", $"value")
        .withWatermark("ts", "1 minutes")
        .select("name", "ts")

      val result =
        ds1.groupBy("name").count()

      testStream(result, OutputMode.Complete())(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        AddData(inputStream1, Timestamp.valueOf("2023-01-02 00:00:00")),
        ProcessAllAvailable()
      )

      val ds2 = inputStream1
        .toDS()
        .withColumn("name", lit("ds2"))
        .withColumn("ts", $"value")
        .withWatermark("ts", "1 minutes")
        .select("name", "ts")

      val result2 =
        ds2.groupBy("name").count()

      testStream(result2, OutputMode.Complete())(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        AddData(inputStream1, Timestamp.valueOf("2023-01-03 00:00:00")),
        ProcessAllAvailable(),
        CheckNewAnswer(Row("ds1", 1), Row("ds2", 1)),
        AddData(inputStream1, Timestamp.valueOf("2023-01-04 00:00:00")),
        ProcessAllAvailable(),
        CheckNewAnswer(Row("ds1", 1), Row("ds2", 2))
      )
    }
  }

  test("streaming aggregate with literal and watermark before literal column" +
    " produces correct results on query change") {
    withTempDir { dir =>
      val inputStream1 = MemoryStream[Timestamp]
      val ds1 = inputStream1
        .toDS()
        .withColumn("ts", $"value")
        .withWatermark("ts", "1 minutes")
        .withColumn("name", lit("ds1"))
        .select("name", "ts")

      val result =
        ds1.groupBy("name").count()

      testStream(result, OutputMode.Complete())(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        AddData(inputStream1, Timestamp.valueOf("2023-01-02 00:00:00")),
        ProcessAllAvailable()
      )

      val ds2 = inputStream1
        .toDS()
        .withColumn("ts", $"value")
        .withWatermark("ts", "1 minutes")
        .withColumn("name", lit("ds2"))
        .select("name", "ts")

      val result2 =
        ds2.groupBy("name").count()

      testStream(result2, OutputMode.Complete())(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        AddData(inputStream1, Timestamp.valueOf("2023-01-03 00:00:00")),
        ProcessAllAvailable(),
        CheckNewAnswer(Row("ds1", 1), Row("ds2", 1)),
        AddData(inputStream1, Timestamp.valueOf("2023-01-04 00:00:00")),
        ProcessAllAvailable(),
        CheckNewAnswer(Row("ds1", 1), Row("ds2", 2))
      )
    }
  }

  test("streaming aggregate with literal" +
    " produces correct results on query change") {
    withTempDir { dir =>
      val inputStream1 = MemoryStream[Int]
      val ds1 = inputStream1
        .toDS()
        .withColumn("name", lit("ds1"))
        .withColumn("count", $"value")
        .select("name", "count")

      val result =
        ds1.groupBy("name").count()

      testStream(result, OutputMode.Complete())(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        AddData(inputStream1, 1),
        ProcessAllAvailable()
      )

      val ds2 = inputStream1
        .toDS()
        .withColumn("name", lit("ds2"))
        .withColumn("count", $"value")
        .select("name", "count")

      val result2 =
        ds2.groupBy("name").count()

      testStream(result2, OutputMode.Complete())(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        AddData(inputStream1, 1),
        ProcessAllAvailable(),
        CheckNewAnswer(Row("ds1", 1), Row("ds2", 1))
      )
    }
  }

  test("stream stream join with literal" +
    " produces correct results") {
    withTempDir { dir =>
      import java.sql.Timestamp
      val inputStream1 = MemoryStream[Int]
      val inputStream2 = MemoryStream[Int]

      val ds1 = inputStream1
        .toDS()
        .withColumn("name", lit(Timestamp.valueOf("2023-01-01 00:00:00")))
        .withWatermark("name", "1 minutes")
        .withColumn("count1", lit(1))

      val ds2 = inputStream2
        .toDS()
        .withColumn("name", lit(Timestamp.valueOf("2023-01-02 00:00:00")))
        .withWatermark("name", "1 minutes")
        .withColumn("count2", lit(2))


      val result =
        ds1.join(ds2, "name", "full")
          .select("name", "count1", "count2")

      testStream(result, OutputMode.Append())(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        AddData(inputStream1, 1),
        ProcessAllAvailable(),
        AddData(inputStream2, 1),
        ProcessAllAvailable(),
        AddData(inputStream1, 2),
        ProcessAllAvailable(),
        AddData(inputStream2, 2),
        ProcessAllAvailable(),
        CheckNewAnswer()
      )

      // modify the query and update literal values for name
      val ds3 = inputStream1
        .toDS()
        .withColumn("name", lit(Timestamp.valueOf("2023-02-01 00:00:00")))
        .withWatermark("name", "1 minutes")
        .withColumn("count1", lit(3))

      val ds4 = inputStream2
        .toDS()
        .withColumn("name", lit(Timestamp.valueOf("2023-02-02 00:00:00")))
        .withWatermark("name", "1 minutes")
        .withColumn("count2", lit(4))

      val result2 =
        ds3.join(ds4, "name", "full")
          .select("name", "count1", "count2")

      testStream(result2, OutputMode.Append())(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        AddData(inputStream1, 1),
        ProcessAllAvailable(),
        AddData(inputStream2, 1),
        ProcessAllAvailable(),
        AddData(inputStream1, 2),
        ProcessAllAvailable(),
        AddData(inputStream2, 2),
        ProcessAllAvailable(),
        CheckNewAnswer(
          Row(Timestamp.valueOf("2023-01-01 00:00:00"),
            1, null.asInstanceOf[java.lang.Integer]),
          Row(Timestamp.valueOf("2023-01-01 00:00:00"),
            1, null.asInstanceOf[java.lang.Integer]),
          Row(Timestamp.valueOf("2023-01-02 00:00:00"),
            null.asInstanceOf[java.lang.Integer], 2),
          Row(Timestamp.valueOf("2023-01-02 00:00:00"),
            null.asInstanceOf[java.lang.Integer], 2)
        )
      )
    }
  }

  test("streaming SQL distinct usage with literal grouping" +
    " key produces correct results") {
    val inputStream1 = MemoryStream[Int]
    val ds1 = inputStream1
      .toDS()
      .withColumn("name", lit("ds1"))
      .withColumn("count", $"value")
      .select("name", "count")

    val inputStream2 = MemoryStream[Int]
    val ds2 = inputStream2
      .toDS()
      .withColumn("name", lit("ds2"))
      .withColumn("count", $"value")
      .select("name", "count")

    val result =
      ds1.union(ds2)
        .groupBy("name")
        .as[String, (String, Int, Int)]
        .keys

    testStream(result, OutputMode.Complete())(
      AddData(inputStream1, 1),
      ProcessAllAvailable(),
      AddData(inputStream2, 1),
      ProcessAllAvailable(),
      CheckNewAnswer(Row("ds1"), Row("ds2"))
    )
  }

  test("streaming window aggregation with literal time column" +
    " key produces correct results") {
    val inputStream1 = MemoryStream[Int]
    val ds1 = inputStream1
      .toDS()
      .withColumn("name", lit(Timestamp.valueOf("2023-01-01 00:00:00")))
      .withColumn("count", $"value")
      .select("name", "count")

    val inputStream2 = MemoryStream[Int]
    val ds2 = inputStream2
      .toDS()
      .withColumn("name", lit(Timestamp.valueOf("2023-01-02 00:00:00")))
      .withColumn("count", $"value")
      .select("name", "count")

    val result =
      ds1.union(ds2)
        .groupBy(
          window($"name", "1 second", "1 second")
        )
        .count()

    testStream(result, OutputMode.Complete())(
      AddData(inputStream1, 1),
      ProcessAllAvailable(),
      AddData(inputStream2, 1),
      ProcessAllAvailable(),
      CheckNewAnswer(
        Row(
          Row(Timestamp.valueOf("2023-01-01 00:00:00"), Timestamp.valueOf("2023-01-01 00:00:01")),
          1),
        Row(
          Row(Timestamp.valueOf("2023-01-02 00:00:00"), Timestamp.valueOf("2023-01-02 00:00:01")),
          1))
    )
  }

  test("stream stream join with literals produces correct value") {
    withTempDir { dir =>
      val input1 = MemoryStream[Int]
      val input2 = MemoryStream[Int]

      val df1 = input1
        .toDF()
        .withColumn("key", $"value")
        .withColumn("leftValue", lit(1))
        .select("key", "leftValue")

      val df2 = input2
        .toDF()
        .withColumn("key", $"value")
        .withColumn("rightValue", lit(2))
        .select("key", "rightValue")

      val result = df1
        .join(df2, "key")
        .select("key", "leftValue", "rightValue")

      testStream(result, OutputMode.Append())(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        AddData(input1, 1),
        ProcessAllAvailable(),
        AddData(input2, 1),
        ProcessAllAvailable(),
        CheckAnswer(Row(1, 1, 2))
      )
    }
  }

  test("stream stream join with literals produces correct value on query change") {
    withTempDir { dir =>
      val input1 = MemoryStream[Int]
      val input2 = MemoryStream[Int]

      val df1 = input1
        .toDF()
        .withColumn("key", lit("key1"))
        .withColumn("leftValue", lit(1))
        .select("key", "leftValue")

      val df2 = input2
        .toDF()
        .withColumn("key", lit("key2"))
        .withColumn("rightValue", lit(2))
        .select("key", "rightValue")

      val result = df1
        .join(df2, "key")
        .select("key", "leftValue", "rightValue")

      testStream(result, OutputMode.Append())(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        AddData(input1, 1),
        ProcessAllAvailable(),
        AddData(input2, 1),
        ProcessAllAvailable()
      )

      val df3 = input1
        .toDF()
        .withColumn("key", lit("key2"))
        .withColumn("leftValue", lit(3))
        .select("key", "leftValue")

      val df4 = input2
        .toDF()
        .withColumn("key", lit("key1"))
        .withColumn("rightValue", lit(4))
        .select("key", "rightValue")

      val result2 = df3
        .join(df4, "key")
        .select("key", "leftValue", "rightValue")

      testStream(result2, OutputMode.Append())(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        AddData(input1, 1),
        ProcessAllAvailable(),
        AddData(input2, 1),
        ProcessAllAvailable(),
        CheckAnswer(
          Row("key1", 1, 4),
          Row("key2", 3, 2))
      )
    }
  }
}
