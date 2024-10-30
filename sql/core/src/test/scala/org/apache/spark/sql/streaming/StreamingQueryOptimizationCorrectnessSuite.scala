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
import org.apache.spark.sql.functions.{count, expr, lit, timestamp_seconds, window}
import org.apache.spark.sql.internal.SQLConf

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

  test("SPARK-48267: regression test, stream-stream union followed by stream-batch join") {
    withTempDir { dir =>
      val input1 = MemoryStream[Int]
      val input2 = MemoryStream[Int]

      val df1 = input1.toDF().withColumn("code", lit(1))
      val df2 = input2.toDF().withColumn("code", lit(null))

      // NOTE: The column 'ref_code' is known to be non-nullable.
      val batchDf = spark.range(1, 5).select($"id".as("ref_code"))

      val unionDf = df1.union(df2)
        .join(batchDf, expr("code = ref_code"))
        .select("value")

      testStream(unionDf)(
        StartStream(checkpointLocation = dir.getAbsolutePath),

        AddData(input1, 1, 2, 3),
        CheckNewAnswer(1, 2, 3),

        AddData(input2, 1, 2, 3),
        // The test failed before SPARK-47305 - the test failed with below error message:
        // org.apache.spark.sql.streaming.StreamingQueryException: Stream-stream join without
        // equality predicate is not supported.;
        // Join Inner
        // :- StreamingDataSourceV2ScanRelation[value#3] MemoryStreamDataSource
        // +- LocalRelation <empty>
        // Note that LocalRelation <empty> is actually a batch source (Range) but due to
        // a bug, it was incorrect marked to the streaming. SPARK-47305 fixed the bug.
        CheckNewAnswer()
      )
    }
  }

  test("SPARK-48481: DISTINCT with empty stream source should retain AGGREGATE") {
    def doTest(numExpectedStatefulOperatorsForOneEmptySource: Int): Unit = {
      withTempView("tv1", "tv2") {
        val inputStream1 = MemoryStream[Int]
        val ds1 = inputStream1.toDS()
        ds1.registerTempTable("tv1")

        val inputStream2 = MemoryStream[Int]
        val ds2 = inputStream2.toDS()
        ds2.registerTempTable("tv2")

        // DISTINCT is rewritten to AGGREGATE, hence an AGGREGATEs for each source
        val unioned = spark.sql(
          """
            | WITH u AS (
            |   SELECT DISTINCT value AS value FROM tv1
            | ), v AS (
            |   SELECT DISTINCT value AS value FROM tv2
            | )
            | SELECT value FROM u UNION ALL SELECT value FROM v
            |""".stripMargin
        )

        testStream(unioned, OutputMode.Update())(
          MultiAddData(inputStream1, 1, 1, 2)(inputStream2, 1, 1, 2),
          CheckNewAnswer(1, 2, 1, 2),
          Execute { qe =>
            val stateOperators = qe.lastProgress.stateOperators
            // Aggregate should be "stateful" one
            assert(stateOperators.length === 2)
            stateOperators.zipWithIndex.foreach { case (op, id) =>
              assert(op.numRowsUpdated === 2, s"stateful OP ID: $id")
            }
          },
          AddData(inputStream2, 2, 2, 3),
          // NOTE: this is probably far from expectation to have 2 as output given user intends
          // deduplicate, but the behavior is still correct with rewritten node and output mode:
          // Aggregate & Update mode.
          // TODO: Probably we should disallow DISTINCT or rewrite to
          //  dropDuplicates(WithinWatermark) for streaming source?
          CheckNewAnswer(2, 3),
          Execute { qe =>
            val stateOperators = qe.lastProgress.stateOperators
            // Aggregate should be "stateful" one
            assert(stateOperators.length === numExpectedStatefulOperatorsForOneEmptySource)
            val opWithUpdatedRows = stateOperators.zipWithIndex.filterNot(_._1.numRowsUpdated == 0)
            assert(opWithUpdatedRows.length === 1)
            // If this were dropDuplicates, numRowsUpdated should have been 1.
            assert(opWithUpdatedRows.head._1.numRowsUpdated === 2,
              s"stateful OP ID: ${opWithUpdatedRows.head._2}")
          },
          AddData(inputStream1, 4, 4, 5),
          CheckNewAnswer(4, 5),
          Execute { qe =>
            val stateOperators = qe.lastProgress.stateOperators
            assert(stateOperators.length === numExpectedStatefulOperatorsForOneEmptySource)
            val opWithUpdatedRows = stateOperators.zipWithIndex.filterNot(_._1.numRowsUpdated == 0)
            assert(opWithUpdatedRows.length === 1)
            assert(opWithUpdatedRows.head._1.numRowsUpdated === 2,
              s"stateful OP ID: ${opWithUpdatedRows.head._2}")
          }
        )
      }
    }

    doTest(numExpectedStatefulOperatorsForOneEmptySource = 2)

    withSQLConf(SQLConf.STREAMING_OPTIMIZE_ONE_ROW_PLAN_ENABLED.key -> "true") {
      doTest(numExpectedStatefulOperatorsForOneEmptySource = 1)
    }
  }

  test("SPARK-49699: observe node is not pruned out from PruneFilters") {
    val input1 = MemoryStream[Int]
    val df = input1.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .observe("observation", count(lit(1)).as("rows"))
      // Enforce PruneFilters to come into play and prune subtree. We could do the same
      // with the reproducer of SPARK-48267, but let's just be simpler.
      .filter(expr("false"))

    testStream(df)(
      AddData(input1, 1, 2, 3),
      CheckNewAnswer(),
      Execute { qe =>
        val observeRow = qe.lastExecution.observedMetrics.get("observation")
        assert(observeRow.get.getAs[Long]("rows") == 3L)
      }
    )
  }

  test("SPARK-49699: watermark node is not pruned out from PruneFilters") {
    // NOTE: The test actually passes without SPARK-49699, because of the trickiness of
    // filter pushdown and PruneFilters. Unlike observe node, the `false` filter is pushed down
    // below to watermark node, hence PruneFilters rule does not prune out watermark node even
    // before SPARK-49699. Propagate empty relation does not also propagate emptiness into
    // watermark node, so the node is retained. The test is added for preventing regression.

    val input1 = MemoryStream[Int]
    val df = input1.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "0 second")
      // Enforce PruneFilter to come into play and prune subtree. We could do the same
      // with the reproducer of SPARK-48267, but let's just be simpler.
      .filter(expr("false"))

    testStream(df)(
      AddData(input1, 1, 2, 3),
      CheckNewAnswer(),
      Execute { qe =>
        // If the watermark node is pruned out, this would be null.
        assert(qe.lastProgress.eventTime.get("watermark") != null)
      }
    )
  }

  test("SPARK-49699: stateful operator node is not pruned out from PruneFilters") {
    val input1 = MemoryStream[Int]
    val df = input1.toDF()
      .groupBy("value")
      .count()
      // Enforce PruneFilter to come into play and prune subtree. We could do the same
      // with the reproducer of SPARK-48267, but let's just be simpler.
      .filter(expr("false"))

    testStream(df, OutputMode.Complete())(
      AddData(input1, 1, 2, 3),
      CheckNewAnswer(),
      Execute { qe =>
        assert(qe.lastProgress.stateOperators.length == 1)
      }
    )
  }
}
