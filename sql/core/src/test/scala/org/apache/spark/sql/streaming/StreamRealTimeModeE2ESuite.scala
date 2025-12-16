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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.execution.datasources.v2.LowLatencyClock
import org.apache.spark.sql.execution.streaming.LowLatencyMemoryStream
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.util.GlobalSingletonManualClock
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class StreamRealTimeModeE2ESuite extends StreamRealTimeModeE2ESuiteBase {

  import testImplicits._

  override protected def createSparkSession =
    new TestSparkSession(
      new SparkContext(
        "local[15]",
        "streaming-rtm-e2e-context",
        sparkConf.set("spark.sql.shuffle.partitions", "5")
      )
    )

  private def runForeachTest(withUnion: Boolean): Unit = {
    var query: StreamingQuery = null
    try {
      withTempDir { checkpointDir =>
        val clock = new GlobalSingletonManualClock()
        LowLatencyClock.setClock(clock)
        val uniqueSinkName = if (withUnion) {
          sinkName + "-union"
        } else {
          sinkName
        }

        val read = LowLatencyMemoryStream[(String, Int)](5)
        val read1 = LowLatencyMemoryStream[(String, Int)](5)
        val dataframe = if (withUnion) {
          read.toDF().union(read1.toDF())
        } else {
          read.toDF()
        }

        query = dataframe
          .select(col("_1").as("key"), col("_2").as("value"))
          .select(
            concat(
              col("key").cast("STRING"),
              lit("-"),
              col("value").cast("STRING")
            ).as("output")
          )
          .writeStream
          .outputMode(OutputMode.Update())
          .foreach(new ForeachWriter[Row] {
            private var batchPartitionId: String = null
            private val processedThisBatch = new ConcurrentLinkedQueue[String]()
            override def open(partitionId: Long, epochId: Long): Boolean = {
              ResultsCollector
                .computeIfAbsent(uniqueSinkName, (_) => new ConcurrentLinkedQueue[String]())
              batchPartitionId = s"$uniqueSinkName-$epochId-$partitionId"
              assert(
                !ResultsCollector.containsKey(batchPartitionId),
                s"should NOT contain batchPartitionId ${batchPartitionId}"
              )
              ResultsCollector
                .put(batchPartitionId, new ConcurrentLinkedQueue[String]())
              true
            }

            override def process(value: Row): Unit = {
              val v = value.getAs[String]("output")
              ResultsCollector.get(uniqueSinkName).add(v)
              processedThisBatch.add(v)
            }

            override def close(errorOrNull: Throwable): Unit = {

              assert(
                ResultsCollector.containsKey(batchPartitionId),
                s"should contain batchPartitionId ${batchPartitionId}"
              )
              ResultsCollector.get(batchPartitionId).addAll(processedThisBatch)
              processedThisBatch.clear()
            }
          })
          .option("checkpointLocation", checkpointDir.getName)
          .queryName("foreach")
          // doesn't matter the batch duration set here since we are going
          // to manually control batch durations via manual clock
          .trigger(defaultTrigger)
          .start()

        val expectedResults = mutable.ListBuffer[String]()
        val expectedResultsByBatch = mutable.HashMap[Int, mutable.ListBuffer[String]]()

        val numRows = 10
        for (i <- 0 until 3) {
          expectedResultsByBatch(i) = new mutable.ListBuffer[String]()
          for (key <- List("a", "b", "c")) {
            for (j <- 1 to numRows) {
              read.addData((key, 1))
              val data = s"$key-1"
              expectedResults += data
              expectedResultsByBatch(i) += data
            }
          }

          if (withUnion) {
            for (key <- List("d", "e", "f")) {
              for (j <- 1 to numRows) {
                read1.addData((key, 2))
                val data = s"$key-2"
                expectedResults += data
                expectedResultsByBatch(i) += data
              }
            }
          }

          eventually(timeout(60.seconds)) {
            ResultsCollector
              .get(uniqueSinkName)
              .toArray(new Array[String](ResultsCollector.get(uniqueSinkName).size()))
              .toList
              .sorted should equal(expectedResults.sorted)
          }

          clock.advance(defaultTrigger.batchDurationMs)
          eventually(timeout(60.seconds)) {
            query
              .asInstanceOf[StreamingQueryWrapper]
              .streamingQuery
              .getLatestExecutionContext()
              .batchId should be(i + 1)
            query.lastProgress.sources(0).numInputRows should be(numRows * 3)

            val commitedResults = new mutable.ListBuffer[String]()
            val numPartitions = if (withUnion) 10 else 5
            for (v <- 0 until numPartitions) {
              val it = ResultsCollector.get(s"$uniqueSinkName-${i}-$v").iterator()
              while (it.hasNext) {
                commitedResults += it.next()
              }
            }

            commitedResults.sorted should equal(expectedResultsByBatch(i).sorted)
          }
        }
      }
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }

  private def runMapPartitionsTest(withUnion: Boolean): Unit = {
    var query: StreamingQuery = null
    try {
      withTempDir { checkpointDir =>
        val clock = new GlobalSingletonManualClock()
        LowLatencyClock.setClock(clock)
        val uniqueSinkName = if (withUnion) {
          sinkName + "mapPartitions-union"
        } else {
          sinkName + "mapPartitions"
        }

        val read = LowLatencyMemoryStream[(String, Int)](5)
        val read1 = LowLatencyMemoryStream[(String, Int)](5)
        val dataframe = if (withUnion) {
          read.toDF().union(read1.toDF())
        } else {
          read.toDF()
        }

        val df = dataframe
          .select(col("_1").as("key"), col("_2").as("value"))
          .select(
            concat(
              col("key").cast("STRING"),
              lit("-"),
              col("value").cast("STRING")
            ).as("output")
          )
          .as[String]
          .mapPartitions(rows => {
            rows.map(row => {
              val collector = ResultsCollector
                .computeIfAbsent(uniqueSinkName, (_) => new ConcurrentLinkedQueue[String]())
              collector.add(row)
              row
            })
          })
          .toDF()

        query = runStreamingQuery(sinkName, df)

        val expectedResults = mutable.ListBuffer[String]()
        val expectedResultsByBatch = mutable.HashMap[Int, mutable.ListBuffer[String]]()

        val numRows = 10
        for (i <- 0 until 3) {
          expectedResultsByBatch(i) = new mutable.ListBuffer[String]()
          for (key <- List("a", "b", "c")) {
            for (j <- 1 to numRows) {
              read.addData((key, 1))
              val data = s"$key-1"
              expectedResults += data
              expectedResultsByBatch(i) += data
            }
          }

          if (withUnion) {
            for (key <- List("d", "e", "f")) {
              for (j <- 1 to numRows) {
                read1.addData((key, 2))
                val data = s"$key-2"
                expectedResults += data
                expectedResultsByBatch(i) += data
              }
            }
          }

          // results collected from mapPartitions
          eventually(timeout(60.seconds)) {
            ResultsCollector
              .get(uniqueSinkName)
              .toArray(new Array[String](ResultsCollector.get(uniqueSinkName).size()))
              .toList
              .sorted should equal(expectedResults.sorted)
          }

          // results collected from foreach sink
          eventually(timeout(60.seconds)) {
            ResultsCollector
              .get(sinkName)
              .toArray(new Array[String](ResultsCollector.get(sinkName).size()))
              .toList
              .sorted should equal(expectedResults.sorted)
          }

          clock.advance(defaultTrigger.batchDurationMs)
          eventually(timeout(60.seconds)) {
            query
              .asInstanceOf[StreamingQueryWrapper]
              .streamingQuery
              .getLatestExecutionContext()
              .batchId should be(i + 1)
            query.lastProgress.sources(0).numInputRows should be(numRows * 3)
          }
        }
      }
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }

  test("foreach") {
    runForeachTest(withUnion = false)
  }

  test("union - foreach") {
    runForeachTest(withUnion = true)
  }

  test("mapPartitions") {
    runMapPartitionsTest(withUnion = false)
  }

  test("union - mapPartitions") {
    runMapPartitionsTest(withUnion = true)
  }

  test("scala stateless UDF") {
    val myUDF = (id: Int) => id + 1
    val udf = spark.udf.register("myUDF", myUDF)
    val (read, clock) = createMemoryStream()

    val df = read
      .toDF()
      .select(col("_1").as("key"), udf(col("_2")).as("value_plus_1"))
      .select(concat(col("key"), lit("-"), col("value_plus_1").cast("STRING")).as("output"))

    var query: StreamingQuery = null
    try {
      query = runStreamingQuery("scala_udf", df)
      processBatches(query, read, clock, 10, 3, (key, value) => Array(s"$key-${value + 1}"))
    } finally {
      if (query != null) query.stop()
    }
  }

  test("stream static join") {
    val (read, clock) = createMemoryStream()
    val staticDf = spark
      .range(1, 31, 1, 10)
      .selectExpr("id AS join_key", "id AS join_value")
      // This will produce HashAggregateExec which should not be blocked by allowList
      // since it's the batch subquery
      .groupBy("join_key")
      .agg(max($"join_value").as("join_value"))

    val df = read
      .toDF()
      .select(col("_1").as("key"), col("_2").as("value"))
      .join(staticDf, col("value") === col("join_key"))
      .select(concat(col("key"), lit("-"), col("value"), lit("-"), col("join_value")).as("output"))

    var query: StreamingQuery = null
    try {
      query = runStreamingQuery("stream_static_join", df)
      processBatches(query, read, clock, 10, 3, (key, value) => Array(s"$key-$value-$value"))
    } finally {
      if (query != null) query.stop()
    }
  }

  test("to_json and from_json round-trip") {
    val (read, clock) = createMemoryStream()
    val schema = new StructType().add("key", StringType).add("value", IntegerType)

    val df = read
      .toDF()
      .select(struct(col("_1").as("key"), col("_2").as("value")).as("json"))
      .select(from_json(to_json(col("json")), schema).as("json"))
      .select(concat(col("json.key"), lit("-"), col("json.value")))

    var query: StreamingQuery = null
    try {
      query = runStreamingQuery("json_roundtrip", df)
      processBatches(query, read, clock, 10, 3, (key, value) => Array(s"$key-$value"))
    } finally {
      if (query != null) query.stop()
    }
  }

  test("generateExec passthrough") {
    val (read, clock) = createMemoryStream()

    val df = read
      .toDF()
      .select(col("_1").as("key"), col("_2").as("value"))
      .withColumn("value_array", array(col("value"), -col("value")))
    df.createOrReplaceTempView("tempView")
    val explodeDF =
      spark
        .sql("select key, explode(value_array) as exploded_value from tempView")
        .select(concat(col("key"), lit("-"), col("exploded_value").cast("STRING")).as("output"))

    var query: StreamingQuery = null
    try {
      query = runStreamingQuery("generateExec_passthrough", explodeDF)
      processBatches(
        query,
        read,
        clock,
        10,
        3,
        (key, value) => Array(s"$key-$value", s"$key--$value")
      )
    } finally {
      if (query != null) query.stop()
    }
  }
}
