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

package org.apache.spark.sql.connect.streaming

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Paths}
import java.sql.Timestamp

import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.apache.spark.sql.connect.SparkSession
import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ListState, MapState, OutputMode, StatefulProcessor, StatefulProcessorWithInitialState, TimeMode, TimerValues, TTLConfig, ValueState}
import org.apache.spark.sql.types._

case class InputRowForConnectTest(key: String, value: String)
case class OutputRowForConnectTest(key: String, value: String)
case class StateRowForConnectTest(count: Long)

// A basic stateful processor which will return the occurrences of key
class BasicCountStatefulProcessor
    extends StatefulProcessor[String, InputRowForConnectTest, OutputRowForConnectTest]
    with Logging {
  @transient protected var _countState: ValueState[StateRowForConnectTest] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[StateRowForConnectTest](
      "countState",
      Encoders.product[StateRowForConnectTest],
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InputRowForConnectTest],
      timerValues: TimerValues): Iterator[OutputRowForConnectTest] = {
    val count =
      _countState.getOption().getOrElse(StateRowForConnectTest(0L)).count + inputRows.toSeq.length
    _countState.update(StateRowForConnectTest(count))
    Iterator(OutputRowForConnectTest(key, count.toString))
  }
}

// A stateful processor with initial state which will return the occurrences of key
class TestInitialStatefulProcessor
    extends StatefulProcessorWithInitialState[
      String,
      (String, String),
      (String, String),
      (String, String, String)]
    with Logging {
  @transient protected var _countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = _countState.getOption().getOrElse(0L) + inputRows.toSeq.length
    _countState.update(count)
    Iterator((key, count.toString))
  }

  override def handleInitialState(
      key: String,
      initialState: (String, String, String),
      timerValues: TimerValues): Unit = {
    val count = _countState.getOption().getOrElse(0L) + 1
    _countState.update(count)
  }
}

case class OutputEventTimeRow(key: String, outputTimestamp: Timestamp)

// A stateful processor which will return timestamp of the first item from input rows
class ChainingOfOpsStatefulProcessor
    extends StatefulProcessor[String, (String, Timestamp), OutputEventTimeRow] {
  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {}

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Timestamp)],
      timerValues: TimerValues): Iterator[OutputEventTimeRow] = {
    val timestamp = inputRows.next()._2
    Iterator(OutputEventTimeRow(key, timestamp))
  }
}

// A basic stateful processor contains composite state variables and TTL
class TTLTestStatefulProcessor
    extends StatefulProcessor[String, (String, String), (String, String)] {
  import java.time.Duration

  @transient protected var countState: ValueState[Int] = _
  @transient protected var ttlCountState: ValueState[Int] = _
  @transient protected var ttlListState: ListState[Int] = _
  @transient protected var ttlMapState: MapState[String, Int] = _
  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState[Int]("countState", Encoders.scalaInt, TTLConfig.NONE)
    ttlCountState = getHandle
      .getValueState[Int]("ttlCountState", Encoders.scalaInt, TTLConfig(Duration.ofMillis(1000)))
    ttlListState = getHandle
      .getListState[Int]("ttlListState", Encoders.scalaInt, TTLConfig(Duration.ofMillis(1000)))
    ttlMapState = getHandle.getMapState[String, Int](
      "ttlMapState",
      Encoders.STRING,
      Encoders.scalaInt,
      TTLConfig(Duration.ofMillis(1000)))
  }
  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    var count = 0
    var ttlCount = 0
    var ttlListStateCount = 0
    var ttlMapStateCount = 0
    if (countState.exists()) {
      count = countState.get()
    }
    if (ttlCountState.exists()) {
      ttlCount = ttlCountState.get()
    }
    if (ttlListState.exists()) {
      for (value <- ttlListState.get()) {
        ttlListStateCount += value
      }
    }
    if (ttlMapState.exists()) {
      ttlMapStateCount = ttlMapState.getValue(key)
    }
    for (_ <- inputRows) {
      count += 1
      ttlCount += 1
      ttlListStateCount += 1
      ttlMapStateCount += 1
    }
    countState.update(count)
    if (key != "0") {
      ttlCountState.update(ttlCount)
      ttlListState.put(Array(ttlListStateCount, ttlListStateCount))
      ttlMapState.updateValue(key, ttlMapStateCount)
    }
    val output = List(
      (s"count-$key", count.toString),
      (s"ttlCount-$key", ttlCount.toString),
      (s"ttlListState-$key", ttlListStateCount.toString),
      (s"ttlMapState-$key", ttlMapStateCount.toString))
    output.iterator
  }
}

class TransformWithStateConnectSuite extends QueryTest with RemoteSparkSession with Logging {
  val testData: Seq[(String, String)] = Seq(("a", "1"), ("b", "1"), ("a", "2"))
  val twsAdditionalSQLConf = Seq(
    "spark.sql.streaming.stateStore.providerClass" ->
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    "spark.sql.shuffle.partitions" -> "5",
    "spark.sql.session.timeZone" -> "UTC",
    "spark.sql.streaming.noDataMicroBatches.enabled" -> "false")

  test("transformWithState - streaming with state variable, case class type") {
    withSQLConf(twsAdditionalSQLConf: _*) {
      val session: SparkSession = spark
      import session.implicits._

      spark.sql("DROP TABLE IF EXISTS my_sink")

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        testData
          .toDS()
          .toDF("key", "value")
          .repartition(3)
          .write
          .parquet(path)

        val testSchema =
          StructType(Array(StructField("key", StringType), StructField("value", StringType)))

        val q = spark.readStream
          .schema(testSchema)
          .option("maxFilesPerTrigger", 1)
          .parquet(path)
          .as[InputRowForConnectTest]
          .groupByKey(x => x.key)
          .transformWithState[OutputRowForConnectTest](
            new BasicCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())
          .writeStream
          .format("memory")
          .queryName("my_sink")
          .start()

        try {
          q.processAllAvailable()
          eventually(timeout(30.seconds)) {
            checkDataset(
              spark.table("my_sink").toDF().as[(String, String)].orderBy("key"),
              ("a", "1"),
              ("a", "2"),
              ("b", "1"))
          }
        } finally {
          q.stop()
          spark.sql("DROP TABLE IF EXISTS my_sink")
        }
      }
    }
  }

  test("transformWithState - streaming with initial state") {
    withSQLConf(twsAdditionalSQLConf: _*) {
      val session: SparkSession = spark
      import session.implicits._

      spark.sql("DROP TABLE IF EXISTS my_sink")

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        testData
          .toDS()
          .toDF("key", "value")
          .repartition(3)
          .write
          .parquet(path)

        val testSchema =
          StructType(Array(StructField("key", StringType), StructField("value", StringType)))

        val initDf = Seq(("init_1", "40.0", "a"), ("init_2", "100.0", "b"))
          .toDS()
          .groupByKey(x => x._3)
          .mapValues(x => x)

        val q = spark.readStream
          .schema(testSchema)
          .option("maxFilesPerTrigger", 1)
          .parquet(path)
          .as[(String, String)]
          .groupByKey(x => x._1)
          .transformWithState(
            new TestInitialStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update(),
            initialState = initDf)
          .writeStream
          .format("memory")
          .queryName("my_sink")
          .start()

        try {
          q.processAllAvailable()
          eventually(timeout(30.seconds)) {
            checkDataset(
              spark.table("my_sink").toDF().as[(String, String)].orderBy("_1"),
              ("a", "2"),
              ("a", "3"),
              ("b", "2"))
          }
        } finally {
          q.stop()
          spark.sql("DROP TABLE IF EXISTS my_sink")
        }
      }
    }
  }

  test("transformWithState - streaming with chaining of operators") {
    withSQLConf(twsAdditionalSQLConf: _*) {
      val session: SparkSession = spark
      import session.implicits._

      def timestamp(num: Int): Timestamp = {
        new Timestamp(num * 1000)
      }

      val checkResultFunc: (Dataset[Row], Long) => Unit = { (batchDF, batchId) =>
        val realDf = batchDF.orderBy("outputTimestamp").collect().toSet
        if (batchId == 0) {
          assert(realDf.isEmpty, s"BatchId: $batchId, RealDF: $realDf")
        } else if (batchId == 1) {
          val expectedDF = Seq(Row(timestamp(10), 1L)).toSet
          assert(
            realDf == expectedDF,
            s"BatchId: $batchId, expectedDf: $expectedDF, RealDF: $realDf")
        } else if (batchId == 2) {
          val expectedDF = Seq(Row(timestamp(11), 1L), Row(timestamp(15), 1L)).toSet
          assert(
            realDf == expectedDF,
            s"BatchId: $batchId, expectedDf: $expectedDF, RealDF: $realDf")
        }
      }

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        prepareInputData(path + "/text-test3.txt", Seq("a", "b"), Seq(10, 15))
        Thread.sleep(2000)
        prepareInputData(path + "/text-test4.txt", Seq("a", "c"), Seq(11, 25))
        Thread.sleep(2000)
        prepareInputData(path + "/text-test1.txt", Seq("a"), Seq(5))

        val q = buildTestDf(path, spark)
          .select(col("key").as("key"), timestamp_seconds(col("value")).as("eventTime"))
          .withWatermark("eventTime", "5 seconds")
          .as[(String, Timestamp)]
          .groupByKey(x => x._1)
          .transformWithState[OutputEventTimeRow](
            new ChainingOfOpsStatefulProcessor(),
            "outputTimestamp",
            OutputMode.Append())
          .groupBy("outputTimestamp")
          .count()
          .writeStream
          .foreachBatch(checkResultFunc)
          .outputMode("Append")
          .start()

        q.processAllAvailable()
        eventually(timeout(30.seconds)) {
          q.stop()
        }
      }
    }
  }

  test("transformWithState - streaming with TTL and composite state variables") {
    withSQLConf(twsAdditionalSQLConf: _*) {
      val session: SparkSession = spark
      import session.implicits._

      val checkResultFunc = (batchDF: Dataset[(String, String)], batchId: Long) => {
        if (batchId == 0) {
          val expectedDF = Seq(
            ("count-0", "1"),
            ("ttlCount-0", "1"),
            ("ttlListState-0", "1"),
            ("ttlMapState-0", "1"),
            ("count-1", "1"),
            ("ttlCount-1", "1"),
            ("ttlListState-1", "1"),
            ("ttlMapState-1", "1")).toSet

          val realDf = batchDF.collect().toSet
          assert(realDf == expectedDF)

        } else if (batchId == 1) {
          val expectedDF = Seq(
            ("count-0", "2"),
            ("ttlCount-0", "1"),
            ("ttlListState-0", "1"),
            ("ttlMapState-0", "1"),
            ("count-1", "2"),
            ("ttlCount-1", "1"),
            ("ttlListState-1", "1"),
            ("ttlMapState-1", "1")).toSet

          val realDf = batchDF.collect().toSet
          assert(realDf == expectedDF)
        }

        if (batchId == 0) {
          // let ttl state expires
          Thread.sleep(2000)
        }
      }

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        prepareInputData(path + "/text-test3.txt", Seq("1", "0"), Seq(0, 0))
        Thread.sleep(2000)
        prepareInputData(path + "/text-test4.txt", Seq("1", "0"), Seq(0, 0))

        val q = buildTestDf(path, spark)
          .as[(String, String)]
          .groupByKey(x => x._1)
          .transformWithState(
            new TTLTestStatefulProcessor(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
          .writeStream
          .foreachBatch(checkResultFunc)
          .outputMode("Update")
          .start()
        q.processAllAvailable()

        eventually(timeout(30.seconds)) {
          q.stop()
        }
      }
    }
  }

  test("transformWithState - batch query") {
    withSQLConf(twsAdditionalSQLConf: _*) {
      val session: SparkSession = spark
      import session.implicits._

      spark.sql("DROP TABLE IF EXISTS my_sink")

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        testData
          .toDS()
          .toDF("key", "value")
          .repartition(3)
          .write
          .parquet(path)

        val testSchema =
          StructType(Array(StructField("key", StringType), StructField("value", StringType)))

        spark.read
          .schema(testSchema)
          .parquet(path)
          .as[InputRowForConnectTest]
          .groupByKey(x => x.key)
          .transformWithState[OutputRowForConnectTest](
            new BasicCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())
          .write
          .saveAsTable("my_sink")

        checkDataset(
          spark.table("my_sink").toDF().as[(String, String)].orderBy("key"),
          ("a", "2"),
          ("b", "1"))
      }
    }
  }

  /* Utils functions for tests */
  def prepareInputData(inputPath: String, col1: Seq[String], col2: Seq[Int]): Unit = {
    // Ensure the parent directory exists
    val file = Paths.get(inputPath)
    val parentDir = file.getParent
    if (parentDir != null && !Files.exists(parentDir)) {
      Files.createDirectories(parentDir)
    }

    val writer = new BufferedWriter(new FileWriter(inputPath))
    try {
      col1.zip(col2).foreach { case (e1, e2) =>
        writer.write(s"$e1, $e2\n")
      }
    } finally {
      writer.close()
    }
  }

  def buildTestDf(inputPath: String, sparkSession: SparkSession): DataFrame = {
    val df = sparkSession.readStream
      .format("text")
      .option("maxFilesPerTrigger", 1)
      .load(inputPath)

    val dfSplit = df.withColumn("split_values", split(col("value"), ","))
    val dfFinal = dfSplit.select(
      col("split_values").getItem(0).alias("key").cast("string"),
      col("split_values").getItem(1).alias("value").cast("int"))

    dfFinal
  }
}
