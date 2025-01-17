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

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Paths}
import java.sql.Timestamp

import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.{QueryTest, RemoteSparkSession}
import org.apache.spark.sql.types._

class TestInitialStatefulProcessor
  extends StatefulProcessorWithInitialState[
    String, (String, String), (String, String), (String, String, String)]
    with Logging {
  @transient protected var _countState: ValueState[Long] = _

  override def init(
                     outputMode: OutputMode,
                     timeMode: TimeMode): Unit = {
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

class BasicCountStatefulProcessor
  extends StatefulProcessor[String, (String, String), (String, String)]
    with Logging {
  @transient protected var _countState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
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
}

case class OutputEventTimeRow2(key: String, outputTimestamp: Timestamp)

class ChainingOfOpsStatefulProcessor2
  extends StatefulProcessor[String, (String, Timestamp), OutputEventTimeRow2] {
  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {}

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Timestamp)],
      timerValues: TimerValues): Iterator[OutputEventTimeRow2] = {
    val timestamp = inputRows.next()._2
    Iterator(OutputEventTimeRow2(key, timestamp))
  }
}


class TransformWithStateStreamingSuite extends QueryTest with RemoteSparkSession with Logging {
  val testData: Seq[(String, String)] = Seq(("a", "1"), ("b", "1"), ("a", "2"))
  val twsAdditionalSQLConf = Seq(
    "spark.sql.streaming.stateStore.providerClass" ->
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    "spark.sql.shuffle.partitions" -> "5",
    "spark.sql.session.timeZone" -> "UTC"
  )

  test("transformWithState - streaming with state variable") {
    withSQLConf(twsAdditionalSQLConf: _*) {
      val session: SparkSession = spark
      import session.implicits._

      spark.sql("DROP TABLE IF EXISTS my_sink")

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        testData.toDS().toDF("id", "value")
          .repartition(3).write.parquet(path)

        val testSchema = StructType(Array(
          StructField("id", StringType), StructField("value", StringType)))

        val q = spark.readStream
          .schema(testSchema)
          .option("maxFilesPerTrigger", 1)
          .parquet(path)
          .as[(String, String)]
          .groupByKey(x => x._1)
          .transformWithState(
            new BasicCountStatefulProcessor(),
            TimeMode.None(), OutputMode.Update())
          .writeStream
          .format("memory")
          .queryName("my_sink")
          .start()

        try {
          q.processAllAvailable()
          eventually(timeout(30.seconds)) {
            checkDataset(
              spark.table("my_sink").toDF().as[(String, String)].orderBy("_1"),
              ("a", "1"), ("a", "2"), ("b", "1")
            )
          }
        } finally {
          q.stop()
          spark.sql("DROP TABLE IF EXISTS my_sink")
        }
      }
    }
  }

  test("transformWithState with initial state") {
    withSQLConf(twsAdditionalSQLConf: _*) {
      val session: SparkSession = spark
      import session.implicits._

      spark.sql("DROP TABLE IF EXISTS my_sink")

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        testData.toDS().toDF("id", "value")
          .repartition(3).write.parquet(path)

        val testSchema = StructType(Array(
          StructField("id", StringType), StructField("value", StringType)))

        val initDf = Seq(("init_1", "40.0", "a"), ("init_2", "100.0", "b")).toDS()
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
            TimeMode.None(), OutputMode.Update(),
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
              ("a", "2"), ("a", "3"), ("b", "2")
            )
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

      val checkResultFunc : (Dataset[Row], Long) => Unit = { (batchDF, batchId) =>
        val realDf = batchDF.orderBy("outputTimestamp").collect().toSet
        if (batchId == 0) {
          assert(realDf.isEmpty, s"BatchId: $batchId, RealDF: $realDf")
        } else if (batchId == 1) {
          val expectedDF = Seq(Row(timestamp(10), 1L)).toSet
          assert(realDf == expectedDF,
            s"BatchId: $batchId, expectedDf: $expectedDF, RealDF: $realDf")
        } else if (batchId == 2) {
          val expectedDF = Seq(Row(timestamp(11), 1L),
            Row(timestamp(15), 1L)).toSet
          assert(realDf == expectedDF,
            s"BatchId: $batchId, expectedDf: $expectedDF, RealDF: $realDf")
        }
      }

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        prepareInputData(path + "/text-test3.txt", Seq("a", "b"), Seq(10, 15))
        prepareInputData(path + "/text-test4.txt", Seq("a", "c"), Seq(11, 25))
        prepareInputData(path + "/text-test1.txt", Seq("a"), Seq(5))

        val q = buildTestDf(path, spark)
          .select(col("id").as("id"),
            timestamp_seconds(col("value")).as("eventTime"))
          .withWatermark("eventTime", "5 seconds")
          .as[(String, Timestamp)]
          .groupByKey(x => x._1)
          .transformWithState[OutputEventTimeRow2](
            new ChainingOfOpsStatefulProcessor2(),
            "outputTimestamp", OutputMode.Append()
          )
          .groupBy("outputTimestamp")
          .count()
          .writeStream
          .foreachBatch(checkResultFunc)
          .outputMode("Append")
          .start()

        try {
          q.processAllAvailable()
          eventually(timeout(30.seconds)) {
            q.stop()
          }
        } finally {
          q.stop()
          spark.sql("DROP TABLE IF EXISTS my_sink")
        }
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
      col("split_values").getItem(0).alias("id").cast("string"),
      col("split_values").getItem(1).alias("value").cast("int")
    )

    dfFinal
  }
}
