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

package org.apache.spark.sql.kafka010

import java.util.UUID

import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkContext, SparkIllegalStateException}
import org.apache.spark.sql.execution.datasources.v2.LowLatencyClock
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemorySink
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.sql.streaming.util.GlobalSingletonManualClock
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.util.SystemClock

class KafkaRealTimeModeSuite
  extends KafkaSourceTest
    with Matchers {

  override protected val defaultTrigger = RealTimeTrigger.apply("3 seconds")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        SQLConf.STATE_STORE_PROVIDER_CLASS,
        classOf[RocksDBStateStoreProvider].getName)
  }

  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[8]", // Ensure enough number of cores to ensure concurrent schedule of all tasks.
      "streaming-rtm-context",
      sparkConf.set("spark.sql.testkey", "true")))


  import testImplicits._

  val sleepOneSec = new ExternalAction() {
    override def runAction(): Unit = {
      Thread.sleep(1000)
    }
  }

  var clock = new GlobalSingletonManualClock()

  private def advanceRealTimeClock(timeMs: Int) = new ExternalAction {
    override def runAction(): Unit = {
      clock.advance(timeMs)
    }

    override def toString(): String = {
      s"advanceRealTimeClock($timeMs)"
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      SQLConf.STREAMING_REAL_TIME_MODE_MIN_BATCH_DURATION,
      defaultTrigger.batchDurationMs
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    GlobalSingletonManualClock.reset()
  }

  override def afterEach(): Unit = {
    LowLatencyClock.setClock(new SystemClock)
    super.afterEach()
  }

  def waitUntilBatchStartedOrProcessed(q: StreamingQuery, batchId: Long): Unit = {
    eventually(timeout(60.seconds)) {
      val tasksRunning =
        spark.sparkContext.statusTracker.getExecutorInfos.map(_.numRunningTasks()).sum
      val lastBatch = {
        if (q.lastProgress == null) {
          -1
        } else {
          q.lastProgress.batchId
        }
      }
      val batchStarted = tasksRunning >= 1 && lastBatch >= batchId - 1
      val batchProcessed = lastBatch >= batchId
      assert(batchStarted || batchProcessed,
        s"tasksRunning: ${tasksRunning} lastBatch: ${lastBatch}")
    }
  }

  // A simple unit test that reads from Kakfa source, does a simple map and writes to memory
  // sink.
  test("simple map") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)

    testUtils.sendMessages(topic, Array("1", "2"), Some(0))
    testUtils.sendMessages(topic, Array("3"), Some(1))

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

    testStream(reader, Update, sink = new ContinuousMemorySink())(
      StartStream(),
      CheckAnswerWithTimeout(60000, 2, 3, 4),
      sleepOneSec,
      sleepOneSec,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("4", "5"), Some(0))
          testUtils.sendMessages(topic, Array("6"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 5, 6, 7),
      WaitUntilCurrentBatchProcessed,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("7"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 5, 6, 7, 8),
      WaitUntilCurrentBatchProcessed,
      StopStream,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("8"), Some(0))
          testUtils.sendMessages(topic, Array("9"), Some(1))
        }
      },
      StartStream(),
      CheckAnswerWithTimeout(5000, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      WaitUntilCurrentBatchProcessed)
  }

  // A simple unit test that reads from Kakfa source, does a simple map and writes to memory
  // sink. Make sure there is no data for a whole batch. Also, after restart the first batch
  // has no data.
  test("simple map with empty batch") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)

    val reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

    testStream(reader, Update, sink = new ContinuousMemorySink())(
      StartStream(),
      WaitUntilBatchProcessed(0),
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("1"), Some(0))
          testUtils.sendMessages(topic, Array("2"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3),
      WaitUntilCurrentBatchProcessed,
      WaitUntilCurrentBatchProcessed,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("3"), Some(1))
        }
      },
      WaitUntilCurrentBatchProcessed,
      CheckAnswerWithTimeout(5000, 2, 3, 4),
      StopStream,
      StartStream(),
      WaitUntilCurrentBatchProcessed,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("4"), Some(0))
          testUtils.sendMessages(topic, Array("5"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 5, 6),
      WaitUntilCurrentBatchProcessed
    )
  }

  // A simple unit test that reads from Kakfa source, does a simple map and writes to memory
  // sink.
  test("add partition") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)

    testUtils.sendMessages(topic, Array("1", "2", "3"), Some(0))

    val reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

    testStream(reader, Update, sink = new ContinuousMemorySink())(
      StartStream(),
      CheckAnswerWithTimeout(60000, 2, 3, 4),
      sleepOneSec,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.addPartitions(topic, 2)
          testUtils.sendMessages(topic, Array("4", "5"), Some(0))
          testUtils.sendMessages(topic, Array("6"), Some(1))
        }
      },
      CheckAnswerWithTimeout(15000, 2, 3, 4, 5, 6, 7),
      WaitUntilCurrentBatchProcessed,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.addPartitions(topic, 4)
          testUtils.sendMessages(topic, Array("7"), Some(2))
        }
      },
      CheckAnswerWithTimeout(15000, 2, 3, 4, 5, 6, 7, 8),
      WaitUntilCurrentBatchProcessed,
      StopStream,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("8"), Some(3))
          testUtils.sendMessages(topic, Array("9"), Some(2))
        }
      },
      StartStream(),
      CheckAnswerWithTimeout(15000, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      WaitUntilCurrentBatchProcessed
    )
  }

  test("Real-Time Mode fetches latestOffset again at end of the batch") {
    // LowLatencyClock does not affect the wait time of kafka iterator, so advancing the clock
    // does not affect the test finish time. The purpose of using it is to make the query start
    // time consistent, so the test behaves the same.
    LowLatencyClock.setClock(clock)
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)

    val reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      // extra large number to make sure fetch does
      // not return within batch duration
      .option("kafka.fetch.max.wait.ms", "20000000")
      .option("kafka.fetch.min.bytes", "20000000")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

    testStream(reader, Update, sink = new ContinuousMemorySink())(
      StartStream(Trigger.RealTime(10000)),
      advanceRealTimeClock(2000),
      Execute { q =>
        waitUntilBatchStartedOrProcessed(q, 0)
        testUtils.sendMessages(topic, Array("1"), Some(0))
      },
      advanceRealTimeClock(8000),
      WaitUntilBatchProcessed(0),
      Execute { q =>
        val expectedMetrics = Map(
          "minOffsetsBehindLatest" -> "1",
          "maxOffsetsBehindLatest" -> "1",
          "avgOffsetsBehindLatest" -> "1.0",
          "estimatedTotalBytesBehindLatest" -> null
        )
        eventually(timeout(60.seconds)) {
          expectedMetrics.foreach { case (metric, expectedValue) =>
            assert(q.lastProgress.sources(0).metrics.get(metric) === expectedValue)
          }
        }
      },
      advanceRealTimeClock(2000),
      Execute { q =>
        waitUntilBatchStartedOrProcessed(q, 1)
        testUtils.sendMessages(topic, Array("2", "3"), Some(0))
      },
      advanceRealTimeClock(8000),
      WaitUntilBatchProcessed(1),
      Execute { q =>
        val expectedMetrics = Map(
          "minOffsetsBehindLatest" -> "3",
          "maxOffsetsBehindLatest" -> "3",
          "avgOffsetsBehindLatest" -> "3.0",
          "estimatedTotalBytesBehindLatest" -> null
        )
        eventually(timeout(60.seconds)) {
          expectedMetrics.foreach { case (metric, expectedValue) =>
            assert(q.lastProgress.sources(0).metrics.get(metric) === expectedValue)
          }
        }
      }
    )
  }

  // Validate the query fails with minOffsetPerTrigger option set.
  Seq(
    "maxoffsetspertrigger",
    "minoffsetspertrigger",
    "minpartitions",
    "endingtimestamp",
    "maxtriggerdelay").foreach { opt =>
    test(s"$opt incompatible") {
      val topic = newTopic()
      testUtils.createTopic(topic, partitions = 2)

      val reader = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option(opt, "5")
        .load()
      testStream(reader, Update, sink = new ContinuousMemorySink())(
        StartStream(),
        ExpectFailure[UnsupportedOperationException] { (t: Throwable) => {
          assert(t.getMessage.toLowerCase().contains(opt))
        }
        }
      )
    }
  }

  test("union 2 dataframes after projection") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)

    val topic1 = newTopic()
    testUtils.createTopic(topic1, partitions = 2)

    testUtils.sendMessages(topic, Array("1", "2"), Some(0))
    testUtils.sendMessages(topic, Array("3"), Some(1))

    testUtils.sendMessages(topic1, Array("11", "12"), Some(0))
    testUtils.sendMessages(topic1, Array("13"), Some(1))

    val reader1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

    val reader2 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic1)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

    val unionedReader = reader1.union(reader2)

    testStream(unionedReader, Update, sink = new ContinuousMemorySink())(
      StartStream(),
      CheckAnswerWithTimeout(60000, 2, 3, 4, 12, 13, 14),
      sleepOneSec,
      sleepOneSec,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("4", "5"), Some(0))
          testUtils.sendMessages(topic, Array("6"), Some(1))
          testUtils.sendMessages(topic1, Array("14", "15"), Some(0))
          testUtils.sendMessages(topic1, Array("16"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 12, 13, 14, 5, 6, 7, 15, 16, 17),
      WaitUntilCurrentBatchProcessed,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("7"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 12, 13, 14, 5, 6, 7, 15, 16, 17, 8),
      WaitUntilCurrentBatchProcessed,
      StopStream,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("8"), Some(0))
          testUtils.sendMessages(topic, Array("9"), Some(1))
          testUtils.sendMessages(topic1, Array("19"), Some(1))
        }
      },
      StartStream(),
      CheckAnswerWithTimeout(5000, 2, 3, 4, 12, 13, 14, 5, 6, 7, 15, 16, 17, 8, 9, 10, 20),
      WaitUntilCurrentBatchProcessed)
  }

  test("union 3 dataframes with and without maxPartitions") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)

    val topic1 = newTopic()
    testUtils.createTopic(topic1, partitions = 2)

    val topic2 = newTopic()
    testUtils.createTopic(topic2, partitions = 2)

    testUtils.sendMessages(topic, Array("1", "2"), Some(0))
    testUtils.sendMessages(topic, Array("3"), Some(1))

    testUtils.sendMessages(topic1, Array("11", "12"), Some(0))
    testUtils.sendMessages(topic1, Array("13"), Some(1))

    testUtils.sendMessages(topic2, Array("21", "22"), Some(0))
    testUtils.sendMessages(topic2, Array("23"), Some(1))

    val reader1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("maxPartitions", "1")
      .load()

    val reader2 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic1)
      .option("startingOffsets", "earliest")
      .load()

    val reader3 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic2)
      .option("startingOffsets", "earliest")
      .option("maxPartitions", "3")
      .load()

    val unionedReader = reader1.union(reader2).union(reader3)
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

    testStream(unionedReader, Update, sink = new ContinuousMemorySink())(
      StartStream(),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 12, 13, 14, 22, 23, 24),
      sleepOneSec,
      sleepOneSec,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("4"), Some(0))
          testUtils.sendMessages(topic, Array("5"), Some(1))
          testUtils.sendMessages(topic1, Array("14", "15"), Some(0))
          testUtils.sendMessages(topic2, Array("24"), Some(0))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 12, 13, 14, 22, 23, 24, 5, 6, 15, 16, 25),
      WaitUntilCurrentBatchProcessed,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("6"), Some(1))
          testUtils.sendMessages(topic2, Array("25"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 12, 13, 14, 22, 23, 24, 5, 6, 15, 16, 25, 7, 26),
      WaitUntilCurrentBatchProcessed,
      StopStream,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic1, Array("16"), Some(1))
        }
      },
      StartStream(),
      CheckAnswerWithTimeout(5000, 2, 3, 4, 12, 13, 14, 22, 23, 24, 5, 6, 15, 16, 25, 7, 26, 17),
      WaitUntilCurrentBatchProcessed)
  }

  test("self union workaround") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)

    testUtils.sendMessages(topic, Array("1", "2"), Some(0))
    testUtils.sendMessages(topic, Array("3"), Some(1))

    val reader1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()


    val reader2 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val unionedReader = reader1.union(reader2)
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

    testStream(unionedReader, Update, sink = new ContinuousMemorySink())(
      StartStream(),
      CheckAnswerWithTimeout(60000, 2, 3, 4, 2, 3, 4),
      sleepOneSec,
      sleepOneSec,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("4", "5"), Some(0))
          testUtils.sendMessages(topic, Array("6"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 2, 3, 4, 5, 6, 7, 5, 6, 7),
      WaitUntilCurrentBatchProcessed,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("7"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 2, 3, 4, 5, 6, 7, 5, 6, 7, 8, 8),
      WaitUntilCurrentBatchProcessed,
      StopStream,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("8"), Some(0))
          testUtils.sendMessages(topic, Array("9"), Some(1))
        }
      },
      StartStream(),
      CheckAnswerWithTimeout(5000, 2, 3, 4, 2, 3, 4, 5, 6, 7, 5, 6, 7, 8, 8, 9, 10, 9, 10),
      WaitUntilCurrentBatchProcessed)
  }

  test("union 2 different sources - Kafka and LowLatencyMemoryStream") {
    import testImplicits._
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)

    val memoryStreamRead = LowLatencyMemoryStream[String](2)

    testUtils.sendMessages(topic, Array("1", "2"), Some(0))
    testUtils.sendMessages(topic, Array("3"), Some(1))
    memoryStreamRead.addData("11", "12", "13")

    val reader1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]


    val reader2 = memoryStreamRead.toDF()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val unionedReader = reader1.union(reader2)
      .map(_.toInt)
      .map(_ + 1)

    testStream(unionedReader, Update, sink = new ContinuousMemorySink())(
      StartStream(),
      CheckAnswerWithTimeout(60000, 2, 3, 4, 12, 13, 14),
      sleepOneSec,
      sleepOneSec,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("4", "5"), Some(0))
          testUtils.sendMessages(topic, Array("6"), Some(1))
          memoryStreamRead.addData("14")
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 12, 13, 14, 5, 6, 7, 15),
      WaitUntilCurrentBatchProcessed,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("7"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 12, 13, 14, 5, 6, 7, 15, 8),
      WaitUntilCurrentBatchProcessed,
      StopStream,
      new ExternalAction() {
        override def runAction(): Unit = {
          testUtils.sendMessages(topic, Array("8"), Some(0))
          testUtils.sendMessages(topic, Array("9"), Some(1))
          memoryStreamRead.addData("15", "16", "17")
        }
      },
      StartStream(),
      CheckAnswerWithTimeout(5000, 2, 3, 4, 12, 13, 14, 5, 6, 7, 15, 8, 9, 10, 16, 17, 18),
      WaitUntilCurrentBatchProcessed)
  }

  test("self union - not allowed") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val unionedReader = reader.union(reader)
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

      testStream(unionedReader, Update, sink = new ContinuousMemorySink())(
        StartStream(),
        ExpectFailure[SparkIllegalStateException] { ex =>
          checkErrorMatchPVals(
            ex.asInstanceOf[SparkIllegalStateException],
            "STREAMING_REAL_TIME_MODE.IDENTICAL_SOURCES_IN_UNION_NOT_SUPPORTED",
            parameters =
              Map("sources" -> "(?s).*")
          )
        }
      )
  }
}

class KafkaConsumerPoolRealTimeModeSuite
  extends KafkaSourceTest
  with Matchers {
  override protected val defaultTrigger = RealTimeTrigger.apply("3 seconds")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        SQLConf.STATE_STORE_PROVIDER_CLASS,
        classOf[RocksDBStateStoreProvider].getName)
  }

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      SQLConf.STREAMING_REAL_TIME_MODE_MIN_BATCH_DURATION,
      defaultTrigger.batchDurationMs
    )
  }

  test("SPARK-54200: Kafka consumers in consumer pool should be properly reused") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)

    testUtils.sendMessages(topic, Array("1", "2"), Some(0))
    testUtils.sendMessages(topic, Array("3"), Some(1))

    val groupIdPrefix = UUID.randomUUID().toString

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("groupIdPrefix", groupIdPrefix)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

    // At any point of time, Kafka consumer pool should only contain at most 2 active instances.
    testStream(reader, Update, sink = new ContinuousMemorySink())(
      StartStream(),
      CheckAnswerWithTimeout(60000, 2, 3, 4),
      WaitUntilCurrentBatchProcessed,
      // After completion of batch 0
      new ExternalAction() {
        override def runAction(): Unit = {
          assertActiveSizeOnConsumerPool(groupIdPrefix, 2)

          testUtils.sendMessages(topic, Array("4", "5"), Some(0))
          testUtils.sendMessages(topic, Array("6"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 5, 6, 7),
      WaitUntilCurrentBatchProcessed,
      // After completion of batch 1
      new ExternalAction() {
        override def runAction(): Unit = {
          assertActiveSizeOnConsumerPool(groupIdPrefix, 2)

          testUtils.sendMessages(topic, Array("7"), Some(1))
        }
      },
      CheckAnswerWithTimeout(5000, 2, 3, 4, 5, 6, 7, 8),
      WaitUntilCurrentBatchProcessed,
      // After completion of batch 2
      new ExternalAction() {
        override def runAction(): Unit = {
          assertActiveSizeOnConsumerPool(groupIdPrefix, 2)
        }
      },
      StopStream
    )
  }

  /**
   * NOTE: This method leverages that we run test code, driver and executor in a same process in
   * a normal unit test setup (say, local[<number, or *>] in spark master). With that setup, we
   * can access singleton object directly.
   */
  private def assertActiveSizeOnConsumerPool(
      groupIdPrefix: String,
      maxAllowedActiveSize: Int): Unit = {
    val activeSize = KafkaDataConsumer.getActiveSizeInConsumerPool(groupIdPrefix)
    assert(activeSize <= maxAllowedActiveSize, s"Consumer pool size is expected to be less " +
      s"than $maxAllowedActiveSize, but $activeSize.")
  }
}
