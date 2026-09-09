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

import java.io.File
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkContext, ThreadAudit}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.v2.LowLatencyClock
import org.apache.spark.sql.execution.streaming.RealTimeTrigger
import org.apache.spark.sql.execution.streaming.sources.LowLatencyMemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.streaming.util.{GlobalSingletonManualClock, StreamManualClock}
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.util.Utils

abstract class KafkaRealTimeModeBaseSuite
    extends KafkaSourceTest
    with ThreadAudit
    with BeforeAndAfterEach
    with Matchers {

  import testImplicits._

  private def defaultTriggerBatchDurationMs: Long = 1000L

  override def beforeAll(): Unit = {
    super.beforeAll()
    // testing to make sure the cluster is usable
    testUtils.createTopic("_test")
    testUtils.sendMessage(new ProducerRecord[String, String]("_test", "", ""))
    testUtils.deleteTopic("_test")
    logInfo("Kafka cluster setup complete....")

    spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 5)
    spark.conf.set(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
    )
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows", "false")
    spark.conf.set("spark.sql.streaming.stateStore.checkpointFormatVersion", "2")
    spark.conf.set(
      SQLConf.STREAMING_REAL_TIME_MODE_MIN_BATCH_DURATION,
      defaultTriggerBatchDurationMs
    )
  }

  override protected def createSparkSession =
    new TestSparkSession(
      new SparkContext(
        // Ensure we have enough for both stages. 5 source partitions and 5 shuffle partitions
        "local[15]",
        "microbatch-context",
        sparkConf
          .set("spark.sql.testkey", "true")
          .set("spark.sql.shuffle.partitions", "5")
          .set("spark.sql.adaptive.enabled", "false")
          .set(
            "spark.executor.extraJavaOptions",
            "-Dio.netty.leakDetection.level=paranoid"
          )
      )
    )

  override def beforeEach(): Unit = {
    super.beforeEach()
    GlobalSingletonManualClock.reset()
  }

  protected def writeToKafka(
      queryName: String,
      outputTopic: String,
      checkpointDir: File,
      df: DataFrame): StreamingQuery = {
    df.writeStream
      .outputMode(OutputMode.Update())
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("topic", outputTopic)
      .option("checkpointLocation", checkpointDir.getAbsolutePath)
      .queryName(queryName)
      // The batch duration set here doesn't matter because we manually control batch durations
      // via the manual clock.
      .trigger(RealTimeTrigger(defaultTriggerBatchDurationMs))
      .start()
  }

  protected def readKafkaTopic(topic: String): DataFrame = {
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .select(col("value").cast("STRING"))
  }


  protected def runTest(test: (TestParams) => Unit): Unit = {
    withTempDir { checkpointDir =>
      val outputTopic = newTopic()
      testUtils.createTopic(outputTopic, partitions = 5)

      val clock = new GlobalSingletonManualClock()

      LowLatencyClock.setClock(clock)
      val read = LowLatencyMemoryStream[(Long, Int)](5)
      val param = TestParams(null, clock, read, outputTopic, checkpointDir)
      try {
        test(param)
      } finally {
        if (param.query != null) {
          param.query.stop()
        }

        try {
          eventually(timeout(60.seconds)) {

            val currentRunningTasks = sparkContext.statusTracker.getExecutorInfos
              .map(
                i =>
                  s"[host: ${i.host()}" +
                  s"  port: ${i.port} tasks:${i.numRunningTasks()}]"
              )
              .toList

            logInfo(s"Current tasks: ${currentRunningTasks}")

            assert(
              spark.sparkContext.statusTracker.getExecutorInfos.map(_.numRunningTasks()).sum <= 0,
              currentRunningTasks
            )
          }
        } catch {
          case t: Throwable =>
            // Best-effort diagnostic for a task that never wound down.
            logWarning(s"Tasks still running after the query stopped", t)
            logWarning(Utils.getThreadDump().map(_.toString).mkString("\n"))
            throw t
        }
      }
    }
  }

  protected def getDateTimeString(millis: Long): String = {
    val instant = Instant.ofEpochMilli(millis)
    val formatter = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss")
      .withZone(ZoneId.systemDefault())
    formatter.format(instant)
  }
}

case class TestParams(
    var query: StreamingQuery,
    clock: StreamManualClock,
    read: LowLatencyMemoryStream[(Long, Int)],
    outputTopic: String,
    checkpointDir: File)
