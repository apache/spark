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

import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkContext
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.datasources.v2.{LowLatencyClock, StreamingDataSourceV2Relation, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.RealTimeTrigger
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemorySink
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.util.SystemClock

/**
 * Tests for streaming source naming and source evolution
 * ([[SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION]]) with the Kafka source under Real-Time Mode.
 *
 * This is the Kafka counterpart to
 * `org.apache.spark.sql.streaming.RealTimeModeSourceEvolutionSuite`. Because Kafka is reachable
 * through `spark.readStream`, sources are named with the `.name()` API directly rather than by
 * wrapping the logical plan, which is how a user actually names a Kafka source. The checks confirm
 * that named Kafka sources get name-keyed checkpoint paths and a v2 name-keyed offset log under
 * RTM, and that the naming enforcement rules fire on the RTM path.
 */
class KafkaRealTimeModeSourceEvolutionSuite extends KafkaSourceTest with Matchers {
  import testImplicits._

  override protected val defaultTrigger = RealTimeTrigger.apply("3 seconds")

  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[8]", // Ensure enough cores to concurrently schedule all RTM tasks.
      "streaming-rtm-context",
      sparkConf.set("spark.sql.testkey", "true")))

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      SQLConf.STREAMING_REAL_TIME_MODE_MIN_BATCH_DURATION,
      defaultTrigger.batchDurationMs)
  }

  override def afterEach(): Unit = {
    spark.streams.active.foreach(_.stop())
    LowLatencyClock.setClock(new SystemClock)
    super.afterEach()
  }

  /**
   * Timeout for RTM answer checks. RTM batches are time-boxed rather than data-driven, so a check
   * has to wait for at least one batch boundary.
   */
  private val rtmTimeoutMs = 60000L

  // ==============
  // Helper Methods
  // ==============

  /** Runs `testBody` with source evolution (and therefore named-source enforcement) enabled. */
  private def testWithSourceEvolution(testName: String)(testBody: => Any): Unit = {
    test(testName) {
      withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
        testBody
      }
    }
  }

  /**
   * A streaming Dataset over `topic`, optionally named via the `.name()` API. Values are decoded
   * as ints so multiple named sources can be unioned into a single `Dataset[Int]`.
   */
  private def namedKafka(topic: String, sourceName: Option[String]) = {
    var reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
    sourceName.foreach { n => reader = reader.name(n) }
    reader.load().selectExpr("CAST(value AS STRING)").as[String].map(_.toInt)
  }

  /**
   * The per-source checkpoint path suffixes the query resolved, read off the executed plan's
   * `StreamingDataSourceV2Relation.metadataPath`.
   */
  private def sourceMetadataPathSuffixes(query: StreamExecution): Set[String] = {
    val paths = query.logicalPlan.collect {
      case r: StreamingDataSourceV2ScanRelation => r.relation.metadataPath
      case r: StreamingDataSourceV2Relation => r.metadataPath
    }
    assert(paths.nonEmpty, s"Expected at least one V2 streaming relation in ${query.logicalPlan}")
    paths.map { path =>
      val marker = "/sources/"
      val index = path.indexOf(marker)
      assert(index >= 0, s"Expected a sources path, got $path")
      path.substring(index + marker.length)
    }.toSet
  }

  /** Reads the latest offset log entry, asserting it is a v2 name-keyed [[OffsetMap]]. */
  private def latestOffsetMap(checkpointDir: File): OffsetMap = {
    val offsetLog = new OffsetSeqLog(spark, checkpointDir.getAbsolutePath + "/offsets")
    val latest = offsetLog.getLatest()
    assert(latest.isDefined, "Offset log should have at least one entry")
    latest.get._2 match {
      case offsetMap: OffsetMap => offsetMap
      case other =>
        fail(s"Expected OffsetMap (offset log v2) but got ${other.getClass.getSimpleName}")
    }
  }

  // =============================================
  // Named sources drive the RTM checkpoint layout
  // =============================================

  testWithSourceEvolution("RTM named Kafka source uses name-keyed checkpoint paths and offsets") {
    withTempDir { checkpointDir =>
      val topic = newTopic()
      testUtils.createTopic(topic, partitions = 2)
      testUtils.sendMessages(topic, Array("1", "2"), Some(0))
      testUtils.sendMessages(topic, Array("3"), Some(1))

      testStream(namedKafka(topic, Some("alpha")), Update, sink = new ContinuousMemorySink())(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        CheckAnswerRowsContainsWithTimeout(rtmTimeoutMs, 1, 2, 3),
        // The source's metadata path is keyed by name, not by positional id.
        Execute { q => assert(sourceMetadataPathSuffixes(q) === Set("alpha")) },
        WaitUntilCurrentBatchProcessed,
        StopStream
      )

      // The end offsets RTM writes at end of batch must resolve back to the source name.
      val offsetMap = latestOffsetMap(checkpointDir)
      assert(offsetMap.offsetsMap.keySet === Set("alpha"))
      assert(offsetMap.offsetsMap("alpha").isDefined,
        "The active named source should have a real offset")

      // Source evolution forces offset log v2.
      assert(offsetMap.version === 2)

      // The enforcement flag is pinned in the offset metadata so restarts inherit it.
      val metadata = offsetMap.metadataOpt
      assert(metadata.isDefined, "Offset metadata should be present")
      assert(metadata.get.conf.get(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key)
        .contains("true"))
    }
  }

  testWithSourceEvolution("RTM named Kafka sources in a union get separate name-keyed paths") {
    withTempDir { checkpointDir =>
      val topic1 = newTopic()
      val topic2 = newTopic()
      testUtils.createTopic(topic1, partitions = 1)
      testUtils.createTopic(topic2, partitions = 1)
      testUtils.sendMessages(topic1, Array("1", "2"), Some(0))
      testUtils.sendMessages(topic2, Array("3", "4"), Some(0))

      val unioned = namedKafka(topic1, Some("alpha")).union(namedKafka(topic2, Some("beta")))

      testStream(unioned, Update, sink = new ContinuousMemorySink())(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        CheckAnswerRowsContainsWithTimeout(rtmTimeoutMs, 1, 2, 3, 4),
        Execute { q => assert(sourceMetadataPathSuffixes(q) === Set("alpha", "beta")) },
        WaitUntilCurrentBatchProcessed,
        StopStream
      )

      // Both RTM scans' end offsets must resolve to their own names rather than colliding.
      val offsetMap = latestOffsetMap(checkpointDir)
      assert(offsetMap.offsetsMap.keySet === Set("alpha", "beta"))
      assert(offsetMap.offsetsMap.values.forall(_.isDefined),
        s"Both named sources should have real offsets, got ${offsetMap.offsetsMap}")
    }
  }

  // ==========================
  // Enforcement under RTM
  // ==========================

  testWithSourceEvolution("RTM rejects an unnamed Kafka source when enforcement is enabled") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)

    // An unnamed source under enforcement is rejected during analysis, which for the readStream
    // path happens at load() time (before any query is started).
    checkError(
      exception = intercept[AnalysisException] {
        namedKafka(topic, sourceName = None)
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
      parameters = Map("sourceInfo" -> ".*"),
      matchPVals = true)
  }

  testWithSourceEvolution("RTM rejects duplicate Kafka source names") {
    withTempDir { checkpointDir =>
      val topic1 = newTopic()
      val topic2 = newTopic()
      testUtils.createTopic(topic1, partitions = 1)
      testUtils.createTopic(topic2, partitions = 1)

      // Two distinct Kafka sources sharing one name would collide on a single `sources/<name>`
      // path and offset-map key. The duplicate check runs during start() analysis and throws
      // synchronously, so it surfaces out of testStream.
      checkError(
        exception = intercept[AnalysisException] {
          testStream(
            namedKafka(topic1, Some("same")).union(namedKafka(topic2, Some("same"))),
            Update,
            sink = new ContinuousMemorySink())(
            StartStream(checkpointLocation = checkpointDir.getAbsolutePath)
          )
        },
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.DUPLICATE_SOURCE_NAMES",
        parameters = Map("names" -> "'same'"))
    }
  }

  // ===========================================
  // Baseline: RTM without source evolution
  // ===========================================

  test("RTM without source evolution keeps positional Kafka source paths") {
    withTempDir { checkpointDir =>
      val topic = newTopic()
      testUtils.createTopic(topic, partitions = 1)
      testUtils.sendMessages(topic, Array("1", "2"), Some(0))

      // With enforcement off, an RTM query is unchanged and still uses positional source ids.
      testStream(namedKafka(topic, sourceName = None), Update, sink = new ContinuousMemorySink())(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        CheckAnswerRowsContainsWithTimeout(rtmTimeoutMs, 1, 2),
        Execute { q => assert(sourceMetadataPathSuffixes(q) === Set("0")) },
        WaitUntilCurrentBatchProcessed,
        StopStream
      )
    }
  }
}
