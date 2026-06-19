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

package org.apache.spark.sql.streaming.test

import org.scalatest.{BeforeAndAfterEach, Tag}

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.util.Utils

/**
 * Test suite for streaming sink evolution features including:
 * - Sink naming via DataStreamWriter.name()
 * - Sink name validation
 * - Sink evolution enforcement
 */
class StreamingSinkEvolutionSuite extends StreamTest with BeforeAndAfterEach {
  import testImplicits._

  private def newMetadataDir =
    Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath

  override def afterEach(): Unit = {
    spark.streams.active.foreach(_.stop())
    super.afterEach()
  }

  // =========================
  // Sink Name Validation Tests
  // =========================

  testWithSinkEvolution("invalid sink name - contains hyphen") {
    val input = MemoryStream[Int]
    input.addData(1, 2, 3)
    checkError(
      exception = intercept[AnalysisException] {
        input.toDF().writeStream
          .format("noop")
          .name("my-sink")
          .option("checkpointLocation", newMetadataDir)
          .start()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SINK_NAME",
      parameters = Map("sinkName" -> "my-sink"))
  }

  testWithSinkEvolution("invalid sink name - contains space") {
    val input = MemoryStream[Int]
    input.addData(1, 2, 3)
    checkError(
      exception = intercept[AnalysisException] {
        input.toDF().writeStream
          .format("noop")
          .name("my sink")
          .option("checkpointLocation", newMetadataDir)
          .start()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SINK_NAME",
      parameters = Map("sinkName" -> "my sink"))
  }

  testWithSinkEvolution("invalid sink name - contains special characters") {
    val input = MemoryStream[Int]
    input.addData(1, 2, 3)
    checkError(
      exception = intercept[AnalysisException] {
        input.toDF().writeStream
          .format("noop")
          .name("my.sink@123!")
          .option("checkpointLocation", newMetadataDir)
          .start()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SINK_NAME",
      parameters = Map("sinkName" -> "my.sink@123!"))
  }

  testWithSinkEvolution("valid sink names - various patterns") {
    Seq("mySink", "my_sink", "MySink123", "_private", "sink_123_test", "123sink")
      .foreach { sinkName =>
        val checkpointDir = newMetadataDir
        val input = MemoryStream[Int]
        input.addData(1, 2, 3)
        val q = input.toDF().writeStream
          .format("noop")
          .name(sinkName)
          .option("checkpointLocation", checkpointDir)
          .start()
        q.processAllAvailable()
        q.stop()
      }
  }

  // ===========================
  // Sink Evolution Enforcement
  // ===========================

  testWithSinkEvolution("unnamed sink with sink evolution enabled throws error") {
    val input = MemoryStream[Int]
    input.addData(1, 2, 3)
    val exception = intercept[SparkException] {
      val q = input.toDF().writeStream
        .format("noop")
        // No .name() call - sink is unnamed
        .option("checkpointLocation", newMetadataDir)
        .start()
      q.processAllAvailable()
      q.stop()
    }

    checkError(
      exception = exception,
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SINKS_WITH_ENFORCEMENT",
      parameters = Map.empty)
  }

  test("unnamed sink without sink evolution enabled uses default name") {
    withSQLConf(
      SQLConf.ENABLE_STREAMING_SINK_EVOLUTION.key -> "false") {
      val input = MemoryStream[Int]
      input.addData(1, 2, 3)
      // Should succeed - no name required when sink evolution is disabled
      val q = input.toDF().writeStream
        .format("noop")
        .option("checkpointLocation", newMetadataDir)
        .start()
      q.processAllAvailable()
      q.stop()
    }
  }

  testWithSinkEvolution("named sink succeeds with sink evolution enabled") {
    val input = MemoryStream[Int]
    input.addData(1, 2, 3)
    val q = input.toDF().writeStream
      .format("noop")
      .name("my_sink")
      .option("checkpointLocation", newMetadataDir)
      .start()
    q.processAllAvailable()
    q.stop()
  }

  testWithSinkEvolution("continuing with same sink name works") {
    val checkpointDir = newMetadataDir
    val input = MemoryStream[Int]

    // Start with my_sink
    input.addData(1, 2, 3)
    val q1 = input.toDF().writeStream
      .format("noop")
      .name("my_sink")
      .option("checkpointLocation", checkpointDir)
      .start()
    q1.processAllAvailable()
    q1.stop()

    // Restart with same sink name - should work
    input.addData(4, 5, 6)
    val q2 = input.toDF().writeStream
      .format("noop")
      .name("my_sink")
      .option("checkpointLocation", checkpointDir)
      .start()
    q2.processAllAvailable()
    q2.stop()
  }

  // ==============
  // Helper Methods
  // ==============

  /**
   * Helper method to run tests with sink evolution enabled.
   */
  def testWithSinkEvolution(testName: String, testTags: Tag*)(testBody: => Any): Unit = {
    test(testName, testTags: _*) {
      withSQLConf(
        SQLConf.ENABLE_STREAMING_SINK_EVOLUTION.key -> "true") {
        testBody
      }
    }
  }
}
