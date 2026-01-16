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

import org.scalatest.Tag

import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest

/**
 * Test suite for streaming source naming and validation.
 * Tests cover the naming API, validation rules, and resolution pipeline.
 */
class StreamingQueryEvolutionSuite extends StreamTest {

  // ====================
  // Name Validation Tests
  // ====================

  testWithSourceEvolution("invalid source name - contains hyphen") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my-source")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my-source"))
  }

  testWithSourceEvolution("invalid source name - contains space") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my source")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my source"))
  }

  testWithSourceEvolution("invalid source name - contains dot") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my.source")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my.source"))
  }

  testWithSourceEvolution("invalid source name - contains special characters") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my.source@123")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my.source@123"))
  }

  testWithSourceEvolution("valid source names - various patterns") {
    // Test that valid names work correctly
    Seq("mySource", "my_source", "MySource123", "_private", "source_123_test", "123source")
      .foreach { name =>
        val df = spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name(name)
          .load()
        assert(df.isStreaming, s"DataFrame should be streaming for name: $name")
      }
  }

  testWithSourceEvolution("method chaining - name() returns reader for chaining") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("my_source")
      .option("opt1", "value1")
      .load()

    assert(df.isStreaming, "DataFrame should be streaming")
  }

  // ==========================
  // Duplicate Detection Tests
  // ==========================

  testWithSourceEvolution("duplicate source names - rejected when starting stream") {
    withTempDir { checkpointDir =>
      val df1 = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .name("duplicate_name")
        .load()

      val df2 = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .name("duplicate_name")  // Same name - should fail
        .load()

      checkError(
        exception = intercept[AnalysisException] {
          df1.union(df2).writeStream
            .format("org.apache.spark.sql.streaming.test")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .start()
        },
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.DUPLICATE_SOURCE_NAMES",
        parameters = Map("names" -> "'duplicate_name'"))
    }
  }

  testWithSourceEvolution("enforcement enabled - unnamed source rejected") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .load() // Unnamed - throws error at load() time
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
      parameters = Map("sourceInfo" -> ".*"),
      matchPVals = true)
  }

  testWithSourceEvolution("enforcement enabled - all sources named succeeds") {
    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("alpha")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("beta")
      .load()

    // Should not throw - all sources are named
    val union = df1.union(df2)
    assert(union.isStreaming, "Union should be streaming")
  }

  // ==============
  // Helper Methods
  // ==============

  /**
   * Helper method to run tests with source evolution enabled.
   */
  def testWithSourceEvolution(testName: String, testTags: Tag*)(testBody: => Any): Unit = {
    test(testName, testTags: _*) {
      withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
        testBody
      }
    }
  }
}
