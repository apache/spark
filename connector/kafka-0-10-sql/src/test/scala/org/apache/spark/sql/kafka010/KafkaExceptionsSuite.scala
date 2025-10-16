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

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkFunSuite

/**
 * Test suite for Kafka exceptions, particularly SQL state prioritization.
 */
class KafkaExceptionsSuite extends SparkFunSuite {

  test("Custom SQL state takes precedence - KafkaIllegalStateException") {
    // Test without custom SQL state - should fall back to error class reader
    val exceptionWithoutCustom = new KafkaIllegalStateException(
      errorClass = "KAFKA_DATA_LOSS.START_OFFSET_RESET",
      messageParameters = Map(
        "topicPartition" -> "test-0",
        "offset" -> "100",
        "fetchedOffset" -> "50"))

    // The error class reader should provide a SQL state from kafka-error-conditions.json
    assert(exceptionWithoutCustom.getSqlState != null,
      "Should use error class reader SQL state")

    // Test with custom SQL state - should return the custom one
    val exceptionWithCustom = new KafkaIllegalStateException(
      errorClass = "KAFKA_DATA_LOSS.START_OFFSET_RESET",
      messageParameters = Map(
        "topicPartition" -> "test-0",
        "offset" -> "100",
        "fetchedOffset" -> "50"),
      cause = null,
      sqlState = Some("CUSTOM"))

    assert(exceptionWithCustom.getSqlState == "CUSTOM",
      "Custom SQL state should take precedence over error class reader")

    // Test with None custom SQL state - should fall back to error class reader
    val exceptionWithNone = new KafkaIllegalStateException(
      errorClass = "KAFKA_DATA_LOSS.START_OFFSET_RESET",
      messageParameters = Map(
        "topicPartition" -> "test-0",
        "offset" -> "100",
        "fetchedOffset" -> "50"),
      cause = null,
      sqlState = None)

    val fallbackSqlState = exceptionWithNone.getSqlState
    assert(fallbackSqlState == exceptionWithoutCustom.getSqlState,
      "Should fall back to same error class reader SQL state when custom is None")
  }

  test("Custom SQL state takes precedence - KafkaIllegalArgumentException") {
    // Test without custom SQL state - should fall back to error class reader
    val exceptionWithoutCustom = new KafkaIllegalArgumentException(
      errorClass = "MISMATCHED_TOPIC_PARTITIONS_BETWEEN_START_OFFSET_AND_END_OFFSET",
      messageParameters = Map(
        "tpsForStartOffset" -> "tp1, tp2",
        "tpsForEndOffset" -> "tp3, tp4"))

    // The error class reader should provide a SQL state
    assert(exceptionWithoutCustom.getSqlState != null,
      "Should use error class reader SQL state")

    // Test with custom SQL state - should return the custom one
    val exceptionWithCustom = new KafkaIllegalArgumentException(
      errorClass = "MISMATCHED_TOPIC_PARTITIONS_BETWEEN_START_OFFSET_AND_END_OFFSET",
      messageParameters = Map(
        "tpsForStartOffset" -> "tp1, tp2",
        "tpsForEndOffset" -> "tp3, tp4"),
      cause = null,
      sqlState = Some("CUST1"))

    assert(exceptionWithCustom.getSqlState == "CUST1",
      "Custom SQL state should take precedence over error class reader")

    // Test with None custom SQL state - should fall back to error class reader
    val exceptionWithNone = new KafkaIllegalArgumentException(
      errorClass = "MISMATCHED_TOPIC_PARTITIONS_BETWEEN_START_OFFSET_AND_END_OFFSET",
      messageParameters = Map(
        "tpsForStartOffset" -> "tp1, tp2",
        "tpsForEndOffset" -> "tp3, tp4"),
      cause = null,
      sqlState = None)

    val fallbackSqlState = exceptionWithNone.getSqlState
    assert(fallbackSqlState == exceptionWithoutCustom.getSqlState,
      "Should fall back to same error class reader SQL state when custom is None")
  }

  test("SQL state consistency across different Kafka exception types") {
    val customSqlState = "99999"

    val illegalStateException = new KafkaIllegalStateException(
      errorClass = "KAFKA_NULL_TOPIC_IN_DATA",
      messageParameters = Map.empty,
      cause = null,
      sqlState = Some(customSqlState))

    val illegalArgumentException = new KafkaIllegalArgumentException(
      errorClass = "UNRESOLVED_START_OFFSET_GREATER_THAN_END_OFFSET",
      messageParameters = Map(
        "offsetType" -> "offset",
        "startOffset" -> "100",
        "endOffset" -> "50",
        "topic" -> "test",
        "partition" -> "0"),
      cause = null,
      sqlState = Some(customSqlState))

    assert(illegalStateException.getSqlState == customSqlState)
    assert(illegalArgumentException.getSqlState == customSqlState)
    assert(illegalStateException.getSqlState == illegalArgumentException.getSqlState,
      "Both exception types should return the same custom SQL state")
  }
}
