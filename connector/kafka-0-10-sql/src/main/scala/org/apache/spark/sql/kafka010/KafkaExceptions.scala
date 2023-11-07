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

import org.apache.spark.{ErrorClassesJsonReader, SparkException}

object KafkaExceptions {
  private val errorClassesJsonReader: ErrorClassesJsonReader =
    new ErrorClassesJsonReader(
      Seq(getClass.getClassLoader.getResource("error/kafka-error-classes.json")))

  def mismatchedTopicPartitionsBetweenEndOffsetAndPrefetched(
      tpsForPrefetched: Set[TopicPartition],
      tpsForEndOffset: Set[TopicPartition]): SparkException = {
    val errMsg = errorClassesJsonReader.getErrorMessage(
      "MISMATCHED_TOPIC_PARTITIONS_BETWEEN_END_OFFSET_AND_PREFETCHED",
      Map(
        "tpsForPrefetched" -> tpsForPrefetched.toString(),
        "tpsForEndOffset" -> tpsForEndOffset.toString()
      )
    )
    new SparkException(errMsg)
  }

  def endOffsetHasGreaterOffsetForTopicPartitionThanPrefetched(
      prefetchedOffset: Map[TopicPartition, Long],
      endOffset: Map[TopicPartition, Long]): SparkException = {
    val errMsg = errorClassesJsonReader.getErrorMessage(
      "END_OFFSET_HAS_GREATER_OFFSET_FOR_TOPIC_PARTITION_THAN_PREFETCHED",
      Map(
        "prefetchedOffset" -> prefetchedOffset.toString(),
        "endOffset" -> endOffset.toString()
      )
    )
    new SparkException(errMsg)
  }

  def lostTopicPartitionsInEndOffsetWithTriggerAvailableNow(
      tpsForLatestOffset: Set[TopicPartition],
      tpsForEndOffset: Set[TopicPartition]): SparkException = {
    val errMsg = errorClassesJsonReader.getErrorMessage(
      "LOST_TOPIC_PARTITIONS_IN_END_OFFSET_WITH_TRIGGER_AVAILABLENOW",
      Map(
        "tpsForLatestOffset" -> tpsForLatestOffset.toString(),
        "tpsForEndOffset" -> tpsForEndOffset.toString()
      )
    )
    new SparkException(errMsg)
  }

  def endOffsetHasGreaterOffsetForTopicPartitionThanLatestWithTriggerAvailableNow(
      latestOffset: Map[TopicPartition, Long],
      endOffset: Map[TopicPartition, Long]): SparkException = {
    val errMsg = errorClassesJsonReader.getErrorMessage(
      "END_OFFSET_HAS_GREATER_OFFSET_FOR_TOPIC_PARTITION_THAN_LATEST_WITH_TRIGGER_AVAILABLENOW",
      Map(
        "latestOffset" -> latestOffset.toString(),
        "endOffset" -> endOffset.toString()
      )
    )
    new SparkException(errMsg)
  }
}
