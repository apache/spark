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

import scala.jdk.CollectionConverters._

import org.apache.kafka.common.TopicPartition

import org.apache.spark.{ErrorClassesJsonReader, SparkException, SparkThrowable}

private object KafkaExceptionsHelper {
  val errorClassesJsonReader: ErrorClassesJsonReader =
    new ErrorClassesJsonReader(
      // Note that though we call them "error classes" here, the proper name is "error conditions",
      // hence why the name of the JSON file is different. We will address this inconsistency as
      // part of this ticket: https://issues.apache.org/jira/browse/SPARK-47429
      Seq(getClass.getClassLoader.getResource("error/kafka-error-conditions.json")))
}

object KafkaExceptions {
  def mismatchedTopicPartitionsBetweenEndOffsetAndPrefetched(
      tpsForPrefetched: Set[TopicPartition],
      tpsForEndOffset: Set[TopicPartition]): SparkException = {
    val errMsg = KafkaExceptionsHelper.errorClassesJsonReader.getErrorMessage(
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
    val errMsg = KafkaExceptionsHelper.errorClassesJsonReader.getErrorMessage(
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
    val errMsg = KafkaExceptionsHelper.errorClassesJsonReader.getErrorMessage(
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
    val errMsg = KafkaExceptionsHelper.errorClassesJsonReader.getErrorMessage(
      "END_OFFSET_HAS_GREATER_OFFSET_FOR_TOPIC_PARTITION_THAN_LATEST_WITH_TRIGGER_AVAILABLENOW",
      Map(
        "latestOffset" -> latestOffset.toString(),
        "endOffset" -> endOffset.toString()
      )
    )
    new SparkException(errMsg)
  }

  def couldNotReadOffsetRange(
      startOffset: Long,
      endOffset: Long,
      topicPartition: TopicPartition,
      groupId: String,
      cause: Throwable): KafkaIllegalStateException = {
    new KafkaIllegalStateException(
      errorClass = "KAFKA_DATA_LOSS.COULD_NOT_READ_OFFSET_RANGE",
      messageParameters = Map(
        "startOffset" -> startOffset.toString,
        "endOffset" -> endOffset.toString,
        "topicPartition" -> topicPartition.toString,
        "groupId" -> Option(groupId).getOrElse("null")),
      cause = cause)
  }

  def startOffsetReset(
      topicPartition: TopicPartition,
      offset: Long,
      fetchedOffset: Long): KafkaIllegalStateException = {
    new KafkaIllegalStateException(
      errorClass = "KAFKA_DATA_LOSS.START_OFFSET_RESET",
      messageParameters = Map(
        "topicPartition" -> topicPartition.toString,
        "offset" -> offset.toString,
        "fetchedOffset" -> fetchedOffset.toString))
  }

  def initialOffsetNotFoundForPartitions(
      partitions: Set[TopicPartition]): KafkaIllegalStateException = {
    new KafkaIllegalStateException(
      errorClass = "KAFKA_DATA_LOSS.INITIAL_OFFSET_NOT_FOUND_FOR_PARTITIONS",
      messageParameters = Map("partitions" -> partitions.toString))
  }

  def addedPartitionDoesNotStartFromZero(
      topicPartition: TopicPartition,
      startOffset: Long): KafkaIllegalStateException = {
    new KafkaIllegalStateException(
      errorClass = "KAFKA_DATA_LOSS.ADDED_PARTITION_DOES_NOT_START_FROM_OFFSET_ZERO",
      messageParameters =
        Map("topicPartition" -> topicPartition.toString, "startOffset" -> startOffset.toString))
  }

  def partitionsDeleted(
      partitions: Set[TopicPartition],
      groupIdConfigName: Option[String]): KafkaIllegalStateException = {
    groupIdConfigName match {
      case Some(config) =>
        new KafkaIllegalStateException(
          errorClass = "KAFKA_DATA_LOSS.PARTITIONS_DELETED_AND_GROUP_ID_CONFIG_PRESENT",
          messageParameters = Map("partitions" -> partitions.toString, "groupIdConfig" -> config))
      case None =>
        new KafkaIllegalStateException(
          errorClass = "KAFKA_DATA_LOSS.PARTITIONS_DELETED",
          messageParameters = Map("partitions" -> partitions.toString))
    }
  }

  def partitionOffsetChanged(
      topicPartition: TopicPartition,
      prevOffset: Long,
      newOffset: Long): KafkaIllegalStateException = {
    new KafkaIllegalStateException(
      errorClass = "KAFKA_DATA_LOSS.PARTITION_OFFSET_CHANGED",
      messageParameters = Map(
        "topicPartition" -> topicPartition.toString,
        "prevOffset" -> prevOffset.toString,
        "newOffset" -> newOffset.toString))
  }

  def startOffsetDoesNotMatchAssigned(
      specifiedPartitions: Set[TopicPartition],
      assignedPartitions: Set[TopicPartition]): KafkaIllegalStateException = {
    new KafkaIllegalStateException(
      errorClass = "KAFKA_START_OFFSET_DOES_NOT_MATCH_ASSIGNED",
      messageParameters = Map(
        "specifiedPartitions" -> specifiedPartitions.toString,
        "assignedPartitions" -> assignedPartitions.toString))
  }

  def timestampOffsetDoesNotMatchAssigned(
      isStartingOffsets: Boolean,
      specifiedPartitions: Set[TopicPartition],
      assignedPartitions: Set[TopicPartition]): KafkaIllegalStateException = {
    new KafkaIllegalStateException(
      errorClass = "KAFKA_TIMESTAMP_OFFSET_DOES_NOT_MATCH_ASSIGNED",
      messageParameters = Map(
        "position" -> (if (isStartingOffsets) "start" else "end"),
        "specifiedPartitions" -> specifiedPartitions.toString,
        "assignedPartitions" -> assignedPartitions.toString))
  }

  def nullTopicInData(): KafkaIllegalStateException = {
    new KafkaIllegalStateException(
      errorClass = "KAFKA_NULL_TOPIC_IN_DATA",
      messageParameters = Map.empty)
  }
}

/**
 * Illegal state exception thrown with an error class.
 */
private[kafka010] class KafkaIllegalStateException(
    errorClass: String,
    messageParameters: Map[String, String],
    cause: Throwable = null)
  extends IllegalStateException(
    KafkaExceptionsHelper.errorClassesJsonReader.getErrorMessage(
      errorClass, messageParameters), cause)
  with SparkThrowable {

  override def getSqlState: String =
    KafkaExceptionsHelper.errorClassesJsonReader.getSqlState(errorClass)

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass
}
