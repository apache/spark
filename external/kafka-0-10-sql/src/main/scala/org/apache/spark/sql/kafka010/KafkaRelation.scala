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

import java.sql.Timestamp
import java.util.UUID

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String



private[kafka010] class KafkaRelation(
    override val sqlContext: SQLContext,
    strategy: ConsumerStrategy,
    sourceOptions: Map[String, String],
    specifiedKafkaParams: Map[String, String],
    failOnDataLoss: Boolean,
    startingOffsets: KafkaOffsetRangeLimit,
    endingOffsets: KafkaOffsetRangeLimit)
    extends BaseRelation with PrunedFilteredScan with Logging {
  assert(startingOffsets != LatestOffsetRangeLimit,
    "Starting offset not allowed to be set to latest offsets.")
  assert(endingOffsets != EarliestOffsetRangeLimit,
    "Ending offset not allowed to be set to earliest offsets.")

  private val pollTimeoutMs = sourceOptions.getOrElse(
    "kafkaConsumer.pollTimeoutMs",
    (sqlContext.sparkContext.conf.getTimeAsSeconds(
      "spark.network.timeout",
      "120s") * 1000L).toString
  ).toLong

  override def schema: StructType = KafkaOffsetReader.kafkaSchema

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Each running query should use its own group id. Otherwise, the query may be only assigned
    // partial data since Kafka will assign partitions to multiple consumers having the same group
    // id. Hence, we should generate a unique id for each query.
    val uniqueGroupId = s"spark-kafka-relation-${UUID.randomUUID}"

    val kafkaOffsetReader = new KafkaOffsetReader(
      strategy,
      KafkaSourceProvider.kafkaParamsForDriver(specifiedKafkaParams),
      sourceOptions,
      driverGroupIdPrefix = s"$uniqueGroupId-driver")

    // Leverage the KafkaReader to obtain the relevant partition offsets
    val offsetRanges = new PartitionOffsetCalculator(filters, kafkaOffsetReader)
        .calculateOffsets()
        .checkStartEndHavingSamePartitions()
        .calculateRDDOffsetRanges(filters)

    logInfo("GetBatch generating RDD of offset range: " +
      offsetRanges.sortBy(_.topicPartition.toString).mkString(", "))

    // Create an RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val executorKafkaParams =
      KafkaSourceProvider.kafkaParamsForExecutors(specifiedKafkaParams, uniqueGroupId)
    val requiredColumnsList = requiredColumns.toList
    val rdd = new KafkaSourceRDD(
      sqlContext.sparkContext, executorKafkaParams, offsetRanges,
      pollTimeoutMs, failOnDataLoss, reuseKafkaConsumer = false).map { cr =>
        val columns = new ConsumerRecordInspector(cr).getValues(requiredColumnsList)
        InternalRow.fromSeq(columns)
      }
    val schemaProjected = StructType(requiredColumns.map{schema(_)})
    sqlContext.internalCreateDataFrame(rdd.setName("kafka"), schemaProjected).rdd
  }

  override def toString: String =
    s"KafkaRelation(strategy=$strategy, start=$startingOffsets, end=$endingOffsets)"

  case class PartitionOffsetsRange(start: Map[TopicPartition, Long],
                                   end: Map[TopicPartition, Long]) {

    def checkStartEndHavingSamePartitions(): PartitionOffsetsRange = {
      // Obtain topicPartitions in both from and until partition offset, ignoring
      // topic partitions that were added and/or deleted between the two above calls.
      if (start.keySet != end.keySet) {
        implicit val topicOrdering: Ordering[TopicPartition] = Ordering.by(t => t.topic())
        val fromTopics = start.keySet.toList.sorted.mkString(",")
        val untilTopics = end.keySet.toList.sorted.mkString(",")
        throw new IllegalStateException("different topic partitions " +
          s"for starting offsets topics[${fromTopics}] and " +
          s"ending offsets topics[${untilTopics}]")
      }
      this
    }

    def calculateRDDOffsetRanges(filters: Array[Filter]):
        Array[KafkaSourceRDDOffsetRange] = {
      end.keySet.map { tp =>
        val fromOffset = start.getOrElse(tp,
          // This should not happen since topicPartitions contains all partitions not in
          // fromPartitionOffsets
          throw new IllegalStateException(s"$tp doesn't have a from offset"))
        var untilOffset = end(tp)
        untilOffset = if (areOffsetsInLine(fromOffset, untilOffset)) {
          untilOffset
        } else {
          logWarning(s"Predicate(s) out of offset range for partition: $tp. " +
            s"Range start: $startingOffsets, range end: $endingOffsets. " +
            s"Predicates: ${filters.mkString(",")}")
          fromOffset
        }
        KafkaSourceRDDOffsetRange(tp, fromOffset, untilOffset, None)
      }.toArray
    }

    private def areOffsetsInLine(fromOffset: Long, untilOffset: Long): Boolean = {
      untilOffset >= fromOffset || untilOffset < 0 || fromOffset < 0
    }
  }

  case class PartitionOffsetRangeOpts(start: Map[TopicPartition, Option[Long]],
                                      end: Map[TopicPartition, Option[Long]]) {

    /**
    * When we request timestamp -> KafkaOffset mapping,
    * Kafka may return null for some partitions.
    * This means that specific partition doesn't contain any record
    * which timestamp is equal or greater to the given timestamp.
    * If any of the start or the end offset is None, then user provided
    * a predicate which doesn't match any record in the partition.
    * We change both start and end to 0, so given partition will not serve data.
    *
    * @return specific partition offsets (not Option)
    */
    def invalidateNoneOffsets(): PartitionOffsetsRange = {
      val merged = start.map { case (k, v) => k -> ((v, end(k)))}
      val invalidated = merged.map {
        case(k, (None, _)) => k -> ((0L, 0L))
        case(k, (_, None)) => k -> ((0L, 0L))
        case(k, (s, e)) => k -> ((s.get, e.get))
      }
      PartitionOffsetsRange(
        start = invalidated.map{case(k, (s, _)) => k -> s},
        end = invalidated.map{case(k, (_, e)) => k -> e})
    }
  }

  class PartitionOffsetCalculator(
     filters: Array[Filter],
     kafkaReader: KafkaOffsetReader) {

    def calculateOffsets(): PartitionOffsetsRange = {
      try {
        calculateStartAndEndOffsets().invalidateNoneOffsets()
      } finally {
        kafkaReader.close()
      }
    }

    private def calculateStartAndEndOffsets(): PartitionOffsetRangeOpts = {
      PartitionOffsetRangeOpts(
        start = getStartingPartitionOffsets(filters),
        end = getEndingPartitionOffsets(filters)
      )
    }

    private def getEndingPartitionOffsets(
       filters: Array[Filter]): Map[TopicPartition, Option[Long]] = {

      val offsetsByDSOpts = getOffsetsByDataSourceOpts(endingOffsets)
      mergeWithEndingOffsetsByFilter(offsetsByDSOpts)
    }

    private def getStartingPartitionOffsets(
       filters: Array[Filter]): Map[TopicPartition, Option[Long]] = {

      val offsetsByDSOpts = getOffsetsByDataSourceOpts(startingOffsets)
      mergeWithStartingOffsetsByFilter(offsetsByDSOpts)
    }

    /**
    * Old versions of Kafka that doesn't support timestamp APIs will
    * log error message but continue to work without pushdown
    */
    private def mergeWithEndingOffsetsByFilter(offsetsByDSOpts: Map[TopicPartition, Long]) = {
      try {
        mergeWithEndingOffsetsByFilterUnsafe(offsetsByDSOpts)
      } catch { case e: Exception =>
          handleTimestampFetchError(offsetsByDSOpts, e)
      }
    }

    /**
    * Old versions of Kafka that doesn't support timestamp APIs will
    * log error message but continue to work without pushdown
    */
    private def mergeWithStartingOffsetsByFilter(offsetsByDSOpts: Map[TopicPartition, Long]) = {
      try {
        mergeWithStartingOffsetsByFilterUnsafe(offsetsByDSOpts)
      } catch { case e: Exception =>
          handleTimestampFetchError(offsetsByDSOpts, e)
      }
    }

    private def handleTimestampFetchError(
         offsetsByDSOpts: Map[TopicPartition, Long], e: Exception) = {
      logError(
        "Couldn't fetch offsetForTimestamps from Kafka." +
          "Timestamp will not be pushed-down to kafka.", e)
      offsetsByDSOpts.mapValues(Option(_))
    }

    private def getOffsetsByDataSourceOpts(
        kafkaOffsets: KafkaOffsetRangeLimit): Map[TopicPartition, Long] = {
      def validateTopicPartitions(partitions: Set[TopicPartition],
          partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
        assert(partitions == partitionOffsets.keySet,
          "If startingOffsets contains specific offsets, you must specify all TopicPartitions.\n" +
            "Use -1 for latest, -2 for earliest, if you don't care.\n" +
            s"Specified: ${partitionOffsets.keySet} Assigned: ${partitions}")
        logDebug(s"Partitions assigned to consumer: $partitions. Seeking to $partitionOffsets")
        partitionOffsets
      }
      val partitions = kafkaReader.fetchTopicPartitions()
      // Obtain TopicPartition offsets with late binding support
      kafkaOffsets match {
        case EarliestOffsetRangeLimit => partitions.map {
          case tp => tp -> KafkaOffsetRangeLimit.EARLIEST
        }.toMap
        case LatestOffsetRangeLimit => partitions.map {
          case tp => tp -> KafkaOffsetRangeLimit.LATEST
        }.toMap
        case SpecificOffsetRangeLimit(partitionOffsets) =>
          validateTopicPartitions(partitions, partitionOffsets)
      }
    }

    private val TIMESTAMP_ATTR = "timestamp"

    private def mergeWithStartingOffsetsByFilterUnsafe(
        toMerge : Map[TopicPartition, Long]): Map[TopicPartition, Option[Long]] = {
      // Generates array of Tuples (topicPartition -> Option(filterOffset)) for each filter
      val partitionWithOffsetArray: Array[(TopicPartition, Option[Long])] = filters.flatMap {
        case op: GreaterThan if op.attribute == TIMESTAMP_ATTR =>
          val times = toMerge.map { case (tp, _) =>
            tp -> (op.value.asInstanceOf[Timestamp].getTime + 1)}
          kafkaReader.fetchOffsetsByTime(times)
        case op: EqualTo if op.attribute == TIMESTAMP_ATTR =>
          val times = toMerge.map { case (tp, _) =>
            tp -> op.value.asInstanceOf[Timestamp].getTime}
          kafkaReader.fetchOffsetsByTime(times)
        case op: GreaterThanOrEqual if op.attribute == TIMESTAMP_ATTR =>
          val times = toMerge.map { case (tp, _) =>
            tp -> op.value.asInstanceOf[Timestamp].getTime}
          kafkaReader.fetchOffsetsByTime(times)
        case _ => None
      }
      // Let's take the maximum of all filterOffsets per partition
      // If any partition have None assigned we need to return None
      val filterOffsets: Map[TopicPartition, Option[Long]] =
        partitionWithOffsetArray.groupBy(_._1)
          .mapValues(_.map{_._2}.maxBy(_.getOrElse(Long.MaxValue)))
      // Take maximum of filterOffsets and offsets from argument per partition
      toMerge.map {case (tp, offset) =>
        val filterOffset: Option[Long] = filterOffsets.getOrElse(tp, Some(offset))
        tp -> filterOffset.flatMap{o => Some(Math.max(offset, o))}
      }
    }

    private def mergeWithEndingOffsetsByFilterUnsafe(
       toMerge : Map[TopicPartition, Long]): Map[TopicPartition, Option[Long]] = {
      // Generates array of Tuples (topicPartition -> Option(filterOffset)) for each filter
      val partitionWithOffsetArray = filters.flatMap {
        case op: LessThan if op.attribute == TIMESTAMP_ATTR =>
          val times = toMerge.map { case (tp, _) =>
            tp -> op.value.asInstanceOf[Timestamp].getTime}
          kafkaReader.fetchOffsetsByTime(times)
        case op: LessThanOrEqual if op.attribute == TIMESTAMP_ATTR =>
          val times = toMerge.map { case (tp, _) =>
            tp -> (op.value.asInstanceOf[Timestamp].getTime + 1)}
          kafkaReader.fetchOffsetsByTime(times)
        case op: EqualTo if op.attribute == TIMESTAMP_ATTR =>
          val times = toMerge.map { case (tp, _) =>
            tp -> (op.value.asInstanceOf[Timestamp].getTime + 1)}
          kafkaReader.fetchOffsetsByTime(times)
        case _ => None
      }
      // Let's take the minimum of all filterOffsets per partition
      // Natural ordering of "min" will return None if any partition have None assigned
      val filterOffsets: Map[TopicPartition, Option[Long]] =
         partitionWithOffsetArray.groupBy(_._1).mapValues(_.map{_._2}.min)
      // Take minimum of filterOffsets and offsets from argument per partition
      toMerge.map {case (tp, offset) =>
        var newOffset: Option[Long] = filterOffsets.getOrElse(tp, Some(offset))
        if (isNotLatestOrEarliest(offset)) {
          newOffset = newOffset.flatMap{o => Option(Math.min(offset, o))}
        }
        tp -> newOffset
      }
    }

    private def isNotLatestOrEarliest(offset: Long): Boolean = {
      offset >= 0
    }
  }
}

class ConsumerRecordInspector(cr: ConsumerRecord[Array[Byte], Array[Byte]]) {
  def getValues(requiredColumns: List[String]) : Seq[Any] = {
    requiredColumns match {
      case "key"::rest => cr.key +: getValues(rest)
      case "value"::rest => cr.value +: getValues(rest)
      case "topic"::rest => UTF8String.fromString(cr.topic) +: getValues(rest)
      case "partition"::rest => cr.partition +: getValues(rest)
      case "offset"::rest => cr.offset +: getValues(rest)
      case "timestamp"::rest => DateTimeUtils.
        fromJavaTimestamp(new java.sql.Timestamp(cr.timestamp)) +: getValues(rest)
      case "timestampType"::rest => cr.timestampType.id +: getValues(rest)
      case Seq() => Seq.empty
    }
  }
}
