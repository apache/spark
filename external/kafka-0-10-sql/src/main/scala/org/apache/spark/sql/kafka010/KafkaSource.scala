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

import java.{util => ju}

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.kafka010.KafkaSource._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

/**
 * A [[Source]] that uses Kafka's own [[KafkaConsumer]] API to reads data from Kafka. The design
 * for this source is as follows.
 *
 * - The [[KafkaSourceOffset]] is the custom [[Offset]] defined for this source that contains
 *   a map of TopicPartition -> offset. Note that this offset is 1 + (available offset). For
 *   example if the last record in a Kafka topic "t", partition 2 is offset 5, then
 *   KafkaSourceOffset will contain TopicPartition("t", 2) -> 6. This is done keep it consistent
 *   with the semantics of `KafkaConsumer.position()`.
 *
 * - The [[ConsumerStrategy]] class defines which Kafka topics and partitions should be read
 *   by this source. These strategies directly correspond to the different consumption options
 *   in . This class is designed to return a configured
 *   [[KafkaConsumer]] that is used by the [[KafkaSource]] to query for the offsets.
 *   See the docs on [[org.apache.spark.sql.kafka010.KafkaSource.ConsumerStrategy]] for
 *   more details.
 *
 * - The [[KafkaSource]] written to do the following.
 *
 *  - As soon as the source is created, the pre-configured KafkaConsumer returned by the
 *    [[ConsumerStrategy]] is used to query the initial offsets that this source should
 *    start reading from. This used to create the first batch.
 *
 *   - `getOffset()` uses the KafkaConsumer to query the latest available offsets, which are
 *   returned as a [[KafkaSourceOffset]].
 *
 *   - `getBatch()` returns a DF that reads from the 'start offset' until the 'end offset' in
 *     for each partition. The end offset is excluded to be consistent with the semantics of
 *     [[KafkaSourceOffset]] and `KafkaConsumer.position()`.
 *
 *   - The DF returned is based on [[KafkaSourceRDD]] which is constructed such that the
 *     data from Kafka topic + partition is consistently read by the same executors across
 *     batches, and cached KafkaConsumers in the executors can be reused efficiently. See the
 *     docs on [[KafkaSourceRDD]] for more details.
 */
private[kafka010] case class KafkaSource(
    sqlContext: SQLContext,
    consumerStrategy: ConsumerStrategy[Array[Byte], Array[Byte]],
    executorKafkaParams: ju.Map[String, Object],
    sourceOptions: Map[String, String])
  extends Source with Logging {

  @transient private val consumer = consumerStrategy.createConsumer()
  @transient private val sc = sqlContext.sparkContext
  @transient private val initialPartitionOffsets = fetchPartitionOffsets(seekToLatest = false)
  logInfo(s"Initial offsets: " + initialPartitionOffsets)

  override def schema: StructType = KafkaSource.kafkaSchema

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    val offset = KafkaSourceOffset(fetchPartitionOffsets(seekToLatest = true))
    logDebug(s"GetOffset: $offset")
    Some(offset)
  }

  /**
   * Returns the data that is between the offsets [`start`, `end`), i.e. end is exclusive.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logDebug(s"GetBatch called with start = $start, end = $end")
    val untilPartitionOffsets = KafkaSourceOffset.getPartitionOffsets(end)
    val fromPartitionOffsets = start match {
      case Some(prevBatchEndOffset) =>
        KafkaSourceOffset.getPartitionOffsets(prevBatchEndOffset)
      case None =>
        initialPartitionOffsets
    }

    // Sort the partitions and current list of executors to consistently assign each partition
    // to the executor. This allows cached KafkaConsumers in the executors to be re-used to
    // read the same partition in every batch.
    val topicPartitionOrdering = new Ordering[TopicPartition] {
      override def compare(l: TopicPartition, r: TopicPartition): Int = {
        implicitly[Ordering[(String, Long)]].compare(
          (l.topic, l.partition),
          (r.topic, r.partition))
      }
    }
    val sortedTopicPartitions = untilPartitionOffsets.keySet.toSeq.sorted(topicPartitionOrdering)
    val sortedExecutors = getSortedExecutorList(sc)
    val numExecutors = sortedExecutors.size
    logDebug("Sorted executors: " + sortedExecutors.mkString(", "))
    val offsetRanges = sortedTopicPartitions.map { tp =>
      // If fromPartitionOffsets doesn't contain tp, then it's a new partition.
      // So use 0 as the start offset.
      val fromOffset = fromPartitionOffsets.get(tp).getOrElse(0L)
      val untilOffset = untilPartitionOffsets(tp)
      val preferredLoc = if (numExecutors > 0) {
        Some(sortedExecutors(positiveMod(tp.hashCode, numExecutors)))
      } else None
      KafkaSourceRDD.OffsetRange(tp, fromOffset, untilOffset, preferredLoc)
    }.toArray

    // Create a RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val rdd = new KafkaSourceRDD[Array[Byte], Array[Byte]](
      sc, executorKafkaParams, offsetRanges, sourceOptions).map { cr =>
        Row(cr.checksum, cr.key, cr.offset, cr.partition, cr.serializedKeySize,
          cr.serializedValueSize, cr.timestamp, cr.timestampType.id, cr.topic, cr.value)
    }

    logInfo("GetBatch: " + offsetRanges.sortBy(_.topicPartition.toString).mkString(", "))
    sqlContext.createDataFrame(rdd, schema)
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = synchronized {
    consumer.close()
  }

  override def toString(): String = s"KafkaSource[$consumerStrategy]"

  private def fetchPartitionOffsets(seekToLatest: Boolean): Map[TopicPartition, Long] = {
    synchronized {
      logTrace("\tPolling")
      consumer.poll(0)
      val partitions = consumer.assignment()
      consumer.pause(partitions)
      logDebug(s"\tPartitioned assigned to consumer: $partitions")
      if (seekToLatest) {
        consumer.seekToEnd(partitions)
        logDebug("\tSeeked to the end")
      }
      logTrace("Getting positions")
      val partitionToOffsets = partitions.asScala.map(p => p -> consumer.position(p))
      logDebug(s"Got positions $partitionToOffsets")
      partitionToOffsets.toMap
    }
  }

  private def positiveMod(a: Long, b: Int): Int = ((a % b).toInt + b) % b
}

/** Companion object for the [[KafkaSource]]. */
private[kafka010] object KafkaSource {

  def kafkaSchema: StructType = StructType(Seq(
    StructField("checksum", LongType),
    StructField("key", BinaryType),
    StructField("offset", LongType),
    StructField("partition", IntegerType),
    StructField("serializedKeySize", IntegerType),
    StructField("serializedValueSize", IntegerType),
    StructField("timestamp", LongType),
    StructField("timestampType", IntegerType),
    StructField("topic", StringType),
    StructField("value", BinaryType)
  ))

  sealed trait ConsumerStrategy[K, V] {
    def createConsumer(): Consumer[K, V]
  }

  case class SubscribeStrategy[K, V](topics: Seq[String], kafkaParams: ju.Map[String, Object])
    extends ConsumerStrategy[K, V] {
    override def createConsumer(): Consumer[K, V] = {
      val consumer = new KafkaConsumer[K, V](kafkaParams)
      consumer.subscribe(topics.asJava)
      consumer.poll(0)
      consumer
    }

    override def toString: String = s"Subscribe[${topics.mkString(", ")}]"
  }

  case class SubscribePatternStrategy[K, V](
    topicPattern: String, kafkaParams: ju.Map[String, Object])
    extends ConsumerStrategy[K, V] {
    override def createConsumer(): Consumer[K, V] = {
      val consumer = new KafkaConsumer[K, V](kafkaParams)
      consumer.subscribe(
        ju.regex.Pattern.compile(topicPattern),
        new NoOpConsumerRebalanceListener())
      consumer.poll(0)
      consumer
    }

    override def toString: String = s"SubscribePattern[$topicPattern]"
  }

  def getSortedExecutorList(sc: SparkContext): Array[String] = {
    def compare(a: ExecutorCacheTaskLocation, b: ExecutorCacheTaskLocation): Boolean = {
      if (a.host == b.host) { a.executorId > b.executorId } else { a.host > b.host }
    }

    val bm = sc.env.blockManager
    bm.master.getPeers(bm.blockManagerId).toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compare)
      .map(_.toString)
  }
}


/** An [[Offset]] for the [[KafkaSource]]. */
private[kafka010]
case class KafkaSourceOffset(partitionToOffsets: Map[TopicPartition, Long]) extends Offset {
  /**
   * Returns a negative integer, zero, or a positive integer as this object is less than, equal to,
   * or greater than the specified object.
   */
  override def compareTo(other: Offset): Int = other match {
    case KafkaSourceOffset(otherOffsets) =>
      val allTopicAndPartitions = (this.partitionToOffsets.keySet ++ otherOffsets.keySet).toSeq

      val comparisons = allTopicAndPartitions.map { tp =>
        (this.partitionToOffsets.get(tp), otherOffsets.get(tp)) match {
          case (Some(a), Some(b)) =>
            if (a < b) {
              -1
            } else if (a > b) {
              1
            } else {
              0
            }
          case (None, _) => -1
          case (_, None) => 1
        }
      }
      val nonZeroSigns = comparisons.filter { _ != 0 }.toSet
      nonZeroSigns.size match {
        case 0 => 0 // if both empty or only 0s
        case 1 => nonZeroSigns.head // if there are only (0s and 1s) or (0s and -1s)
        case _ => // there are both 1s and -1s
          throw new IllegalArgumentException(
            s"Invalid comparison between non-linear histories: $this <=> $other")
      }

    case _ =>
      throw new IllegalArgumentException(s"Cannot compare $this <=> $other")
  }

  override def toString(): String = {
    partitionToOffsets.toSeq.sortBy(_._1.toString).mkString("[", ", ", "]")
  }
}

/** Companion object of the [[KafkaSourceOffset]] */
private[kafka010] object KafkaSourceOffset {

  def getPartitionOffsets(offset: Offset): Map[TopicPartition, Long] = {
    offset match {
      case o: KafkaSourceOffset => o.partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
    }
  }

  /**
   * Returns [[KafkaSourceOffset]] from a variable sequence of (topic, partitionId, offset)
   * tuples.
   */
  def apply(offsetTuples: (String, Int, Long)*): KafkaSourceOffset = {
    KafkaSourceOffset(offsetTuples.map { case(t, p, o) => (new TopicPartition(t, p), o) }.toMap)
  }
}


/**
 * The provider class for the [[KafkaSource]]. This provider is designed such that it throws
 * IllegalArgumentException when the Kafka Dataset is created, so that it can catch
 * missing options even before the query is started.
 */
private[kafka010] class KafkaSourceProvider extends StreamSourceProvider
  with DataSourceRegister with Logging {
  private val strategyOptionNames = Set("subscribe", "subscribepattern")

  /** Class to conveniently update Kafka config params, while logging the changes */
  private case class ConfigUpdater(module: String, kafkaParams: Map[String, String]) {
    private val map = new ju.HashMap[String, Object](kafkaParams.asJava)
    def set(key: String, value: Object): this.type = {
      map.put(key, value)
      logInfo(s"$module: Set $key to $value, earlier value: ${kafkaParams.get(key).getOrElse("")}")
      this
    }

    def setIfUnset(key: String, value: Object): ConfigUpdater = {
      if (!map.containsKey(key)) {
        map.put(key, value)
        logInfo(s"$module: Set $key to $value")
      }
      this
    }

    def build(): ju.Map[String, Object] = map
  }

  /**
   * Returns the name and schema of the source. In addition, it also verifies whether the options
   * are correct and sufficient to create the [[KafkaSource]] when the query is started.
   */
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    validateOptions(parameters)
    ("kafka", KafkaSource.kafkaSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    validateOptions(parameters)
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
    val specifiedKafkaParams =
      parameters
        .keySet
        .filter(_.toLowerCase.startsWith("kafka."))
        .map { k => k.drop(6).toString -> parameters(k) }
        .toMap

    val deserClassName = classOf[ByteArrayDeserializer].getName

    val kafkaParamsForStrategy =
      ConfigUpdater("source", specifiedKafkaParams)
        .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
        .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)
        .build()

    val kafkaParamsForExecutors =
      ConfigUpdater("source", specifiedKafkaParams)
        .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
        .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)

        // So that consumers in executors never throw NoOffsetForPartitionException
        .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

        // So that consumers in executors do not mess with user-specified group id
        .set(ConsumerConfig.GROUP_ID_CONFIG,
          "spark-executor-" + specifiedKafkaParams(ConsumerConfig.GROUP_ID_CONFIG))

        // So that consumers in executors no keep committing offsets unnecessaribly
        .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

        // If buffer config is not set, it to reasonable value to work around
        // buffer issues (see KAFKA-3135)
        .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
        .build()

    val strategy = caseInsensitiveParams.find(x => strategyOptionNames.contains(x._1)).get match {
      case ("subscribe", value) =>
        SubscribeStrategy[Array[Byte], Array[Byte]](
          value.split(",").map(_.trim()).filter(_.nonEmpty),
          kafkaParamsForStrategy)
      case ("subscribepattern", value) =>
        SubscribePatternStrategy[Array[Byte], Array[Byte]](
          value.trim(),
          kafkaParamsForStrategy)
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown option")
    }

    new KafkaSource(sqlContext, strategy, kafkaParamsForExecutors, parameters)
  }

  private def validateOptions(parameters: Map[String, String]): Unit = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
    val specifiedStrategies =
      caseInsensitiveParams.filter { case(k, _) => strategyOptionNames.contains(k) }.toSeq
    if (specifiedStrategies.isEmpty) {
      throw new IllegalArgumentException(
        "One of the following options must be specified for Kafka source: "
          + strategyOptionNames.mkString(", ") + ". See docs for more details.")
    } else if (specifiedStrategies.size > 1) {
      throw new IllegalArgumentException(
        "Only one of the following options can be specified for Kafka source: "
          + strategyOptionNames.mkString(", ") + ". See docs for more details.")
    }

    val strategy = caseInsensitiveParams.find(x => strategyOptionNames.contains(x._1)).get match {
      case ("subscribe", value) =>
        val topics = value.split(",").map(_.trim).filter(_.nonEmpty)
        if (topics.isEmpty) {
          throw new IllegalArgumentException(
            "No topics to subscribe to as specified value for option " +
              s"'subscribe' is '$value'")
        }
      case ("subscribepattern", value) =>
        val pattern = caseInsensitiveParams("subscribepattern").trim()
        if (pattern.isEmpty) {
          throw new IllegalArgumentException(
            "Pattern to subscribe is empty as specified value for option " +
              s"'subscribePattern' is '$value'")
        }
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown option")
    }

    if (!caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}")) {
      throw new IllegalArgumentException(
        "Option 'kafka.bootstrap.servers' must be specified for configuring Kafka consumer")
    }

    if (!caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.GROUP_ID_CONFIG}")) {
      throw new IllegalArgumentException(
        "Option 'kafka.group.id' must be specified for configuring Kafka consumer")
    }
  }

  override def shortName(): String = "kafka"
}
