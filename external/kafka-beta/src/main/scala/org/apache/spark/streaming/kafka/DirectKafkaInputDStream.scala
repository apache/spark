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

package org.apache.spark.streaming.kafka

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag

import java.{ util => ju }

import org.apache.kafka.clients.consumer.{
  ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer
}
import org.apache.kafka.common.{ PartitionInfo, TopicPartition }

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator

import scala.collection.JavaConverters._

/**
 *  A stream of {@link org.apache.spark.streaming.kafka.KafkaRDD} where
 * each given Kafka topic/partition corresponds to an RDD partition.
 * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
 *  of messages
 * per second that each '''partition''' will accept.
 * Starting offsets are specified in advance,
 * and this DStream is not responsible for committing offsets,
 * so that you can control exactly-once semantics.
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
 * configuration parameters</a>.
 *   Requires  "bootstrap.servers" to be set with Kafka broker(s),
 *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
 */

class DirectKafkaInputDStream[K: ClassTag, V: ClassTag] private[spark] (
    _ssc: StreamingContext,
    val driverKafkaParams: ju.Map[String, Object],
    val executorKafkaParams: ju.Map[String, Object],
    preferredHosts: ju.Map[TopicPartition, String]
  ) extends InputDStream[ConsumerRecord[K,V]](_ssc) with Logging {

  import DirectKafkaInputDStream.{
    PartitionAssignment, Assigned, Subscribed, PatternSubscribed, Unassigned
  }

  assert(1 ==
    driverKafkaParams.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG).asInstanceOf[Int],
    ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG +
      " must be set to 1 for driver kafka params, because the driver should not consume messages")

  assert(false ==
    driverKafkaParams.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).asInstanceOf[Boolean],
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG +
      " must be set to false for driver kafka params, else offsets may commit before processing")

  @transient private var kc: KafkaConsumer[K, V] = null
  private var partitionAssignment: PartitionAssignment = Unassigned
  protected def consumer(): KafkaConsumer[K, V] = this.synchronized {
    if (null == kc) {
      kc = new KafkaConsumer(driverKafkaParams)
      assignPartitions(partitionAssignment)
    }
    kc
  }
  consumer()

  private def listenerFor(className: String): ConsumerRebalanceListener =
    Class.forName(className)
      .newInstance()
      .asInstanceOf[ConsumerRebalanceListener]

  private def assignPartitions(pa: PartitionAssignment): Unit = this.synchronized {
    // using kc directly because consumer() calls this method
    pa match {
      case Assigned(partitions) =>
        kc.assign(partitions)
      case Subscribed(topics, className) =>
        kc.subscribe(topics, listenerFor(className))
      case PatternSubscribed(pattern, className) =>
        kc.subscribe(pattern, listenerFor(className))
      case Unassigned =>
    }

    this.partitionAssignment = pa
  }

  /** Manually assign a list of partitions */
  def assign(partitions: ju.List[TopicPartition]): Unit = {
    assignPartitions(Assigned(partitions))
  }

  /** Subscribe to the given list of topics to get dynamically assigned partitions */
  def subscribe(topics: ju.List[String]): Unit = {
    assignPartitions(Subscribed(topics))
  }

  /** Subscribe to the given list of topics to get dynamically assigned partitions */
  def subscribe(
    topics: ju.List[String],
    consumerRebalanceListenerClassName: String): Unit = {
    assignPartitions(Subscribed(topics, consumerRebalanceListenerClassName))
  }

  /** Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
    * The pattern matching will be done periodically against topics existing at the time of check.
    */
  def subscribe(pattern: ju.regex.Pattern): Unit = {
    assignPartitions(PatternSubscribed(pattern))
  }

  /** Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
    * The pattern matching will be done periodically against topics existing at the time of check.
    */
  def subscribe(
    pattern: ju.regex.Pattern,
    consumerRebalanceListenerClassName: String): Unit = {
    assignPartitions(PatternSubscribed(pattern, consumerRebalanceListenerClassName))
  }

  /** Get the set of partitions currently assigned to the underlying consumer */
  def assignment(): ju.Set[TopicPartition] = this.synchronized {
    consumer.assignment()
  }

  /** Get metadata about the partitions for a given topic. */
  def partitionsFor(topic: String): ju.List[PartitionInfo] = this.synchronized {
    consumer.partitionsFor(topic)
  }

  private val pollTimeout =
    context.sparkContext.getConf.getLong("spark.streaming.kafka.consumer.poll.ms", 10)
  /** Necessary to fetch metadata and update subscriptions, won't actually return useful data */
  def poll(): Unit = poll(pollTimeout)

  /** Necessary to fetch metadata and update subscriptions, won't actually return useful data */
  def poll(timeout: Long): Unit = this.synchronized {
    consumer.poll(pollTimeout)
  }

  // TODO is there a better way to distinguish between
  // - want to use leader brokers (null map)
  // - don't care, use consistent executor (empty map)
  // - want to use specific hosts (non-null, non-empty map)
  private def getPreferredHosts: ju.Map[TopicPartition, String] = {
    if (null != preferredHosts) {
      preferredHosts
    } else {
      val result = new ju.HashMap[TopicPartition, String]()
      val hosts = new ju.HashMap[TopicPartition, String]()
      val assignments = assignment().iterator()
      while (assignments.hasNext()) {
        val a = assignments.next()
        if (null == hosts.get(a)) {
          val infos = partitionsFor(a.topic).iterator()
          while (infos.hasNext()) {
            val i = infos.next()
            hosts.put(new TopicPartition(i.topic(), i.partition()), i.leader.host())
          }
        }
        result.put(a, hosts.get(a))
      }
      result
    }
  }

  // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
  private[streaming] override def name: String = s"Kafka direct stream [$id]"

  protected[streaming] override val checkpointData =
    new DirectKafkaInputDStreamCheckpointData


  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
   */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new DirectKafkaRateController(id,
        RateEstimator.create(ssc.conf, context.graph.batchDuration)))
    } else {
      None
    }
  }

  private val maxRateLimitPerPartition: Int = context.sparkContext.getConf.getInt(
      "spark.streaming.kafka.maxRatePerPartition", 0)
  protected def maxMessagesPerPartition: Option[Long] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate().toInt)
    val numPartitions = currentOffsets.keys.size

    val effectiveRateLimitPerPartition = estimatedRateLimit
      .filter(_ > 0)
      .map { limit =>
        if (maxRateLimitPerPartition > 0) {
          Math.min(maxRateLimitPerPartition, (limit / numPartitions))
        } else {
          limit / numPartitions
        }
      }.getOrElse(maxRateLimitPerPartition)

    if (effectiveRateLimitPerPartition > 0) {
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some((secsPerBatch * effectiveRateLimitPerPartition).toLong)
    } else {
      None
    }
  }

  protected var currentOffsets = Map[TopicPartition, Long]()

  protected def latestOffsets(): Map[TopicPartition, Long] = this.synchronized {
    val c = consumer
    c.seekToEnd()
    c.assignment().asScala.map { tp =>
      tp -> c.position(tp)
    }.toMap
  }

  // limits the maximum number of messages per partition
  protected def clamp(
    offsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    maxMessagesPerPartition.map { mmp =>
      offsets.map { case (tp, o) =>
        tp -> Math.min(currentOffsets(tp) + mmp, o)
      }
    }.getOrElse(offsets)
  }

  override def compute(validTime: Time): Option[KafkaRDD[K, V]] = {
    val untilOffsets = clamp(latestOffsets())
    val offsetRanges = untilOffsets.map { case (tp, uo) =>
      val fo = currentOffsets.getOrElse(tp, uo)
      OffsetRange(tp.topic, tp.partition, fo, uo)
    }

    val rdd = KafkaRDD[K, V](
      context.sparkContext, executorKafkaParams, offsetRanges.toArray, getPreferredHosts)

    // Report the record number and metadata of this batch interval to InputInfoTracker.
    val description = offsetRanges.filter { offsetRange =>
      // Don't display empty ranges.
      offsetRange.fromOffset != offsetRange.untilOffset
    }.map { offsetRange =>
      s"topic: ${offsetRange.topic}\tpartition: ${offsetRange.partition}\t" +
        s"offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}"
    }.mkString("\n")
    // Copy offsetRanges to immutable.List to prevent from being modified by the user
    val metadata = Map(
      "offsets" -> offsetRanges.toList,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, rdd.count, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    currentOffsets = untilOffsets
    Some(rdd)
  }

  override def start(): Unit = this.synchronized {
    assert(partitionAssignment != Unassigned, "Must call subscribe or assign before starting")
    val c = consumer
    c.poll(pollTimeout)
    currentOffsets = c.assignment().asScala.map { tp =>
      tp -> c.position(tp)
    }.toMap
  }

  override def stop(): Unit = {
    this.synchronized {
      consumer.close()
    }
  }

  private[streaming]
  class DirectKafkaInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    def batchForTime: mutable.HashMap[Time, Array[(String, Int, Long, Long)]] = {
      data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
    }

    override def update(time: Time) {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val a = kv._2.asInstanceOf[KafkaRDD[K, V]].offsetRanges.map(_.toTuple).toArray
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time) { }

    override def restore() {
      poll()

      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
         logInfo(s"Restoring KafkaRDD for time $t ${b.mkString("[", ", ", "]")}")
         generatedRDDs += t -> KafkaRDD[K, V](
           context.sparkContext, executorKafkaParams, b.map(OffsetRange(_)), getPreferredHosts)
      }
    }
  }

  /**
   * A RateController to retrieve the rate from RateEstimator.
   */
  private[streaming] class DirectKafkaRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override def publish(rate: Long): Unit = ()
  }
}

object DirectKafkaInputDStream {
  protected val defaultListener =
    "org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener"
  /** There are several different ways of specifying partition assignment,
    * and they need to be able to survive checkpointing
    */
  protected sealed trait PartitionAssignment extends Serializable
  /** manual assignment via consumer.assign() */
  protected case class Assigned(partitions: ju.List[TopicPartition]) extends PartitionAssignment
  /** dynamic subscription to list of topics via consumer.subscribe */
  protected case class Subscribed(
    topics: ju.List[String],
    consumerRebalanceListenerClassName: String = defaultListener) extends PartitionAssignment
  /** dynamic subscription to topics matching pattern via consumer.subscribe */
  protected case class PatternSubscribed(
    pattern: ju.regex.Pattern,
    consumerRebalanceListenerClassName: String = defaultListener) extends PartitionAssignment
  /** Not yet assigned */
  protected case object Unassigned extends PartitionAssignment

}
