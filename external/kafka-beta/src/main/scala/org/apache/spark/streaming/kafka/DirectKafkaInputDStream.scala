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
  ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, Consumer
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
    preferredHosts: ju.Map[TopicPartition, String],
    executorKafkaParams: ju.Map[String, Object],
    driverConsumer: () => Consumer[K, V]
  ) extends InputDStream[ConsumerRecord[K,V]](_ssc) with Logging {

  @transient private var kc: Consumer[K, V] = null
  def consumer(): Consumer[K, V] = this.synchronized {
    if (null == kc) {
      kc = driverConsumer()
    }
    kc
  }
  consumer()

  protected def getBrokers = {
    val c = consumer
    val result = new ju.HashMap[TopicPartition, String]()
    val hosts = new ju.HashMap[TopicPartition, String]()
    val assignments = c.assignment().iterator()
    while (assignments.hasNext()) {
      val tp: TopicPartition = assignments.next()
      if (null == hosts.get(tp)) {
        val infos = c.partitionsFor(tp.topic).iterator()
        while (infos.hasNext()) {
          val i = infos.next()
          hosts.put(new TopicPartition(i.topic(), i.partition()), i.leader.host())
        }
      }
      result.put(tp, hosts.get(tp))
    }
    result
  }

  protected def getPreferredHosts: ju.Map[TopicPartition, String] = {
    if (preferredHosts == DirectKafkaInputDStream.preferBrokers) {
      getBrokers
    } else {
      preferredHosts
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

  protected[streaming] def maxMessagesPerPartition(
    offsets: Map[TopicPartition, Long]): Option[Map[TopicPartition, Long]] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate().toInt)

    // calculate a per-partition rate limit based on current lag
    val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {
      case Some(rate) =>
        val lagPerPartition = offsets.map { case (tp, offset) =>
          tp -> Math.max(offset - currentOffsets(tp), 0)
        }
        val totalLag = lagPerPartition.values.sum

        lagPerPartition.map { case (tp, lag) =>
          val backpressureRate = Math.round(lag / totalLag.toFloat * rate)
          tp -> (if (maxRateLimitPerPartition > 0) {
            Math.min(backpressureRate, maxRateLimitPerPartition)} else backpressureRate)
        }
      case None => offsets.map { case (tp, offset) => tp -> maxRateLimitPerPartition }
    }

    if (effectiveRateLimitPerPartition.values.sum > 0) {
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some(effectiveRateLimitPerPartition.map {
        case (tp, limit) => tp -> (secsPerBatch * limit).toLong
      })
    } else {
      None
    }
  }

  protected var currentOffsets = Map[TopicPartition, Long]()

  protected def latestOffsets(): Map[TopicPartition, Long] = {
    val c = consumer
    c.poll(0)
    val parts = c.assignment().asScala

    // make sure new partitions are reflected in currentOffsets
    val newPartitions = parts.diff(currentOffsets.keySet)
    currentOffsets = currentOffsets ++ newPartitions.map(tp => tp -> c.position(tp)).toMap

    c.seekToEnd()
    parts.map(tp => tp -> c.position(tp)).toMap
  }

  // limits the maximum number of messages per partition
  protected def clamp(
    offsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {

    maxMessagesPerPartition(offsets).map { mmp =>
      mmp.map { case (tp, messages) =>
          val uo = offsets(tp)
          tp -> Math.min(currentOffsets(tp) + messages, uo)
      }
    }.getOrElse(offsets)
  }

  override def compute(validTime: Time): Option[KafkaRDD[K, V]] = {
    val untilOffsets = clamp(latestOffsets())
    val offsetRanges = untilOffsets.map { case (tp, uo) =>
      val fo = currentOffsets(tp)
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

  override def start(): Unit = {
    val c = consumer
    c.poll(0)
    if (currentOffsets.isEmpty) {
      currentOffsets = c.assignment().asScala.map { tp =>
        tp -> c.position(tp)
      }.toMap
    }
  }

  override def stop(): Unit = this.synchronized {
    if (kc != null) {
      kc.close()
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
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
         logInfo(s"Restoring KafkaRDD for time $t ${b.mkString("[", ", ", "]")}")
         generatedRDDs += t -> new KafkaRDD[K, V](
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

object DirectKafkaInputDStream extends Logging {
  import org.apache.spark.streaming.api.java.{ JavaInputDStream, JavaStreamingContext }
  import org.apache.spark.api.java.function.{ Function0 => JFunction0 }

  /** Prefer to run on kafka brokers, if they are on same hosts as executors */
  val preferBrokers: ju.Map[TopicPartition, String] = null
  /** Prefer a consistent executor per TopicPartition, evenly from all executors */
  val preferConsistent: ju.Map[TopicPartition, String] = ju.Collections.emptyMap()

  /** Scala constructor */
  def apply[K: ClassTag, V: ClassTag](
      ssc: StreamingContext,
      preferredHosts: ju.Map[TopicPartition, String],
      executorKafkaParams: ju.Map[String, Object],
      driverConsumer: () => Consumer[K,V]
    ): DirectKafkaInputDStream[K, V] = {
    val ph = new ju.HashMap[TopicPartition, String](preferredHosts)
    val ekp = new ju.HashMap[String, Object](executorKafkaParams)
    KafkaRDD.fixKafkaParams(ekp)
    val cleaned = ssc.sparkContext.clean(driverConsumer)

    new DirectKafkaInputDStream[K, V](ssc, ph, ekp, cleaned)
  }

  /** Java constructor */
  def create[K, V](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      preferredHosts: ju.Map[TopicPartition, String],
      executorKafkaParams: ju.Map[String, Object],
      driverConsumer: JFunction0[Consumer[K, V]]
    ): JavaInputDStream[ConsumerRecord[K, V]] = {

    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)

    new JavaInputDStream(
      DirectKafkaInputDStream[K, V](
        jssc.ssc, preferredHosts, executorKafkaParams, driverConsumer.call _))
  }

}
