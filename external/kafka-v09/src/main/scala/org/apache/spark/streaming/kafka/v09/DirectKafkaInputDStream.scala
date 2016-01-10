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

package org.apache.spark.streaming.kafka.v09

import scala.collection.mutable
import scala.reflect.ClassTag
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.Logging
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.v09.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator

/**
 * A stream of {@link org.apache.spark.streaming.kafka.KafkaRDD} where
 * each given Kafka topic/partition corresponds to an RDD partition.
 * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
 * of messages
 * per second that each '''partition''' will accept.
 * Starting offsets are specified in advance,
 * and this DStream is not responsible for committing offsets,
 * so that you can control exactly-once semantics.
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 *                    configuration parameters</a>.
 *                    Requires "metadata.broker.list" or "bootstrap.servers" to be set
 *                    with Kafka broker(s),
 *                    NOT zookeeper servers, specified in host1:port1,host2:port2 form.
 * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
 *                    starting point of the stream
 */
private[streaming]
class DirectKafkaInputDStream[
  K: ClassTag,
  V: ClassTag,
  R: ClassTag](
    @transient ssc_ : StreamingContext,
    val kafkaParams: Map[String, String],
    @transient val fromOffsets: Map[TopicPartition, Long],
    messageHandler: ConsumerRecord[K, V] => R
  ) extends InputDStream[R](ssc_) with Logging {

  val maxRetries = context.sparkContext.getConf.getInt(
    "spark.streaming.kafka.maxRetries", 1)

  // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
  private[streaming] override def name: String = s"Kafka 0.9 direct stream [$id]"

  protected[streaming] override val checkpointData =
    new DirectKafkaInputDStreamCheckpointData


  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
   */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new DirectKafkaRateController(id,
        RateEstimator.create(ssc.conf, ssc_.graph.batchDuration)))
    } else {
      None
    }
  }

  protected var kafkaCluster = new KafkaCluster[K, V](kafkaParams)

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

  // temp fix for serialization issue of TopicPartition
  protected var serCurrentOffsets = fromOffsets.map { case(tp, l) =>
    (tp.topic, tp.partition, l);
  }

  @transient
  protected var currentOffsets: Map[TopicPartition, Long] = null

  protected final def latestLeaderOffsets(): Map[TopicPartition, LeaderOffset] = {
    kafkaCluster.getLatestOffsetsWithLeaders(currentOffsets.keySet)
  }

  // limits the maximum number of messages per partition
  protected def clamp(
      leaderOffsets: Map[TopicPartition, LeaderOffset]
    ): Map[TopicPartition, LeaderOffset] = {
    maxMessagesPerPartition.map { mmp =>
      leaderOffsets.map { case (tp, lo) =>
        tp -> lo.copy(offset = Math.min(currentOffsets(tp) + mmp, lo.offset))
      }
    }.getOrElse(leaderOffsets)
  }

  override def compute(validTime: Time): Option[KafkaRDD[K, V, R]] = {
    currentOffsets = serCurrentOffsets.map { i => new TopicPartition(i._1, i._2) -> i._3 }.toMap
    val untilOffsets = clamp(latestLeaderOffsets())
    val rdd = KafkaRDD[K, V, R](
      context.sparkContext, kafkaParams, currentOffsets, untilOffsets, messageHandler)

    // Report the record number and metadata of this batch interval to InputInfoTracker.
    val offsetRanges = currentOffsets.map { case (tp, fo) =>
      val uo = untilOffsets(tp)
      OffsetRange(tp.topic, tp.partition, fo, uo.offset)
    }
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

    serCurrentOffsets = untilOffsets.map { kv => (kv._1.topic, kv._1.partition, kv._2.offset) }
    Some(rdd)
  }

  override def start(): Unit = {
  }

  def stop(): Unit = {
    if (kafkaCluster != null) {
      kafkaCluster.close()
      kafkaCluster = null
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
        val a = kv._2.asInstanceOf[KafkaRDD[K, V, R]].offsetRanges.map(_.toTuple).toArray
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time) {}

    override def restore() {
      // this is assuming that the topics don't change during execution, which is true currently

      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
        logInfo(s"Restoring KafkaRDD for time $t ${b.mkString("[", ", ", "]")}")
        generatedRDDs += t -> new KafkaRDD[K, V, R](
          context.sparkContext, kafkaParams, b.map(OffsetRange(_)), messageHandler)
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
