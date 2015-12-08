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

import java.util.Properties

import scala.reflect.{classTag, ClassTag}

import kafka.api.{FetchRequestBuilder, FetchResponse}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.{KafkaStream, Consumer, ConsumerConfig, ConsumerConnector, SimpleConsumer}
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

import org.apache.spark.{Logging, SparkConf, SparkException}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, DStreamGraph, StreamingContext}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.util.{NextIterator, ThreadUtils}


/**
 * Input stream that pulls messages from a Kafka Broker.
 *
 * @param kafkaParams Map of kafka configuration parameters.
 *                    See: http://kafka.apache.org/configuration.html
 *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
 *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
 * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
 *  starting point of the stream
 * @param storageLevel RDD storage level.
 */
private[streaming]
class KafkaDirectInputDStream[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    ssc_ : StreamingContext,
    kafkaParams: Map[String, String],
    val fromOffsets: Map[TopicAndPartition, Long],
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[(K, V)](ssc_) with Logging {

  // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
  private[streaming] override def name: String = s"Kafka direct receiver stream [$id]"

  def getReceiver(): Receiver[(K, V)] = {
    new KafkaDirectReceiver[K, V, U, T](ssc_.conf, context.graph.batchDuration, kafkaParams,
      fromOffsets, storageLevel)
  }
}

private[streaming]
class KafkaDirectReceiver[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    conf: SparkConf,
    duration: Duration,
    kafkaParams: Map[String, String],
    val fromOffsets: Map[TopicAndPartition, Long],
    storageLevel: StorageLevel
  ) extends Receiver[(K, V)](storageLevel) with Logging {

  val maxRetries = conf.getInt(
    "spark.streaming.kafka.maxRetries", 1)

  private val maxRateLimitPerPartition: Int = conf.getInt(
      "spark.streaming.kafka.maxRatePerPartition", 0)

  private def maxMessagesPerPartition: Option[Long] = {
    val numPartitions = fromOffsets.keys.size

    val effectiveRateLimitPerPartition =
      Math.max(0, maxRateLimitPerPartition)

    if (effectiveRateLimitPerPartition > 0 && duration != null) {
      val secsPerBatch = duration.milliseconds.toDouble / 1000
      Some((secsPerBatch * effectiveRateLimitPerPartition).toLong)
    } else {
      None
    }
  }

  protected final def latestLeaderOffsets(
      kc: KafkaCluster,
      topicAndPartition: Set[TopicAndPartition],
      retries: Int): Map[TopicAndPartition, LeaderOffset] = {
    val o = kc.getLatestLeaderOffsets(topicAndPartition)
    // Either.fold would confuse @tailrec, do it manually
    if (o.isLeft) {
      val err = o.left.get.toString
      if (retries <= 0) {
        throw new SparkException(err)
      } else {
        log.error(err)
        Thread.sleep(kc.config.refreshLeaderBackoffMs)
        latestLeaderOffsets(kc, topicAndPartition, retries - 1)
      }
    } else {
      o.right.get
    }
  }

  // limits the maximum number of messages per partition
  protected def clamp(
    currentOffset: Long,
    leaderOffsets: Map[TopicAndPartition, LeaderOffset]): Map[TopicAndPartition, LeaderOffset] = {
    maxMessagesPerPartition.map { mmp =>
      leaderOffsets.map { case (tp, lo) =>
        tp -> lo.copy(offset = Math.min(currentOffset + mmp, lo.offset))
      }
    }.getOrElse(leaderOffsets)
  }

  def onStop() {
  }

  def onStart() {
    logInfo("Starting Kafka Consumer Direct Stream")

    val executorPool =
      ThreadUtils.newDaemonFixedThreadPool(fromOffsets.keys.size, "KafkaMessageHandler")
    try {
      // Start the messages handler
      fromOffsets.keys.foreach { partition =>
        executorPool.submit(new MessageHandler(partition, fromOffsets(partition)))
      }
    } finally {
      executorPool.shutdown() // Just causes thread to terminate after work is done
    }
  }

  private class KafkaDirectIterator(
      kc: KafkaCluster,
      part: TopicAndPartition,
      fromOffset: Long,
      untilOffset: Long) extends NextIterator[MessageAndMetadata[K, V]] {
    val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(kc.config.props)
      .asInstanceOf[Decoder[K]]
    val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(kc.config.props)
      .asInstanceOf[Decoder[V]]
    val consumer = connectLeader
    var requestOffset = fromOffset
    var iter: Iterator[MessageAndOffset] = null

    private def connectLeader: SimpleConsumer = {
      kc.connectLeader(part.topic, part.partition).fold(
        errs => throw new SparkException(
          s"Couldn't connect to leader for topic ${part.topic} ${part.partition}: " +
            errs.mkString("\n")),
        consumer => consumer
      )
    }

    private def handleFetchErr(resp: FetchResponse) {
      if (resp.hasError) {
        val err = resp.errorCode(part.topic, part.partition)
        if (err == ErrorMapping.LeaderNotAvailableCode ||
          err == ErrorMapping.NotLeaderForPartitionCode) {
          log.error(s"Lost leader for topic ${part.topic} partition ${part.partition}, " +
            s" sleeping for ${kc.config.refreshLeaderBackoffMs}ms")
          Thread.sleep(kc.config.refreshLeaderBackoffMs)
        }
        // Let normal rdd retry sort out reconnect attempts
        throw ErrorMapping.exceptionFor(err)
      }
    }

    private def errBeginAfterEnd(): String =
      s"Beginning offset ${fromOffset} is after the ending offset ${untilOffset} " +
        s"for topic ${part.topic} partition ${part.partition}. " +
        "You either provided an invalid fromOffset, or the Kafka topic has been damaged"
    
    private def errRanOutBeforeEnd(): String =
      s"Ran out of messages before reaching ending offset ${untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition} start ${fromOffset}." +
      " This should not happen, and indicates that messages may have been lost"
    
    private def errOvershotEnd(itemOffset: Long): String =
      s"Got ${itemOffset} > ending offset ${untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition} start ${fromOffset}." +
      " This should not happen, and indicates a message may have been skipped"

    private def fetchBatch: Iterator[MessageAndOffset] = {
      val req = new FetchRequestBuilder()
        .addFetch(part.topic, part.partition, requestOffset, kc.config.fetchMessageMaxBytes)
        .build()
      val resp = consumer.fetch(req)
      handleFetchErr(resp)
      // kafka may return a batch that starts before the requested offset
      resp.messageSet(part.topic, part.partition)
        .iterator
        .dropWhile(_.offset < requestOffset)
    }

    override def close(): Unit = {
      if (consumer != null) {
        consumer.close()
      }
    }

    override def getNext(): MessageAndMetadata[K, V] = {
      assert(fromOffset <= untilOffset, errBeginAfterEnd())
      if (iter == null || !iter.hasNext) {
        iter = fetchBatch
      }
      if (!iter.hasNext) {
        assert(requestOffset == untilOffset, errRanOutBeforeEnd())
        finished = true
        null.asInstanceOf[MessageAndMetadata[K, V]]
      } else {
        val item = iter.next()
        if (item.offset >= untilOffset) {
          assert(item.offset == untilOffset, errOvershotEnd(item.offset))
          errOvershotEnd(item.offset)
          finished = true
          null.asInstanceOf[MessageAndMetadata[K, V]]
        } else {
          requestOffset = item.nextOffset
          new MessageAndMetadata(
            part.topic, part.partition, item.message, item.offset, keyDecoder, valueDecoder)
        }
      }
    }
  } 

  // Handles Kafka messages
  private class MessageHandler(partition: TopicAndPartition, fromOffset: Long)
    extends Runnable {
    val kc = new KafkaCluster(kafkaParams)
    var currentOffset: Long = fromOffset
    def run() {
      logInfo("Starting MessageHandler.")

      while(true) {
        try {
          val untilOffsets = clamp(currentOffset, latestLeaderOffsets(kc, Set(partition), maxRetries))
          if (currentOffset != untilOffsets(partition).offset) {
            val iter =
              new KafkaDirectIterator(kc, partition, currentOffset, untilOffsets(partition).offset)

            var next: MessageAndMetadata[K, V] = iter.getNext()
            while (next != null) {
              store((next.key, next.message))
              next = iter.getNext()
            }
            currentOffset = untilOffsets(partition).offset
          }
        } catch {
          case e: Throwable =>
            reportError("Error handling message; exiting", e)
        }
      }
    }
  }
}
