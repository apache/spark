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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.{classTag, ClassTag}

import org.apache.spark.{Logging, Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.partial.{PartialResult, BoundedDouble}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator

import kafka.api.{FetchRequestBuilder, FetchResponse}
import kafka.common.ErrorMapping
import kafka.consumer.SimpleConsumer
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

private[streaming] object KafkaIteratorError {
  def errBeginAfterEnd(
      topic: String,
      partition: Int,
      fromOffset: Long,
      untilOffset: Long): String =
    s"Beginning offset ${fromOffset} is after the ending offset ${untilOffset} " +
      s"for topic ${topic} partition ${partition}. " +
      "You either provided an invalid fromOffset, or the Kafka topic has been damaged"

  def errRanOutBeforeEnd(
      topic: String,
      partition: Int,
      fromOffset: Long,
      untilOffset: Long): String =
    s"Ran out of messages before reaching ending offset ${untilOffset} " +
    s"for topic ${topic} partition ${partition} start ${fromOffset}." +
    " This should not happen, and indicates that messages may have been lost"

  def errOvershotEnd(
      itemOffset: Long,
      topic: String,
      partition: Int,
      fromOffset: Long,
      untilOffset: Long): String =
    s"Got ${itemOffset} > ending offset ${untilOffset} " +
    s"for topic ${topic} partition ${partition} start ${fromOffset}." +
    " This should not happen, and indicates a message may have been skipped"
}

private[streaming] class KafkaRDDIterator[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag,
  R: ClassTag](
    kafkaParams: Map[String, String],
    part: KafkaRDDPartition,
    context: TaskContext,
    messageHandler: MessageAndMetadata[K, V] => R)
    extends NextIterator[R] {

  context.addTaskCompletionListener{ context => closeIfNeeded() }

  val kc = new KafkaCluster(kafkaParams)

  val (preferredHost, preferredPort) = if (context.attemptNumber > 0) {
    (Some(part.host), Some(part.port))
  } else {
    (None, None)
  }

  val iter = new KafkaIterator[K, V, U, T](
    kc,
    preferredHost,
    preferredPort,
    part.topic,
    part.partition,
    part.fromOffset,
    part.untilOffset)

  override def close(): Unit = iter.close()

  override def getNext(): R = {
    val nextItem = iter.getNext()
    if (nextItem == null) {
      finished = true
      nextItem.asInstanceOf[R]
    } else {
      messageHandler(nextItem)
    }
  }
}
 
private[streaming] class KafkaIterator[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    kc: KafkaCluster,
    host: Option[String],
    port: Option[Int],
    topic: String,
    partition: Int,
    fromOffset: Long,
    untilOffset: Long) extends NextIterator[MessageAndMetadata[K, V]] with Logging {
 
  log.info(s"Computing topic ${topic}, partition ${partition} " +
    s"offsets ${fromOffset} -> ${untilOffset}")

  val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
    .newInstance(kc.config.props)
    .asInstanceOf[Decoder[K]]
  val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
    .newInstance(kc.config.props)
    .asInstanceOf[Decoder[V]]
  val consumer = connectLeader
  var requestOffset = fromOffset
  var iter: Iterator[MessageAndOffset] = null

  // The idea is to use the provided preferred host, if given
  // ,except on task retry atttempts,
  // to minimize number of kafka metadata requests
  private def connectLeader: SimpleConsumer = {
    if (host.isDefined && port.isDefined) {
      kc.connect(host.get, port.get)
    } else {
      kc.connectLeader(topic, partition).fold(
        errs => throw new SparkException(
          s"Couldn't connect to leader for topic ${topic} ${partition}: " +
            errs.mkString("\n")),
        consumer => consumer
      )
    }
  }

  private def handleFetchErr(resp: FetchResponse) {
    if (resp.hasError) {
      val err = resp.errorCode(topic, partition)
      if (err == ErrorMapping.LeaderNotAvailableCode ||
        err == ErrorMapping.NotLeaderForPartitionCode) {
        log.error(s"Lost leader for topic ${topic} partition ${partition}, " +
          s" sleeping for ${kc.config.refreshLeaderBackoffMs}ms")
        Thread.sleep(kc.config.refreshLeaderBackoffMs)
      }
      // Let normal rdd retry sort out reconnect attempts
      throw ErrorMapping.exceptionFor(err)
    }
  }

  private def fetchBatch: Iterator[MessageAndOffset] = {
    val req = new FetchRequestBuilder()
      .addFetch(topic, partition, requestOffset, kc.config.fetchMessageMaxBytes)
      .build()
    val resp = consumer.fetch(req)
    handleFetchErr(resp)
    // kafka may return a batch that starts before the requested offset
    resp.messageSet(topic, partition)
      .iterator
      .dropWhile(_.offset < requestOffset)
  }

  override def close(): Unit = {
    if (consumer != null) {
      consumer.close()
    }
  }

  override def getNext(): MessageAndMetadata[K, V] = {
    if (iter == null || !iter.hasNext) {
      iter = fetchBatch
    }
    if (!iter.hasNext) {
      assert(requestOffset == untilOffset,
        KafkaIteratorError.errRanOutBeforeEnd(topic, partition, fromOffset, untilOffset))
      finished = true
      null.asInstanceOf[MessageAndMetadata[K, V]]
    } else {
      val item = iter.next()
      if (item.offset >= untilOffset) {
        assert(item.offset == untilOffset,
          KafkaIteratorError.errOvershotEnd(item.offset, topic, partition, fromOffset, untilOffset))
        finished = true
        null.asInstanceOf[MessageAndMetadata[K, V]]
      } else {
        requestOffset = item.nextOffset
        new MessageAndMetadata(
          topic, partition, item.message, item.offset, keyDecoder, valueDecoder)
      }
    }
  }
}
