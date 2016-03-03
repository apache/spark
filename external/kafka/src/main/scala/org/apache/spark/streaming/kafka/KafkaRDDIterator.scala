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

import scala.reflect._

import kafka.api.{FetchRequestBuilder, FetchResponse}
import kafka.common.ErrorMapping
import kafka.consumer.SimpleConsumer
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

import org.apache.spark.{Logging, SparkException, TaskContext}
import org.apache.spark.util.NextIterator

private class KafkaRDDIterator[
K: ClassTag,
V: ClassTag,
U <: Decoder[_]: ClassTag,
T <: Decoder[_]: ClassTag,
R: ClassTag] private[spark] (
  part: KafkaRDDPartition,
  context: TaskContext,
  kafkaParams: Map[String, String],
  messageHandler: MessageAndMetadata[K, V] => R) extends NextIterator[R] with Logging {

  context.addTaskCompletionListener{ context => closeIfNeeded() }

  log.info(s"Computing topic ${part.topic}, partition ${part.partition} " +
    s"offsets ${part.fromOffset} -> ${part.untilOffset}")

  val kc = new KafkaCluster(kafkaParams)
  val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
    .newInstance(kc.config.props)
    .asInstanceOf[Decoder[K]]
  val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
    .newInstance(kc.config.props)
    .asInstanceOf[Decoder[V]]
  val consumer = connectLeader
  var requestOffset = part.fromOffset
  var iter: Iterator[MessageAndOffset] = null

  // The idea is to use the provided preferred host, except on task retry attempts,
  // to minimize number of kafka metadata requests
  private def connectLeader: SimpleConsumer = {
    if (context.attemptNumber > 0) {
      kc.connectLeader(part.topic, part.partition).fold(
        errs => throw new SparkException(
          s"Couldn't connect to leader for topic ${part.topic} ${part.partition}: " +
            errs.mkString("\n")),
        consumer => consumer
      )
    } else {
      kc.connect(part.host, part.port)
    }
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

  override def getNext(): R = {
    if (iter == null || !iter.hasNext) {
      iter = fetchBatch
    }
    if (!iter.hasNext) {
      assert(requestOffset == part.untilOffset, KafkaUtils.errRanOutBeforeEnd(part))
      finished = true
      null.asInstanceOf[R]
    } else {
      val item = iter.next()
      if (item.offset >= part.untilOffset) {
        assert(item.offset == part.untilOffset, KafkaUtils.errOvershotEnd(item.offset, part))
        finished = true
        null.asInstanceOf[R]
      } else {
        requestOffset = item.nextOffset
        messageHandler(new MessageAndMetadata(
          part.topic, part.partition, item.message, item.offset, keyDecoder, valueDecoder))
      }
    }
  }
}
