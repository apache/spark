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

package org.apache.spark.rdd.kafka

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator

import java.util.Properties
import kafka.api.{FetchRequestBuilder, FetchResponse}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

/** A batch-oriented interface for consuming from Kafka.
  * Starting and ending offsets are specified in advance,
  * so that you can control exactly-once semantics.
  * For an easy interface to Kafka-managed offsets,
  *  see {@link org.apache.spark.rdd.kafka.KafkaCluster}
  * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
  * configuration parameters</a>.
  *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
  *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
  * @param batch Each KafkaRDDPartition in the batch corresponds to a
  *   range of offsets for a given Kafka topic/partition
  * @param messageHandler function for translating each message into the desired type
  */
class KafkaRDD[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag,
  R: ClassTag](
    sc: SparkContext,
    val kafkaParams: Map[String, String],
    val batch: Array[KafkaRDDPartition],
    messageHandler: MessageAndMetadata[K, V] => R
  ) extends RDD[R](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = batch.asInstanceOf[Array[Partition]]

  override def getPreferredLocations(thePart: Partition): Seq[String] = {
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    // TODO is additional hostname resolution necessary here
    Seq(part.host)
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[R] = {
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    if (part.fromOffset >= part.untilOffset) {
      log.warn("Beginning offset is same or after ending offset " +
        s"skipping ${part.topic} ${part.partition}")
      Iterator.empty
    } else {
      new NextIterator[R] {
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

        // The idea is to use the provided preferred host, except on task retry atttempts,
        // to minimize number of kafka metadata requests
        private def connectLeader: SimpleConsumer = {
          if (context.attemptNumber > 0) {
            kc.connectLeader(part.topic, part.partition).fold(
              errs => throw new Exception(
                s"Couldn't connect to leader for topic ${part.topic} ${part.partition}: " +
                  errs.mkString("\n")),
              consumer => consumer
            )
          } else {
            kc.connect(part.host, part.port)
          }
        }

        private def handleErr(resp: FetchResponse) {
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

        override def close() = consumer.close()

        override def getNext: R = {
          if (iter == null || !iter.hasNext) {
            val req = new FetchRequestBuilder().
              addFetch(part.topic, part.partition, requestOffset, kc.config.fetchMessageMaxBytes).
              build()
            val resp = consumer.fetch(req)
            handleErr(resp)
            // kafka may return a batch that starts before the requested offset
            iter = resp.messageSet(part.topic, part.partition)
              .iterator
              .dropWhile(_.offset < requestOffset)
          }
          if (!iter.hasNext) {
            assert(requestOffset == part.untilOffset,
              s"ran out of messages before reaching ending offset ${part.untilOffset} " +
                s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
                " This should not happen, and indicates that messages may have been lost")
            finished = true
            null.asInstanceOf[R]
          } else {
            val item = iter.next
            if (item.offset >= part.untilOffset) {
              assert(item.offset == part.untilOffset,
                s"got ${item.offset} > ending offset ${part.untilOffset} " +
                  s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
                  " This should not happen, and indicates a message may have been skipped")
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
    }
  }

}

object KafkaRDD {
  import KafkaCluster.LeaderOffset

  /**
    * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    * configuration parameters</a>.
    *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
    *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
    * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
    *  starting point of the batch
    * @param untilOffsets per-topic/partition Kafka offsets defining the (exclusive)
    *  ending point of the batch
    * @param messageHandler function for translating each message into the desired type
    */
  def apply[
    K: ClassTag,
    V: ClassTag,
    U <: Decoder[_]: ClassTag,
    T <: Decoder[_]: ClassTag,
    R: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      fromOffsets: Map[TopicAndPartition, Long],
      untilOffsets: Map[TopicAndPartition, LeaderOffset],
      messageHandler: MessageAndMetadata[K, V] => R
  ): KafkaRDD[K, V, U, T, R] = {
    assert(fromOffsets.keys == untilOffsets.keys,
      "Must provide both from and until offsets for each topic/partition")

    val partitions  = fromOffsets.zipWithIndex.map { case ((tp, from), index) =>
      val lo = untilOffsets(tp)
      new KafkaRDDPartition(index, tp.topic, tp.partition, from, lo.offset, lo.host, lo.port)
    }.toArray

    new KafkaRDD[K, V, U, T, R](sc, kafkaParams, partitions, messageHandler)
  }
}
