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

import java.{ util => ju }

import scala.reflect.{classTag, ClassTag}

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.{ JavaRDD, JavaSparkContext }
import org.apache.spark.api.java.function.{ Function0 => JFunction0 }
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{ JavaInputDStream, JavaStreamingContext }
import org.apache.spark.streaming.dstream._

@Experimental
object KafkaUtils extends Logging {
  /**
   * Scala constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.htmll#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param preferredHosts map from TopicPartition to preferred host for processing that partition.
   * In most cases, use [[KafkaUtils.preferConsistent]]
   * Use [[KafkaUtils.preferBrokers]] if your executors are on same nodes as brokers.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createRDD[K: ClassTag, V: ClassTag](
      sc: SparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      preferredHosts: ju.Map[TopicPartition, String]
    ): RDD[ConsumerRecord[K, V]] = {
    assert(preferredHosts != KafkaUtils.preferBrokers,
      "If you want to prefer brokers, you must provide a mapping for preferredHosts. " +
        "A single KafkaRDD does not have a driver consumer and cannot look up brokers for you. ")

    val kp = new ju.HashMap[String, Object](kafkaParams)
    fixKafkaParams(kp)
    val osr = offsetRanges.clone()
    val ph = new ju.HashMap[TopicPartition, String](preferredHosts)

    new KafkaRDD[K, V](sc, kp, osr, ph, true)
  }

  /**
   * Java constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.htmll#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param preferredHosts map from TopicPartition to preferred host for processing that partition.
   * In most cases, use [[KafkaUtils.preferConsistent]]
   * Use [[KafkaUtils.preferBrokers]] if your executors are on same nodes as brokers.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createRDD[K, V](
      jsc: JavaSparkContext,
      keyClass: Class[K],
      valueClass: Class[V],
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      preferredHosts: ju.Map[TopicPartition, String]
    ): JavaRDD[ConsumerRecord[K, V]] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)

    new JavaRDD(createRDD[K, V](jsc.sc, kafkaParams, offsetRanges, preferredHosts))
  }

  /** Prefer to run on kafka brokers, if they are on same hosts as executors */
  val preferBrokers: ju.Map[TopicPartition, String] = null
  /** Prefer a consistent executor per TopicPartition, evenly from all executors */
  val preferConsistent: ju.Map[TopicPartition, String] = ju.Collections.emptyMap()

  /**
   * Scala constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
   *  of messages
   * per second that each '''partition''' will accept.
   * @param preferredHosts map from TopicPartition to preferred host for processing that partition.
   * In most cases, use [[KafkaUtils.preferConsistent]]
   * Use [[KafkaUtils.preferBrokers]] if your executors are on same nodes as brokers.
   * @param executorKafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>.
   *   Requires  "bootstrap.servers" to be set with Kafka broker(s),
   *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
   * @param driverConsumer zero-argument function for you to construct a Kafka Consumer,
   *  and subscribe topics or assign partitions.
   *  This consumer will be used on the driver to query for offsets only, not messages.
   *  See <a href="http://kafka.apache.org/documentation.html#newconsumerapi">Consumer doc</a>
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createDirectStream[K: ClassTag, V: ClassTag](
      ssc: StreamingContext,
      preferredHosts: ju.Map[TopicPartition, String],
      executorKafkaParams: ju.Map[String, Object],
      driverConsumer: () => Consumer[K, V]
    ): InputDStream[ConsumerRecord[K, V]] = {
    val ph = if (preferredHosts == preferBrokers) {
      preferredHosts
    } else {
      new ju.HashMap[TopicPartition, String](preferredHosts)
    }
    val ekp = new ju.HashMap[String, Object](executorKafkaParams)
    fixKafkaParams(ekp)
    val cleaned = ssc.sparkContext.clean(driverConsumer)

    new DirectKafkaInputDStream[K, V](ssc, ph, ekp, cleaned)
  }

  /**
   * Java constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param preferredHosts map from TopicPartition to preferred host for processing that partition.
   * In most cases, use [[KafkaUtils.preferConsistent]]
   * Use [[KafkaUtils.preferBrokers]] if your executors are on same nodes as brokers.
   * @param executorKafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>.
   *   Requires  "bootstrap.servers" to be set with Kafka broker(s),
   *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
   * @param driverConsumer zero-argument function for you to construct a Kafka Consumer,
   *  and subscribe topics or assign partitions.
   *  This consumer will be used on the driver to query for offsets only, not messages.
   *  See <a href="http://kafka.apache.org/documentation.html#newconsumerapi">Consumer doc</a>
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createDirectStream[K, V](
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
      createDirectStream[K, V](
        jssc.ssc, preferredHosts, executorKafkaParams, driverConsumer.call _))
  }


  /**
   * Tweak kafka params to prevent issues on executors
   */
  private[kafka] def fixKafkaParams(kafkaParams: ju.HashMap[String, Object]): Unit = {
    logWarning(s"overriding ${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG} to false for executor")
    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)

    logWarning(s"overriding ${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG} to none for executor")
    kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

    // driver and executor should be in different consumer groups
    val groupId = "spark-executor-" + kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG)
    logWarning(s"overriding executor ${ConsumerConfig.GROUP_ID_CONFIG} to ${groupId}")
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    // possible workaround for KAFKA-3135
    val rbb = kafkaParams.get(ConsumerConfig.RECEIVE_BUFFER_CONFIG)
    if (null == rbb || rbb.asInstanceOf[java.lang.Integer] < 65536) {
      logWarning(s"overriding ${ConsumerConfig.RECEIVE_BUFFER_CONFIG} to 65536 see KAFKA-3135")
      kafkaParams.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
    }
  }
}
