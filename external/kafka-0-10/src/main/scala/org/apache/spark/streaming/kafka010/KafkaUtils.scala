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

package org.apache.spark.streaming.kafka010

import java.{ util => ju }

import scala.reflect.ClassTag

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkContext
import org.apache.spark.api.java.{ JavaRDD, JavaSparkContext }
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{ JavaInputDStream, JavaStreamingContext }
import org.apache.spark.streaming.dstream._

/**
 * object for constructing Kafka streams and RDDs
 */
object KafkaUtils extends Logging {
  /**
   * Scala constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createRDD[K, V](
      sc: SparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy
    ): RDD[ConsumerRecord[K, V]] = {
    createRDD[K, V, ConsumerRecord[K, V]](sc, kafkaParams, offsetRanges, locationStrategy,
      (r: ConsumerRecord[K, V]) => r)
  }

  /**
   * Scala constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics. A message handler function
   * can be provided in order to transform Kafka event at an early stage.
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param messageHandler a function that converts Kafka consumer record to a value of type [[R]].
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam R type of the returned message value (after processing by messageHandler)
   */
  def createRDD[K, V, R : ClassTag](
      sc: SparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy,
      messageHandler: ConsumerRecord[K, V] => R
    ): RDD[R] = {
    val preferredHosts = locationStrategy match {
      case PreferBrokers =>
        throw new IllegalArgumentException(
          "If you want to prefer brokers, you must provide a mapping using PreferFixed " +
            "A single KafkaRDD does not have a driver consumer and cannot look up brokers for you.")
      case PreferConsistent => ju.Collections.emptyMap[TopicPartition, String]()
      case PreferFixed(hostMap) => hostMap
    }
    val kp = new ju.HashMap[String, Object](kafkaParams)
    fixKafkaParams(kp)
    val osr = offsetRanges.clone()

    new KafkaRDD[K, V, R](sc, kp, osr, preferredHosts, messageHandler, true)
  }

  /**
   * Java constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createRDD[K, V](
      jsc: JavaSparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy
    ): JavaRDD[ConsumerRecord[K, V]] = {

    new JavaRDD(createRDD[K, V](jsc.sc, kafkaParams, offsetRanges, locationStrategy))
  }

  /**
   * Java constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param messageHandler a function that converts Kafka consumer record to a value of type [[R]].
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam R type of the returned message value (after processing by messageHandler)
   */
  def createRDD[K, V, R : ClassTag](
      jsc: JavaSparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy,
      messageHandler: ConsumerRecord[K, V] => R
    ): JavaRDD[R] = {

    new JavaRDD(createRDD[K, V, R](jsc.sc, kafkaParams, offsetRanges, locationStrategy,
      messageHandler))
  }

  /**
   * Scala constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
   *  of messages
   * per second that each '''partition''' will accept.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    ): InputDStream[ConsumerRecord[K, V]] = {
    val ppc = new DefaultPerPartitionConfig(ssc.sparkContext.getConf)
    createDirectStream[K, V](ssc, locationStrategy, consumerStrategy, ppc)
  }

  /**
   * Scala constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details.
   * @param perPartitionConfig configuration of settings such as max rate on a per-partition basis.
   *   see [[PerPartitionConfig]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V],
      perPartitionConfig: PerPartitionConfig
    ): InputDStream[ConsumerRecord[K, V]] = {
    createDirectStream(ssc, locationStrategy, consumerStrategy,
      perPartitionConfig, (r: ConsumerRecord[K, V]) => r)
  }

  /**
   * Scala constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details.
   * @param perPartitionConfig configuration of settings such as max rate on a per-partition basis.
   *   see [[PerPartitionConfig]] for more details.
   * @param messageHandler a function that converts Kafka consumer record to a value of type [[R]].
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam R type of the returned message value (after processing by messageHandler)
   */
  def createDirectStream[K, V, R : ClassTag](
     ssc: StreamingContext,
     locationStrategy: LocationStrategy,
     consumerStrategy: ConsumerStrategy[K, V],
     perPartitionConfig: PerPartitionConfig,
     messageHandler: ConsumerRecord[K, V] => R
   ): InputDStream[R] = {
    new DirectKafkaInputDStream[K, V, R](ssc, locationStrategy, consumerStrategy,
      perPartitionConfig, messageHandler)
  }

  /**
   * Java constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createDirectStream[K, V](
      jssc: JavaStreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    ): JavaInputDStream[ConsumerRecord[K, V]] = {
    new JavaInputDStream(
      createDirectStream[K, V](
        jssc.ssc, locationStrategy, consumerStrategy))
  }

  /**
   * Java constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details
   * @param perPartitionConfig configuration of settings such as max rate on a per-partition basis.
   *   see [[PerPartitionConfig]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createDirectStream[K, V](
      jssc: JavaStreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V],
      perPartitionConfig: PerPartitionConfig
    ): JavaInputDStream[ConsumerRecord[K, V]] = {
    new JavaInputDStream(
      createDirectStream[K, V](
        jssc.ssc, locationStrategy, consumerStrategy, perPartitionConfig))
  }

  /**
   * Java constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details
   * @param perPartitionConfig configuration of settings such as max rate on a per-partition basis.
   *   see [[PerPartitionConfig]] for more details.
   * @param messageHandler a function that converts Kafka consumer record to a value of type [[R]].
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam R type of the returned message value (after processing by messageHandler)
   */
  def createDirectStream[K, V, R : ClassTag](
      jssc: JavaStreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V],
      perPartitionConfig: PerPartitionConfig,
      messageHandler: ConsumerRecord[K, V] => R
    ): JavaInputDStream[R] = {
    new JavaInputDStream(
      createDirectStream[K, V, R](
        jssc.ssc, locationStrategy, consumerStrategy, perPartitionConfig,
        messageHandler))
  }

  /**
   * Tweak kafka params to prevent issues on executors
   */
  private[kafka010] def fixKafkaParams(kafkaParams: ju.HashMap[String, Object]): Unit = {
    logWarning(s"overriding ${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG} to false for executor")
    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)

    logWarning(s"overriding ${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG} to none for executor")
    kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

    // driver and executor should be in different consumer groups
    val originalGroupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG)
    if (null == originalGroupId) {
      logError(s"${ConsumerConfig.GROUP_ID_CONFIG} is null, you should probably set it")
    }
    val groupId = "spark-executor-" + originalGroupId
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
