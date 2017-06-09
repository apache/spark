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

import java.io.OutputStream
import java.lang.{Integer => JInt, Long => JLong, Number => JNumber}
import java.nio.charset.StandardCharsets
import java.util.{List => JList, Locale, Map => JMap, Set => JSet}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, DefaultDecoder, StringDecoder}
import net.razorvine.pickle.{IObjectPickler, Opcodes, Pickler}

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.util.WriteAheadLogUtils

object KafkaUtils {
  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param ssc       StreamingContext object
   * @param zkQuorum  Zookeeper quorum (hostname:port,hostname:port,..)
   * @param groupId   The group id for this consumer
   * @param topics    Map of (topic_name to numPartitions) to consume. Each partition is consumed
   *                  in its own thread
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   * @return DStream of (Kafka message key, Kafka message value)
   */
  def createStream(
      ssc: StreamingContext,
      zkQuorum: String,
      groupId: String,
      topics: Map[String, Int],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[(String, String)] = {
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000")
    createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, storageLevel)
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param ssc         StreamingContext object
   * @param kafkaParams Map of kafka configuration parameters,
   *                    see http://kafka.apache.org/08/configuration.html
   * @param topics      Map of (topic_name to numPartitions) to consume. Each partition is consumed
   *                    in its own thread.
   * @param storageLevel Storage level to use for storing the received objects
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam U type of Kafka message key decoder
   * @tparam T type of Kafka message value decoder
   * @return DStream of (Kafka message key, Kafka message value)
   */
  def createStream[K: ClassTag, V: ClassTag, U <: Decoder[_]: ClassTag, T <: Decoder[_]: ClassTag](
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Map[String, Int],
      storageLevel: StorageLevel
    ): ReceiverInputDStream[(K, V)] = {
    val walEnabled = WriteAheadLogUtils.enableReceiverLog(ssc.conf)
    new KafkaInputDStream[K, V, U, T](ssc, kafkaParams, topics, walEnabled, storageLevel)
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc      JavaStreamingContext object
   * @param zkQuorum  Zookeeper quorum (hostname:port,hostname:port,..)
   * @param groupId   The group id for this consumer
   * @param topics    Map of (topic_name to numPartitions) to consume. Each partition is consumed
   *                  in its own thread
   * @return DStream of (Kafka message key, Kafka message value)
   */
  def createStream(
      jssc: JavaStreamingContext,
      zkQuorum: String,
      groupId: String,
      topics: JMap[String, JInt]
    ): JavaPairReceiverInputDStream[String, String] = {
    createStream(jssc.ssc, zkQuorum, groupId, Map(topics.asScala.mapValues(_.intValue()).toSeq: _*))
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param jssc      JavaStreamingContext object
   * @param zkQuorum  Zookeeper quorum (hostname:port,hostname:port,..).
   * @param groupId   The group id for this consumer.
   * @param topics    Map of (topic_name to numPartitions) to consume. Each partition is consumed
   *                  in its own thread.
   * @param storageLevel RDD storage level.
   * @return DStream of (Kafka message key, Kafka message value)
   */
  def createStream(
      jssc: JavaStreamingContext,
      zkQuorum: String,
      groupId: String,
      topics: JMap[String, JInt],
      storageLevel: StorageLevel
    ): JavaPairReceiverInputDStream[String, String] = {
    createStream(jssc.ssc, zkQuorum, groupId, Map(topics.asScala.mapValues(_.intValue()).toSeq: _*),
      storageLevel)
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param jssc      JavaStreamingContext object
   * @param keyTypeClass Key type of DStream
   * @param valueTypeClass value type of Dstream
   * @param keyDecoderClass Type of kafka key decoder
   * @param valueDecoderClass Type of kafka value decoder
   * @param kafkaParams Map of kafka configuration parameters,
   *                    see http://kafka.apache.org/08/configuration.html
   * @param topics  Map of (topic_name to numPartitions) to consume. Each partition is consumed
   *                in its own thread
   * @param storageLevel RDD storage level.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam U type of Kafka message key decoder
   * @tparam T type of Kafka message value decoder
   * @return DStream of (Kafka message key, Kafka message value)
   */
  def createStream[K, V, U <: Decoder[_], T <: Decoder[_]](
      jssc: JavaStreamingContext,
      keyTypeClass: Class[K],
      valueTypeClass: Class[V],
      keyDecoderClass: Class[U],
      valueDecoderClass: Class[T],
      kafkaParams: JMap[String, String],
      topics: JMap[String, JInt],
      storageLevel: StorageLevel
    ): JavaPairReceiverInputDStream[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyTypeClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueTypeClass)

    implicit val keyCmd: ClassTag[U] = ClassTag(keyDecoderClass)
    implicit val valueCmd: ClassTag[T] = ClassTag(valueDecoderClass)

    createStream[K, V, U, T](
      jssc.ssc,
      kafkaParams.asScala.toMap,
      Map(topics.asScala.mapValues(_.intValue()).toSeq: _*),
      storageLevel)
  }

  /** get leaders for the given offset ranges, or throw an exception */
  private def leadersForRanges(
      kc: KafkaCluster,
      offsetRanges: Array[OffsetRange]): Map[TopicAndPartition, (String, Int)] = {
    val topics = offsetRanges.map(o => TopicAndPartition(o.topic, o.partition)).toSet
    val leaders = kc.findLeaders(topics)
    KafkaCluster.checkErrors(leaders)
  }

  /** Make sure offsets are available in kafka, or throw an exception */
  private def checkOffsets(
      kc: KafkaCluster,
      offsetRanges: Array[OffsetRange]): Unit = {
    val topics = offsetRanges.map(_.topicAndPartition).toSet
    val result = for {
      low <- kc.getEarliestLeaderOffsets(topics).right
      high <- kc.getLatestLeaderOffsets(topics).right
    } yield {
      offsetRanges.filterNot { o =>
        low(o.topicAndPartition).offset <= o.fromOffset &&
        o.untilOffset <= high(o.topicAndPartition).offset
      }
    }
    val badRanges = KafkaCluster.checkErrors(result)
    if (!badRanges.isEmpty) {
      throw new SparkException("Offsets not available on leader: " + badRanges.mkString(","))
    }
  }

  private[kafka] def getFromOffsets(
      kc: KafkaCluster,
      kafkaParams: Map[String, String],
      topics: Set[String]
    ): Map[TopicAndPartition, Long] = {
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase(Locale.ROOT))
    val result = for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      leaderOffsets.map { case (tp, lo) =>
          (tp, lo.offset)
      }
    }
    KafkaCluster.checkErrors(result)
  }

  /**
   * Create an RDD from Kafka using offset ranges for each topic and partition.
   *
   * @param sc SparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @return RDD of (Kafka message key, Kafka message value)
   */
  def createRDD[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      offsetRanges: Array[OffsetRange]
    ): RDD[(K, V)] = sc.withScope {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
    val kc = new KafkaCluster(kafkaParams)
    val leaders = leadersForRanges(kc, offsetRanges)
    checkOffsets(kc, offsetRanges)
    new KafkaRDD[K, V, KD, VD, (K, V)](sc, kafkaParams, offsetRanges, leaders, messageHandler)
  }

  /**
   * Create an RDD from Kafka using offset ranges for each topic and partition. This allows you
   * specify the Kafka leader to connect to (to optimize fetching) and access the message as well
   * as the metadata.
   *
   * @param sc SparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   * @param leaders Kafka brokers for each TopicAndPartition in offsetRanges.  May be an empty map,
   *   in which case leaders will be looked up on the driver.
   * @param messageHandler Function for translating each message and metadata into the desired type
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @tparam R type returned by messageHandler
   * @return RDD of R
   */
  def createRDD[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag,
    R: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      offsetRanges: Array[OffsetRange],
      leaders: Map[TopicAndPartition, Broker],
      messageHandler: MessageAndMetadata[K, V] => R
    ): RDD[R] = sc.withScope {
    val kc = new KafkaCluster(kafkaParams)
    val leaderMap = if (leaders.isEmpty) {
      leadersForRanges(kc, offsetRanges)
    } else {
      // This could be avoided by refactoring KafkaRDD.leaders and KafkaCluster to use Broker
      leaders.map {
        case (tp: TopicAndPartition, Broker(host, port)) => (tp, (host, port))
      }
    }
    val cleanedHandler = sc.clean(messageHandler)
    checkOffsets(kc, offsetRanges)
    new KafkaRDD[K, V, KD, VD, R](sc, kafkaParams, offsetRanges, leaderMap, cleanedHandler)
  }

  /**
   * Create an RDD from Kafka using offset ranges for each topic and partition.
   *
   * @param jsc JavaSparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   * @param keyClass type of Kafka message key
   * @param valueClass type of Kafka message value
   * @param keyDecoderClass type of Kafka message key decoder
   * @param valueDecoderClass type of Kafka message value decoder
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @return RDD of (Kafka message key, Kafka message value)
   */
  def createRDD[K, V, KD <: Decoder[K], VD <: Decoder[V]](
      jsc: JavaSparkContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      kafkaParams: JMap[String, String],
      offsetRanges: Array[OffsetRange]
    ): JavaPairRDD[K, V] = jsc.sc.withScope {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    new JavaPairRDD(createRDD[K, V, KD, VD](
      jsc.sc, Map(kafkaParams.asScala.toSeq: _*), offsetRanges))
  }

  /**
   * Create an RDD from Kafka using offset ranges for each topic and partition. This allows you
   * specify the Kafka leader to connect to (to optimize fetching) and access the message as well
   * as the metadata.
   *
   * @param jsc JavaSparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   * @param leaders Kafka brokers for each TopicAndPartition in offsetRanges.  May be an empty map,
   *   in which case leaders will be looked up on the driver.
   * @param messageHandler Function for translating each message and metadata into the desired type
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @tparam R type returned by messageHandler
   * @return RDD of R
   */
  def createRDD[K, V, KD <: Decoder[K], VD <: Decoder[V], R](
      jsc: JavaSparkContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      recordClass: Class[R],
      kafkaParams: JMap[String, String],
      offsetRanges: Array[OffsetRange],
      leaders: JMap[TopicAndPartition, Broker],
      messageHandler: JFunction[MessageAndMetadata[K, V], R]
    ): JavaRDD[R] = jsc.sc.withScope {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    val leaderMap = Map(leaders.asScala.toSeq: _*)
    createRDD[K, V, KD, VD, R](
      jsc.sc, Map(kafkaParams.asScala.toSeq: _*), offsetRanges, leaderMap, messageHandler.call(_))
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the `StreamingContext`. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
   *    starting point of the stream
   * @param messageHandler Function for translating each message and metadata into the desired type
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @tparam R type returned by messageHandler
   * @return DStream of R
   */
  def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag,
    R: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      fromOffsets: Map[TopicAndPartition, Long],
      messageHandler: MessageAndMetadata[K, V] => R
  ): InputDStream[R] = {
    val cleanedHandler = ssc.sc.clean(messageHandler)
    new DirectKafkaInputDStream[K, V, KD, VD, R](
      ssc, kafkaParams, fromOffsets, cleanedHandler)
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the `StreamingContext`. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   *   If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
   *   to determine where the stream starts (defaults to "largest")
   * @param topics Names of the topics to consume
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @return DStream of (Kafka message key, Kafka message value)
   */
  def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Set[String]
  ): InputDStream[(K, V)] = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
    val kc = new KafkaCluster(kafkaParams)
    val fromOffsets = getFromOffsets(kc, kafkaParams, topics)
    new DirectKafkaInputDStream[K, V, KD, VD, (K, V)](
      ssc, kafkaParams, fromOffsets, messageHandler)
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the `StreamingContext`. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param jssc JavaStreamingContext object
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param keyDecoderClass Class of the key decoder
   * @param valueDecoderClass Class of the value decoder
   * @param recordClass Class of the records in DStream
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
   *    starting point of the stream
   * @param messageHandler Function for translating each message and metadata into the desired type
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @tparam R type returned by messageHandler
   * @return DStream of R
   */
  def createDirectStream[K, V, KD <: Decoder[K], VD <: Decoder[V], R](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      recordClass: Class[R],
      kafkaParams: JMap[String, String],
      fromOffsets: JMap[TopicAndPartition, JLong],
      messageHandler: JFunction[MessageAndMetadata[K, V], R]
    ): JavaInputDStream[R] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    val cleanedHandler = jssc.sparkContext.clean(messageHandler.call _)
    createDirectStream[K, V, KD, VD, R](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Map(fromOffsets.asScala.mapValues(_.longValue()).toSeq: _*),
      cleanedHandler
    )
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the `StreamingContext`. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param jssc JavaStreamingContext object
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param keyDecoderClass Class of the key decoder
   * @param valueDecoderClass Class type of the value decoder
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   *   If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
   *   to determine where the stream starts (defaults to "largest")
   * @param topics Names of the topics to consume
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @return DStream of (Kafka message key, Kafka message value)
   */
  def createDirectStream[K, V, KD <: Decoder[K], VD <: Decoder[V]](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      kafkaParams: JMap[String, String],
      topics: JSet[String]
    ): JavaPairInputDStream[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    createDirectStream[K, V, KD, VD](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Set(topics.asScala.toSeq: _*)
    )
  }
}

/**
 * This is a helper class that wraps the KafkaUtils.createStream() into more
 * Python-friendly class and function so that it can be easily
 * instantiated and called from Python's KafkaUtils.
 *
 * The zero-arg constructor helps instantiate this class from the Class object
 * classOf[KafkaUtilsPythonHelper].newInstance(), and the createStream()
 * takes care of known parameters instead of passing them from Python
 */
private[kafka] class KafkaUtilsPythonHelper {
  import KafkaUtilsPythonHelper._

  def createStream(
      jssc: JavaStreamingContext,
      kafkaParams: JMap[String, String],
      topics: JMap[String, JInt],
      storageLevel: StorageLevel): JavaPairReceiverInputDStream[Array[Byte], Array[Byte]] = {
    KafkaUtils.createStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      jssc,
      classOf[Array[Byte]],
      classOf[Array[Byte]],
      classOf[DefaultDecoder],
      classOf[DefaultDecoder],
      kafkaParams,
      topics,
      storageLevel)
  }

  def createRDDWithoutMessageHandler(
      jsc: JavaSparkContext,
      kafkaParams: JMap[String, String],
      offsetRanges: JList[OffsetRange],
      leaders: JMap[TopicAndPartition, Broker]): JavaRDD[(Array[Byte], Array[Byte])] = {
    val messageHandler =
      (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => (mmd.key, mmd.message)
    new JavaRDD(createRDD(jsc, kafkaParams, offsetRanges, leaders, messageHandler))
  }

  def createRDDWithMessageHandler(
      jsc: JavaSparkContext,
      kafkaParams: JMap[String, String],
      offsetRanges: JList[OffsetRange],
      leaders: JMap[TopicAndPartition, Broker]): JavaRDD[Array[Byte]] = {
    val messageHandler = (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) =>
      new PythonMessageAndMetadata(
        mmd.topic, mmd.partition, mmd.offset, mmd.key(), mmd.message())
    val rdd = createRDD(jsc, kafkaParams, offsetRanges, leaders, messageHandler).
        mapPartitions(picklerIterator)
    new JavaRDD(rdd)
  }

  private def createRDD[V: ClassTag](
      jsc: JavaSparkContext,
      kafkaParams: JMap[String, String],
      offsetRanges: JList[OffsetRange],
      leaders: JMap[TopicAndPartition, Broker],
      messageHandler: MessageAndMetadata[Array[Byte], Array[Byte]] => V): RDD[V] = {
    KafkaUtils.createRDD[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder, V](
      jsc.sc,
      kafkaParams.asScala.toMap,
      offsetRanges.toArray(new Array[OffsetRange](offsetRanges.size())),
      leaders.asScala.toMap,
      messageHandler
    )
  }

  def createDirectStreamWithoutMessageHandler(
      jssc: JavaStreamingContext,
      kafkaParams: JMap[String, String],
      topics: JSet[String],
      fromOffsets: JMap[TopicAndPartition, JNumber]): JavaDStream[(Array[Byte], Array[Byte])] = {
    val messageHandler =
      (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => (mmd.key, mmd.message)
    new JavaDStream(createDirectStream(jssc, kafkaParams, topics, fromOffsets, messageHandler))
  }

  def createDirectStreamWithMessageHandler(
      jssc: JavaStreamingContext,
      kafkaParams: JMap[String, String],
      topics: JSet[String],
      fromOffsets: JMap[TopicAndPartition, JNumber]): JavaDStream[Array[Byte]] = {
    val messageHandler = (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) =>
      new PythonMessageAndMetadata(mmd.topic, mmd.partition, mmd.offset, mmd.key(), mmd.message())
    val stream = createDirectStream(jssc, kafkaParams, topics, fromOffsets, messageHandler).
      mapPartitions(picklerIterator)
    new JavaDStream(stream)
  }

  private def createDirectStream[V: ClassTag](
      jssc: JavaStreamingContext,
      kafkaParams: JMap[String, String],
      topics: JSet[String],
      fromOffsets: JMap[TopicAndPartition, JNumber],
      messageHandler: MessageAndMetadata[Array[Byte], Array[Byte]] => V): DStream[V] = {

    val currentFromOffsets = if (!fromOffsets.isEmpty) {
      val topicsFromOffsets = fromOffsets.keySet().asScala.map(_.topic)
      if (topicsFromOffsets != topics.asScala.toSet) {
        throw new IllegalStateException(
          s"The specified topics: ${topics.asScala.toSet.mkString(" ")} " +
          s"do not equal to the topic from offsets: ${topicsFromOffsets.mkString(" ")}")
      }
      Map(fromOffsets.asScala.mapValues { _.longValue() }.toSeq: _*)
    } else {
      val kc = new KafkaCluster(Map(kafkaParams.asScala.toSeq: _*))
      KafkaUtils.getFromOffsets(
        kc, Map(kafkaParams.asScala.toSeq: _*), Set(topics.asScala.toSeq: _*))
    }

    KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder, V](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Map(currentFromOffsets.toSeq: _*),
      messageHandler)
  }

  def createOffsetRange(topic: String, partition: JInt, fromOffset: JLong, untilOffset: JLong
    ): OffsetRange = OffsetRange.create(topic, partition, fromOffset, untilOffset)

  def createTopicAndPartition(topic: String, partition: JInt): TopicAndPartition =
    TopicAndPartition(topic, partition)

  def createBroker(host: String, port: JInt): Broker = Broker(host, port)

  def offsetRangesOfKafkaRDD(rdd: RDD[_]): JList[OffsetRange] = {
    val parentRDDs = rdd.getNarrowAncestors
    val kafkaRDDs = parentRDDs.filter(rdd => rdd.isInstanceOf[KafkaRDD[_, _, _, _, _]])

    require(
      kafkaRDDs.length == 1,
      "Cannot get offset ranges, as there may be multiple Kafka RDDs or no Kafka RDD associated" +
        "with this RDD, please call this method only on a Kafka RDD.")

    val kafkaRDD = kafkaRDDs.head.asInstanceOf[KafkaRDD[_, _, _, _, _]]
    kafkaRDD.offsetRanges.toSeq.asJava
  }
}

private object KafkaUtilsPythonHelper {
  private var initialized = false

  def initialize(): Unit = {
    SerDeUtil.initialize()
    synchronized {
      if (!initialized) {
        new PythonMessageAndMetadataPickler().register()
        initialized = true
      }
    }
  }

  initialize()

  def picklerIterator(iter: Iterator[Any]): Iterator[Array[Byte]] = {
    new SerDeUtil.AutoBatchedPickler(iter)
  }

  case class PythonMessageAndMetadata(
      topic: String,
      partition: JInt,
      offset: JLong,
      key: Array[Byte],
      message: Array[Byte])

  class PythonMessageAndMetadataPickler extends IObjectPickler {
    private val module = "pyspark.streaming.kafka"

    def register(): Unit = {
      Pickler.registerCustomPickler(classOf[PythonMessageAndMetadata], this)
      Pickler.registerCustomPickler(this.getClass, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler) {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write(s"$module\nKafkaMessageAndMetadata\n".getBytes(StandardCharsets.UTF_8))
      } else {
        pickler.save(this)
        val msgAndMetaData = obj.asInstanceOf[PythonMessageAndMetadata]
        out.write(Opcodes.MARK)
        pickler.save(msgAndMetaData.topic)
        pickler.save(msgAndMetaData.partition)
        pickler.save(msgAndMetaData.offset)
        pickler.save(msgAndMetaData.key)
        pickler.save(msgAndMetaData.message)
        out.write(Opcodes.TUPLE)
        out.write(Opcodes.REDUCE)
      }
    }
  }
}
