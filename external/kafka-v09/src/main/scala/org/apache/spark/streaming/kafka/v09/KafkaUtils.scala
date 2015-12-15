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

import java.io.OutputStream
import java.lang.{Integer => JInt, Long => JLong}
import java.util.{List => JList, Map => JMap, Set => JSet}

import scala.reflect.ClassTag

import com.google.common.base.Charsets.UTF_8
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, Decoder, StringDecoder}
import net.razorvine.pickle.{Opcodes, Pickler, IObjectPickler}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.{SparkContext, SparkException}

import scala.collection.JavaConverters._
import scala.reflect._
import org.apache.spark.api.java.{JavaSparkContext, JavaPairRDD, JavaRDD}
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}

object KafkaUtils {

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param ssc       StreamingContext object
   * @param servers   Broker servers (for Kafka 0.9) (hostname:port,hostname:port,..)
   * @param groupId   The group id for this consumer
   * @param topics    Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                  in its own thread
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   */
  def createStream(
      ssc: StreamingContext,
      servers: String,
      groupId: String,
      topics: Map[String, Int],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
     ): ReceiverInputDStream[(String, String)] = {
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> servers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "spark.kafka.poll.time" -> "1000"
    )
    createStream[String, String](
      ssc, kafkaParams, topics, storageLevel)
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param ssc         StreamingContext object
   * @param kafkaParams Map of kafka configuration parameters
   * @param topics      Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                    in its own thread.
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream[K: ClassTag, V: ClassTag](
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Map[String, Int],
      storageLevel: StorageLevel
    ): ReceiverInputDStream[(K, V)] = {
    val walEnabled = WriteAheadLogUtils.enableReceiverLog(ssc.conf)
    new KafkaInputDStream[K, V](ssc, kafkaParams, topics, walEnabled, storageLevel)
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param jssc      JavaStreamingContext object
   * @param keyTypeClass Key type of DStream
   * @param valueTypeClass value type of Dstream
   * @param kafkaParams Map of kafka configuration parameters
   * @param topics  Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                in its own thread
   * @param storageLevel RDD storage level.
   */
  def createStream[K, V](
      jssc: JavaStreamingContext,
      keyTypeClass: Class[K],
      valueTypeClass: Class[V],
      kafkaParams: JMap[String, String],
      topics: JMap[String, JInt],
      storageLevel: StorageLevel
    ): JavaPairReceiverInputDStream[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyTypeClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueTypeClass)

    createStream[K, V](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Map(topics.asScala.mapValues(_.intValue()).toSeq: _*),
      storageLevel)
  }

  /** Make sure offsets are available in kafka, or throw an exception */
  private def checkOffsets(
                            kc: KafkaCluster[_, _],
                            offsetRanges: Array[OffsetRange]): Unit = {
    val topics = offsetRanges.map(_.topicAndPartition).toSet
    val result = for {
      low <- kc.getEarliestOffsets(topics).right
      high <- kc.getLatestOffsets(topics).right
    } yield {
      offsetRanges.filterNot { o =>
        low(o.topicAndPartition) <= o.fromOffset &&
          o.untilOffset <= high(o.topicAndPartition)
      }
    }
    val badRanges = KafkaCluster.checkErrors(result)
    if (!badRanges.isEmpty) {
      throw new SparkException("Offsets not available on leader: " + badRanges.mkString(","))
    }
  }


  def createRDD[K: ClassTag, V: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      offsetRanges: Array[OffsetRange]
     ): RDD[(K, V)] = sc.withScope {
    val messageHandler = (cr: ConsumerRecord[K, V]) => (cr.key, cr.value)
    val kc = new KafkaCluster[K, V](kafkaParams)
    checkOffsets(kc, offsetRanges)
    new KafkaRDD[K, V, (K, V)](sc, kafkaParams, offsetRanges, messageHandler)
  }

  /**
   * Create a RDD from Kafka using offset ranges for each topic and partition. This allows you
   * specify the Kafka leader to connect to (to optimize fetching) and access the message as well
   * as the metadata.
   *
   * @param sc SparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *                    configuration parameters</a>. Requires "bootstrap.servers"
   *                    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *                    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *                     range of offsets for a given Kafka topic/partition
   * @param messageHandler Function for translating each message and metadata into the desired type
   */
  def createRDD[K: ClassTag, V: ClassTag, R: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      offsetRanges: Array[OffsetRange],
      messageHandler: ConsumerRecord[K, V] => R
     ): RDD[R] = sc.withScope {
    val kc = new KafkaCluster[K, V](kafkaParams)
    val cleanedHandler = sc.clean(messageHandler)
    checkOffsets(kc, offsetRanges)
    new KafkaRDD[K, V, R](sc, kafkaParams, offsetRanges, cleanedHandler)
  }

  /**
   * Create a RDD from Kafka using offset ranges for each topic and partition.
   *
   * @param jsc JavaSparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "bootstrap.servers"
   *    specified in host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   */
  def createRDD[K, V, KD <: Decoder[K], VD <: Decoder[V]](
      jsc: JavaSparkContext,
      keyClass: Class[K],
      valueClass: Class[V],
      kafkaParams: JMap[String, String],
      offsetRanges: Array[OffsetRange]
    ): JavaPairRDD[K, V] = jsc.sc.withScope {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(createRDD[K, V](
      jsc.sc, Map(kafkaParams.asScala.toSeq: _*), offsetRanges))
  }

  /**
   * Create a RDD from Kafka using offset ranges for each topic and partition. This allows you
   * specify the Kafka leader to connect to (to optimize fetching) and access the message as well
   * as the metadata.
   *
   * @param jsc JavaSparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "bootstrap.servers"
   *    specified in host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   * @param messageHandler Function for translating each message and metadata into the desired type
   */
  def createRDD[K, V, KD <: Decoder[K], VD <: Decoder[V], R](
      jsc: JavaSparkContext,
      keyClass: Class[K],
      valueClass: Class[V],
      recordClass: Class[R],
      kafkaParams: JMap[String, String],
      offsetRanges: Array[OffsetRange],
      messageHandler: JFunction[ConsumerRecord[K, V], R]
    ): JavaRDD[R] = jsc.sc.withScope {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    createRDD[K, V, R](
      jsc.sc, Map(kafkaParams.asScala.toSeq: _*), offsetRanges, messageHandler.call _)
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   * - No receivers: This stream does not use any receiver. It directly queries Kafka
   * - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   * by the stream itself.
   * You can access the offsets used in each batch from the generated RDDs (see
   * [[org.apache.spark.streaming.kafka.v09.HasOffsetRanges]]).
   * - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   * in the [[StreamingContext]]. The information on consumed offset can be
   * recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   * - End-to-end semantics: This stream ensures that every records is effectively received and
   * transformed exactly once, but gives no guarantees on whether the transformed data are
   * outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   * that the output operation is idempotent, or use transactions to output records atomically.
   * See the programming guide for more details.
   *
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *                    configuration parameters</a>. Requires "bootstrap.servers"
   *                    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *                    host1:port1,host2:port2 form.
   * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
   *                    starting point of the stream
   * @param messageHandler Function for translating each message and metadata into the desired type
   */
  def createDirectStream[K: ClassTag, V: ClassTag, R: ClassTag](
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      fromOffsets: Map[TopicAndPartition, Long],
      messageHandler: ConsumerRecord[K, V] => R
     ): InputDStream[R] = {
    val cleanedHandler = ssc.sc.clean(messageHandler)
    new DirectKafkaInputDStream[K, V, R](
      ssc, kafkaParams, fromOffsets, messageHandler)
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   * - No receivers: This stream does not use any receiver. It directly queries Kafka
   * - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   * by the stream itself.
   * You can access the offsets used in each batch from the generated RDDs (see
   * [[org.apache.spark.streaming.kafka.v09.HasOffsetRanges]]).
   * - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   * in the [[StreamingContext]]. The information on consumed offset can be
   * recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   * - End-to-end semantics: This stream ensures that every records is effectively received and
   * transformed exactly once, but gives no guarantees on whether the transformed data are
   * outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   * that the output operation is idempotent, or use transactions to output records atomically.
   * See the programming guide for more details.
   *
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *                    configuration parameters</a>. Requires "bootstrap.servers"
   *                    to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *                    host1:port1,host2:port2 form.
   *                    If not starting from a checkpoint, "auto.offset.reset" may be set to
   *                    "earliest" or "latest" to determine where the stream starts
   *                    (defaults to "latest")
   * @param topics Names of the topics to consume
   */
  def createDirectStream[K: ClassTag, V: ClassTag](
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Set[String]
     ): InputDStream[(K, V)] = {
    val messageHandler = (cr: ConsumerRecord[K, V]) => (cr.key, cr.value)
    val kc = new KafkaCluster[K, V](kafkaParams)
    val reset = kafkaParams.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).map(_.toLowerCase)

    val fromOffsets = if (reset == Some("earliest")) {
      kc.getEarliestOffsets(kc.getPartitions(topics).right.get).right.get
    } else {
      kc.getLatestOffsets(kc.getPartitions(topics).right.get).right.get
    }

    kc.close()

    new DirectKafkaInputDStream[K, V, (K, V)](
      ssc, kafkaParams, fromOffsets, messageHandler)
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   * - No receivers: This stream does not use any receiver. It directly queries Kafka
   * - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   * by the stream itself.
   * You can access the offsets used in each batch from the generated RDDs (see
   * [[org.apache.spark.streaming.kafka.v09.HasOffsetRanges]]).
   * - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   * in the [[StreamingContext]]. The information on consumed offset can be
   * recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   * - End-to-end semantics: This stream ensures that every records is effectively received and
   * transformed exactly once, but gives no guarantees on whether the transformed data are
   * outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   * that the output operation is idempotent, or use transactions to output records atomically.
   * See the programming guide for more details.
   *
   * @param jssc JavaStreamingContext object
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param recordClass Class of the records in DStream
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *                    configuration parameters</a>. Requires "bootstrap.servers"
   *                    specified in host1:port1,host2:port2 form.
   * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
   *                    starting point of the stream
   * @param messageHandler Function for translating each message and metadata into the desired type
   */
  def createDirectStream[K, V, R](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      recordClass: Class[R],
      kafkaParams: JMap[String, String],
      fromOffsets: JMap[TopicAndPartition, JLong],
      messageHandler: JFunction[ConsumerRecord[K, V], R]
     ): JavaInputDStream[R] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    val cleanedHandler = jssc.sparkContext.clean(messageHandler.call _)
    createDirectStream[K, V, R](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Map(fromOffsets.asScala.mapValues {
        _.longValue()
      }.toSeq: _*),
      cleanedHandler
    )
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   * - No receivers: This stream does not use any receiver. It directly queries Kafka
   * - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   * by the stream itself.
   * You can access the offsets used in each batch from the generated RDDs (see
   * [[org.apache.spark.streaming.kafka.v09.HasOffsetRanges]]).
   * - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   * in the [[StreamingContext]]. The information on consumed offset can be
   * recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   * - End-to-end semantics: This stream ensures that every records is effectively received and
   * transformed exactly once, but gives no guarantees on whether the transformed data are
   * outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   * that the output operation is idempotent, or use transactions to output records atomically.
   * See the programming guide for more details.
   *
   * @param jssc JavaStreamingContext object
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *                    configuration parameters</a>. Requires "bootstrap.servers"
   *                    to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *                    host1:port1,host2:port2 form.
   *                    If not starting from a checkpoint, "auto.offset.reset" may be set
   *                    to "latest" or "earliest" to determine where the stream starts
   *                    (defaults to "latest")
   * @param topics Names of the topics to consume
   */
  def createDirectStream[K, V](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      kafkaParams: JMap[String, String],
      topics: JSet[String]
     ): JavaPairInputDStream[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    createDirectStream[K, V](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Set(topics.asScala.toSeq: _*)
    )
  }

  def createOffsetRange(
      topic: String,
      partition: JInt,
      fromOffset: JLong,
      untilOffset: JLong
     ): OffsetRange = OffsetRange.create(topic, partition, fromOffset, untilOffset)

  def createTopicAndPartition(topic: String, partition: JInt): TopicAndPartition =
    TopicAndPartition(topic, partition)
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
        out.write(s"$module\nKafkaMessageAndMetadata\n".getBytes(UTF_8))
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
