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
import java.lang.{ Integer => JInt, Long => JLong }
import java.util.{ List => JList, Map => JMap, Set => JSet }
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import com.google.common.base.Charsets.UTF_8
import kafka.common.TopicAndPartition
import net.razorvine.pickle.{ IObjectPickler, Opcodes, Pickler }
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SslConfigs
import org.apache.spark.{ SSLOptions, SparkContext, SparkException }
import org.apache.spark.api.java.{ JavaPairRDD, JavaRDD, JavaSparkContext }
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }

object KafkaUtils {

  def addSSLOptions(
    kafkaParams: Map[String, String],
    sc: SparkContext): Map[String, String] = {

    val sparkConf = sc.getConf
    val defaultSSLOptions = SSLOptions.parse(sparkConf, "spark.ssl", None)
    val kafkaSSLOptions = SSLOptions.parse(sparkConf, "spark.ssl.kafka", Some(defaultSSLOptions))

    if (kafkaSSLOptions.enabled) {
      val sslParams = Map[String, Option[_]](
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> Some("SSL"),
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> kafkaSSLOptions.trustStore,
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> kafkaSSLOptions.trustStorePassword,
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> kafkaSSLOptions.keyStore,
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> kafkaSSLOptions.keyStorePassword,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG -> kafkaSSLOptions.keyPassword)
      kafkaParams ++ sslParams.filter(_._2.isDefined).mapValues(_.get.toString)
    } else {
      kafkaParams
    }

  }

  /** Make sure offsets are available in kafka, or throw an exception */
  private def checkOffsets(
    kafkaParams: Map[String, String],
    offsetRanges: Array[OffsetRange]): Array[OffsetRange] = {
    val kc = new KafkaCluster(kafkaParams)
    try {
      val topics = offsetRanges.map(_.topicPartition).toSet
      val low = kc.getEarliestOffsets(topics)
      val high = kc.getLatestOffsetsWithLeaders(topics)

      val result = offsetRanges.filterNot { o =>
        low(o.topicPartition()) <= o.fromOffset &&
          o.untilOffset <= high(o.topicPartition()).offset
      }

      if (!result.isEmpty) {
        throw new SparkException("Offsets not available in Kafka: " + result.mkString(","))
      }

      offsetRanges.map { o =>
        OffsetRange(o.topic, o.partition, o.fromOffset, o.untilOffset,
          high(o.topicPartition()).host)
      }
    } finally {
      kc.close()
    }
  }

  /**
   * Create a RDD from Kafka using offset ranges for each topic and partition.
   *
   * @param sc SparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *                    configuration parameters</a>. Requires "bootstrap.servers"
   *                    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *                    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *                     range of offsets for a given Kafka topic/partition
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @return RDD of (Kafka message key, Kafka message value)
   */
  def createRDD[K: ClassTag, V: ClassTag](
    sc: SparkContext,
    kafkaParams: Map[String, String],
    offsetRanges: Array[OffsetRange]): RDD[(K, V)] = sc.withScope {
    val messageHandler = (cr: ConsumerRecord[K, V]) => (cr.key, cr.value)
    new KafkaRDD[K, V, (K, V)](
      sc,
      addSSLOptions(kafkaParams, sc),
      checkOffsets(kafkaParams, offsetRanges),
      messageHandler)
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
   *                       * @tparam K type of Kafka message key
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam R type returned by messageHandler
   * @return RDD of R
   */
  def createRDD[K: ClassTag, V: ClassTag, R: ClassTag](
    sc: SparkContext,
    kafkaParams: Map[String, String],
    offsetRanges: Array[OffsetRange],
    messageHandler: ConsumerRecord[K, V] => R): RDD[R] = sc.withScope {
    val kc = new KafkaCluster[K, V](addSSLOptions(kafkaParams, sc))
    val cleanedHandler = sc.clean(messageHandler)
    new KafkaRDD[K, V, R](sc,
      addSSLOptions(kafkaParams, sc),
      checkOffsets(kafkaParams, offsetRanges),
      cleanedHandler)
  }

  /**
   * Create a RDD from Kafka using offset ranges for each topic and partition.
   *
   * @param jsc JavaSparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *                    configuration parameters</a>. Requires "bootstrap.servers"
   *                    specified in host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *                     range of offsets for a given Kafka topic/partition
   * @param keyClass type of Kafka message key
   * @param valueClass type of Kafka message value
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @return RDD of (Kafka message key, Kafka message value)
   */
  def createRDD[K, V](
    jsc: JavaSparkContext,
    keyClass: Class[K],
    valueClass: Class[V],
    kafkaParams: JMap[String, String],
    offsetRanges: Array[OffsetRange]): JavaPairRDD[K, V] = jsc.sc.withScope {
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
   *                    configuration parameters</a>. Requires "bootstrap.servers"
   *                    specified in host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *                     range of offsets for a given Kafka topic/partition
   * @param messageHandler Function for translating each message and metadata into the desired type
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam R type returned by messageHandler
   * @return RDD of R
   */
  def createRDD[K, V, R](
    jsc: JavaSparkContext,
    keyClass: Class[K],
    valueClass: Class[V],
    recordClass: Class[R],
    kafkaParams: JMap[String, String],
    offsetRanges: Array[OffsetRange],
    messageHandler: JFunction[ConsumerRecord[K, V], R]): JavaRDD[R] = jsc.sc.withScope {
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
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam R type returned by messageHandler
   * @return DStream of R
   */
  def createDirectStream[K: ClassTag, V: ClassTag, R: ClassTag](
    ssc: StreamingContext,
    kafkaParams: Map[String, String],
    fromOffsets: Map[TopicPartition, Long],
    messageHandler: ConsumerRecord[K, V] => R): InputDStream[R] = {
    val cleanedHandler = ssc.sc.clean(messageHandler)
    new DirectKafkaInputDStream[K, V, R](
      ssc, addSSLOptions(kafkaParams, ssc.sparkContext), fromOffsets, messageHandler)
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
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @return DStream of (Kafka message key, Kafka message value)
   */
  def createDirectStream[K: ClassTag, V: ClassTag](
    ssc: StreamingContext,
    kafkaParams: Map[String, String],
    topics: Set[String]): InputDStream[(K, V)] = {
    val messageHandler = (cr: ConsumerRecord[K, V]) => (cr.key, cr.value)
    val fromOffsets = getFromOffsets(kafkaParams, topics)

    new DirectKafkaInputDStream[K, V, (K, V)](
      ssc, addSSLOptions(kafkaParams, ssc.sparkContext), fromOffsets, messageHandler)
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
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam R type returned by messageHandler
   * @return DStream of R
   */
  def createDirectStream[K, V, R](
    jssc: JavaStreamingContext,
    keyClass: Class[K],
    valueClass: Class[V],
    recordClass: Class[R],
    kafkaParams: JMap[String, String],
    fromOffsets: JMap[TopicPartition, JLong],
    messageHandler: JFunction[ConsumerRecord[K, V], R]): JavaInputDStream[R] = {
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
      cleanedHandler)
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
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @return DStream of (Kafka message key, Kafka message value)
   */
  def createDirectStream[K, V](
    jssc: JavaStreamingContext,
    keyClass: Class[K],
    valueClass: Class[V],
    kafkaParams: JMap[String, String],
    topics: JSet[String]): JavaPairInputDStream[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    createDirectStream[K, V](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Set(topics.asScala.toSeq: _*))
  }

  def createOffsetRange(
    topic: String,
    partition: JInt,
    fromOffset: JLong,
    untilOffset: JLong): OffsetRange = OffsetRange.create(topic, partition, fromOffset, untilOffset)

  def createTopicAndPartition(topic: String, partition: JInt): TopicAndPartition =
    TopicAndPartition(topic, partition)

  private[kafka] def getFromOffsets(
    kafkaParams: Map[String, String],
    topics: Set[String]): Map[TopicPartition, Long] = {
    val kc = new KafkaCluster(kafkaParams)
    try {
      val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
      if (reset == Some("earliest")) {
        kc.getEarliestOffsets(kc.getPartitions(topics))
      } else {
        kc.getLatestOffsets(kc.getPartitions(topics))
      }
    } finally {
      kc.close()
    }
  }
}

/**
 * This is a helper class that wraps the KafkaUtils.createStream() into more
 * Python-friendly class and function so that it can be easily
 * instantiated and called from Python's KafkaUtils (see SPARK-6027).
 *
 * The zero-arg constructor helps instantiate this class from the Class object
 * classOf[KafkaUtilsPythonHelper].newInstance(), and the createStream()
 * takes care of known parameters instead of passing them from Python
 */
private[kafka] class KafkaUtilsPythonHelper {

  import KafkaUtilsPythonHelper._

  def createRDDWithoutMessageHandler(
    jsc: JavaSparkContext,
    kafkaParams: JMap[String, String],
    offsetRanges: JList[OffsetRange]): JavaRDD[(Array[Byte], Array[Byte])] = {
    val messageHandler =
      (cr: ConsumerRecord[Array[Byte], Array[Byte]]) => (cr.key, cr.value)
    new JavaRDD(createRDD(jsc, kafkaParams, offsetRanges, messageHandler))
  }

  def createRDDWithMessageHandler(
    jsc: JavaSparkContext,
    kafkaParams: JMap[String, String],
    offsetRanges: JList[OffsetRange]): JavaRDD[Array[Byte]] = {
    val messageHandler = (cr: ConsumerRecord[Array[Byte], Array[Byte]]) =>
      new PythonConsumerRecord(
        cr.topic, cr.partition, cr.offset, cr.key(), cr.value())
    val rdd = createRDD(jsc, kafkaParams, offsetRanges, messageHandler).
      mapPartitions(picklerIterator)
    new JavaRDD(rdd)
  }

  private def createRDD[V: ClassTag](
    jsc: JavaSparkContext,
    kafkaParams: JMap[String, String],
    offsetRanges: JList[OffsetRange],
    messageHandler: ConsumerRecord[Array[Byte], Array[Byte]] => V): RDD[V] = {
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "ByteArrayDeserializer")
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "ByteArrayDeserializer")
    KafkaUtils.createRDD[Array[Byte], Array[Byte], V](
      jsc.sc,
      kafkaParams.asScala.toMap,
      offsetRanges.toArray(new Array[OffsetRange](offsetRanges.size())),
      messageHandler)
  }

  def createDirectStreamWithoutMessageHandler(
    jssc: JavaStreamingContext,
    kafkaParams: JMap[String, String],
    topics: JSet[String],
    fromOffsets: JMap[TopicPartition, JLong]): JavaDStream[(Array[Byte], Array[Byte])] = {
    val messageHandler =
      (cr: ConsumerRecord[Array[Byte], Array[Byte]]) => (cr.key, cr.value)
    new JavaDStream(createDirectStream(jssc, kafkaParams, topics, fromOffsets, messageHandler))
  }

  def createDirectStreamWithMessageHandler(
    jssc: JavaStreamingContext,
    kafkaParams: JMap[String, String],
    topics: JSet[String],
    fromOffsets: JMap[TopicPartition, JLong]): JavaDStream[Array[Byte]] = {
    val messageHandler = (cr: ConsumerRecord[Array[Byte], Array[Byte]]) =>
      new PythonConsumerRecord(cr.topic, cr.partition, cr.offset, cr.key(), cr.value())
    val stream = createDirectStream(jssc, kafkaParams, topics, fromOffsets, messageHandler).
      mapPartitions(picklerIterator)
    new JavaDStream(stream)
  }

  private def createDirectStream[V: ClassTag](
    jssc: JavaStreamingContext,
    kafkaParams: JMap[String, String],
    topics: JSet[String],
    fromOffsets: JMap[TopicPartition, JLong],
    messageHandler: ConsumerRecord[Array[Byte], Array[Byte]] => V): DStream[V] = {

    val currentFromOffsets = if (!fromOffsets.isEmpty) {
      val topicsFromOffsets = fromOffsets.keySet().asScala.map(_.topic)
      if (topicsFromOffsets != topics.asScala.toSet) {
        throw new IllegalStateException(
          s"The specified topics: ${topics.asScala.toSet.mkString(" ")} " +
            s"do not equal to the topic from offsets: ${topicsFromOffsets.mkString(" ")}")
      }
      Map(fromOffsets.asScala.mapValues {
        _.longValue()
      }.toSeq: _*)
    } else {
      KafkaUtils.getFromOffsets(
        Map(kafkaParams.asScala.toSeq: _*), Set(topics.asScala.toSeq: _*))
    }

    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "ByteArrayDeserializer")
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "ByteArrayDeserializer")
    KafkaUtils.createDirectStream[Array[Byte], Array[Byte], V](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Map(currentFromOffsets.toSeq: _*),
      messageHandler)
  }

  def createOffsetRange(
    topic: String,
    partition: JInt,
    fromOffset: JLong,
    untilOffset: JLong): OffsetRange = OffsetRange.create(topic, partition, fromOffset, untilOffset)

  def createTopicAndPartition(topic: String, partition: JInt): TopicPartition =
    new TopicPartition(topic, partition)

  def offsetRangesOfKafkaRDD(rdd: RDD[_]): JList[OffsetRange] = {
    val parentRDDs = rdd.getNarrowAncestors
    val kafkaRDDs = parentRDDs.filter(rdd => rdd.isInstanceOf[KafkaRDD[_, _, _]])

    require(
      kafkaRDDs.length == 1,
      "Cannot get offset ranges, as there may be multiple Kafka RDDs or no Kafka RDD associated" +
        "with this RDD, please call this method only on a Kafka RDD.")

    val kafkaRDD = kafkaRDDs.head.asInstanceOf[KafkaRDD[_, _, _]]
    kafkaRDD.offsetRanges.toSeq.asJava
  }
}

private object KafkaUtilsPythonHelper {
  private var initialized = false

  def initialize(): Unit = {
    SerDeUtil.initialize()
    synchronized {
      if (!initialized) {
        new PythonConsumerRecordPickler().register()
        initialized = true
      }
    }
  }

  initialize()

  def picklerIterator(iter: Iterator[Any]): Iterator[Array[Byte]] = {
    new SerDeUtil.AutoBatchedPickler(iter)
  }

  case class PythonConsumerRecord(
    topic: String,
    partition: JInt,
    offset: JLong,
    key: Array[Byte],
    message: Array[Byte])

  class PythonConsumerRecordPickler extends IObjectPickler {
    private val module = "pyspark.streaming.kafka"

    def register(): Unit = {
      Pickler.registerCustomPickler(classOf[PythonConsumerRecord], this)
      Pickler.registerCustomPickler(this.getClass, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler) {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write(s"$module\nKafkaMessageAndMetadata\n".getBytes(UTF_8))
      } else {
        pickler.save(this)
        val msgAndMetaData = obj.asInstanceOf[PythonConsumerRecord]
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
