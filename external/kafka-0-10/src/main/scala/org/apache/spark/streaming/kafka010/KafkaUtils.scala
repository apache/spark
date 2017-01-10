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

import java.{util => ju}
import java.io.OutputStream
import java.lang.{Integer => JInt, Long => JLong}
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.collection.mutable

import net.razorvine.pickle.{IObjectPickler, Opcodes, Pickler}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaDStream, JavaInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream._

/**
 * :: Experimental ::
 * object for constructing Kafka streams and RDDs
 */
@Experimental
object KafkaUtils extends Logging {
  /**
   * :: Experimental ::
   * Scala constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   *
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
   *   see [[LocationStrategies]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createRDD[K, V](
      sc: SparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy
    ): RDD[ConsumerRecord[K, V]] = {
    val preferredHosts = locationStrategy match {
      case PreferBrokers =>
        throw new AssertionError(
          "If you want to prefer brokers, you must provide a mapping using PreferFixed " +
          "A single KafkaRDD does not have a driver consumer and cannot look up brokers for you.")
      case PreferConsistent => ju.Collections.emptyMap[TopicPartition, String]()
      case PreferFixed(hostMap) => hostMap
    }
    val kp = new ju.HashMap[String, Object](kafkaParams)
    fixKafkaParams(kp)
    val osr = offsetRanges.clone()

    new KafkaRDD[K, V](sc, kp, osr, preferredHosts, true)
  }

  /**
   * :: Experimental ::
   * Java constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   *
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
   *   see [[LocationStrategies]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createRDD[K, V](
      jsc: JavaSparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy
    ): JavaRDD[ConsumerRecord[K, V]] = {

    new JavaRDD(createRDD[K, V](jsc.sc, kafkaParams, offsetRanges, locationStrategy))
  }

  /**
   * :: Experimental ::
   * Scala constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
   *  of messages
   * per second that each '''partition''' will accept.
   * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in ConsumerStrategies.subscribe,
   *   see [[ConsumerStrategies]] for more details
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    ): InputDStream[ConsumerRecord[K, V]] = {
    val ppc = new DefaultPerPartitionConfig(ssc.sparkContext.getConf)
    createDirectStream[K, V](ssc, locationStrategy, consumerStrategy, ppc)
  }

  /**
   * :: Experimental ::
   * Scala constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in ConsumerStrategies.subscribe,
   *   see [[ConsumerStrategies]] for more details.
   * @param perPartitionConfig configuration of settings such as max rate on a per-partition basis.
   *   see [[PerPartitionConfig]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V],
      perPartitionConfig: PerPartitionConfig
    ): InputDStream[ConsumerRecord[K, V]] = {
    new DirectKafkaInputDStream[K, V](ssc, locationStrategy, consumerStrategy, perPartitionConfig)
  }

  /**
   * :: Experimental ::
   * Java constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in ConsumerStrategies.subscribe,
   *   see [[ConsumerStrategies]] for more details
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
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
   * :: Experimental ::
   * Java constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in ConsumerStrategies.subscribe,
   *   see [[ConsumerStrategies]] for more details
   * @param perPartitionConfig configuration of settings such as max rate on a per-partition basis.
   *   see [[PerPartitionConfig]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
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

private[kafka010] class KafkaUtilsPythonHelper extends Logging {
  import KafkaUtilsPythonHelper._

  def createDirectStream(
      jssc: JavaStreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[Array[Byte], Array[Byte]]
    ): JavaDStream[Array[Byte]] = {
    validateKafkaParams(consumerStrategy.executorKafkaParams)
    val stream = KafkaUtils.createDirectStream(jssc.ssc, locationStrategy, consumerStrategy)
      .map { r =>
        PythonConsumerRecord(r.topic(), r.partition(), r.offset(), r.timestamp(),
          r.timestampType().toString, r.checksum(), r.serializedKeySize(), r.serializedValueSize(),
            r.key(), r.value())
      }.mapPartitions(picklerIterator)
    new JavaDStream(stream)
  }

  def createRDD(
      jsc: JavaSparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: ju.List[OffsetRange],
      locationStrategy: LocationStrategy
    ): JavaRDD[Array[Byte]] = {
    validateKafkaParams(kafkaParams)
    val rdd = KafkaUtils.createRDD[Array[Byte], Array[Byte]](
      jsc.sc,
      kafkaParams,
      offsetRanges.toArray(new Array[OffsetRange](offsetRanges.size())),
      locationStrategy)
        .map { r =>
          PythonConsumerRecord(r.topic(), r.partition(), r.offset(), r.timestamp(),
            r.timestampType().toString, r.checksum(), r.serializedKeySize(),
              r.serializedValueSize(), r.key(), r.value())
        }.mapPartitions(picklerIterator)
    new JavaRDD(rdd)
  }

  private def validateKafkaParams(kafkaParams: ju.Map[String, Object]): Unit = {
    val decoder = classOf[ByteArrayDeserializer].getCanonicalName

    val keyDecoder = kafkaParams.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
    if (keyDecoder == null || keyDecoder != decoder) {
      throw new SparkException(s"${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG} $keyDecoder " +
        s"is not supported for python Kafka API, please configured with $decoder")
    }

    val valueDecoder = kafkaParams.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
    if (valueDecoder == null || valueDecoder != decoder) {
      throw new SparkException(s"${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG} $valueDecoder " +
        s"is not supported for python Kafka API, please configured with $decoder")
    }
  }

  // Helper functions to convert Python object to Java object
  def createOffsetRange(topic: String, partition: JInt, fromOffset: JLong, untilOffset: JLong
      ): OffsetRange = OffsetRange.create(topic, partition, fromOffset, untilOffset)

  def createPreferBrokers(): LocationStrategy = LocationStrategies.PreferBrokers

  def createPreferConsistent(): LocationStrategy = LocationStrategies.PreferConsistent

  def createPreferFixed(hostMap: ju.Map[TopicPartition, String]): LocationStrategy = {
    LocationStrategies.PreferFixed(hostMap)
  }

  def createTopicPartition(topic: String, partition: JInt): TopicPartition =
    new TopicPartition(topic, partition)

  def createSubscribe(
      topics: ju.Set[String],
      kafkaParams: ju.Map[String, Object],
      offsets: ju.Map[TopicPartition, JLong]): ConsumerStrategy[Array[Byte], Array[Byte]] =
    ConsumerStrategies.Subscribe(topics, kafkaParams, offsets)

  def createSubscribePattern(
      pattern: String,
      kafkaParams: ju.Map[String, Object],
      offsets: ju.Map[TopicPartition, JLong]): ConsumerStrategy[Array[Byte], Array[Byte]] = {
    ConsumerStrategies.SubscribePattern(ju.regex.Pattern.compile(pattern), kafkaParams, offsets)
  }

  def createAssign(
      topicPartitions: ju.Set[TopicPartition],
      kafkaParams: ju.Map[String, Object],
      offsets: ju.Map[TopicPartition, JLong]): ConsumerStrategy[Array[Byte], Array[Byte]] = {
    ConsumerStrategies.Assign(topicPartitions, kafkaParams, offsets)
  }

  def offsetRangesOfKafkaRDD(rdd: RDD[_]): ju.List[OffsetRange] = {
    val parentRDDs = rdd.getNarrowAncestors
    val kafkaRDDs = parentRDDs.filter(rdd => rdd.isInstanceOf[KafkaRDD[_, _]])

    require(
      kafkaRDDs.length == 1,
      "Cannot get offset ranges, as there may be multiple Kafka RDDs or no Kafka RDD associated" +
        "with this RDD, please call this method only on a Kafka RDD.")

    val kafkaRDD = kafkaRDDs.head.asInstanceOf[KafkaRDD[_, _]]
    kafkaRDD.offsetRanges.toSeq.asJava
  }

  def commitAsyncForKafkaDStream(dstream: DStream[_], offsetRanges: ju.List[OffsetRange]): Unit = {
    val dstreams = new mutable.HashSet[DStream[_]]()

    def visit(parent: DStream[_]): Unit = {
      val parents = parent.dependencies
      parents.filterNot(dstreams.contains).foreach { p =>
        dstreams.add(p)
        visit(p)
      }
    }
    visit(dstream)

    val kafkaDStreams = dstreams.filter(s => s.isInstanceOf[DirectKafkaInputDStream[_, _]])
    require(
      kafkaDStreams.size == 1,
      "Cannot commit offset ranges to DirectKafkaInputDStream, as there may be multiple Kafka " +
        "DStreams or no Kafka DStream associated with this DStream, please call this method only " +
        "on a Kafka DStream."
    )

    val kafkaDStream = kafkaDStreams.head.asInstanceOf[DirectKafkaInputDStream[_, _]]
    kafkaDStream.commitAsync(offsetRanges.asScala.toArray)
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
      timestamp: JLong,
      timestampType: String,
      checksum: JLong,
      serializedKeySize: JInt,
      serializedValueSize: JInt,
      key: Array[Byte],
      value: Array[Byte])

  class PythonConsumerRecordPickler extends IObjectPickler {
    private val module = "pyspark.streaming.kafka010"

    def register(): Unit = {
      Pickler.registerCustomPickler(classOf[PythonConsumerRecord], this)
      Pickler.registerCustomPickler(this.getClass, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write(s"$module\nKafkaConsumerRecord\n".getBytes(StandardCharsets.UTF_8))
      } else {
        pickler.save(this)
        val consumerRecord = obj.asInstanceOf[PythonConsumerRecord]
        out.write(Opcodes.MARK)
        pickler.save(consumerRecord.topic)
        pickler.save(consumerRecord.partition)
        pickler.save(consumerRecord.offset)
        pickler.save(consumerRecord.timestamp)
        pickler.save(consumerRecord.timestampType)
        pickler.save(consumerRecord.checksum)
        pickler.save(consumerRecord.serializedKeySize)
        pickler.save(consumerRecord.serializedValueSize)
        pickler.save(consumerRecord.key)
        pickler.save(consumerRecord.value)
        out.write(Opcodes.TUPLE)
        out.write(Opcodes.REDUCE)
      }
    }
  }
}
