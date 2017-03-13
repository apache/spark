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

package org.apache.spark.sql.kafka010

import java.{util => ju}
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * The provider class for the [[KafkaSource]]. This provider is designed such that it throws
 * IllegalArgumentException when the Kafka Dataset is created, so that it can catch
 * missing options even before the query is started.
 */
private[kafka010] class KafkaSourceProvider extends DataSourceRegister
    with StreamSourceProvider
    with StreamSinkProvider
    with RelationProvider
    with CreatableRelationProvider
    with Logging {
  import KafkaSourceProvider._

  override def shortName(): String = "kafka"

  /**
   * Returns the name and schema of the source. In addition, it also verifies whether the options
   * are correct and sufficient to create the [[KafkaSource]] when the query is started.
   */
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    validateStreamOptions(parameters)
    require(schema.isEmpty, "Kafka source has a fixed schema and cannot be set with a custom one")
    (shortName(), KafkaOffsetReader.kafkaSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    validateStreamOptions(parameters)
    // Each running query should use its own group id. Otherwise, the query may be only assigned
    // partial data since Kafka will assign partitions to multiple consumers having the same group
    // id. Hence, we should generate a unique id for each query.
    val uniqueGroupId = s"spark-kafka-source-${UUID.randomUUID}-${metadataPath.hashCode}"

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
    val specifiedKafkaParams =
      parameters
        .keySet
        .filter(_.toLowerCase.startsWith("kafka."))
        .map { k => k.drop(6).toString -> parameters(k) }
        .toMap

    val startingStreamOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(caseInsensitiveParams,
      STARTING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)

    val kafkaOffsetReader = new KafkaOffsetReader(
      strategy(caseInsensitiveParams),
      kafkaParamsForDriver(specifiedKafkaParams),
      parameters,
      driverGroupIdPrefix = s"$uniqueGroupId-driver")

    new KafkaSource(
      sqlContext,
      kafkaOffsetReader,
      kafkaParamsForExecutors(specifiedKafkaParams, uniqueGroupId),
      parameters,
      metadataPath,
      startingStreamOffsets,
      failOnDataLoss(caseInsensitiveParams))
  }

  /**
   * Returns a new base relation with the given parameters.
   *
   * @note The parameters' keywords are case insensitive and this insensitivity is enforced
   *       by the Map that is passed to the function.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    validateBatchOptions(parameters)
    // Each running query should use its own group id. Otherwise, the query may be only assigned
    // partial data since Kafka will assign partitions to multiple consumers having the same group
    // id. Hence, we should generate a unique id for each query.
    val uniqueGroupId = s"spark-kafka-relation-${UUID.randomUUID}"
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
    val specifiedKafkaParams =
      parameters
        .keySet
        .filter(_.toLowerCase.startsWith("kafka."))
        .map { k => k.drop(6).toString -> parameters(k) }
        .toMap

    val startingRelationOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(
      caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, EarliestOffsetRangeLimit)
    assert(startingRelationOffsets != LatestOffsetRangeLimit)

    val endingRelationOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(caseInsensitiveParams,
      ENDING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)
    assert(endingRelationOffsets != EarliestOffsetRangeLimit)

    val kafkaOffsetReader = new KafkaOffsetReader(
      strategy(caseInsensitiveParams),
      kafkaParamsForDriver(specifiedKafkaParams),
      parameters,
      driverGroupIdPrefix = s"$uniqueGroupId-driver")

    new KafkaRelation(
      sqlContext,
      kafkaOffsetReader,
      kafkaParamsForExecutors(specifiedKafkaParams, uniqueGroupId),
      parameters,
      failOnDataLoss(caseInsensitiveParams),
      startingRelationOffsets,
      endingRelationOffsets)
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val defaultTopic = parameters.get(TOPIC_OPTION_KEY).map(_.trim)
    val specifiedKafkaParams = kafkaParamsForProducer(parameters)
    new KafkaSink(sqlContext,
      new ju.HashMap[String, Object](specifiedKafkaParams.asJava), defaultTopic)
  }

  override def createRelation(
      outerSQLContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    mode match {
      case SaveMode.Overwrite | SaveMode.Ignore =>
        throw new AnalysisException(s"Save mode $mode not allowed for Kafka. " +
          s"Allowed save modes are ${SaveMode.Append} and " +
          s"${SaveMode.ErrorIfExists} (default).")
      case _ => // good
    }
    val topic = parameters.get(TOPIC_OPTION_KEY).map(_.trim)
    val specifiedKafkaParams = kafkaParamsForProducer(parameters)
    KafkaWriter.write(outerSQLContext.sparkSession, data.queryExecution,
      new ju.HashMap[String, Object](specifiedKafkaParams.asJava), topic)

    /* This method is suppose to return a relation that reads the data that was written.
     * We cannot support this for Kafka. Therefore, in order to make things consistent,
     * we return an empty base relation.
     */
    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException
      override def schema: StructType = unsupportedException
      override def needConversion: Boolean = unsupportedException
      override def sizeInBytes: Long = unsupportedException
      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException
      private def unsupportedException =
        throw new UnsupportedOperationException("BaseRelation from Kafka write " +
          "operation is not usable.")
    }
  }

  private def kafkaParamsForProducer(parameters: Map[String, String]): Map[String, String] = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
    if (caseInsensitiveParams.contains(s"kafka.${ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG}' is not supported as keys "
          + "are serialized with ByteArraySerializer.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG}"))
    {
      throw new IllegalArgumentException(
        s"Kafka option '${ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG}' is not supported as "
          + "value are serialized with ByteArraySerializer.")
    }
    parameters
      .keySet
      .filter(_.toLowerCase.startsWith("kafka."))
      .map { k => k.drop(6).toString -> parameters(k) }
      .toMap + (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName)
  }

  private def kafkaParamsForDriver(specifiedKafkaParams: Map[String, String]) =
    ConfigUpdater("source", specifiedKafkaParams)
      .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
      .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)

      // Set to "earliest" to avoid exceptions. However, KafkaSource will fetch the initial
      // offsets by itself instead of counting on KafkaConsumer.
      .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      // So that consumers in the driver does not commit offsets unnecessarily
      .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      // So that the driver does not pull too much data
      .set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, new java.lang.Integer(1))

      // If buffer config is not set, set it to reasonable value to work around
      // buffer issues (see KAFKA-3135)
      .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
      .build()

  private def kafkaParamsForExecutors(
      specifiedKafkaParams: Map[String, String], uniqueGroupId: String) =
    ConfigUpdater("executor", specifiedKafkaParams)
      .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
      .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)

      // Make sure executors do only what the driver tells them.
      .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

      // So that consumers in executors do not mess with any existing group id
      .set(ConsumerConfig.GROUP_ID_CONFIG, s"$uniqueGroupId-executor")

      // So that consumers in executors does not commit offsets unnecessarily
      .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      // If buffer config is not set, set it to reasonable value to work around
      // buffer issues (see KAFKA-3135)
      .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
      .build()

  private def strategy(caseInsensitiveParams: Map[String, String]) =
      caseInsensitiveParams.find(x => STRATEGY_OPTION_KEYS.contains(x._1)).get match {
    case ("assign", value) =>
      AssignStrategy(JsonUtils.partitions(value))
    case ("subscribe", value) =>
      SubscribeStrategy(value.split(",").map(_.trim()).filter(_.nonEmpty))
    case ("subscribepattern", value) =>
      SubscribePatternStrategy(value.trim())
    case _ =>
      // Should never reach here as we are already matching on
      // matched strategy names
      throw new IllegalArgumentException("Unknown option")
  }

  private def failOnDataLoss(caseInsensitiveParams: Map[String, String]) =
    caseInsensitiveParams.getOrElse(FAIL_ON_DATA_LOSS_OPTION_KEY, "true").toBoolean

  private def validateGeneralOptions(parameters: Map[String, String]): Unit = {
    // Validate source options
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
    val specifiedStrategies =
      caseInsensitiveParams.filter { case (k, _) => STRATEGY_OPTION_KEYS.contains(k) }.toSeq

    if (specifiedStrategies.isEmpty) {
      throw new IllegalArgumentException(
        "One of the following options must be specified for Kafka source: "
          + STRATEGY_OPTION_KEYS.mkString(", ") + ". See the docs for more details.")
    } else if (specifiedStrategies.size > 1) {
      throw new IllegalArgumentException(
        "Only one of the following options can be specified for Kafka source: "
          + STRATEGY_OPTION_KEYS.mkString(", ") + ". See the docs for more details.")
    }

    val strategy = caseInsensitiveParams.find(x => STRATEGY_OPTION_KEYS.contains(x._1)).get match {
      case ("assign", value) =>
        if (!value.trim.startsWith("{")) {
          throw new IllegalArgumentException(
            "No topicpartitions to assign as specified value for option " +
              s"'assign' is '$value'")
        }

      case ("subscribe", value) =>
        val topics = value.split(",").map(_.trim).filter(_.nonEmpty)
        if (topics.isEmpty) {
          throw new IllegalArgumentException(
            "No topics to subscribe to as specified value for option " +
              s"'subscribe' is '$value'")
        }
      case ("subscribepattern", value) =>
        val pattern = caseInsensitiveParams("subscribepattern").trim()
        if (pattern.isEmpty) {
          throw new IllegalArgumentException(
            "Pattern to subscribe is empty as specified value for option " +
              s"'subscribePattern' is '$value'")
        }
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown option")
    }

    // Validate user-specified Kafka options

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.GROUP_ID_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.GROUP_ID_CONFIG}' is not supported as " +
          s"user-specified consumer groups is not used to track offsets.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}")) {
      throw new IllegalArgumentException(
        s"""
           |Kafka option '${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}' is not supported.
           |Instead set the source option '$STARTING_OFFSETS_OPTION_KEY' to 'earliest' or 'latest'
           |to specify where to start. Structured Streaming manages which offsets are consumed
           |internally, rather than relying on the kafkaConsumer to do it. This will ensure that no
           |data is missed when when new topics/partitions are dynamically subscribed. Note that
           |'$STARTING_OFFSETS_OPTION_KEY' only applies when a new Streaming query is started, and
           |that resuming will always pick up from where the query left off. See the docs for more
           |details.
         """.stripMargin)
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}' is not supported as keys "
          + "are deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame operations "
          + "to explicitly deserialize the keys.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}"))
    {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}' is not supported as "
          + "value are deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame "
          + "operations to explicitly deserialize the values.")
    }

    val otherUnsupportedConfigs = Seq(
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, // committing correctly requires new APIs in Source
      ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG) // interceptors can modify payload, so not safe

    otherUnsupportedConfigs.foreach { c =>
      if (caseInsensitiveParams.contains(s"kafka.$c")) {
        throw new IllegalArgumentException(s"Kafka option '$c' is not supported")
      }
    }

    if (!caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Option 'kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}' must be specified for " +
          s"configuring Kafka consumer")
    }
  }

  private def validateStreamOptions(caseInsensitiveParams: Map[String, String]) = {
    // Stream specific options
    caseInsensitiveParams.get(ENDING_OFFSETS_OPTION_KEY).map(_ =>
      throw new IllegalArgumentException("ending offset not valid in streaming queries"))
    validateGeneralOptions(caseInsensitiveParams)
  }

  private def validateBatchOptions(caseInsensitiveParams: Map[String, String]) = {
    // Batch specific options
    KafkaSourceProvider.getKafkaOffsetRangeLimit(
      caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, EarliestOffsetRangeLimit) match {
      case EarliestOffsetRangeLimit => // good to go
      case LatestOffsetRangeLimit =>
        throw new IllegalArgumentException("starting offset can't be latest " +
          "for batch queries on Kafka")
      case SpecificOffsetRangeLimit(partitionOffsets) =>
        partitionOffsets.foreach {
          case (tp, off) if off == KafkaOffsetRangeLimit.LATEST =>
            throw new IllegalArgumentException(s"startingOffsets for $tp can't " +
              "be latest for batch queries on Kafka")
          case _ => // ignore
        }
    }

    KafkaSourceProvider.getKafkaOffsetRangeLimit(
      caseInsensitiveParams, ENDING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit) match {
      case EarliestOffsetRangeLimit =>
        throw new IllegalArgumentException("ending offset can't be earliest " +
          "for batch queries on Kafka")
      case LatestOffsetRangeLimit => // good to go
      case SpecificOffsetRangeLimit(partitionOffsets) =>
        partitionOffsets.foreach {
          case (tp, off) if off == KafkaOffsetRangeLimit.EARLIEST =>
            throw new IllegalArgumentException(s"ending offset for $tp can't be " +
              "earliest for batch queries on Kafka")
          case _ => // ignore
        }
    }

    validateGeneralOptions(caseInsensitiveParams)

    // Don't want to throw an error, but at least log a warning.
    if (caseInsensitiveParams.get("maxoffsetspertrigger").isDefined) {
      logWarning("maxOffsetsPerTrigger option ignored in batch queries")
    }
  }

  /** Class to conveniently update Kafka config params, while logging the changes */
  private case class ConfigUpdater(module: String, kafkaParams: Map[String, String]) {
    private val map = new ju.HashMap[String, Object](kafkaParams.asJava)

    def set(key: String, value: Object): this.type = {
      map.put(key, value)
      logInfo(s"$module: Set $key to $value, earlier value: ${kafkaParams.getOrElse(key, "")}")
      this
    }

    def setIfUnset(key: String, value: Object): ConfigUpdater = {
      if (!map.containsKey(key)) {
        map.put(key, value)
        logInfo(s"$module: Set $key to $value")
      }
      this
    }

    def build(): ju.Map[String, Object] = map
  }
}

private[kafka010] object KafkaSourceProvider {
  private val STRATEGY_OPTION_KEYS = Set("subscribe", "subscribepattern", "assign")
  private[kafka010] val STARTING_OFFSETS_OPTION_KEY = "startingoffsets"
  private[kafka010] val ENDING_OFFSETS_OPTION_KEY = "endingoffsets"
  private val FAIL_ON_DATA_LOSS_OPTION_KEY = "failondataloss"
  val TOPIC_OPTION_KEY = "topic"

  private val deserClassName = classOf[ByteArrayDeserializer].getName

  def getKafkaOffsetRangeLimit(
      params: Map[String, String],
      offsetOptionKey: String,
      defaultOffsets: KafkaOffsetRangeLimit): KafkaOffsetRangeLimit = {
    params.get(offsetOptionKey).map(_.trim) match {
      case Some(offset) if offset.toLowerCase == "latest" => LatestOffsetRangeLimit
      case Some(offset) if offset.toLowerCase == "earliest" => EarliestOffsetRangeLimit
      case Some(json) => SpecificOffsetRangeLimit(JsonUtils.partitionOffsets(json))
      case None => defaultOffsets
    }
  }
}
