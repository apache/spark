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
import java.util.{Locale, UUID}

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

import org.apache.spark.internal.Logging
import org.apache.spark.kafka010.KafkaConfigUpdater
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.TableCapability._
import org.apache.spark.sql.sources.v2.reader.{Scan, ScanBuilder}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.sources.v2.writer.WriteBuilder
import org.apache.spark.sql.sources.v2.writer.streaming.StreamingWrite
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The provider class for all Kafka readers and writers. It is designed such that it throws
 * IllegalArgumentException when the Kafka Dataset is created, so that it can catch
 * missing options even before the query is started.
 */
private[kafka010] class KafkaSourceProvider extends DataSourceRegister
    with StreamSourceProvider
    with StreamSinkProvider
    with RelationProvider
    with CreatableRelationProvider
    with TableProvider
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
    val uniqueGroupId = streamingUniqueGroupId(parameters, metadataPath)

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    val specifiedKafkaParams = convertToSpecifiedParams(parameters)

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

  override def getTable(options: CaseInsensitiveStringMap): KafkaTable = {
    new KafkaTable(strategy(options.asScala.toMap))
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
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    val specifiedKafkaParams = convertToSpecifiedParams(parameters)

    val startingRelationOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(
      caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, EarliestOffsetRangeLimit)
    assert(startingRelationOffsets != LatestOffsetRangeLimit)

    val endingRelationOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(caseInsensitiveParams,
      ENDING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)
    assert(endingRelationOffsets != EarliestOffsetRangeLimit)

    new KafkaRelation(
      sqlContext,
      strategy(caseInsensitiveParams),
      sourceOptions = parameters,
      specifiedKafkaParams = specifiedKafkaParams,
      failOnDataLoss = failOnDataLoss(caseInsensitiveParams),
      startingOffsets = startingRelationOffsets,
      endingOffsets = endingRelationOffsets)
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val defaultTopic = parameters.get(TOPIC_OPTION_KEY).map(_.trim)
    val specifiedKafkaParams = kafkaParamsForProducer(parameters)
    new KafkaSink(sqlContext, specifiedKafkaParams, defaultTopic)
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
    KafkaWriter.write(outerSQLContext.sparkSession, data.queryExecution, specifiedKafkaParams,
      topic)

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

  private def strategy(caseInsensitiveParams: Map[String, String]) =
      caseInsensitiveParams.find(x => STRATEGY_OPTION_KEYS.contains(x._1)).get match {
    case (ASSIGN, value) =>
      AssignStrategy(JsonUtils.partitions(value))
    case (SUBSCRIBE, value) =>
      SubscribeStrategy(value.split(",").map(_.trim()).filter(_.nonEmpty))
    case (SUBSCRIBE_PATTERN, value) =>
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
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
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
      case (ASSIGN, value) =>
        if (!value.trim.startsWith("{")) {
          throw new IllegalArgumentException(
            "No topicpartitions to assign as specified value for option " +
              s"'assign' is '$value'")
        }

      case (SUBSCRIBE, value) =>
        val topics = value.split(",").map(_.trim).filter(_.nonEmpty)
        if (topics.isEmpty) {
          throw new IllegalArgumentException(
            "No topics to subscribe to as specified value for option " +
              s"'subscribe' is '$value'")
        }
      case (SUBSCRIBE_PATTERN, value) =>
        val pattern = caseInsensitiveParams(SUBSCRIBE_PATTERN).trim()
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

    // Validate minPartitions value if present
    if (caseInsensitiveParams.contains(MIN_PARTITIONS_OPTION_KEY)) {
      val p = caseInsensitiveParams(MIN_PARTITIONS_OPTION_KEY).toInt
      if (p <= 0) throw new IllegalArgumentException("minPartitions must be positive")
    }

    // Validate user-specified Kafka options

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.GROUP_ID_CONFIG}")) {
      logWarning(CUSTOM_GROUP_ID_ERROR_MESSAGE)
      if (caseInsensitiveParams.contains(GROUP_ID_PREFIX)) {
        logWarning("Option 'groupIdPrefix' will be ignored as " +
          s"option 'kafka.${ConsumerConfig.GROUP_ID_CONFIG}' has been set.")
      }
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}")) {
      throw new IllegalArgumentException(
        s"""
           |Kafka option '${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}' is not supported.
           |Instead set the source option '$STARTING_OFFSETS_OPTION_KEY' to 'earliest' or 'latest'
           |to specify where to start. Structured Streaming manages which offsets are consumed
           |internally, rather than relying on the kafkaConsumer to do it. This will ensure that no
           |data is missed when new topics/partitions are dynamically subscribed. Note that
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
          + "values are deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame "
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
    if (caseInsensitiveParams.get(MAX_OFFSET_PER_TRIGGER.toLowerCase(Locale.ROOT)).isDefined) {
      logWarning("maxOffsetsPerTrigger option ignored in batch queries")
    }
  }

  class KafkaTable(strategy: => ConsumerStrategy) extends Table
    with SupportsRead with SupportsWrite {

    override def name(): String = s"Kafka $strategy"

    override def schema(): StructType = KafkaOffsetReader.kafkaSchema

    override def capabilities(): ju.Set[TableCapability] = {
      Set(MICRO_BATCH_READ, CONTINUOUS_READ, STREAMING_WRITE).asJava
    }

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
      () => new KafkaScan(options)

    override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
      new WriteBuilder {
        private var inputSchema: StructType = _

        override def withInputDataSchema(schema: StructType): WriteBuilder = {
          this.inputSchema = schema
          this
        }

        override def buildForStreaming(): StreamingWrite = {
          import scala.collection.JavaConverters._

          assert(inputSchema != null)
          val topic = Option(options.get(TOPIC_OPTION_KEY)).map(_.trim)
          val producerParams = kafkaParamsForProducer(options.asScala.toMap)
          new KafkaStreamingWrite(topic, producerParams, inputSchema)
        }
      }
    }
  }

  class KafkaScan(options: CaseInsensitiveStringMap) extends Scan {

    override def readSchema(): StructType = KafkaOffsetReader.kafkaSchema

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      val parameters = options.asScala.toMap
      validateStreamOptions(parameters)
      // Each running query should use its own group id. Otherwise, the query may be only assigned
      // partial data since Kafka will assign partitions to multiple consumers having the same group
      // id. Hence, we should generate a unique id for each query.
      val uniqueGroupId = streamingUniqueGroupId(parameters, checkpointLocation)

      val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
      val specifiedKafkaParams = convertToSpecifiedParams(parameters)

      val startingStreamOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(
        caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)

      val kafkaOffsetReader = new KafkaOffsetReader(
        strategy(parameters),
        kafkaParamsForDriver(specifiedKafkaParams),
        parameters,
        driverGroupIdPrefix = s"$uniqueGroupId-driver")

      new KafkaMicroBatchStream(
        kafkaOffsetReader,
        kafkaParamsForExecutors(specifiedKafkaParams, uniqueGroupId),
        options,
        checkpointLocation,
        startingStreamOffsets,
        failOnDataLoss(caseInsensitiveParams))
    }

    override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
      val parameters = options.asScala.toMap
      validateStreamOptions(parameters)
      // Each running query should use its own group id. Otherwise, the query may be only assigned
      // partial data since Kafka will assign partitions to multiple consumers having the same group
      // id. Hence, we should generate a unique id for each query.
      val uniqueGroupId = streamingUniqueGroupId(parameters, checkpointLocation)

      val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
      val specifiedKafkaParams =
        parameters
          .keySet
          .filter(_.toLowerCase(Locale.ROOT).startsWith("kafka."))
          .map { k => k.drop(6).toString -> parameters(k) }
          .toMap

      val startingStreamOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(
        caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)

      val kafkaOffsetReader = new KafkaOffsetReader(
        strategy(caseInsensitiveParams),
        kafkaParamsForDriver(specifiedKafkaParams),
        parameters,
        driverGroupIdPrefix = s"$uniqueGroupId-driver")

      new KafkaContinuousStream(
        kafkaOffsetReader,
        kafkaParamsForExecutors(specifiedKafkaParams, uniqueGroupId),
        options,
        checkpointLocation,
        startingStreamOffsets,
        failOnDataLoss(caseInsensitiveParams))
    }
  }
}

private[kafka010] object KafkaSourceProvider extends Logging {
  private val ASSIGN = "assign"
  private val SUBSCRIBE_PATTERN = "subscribepattern"
  private val SUBSCRIBE = "subscribe"
  private val STRATEGY_OPTION_KEYS = Set(SUBSCRIBE, SUBSCRIBE_PATTERN, ASSIGN)
  private[kafka010] val STARTING_OFFSETS_OPTION_KEY = "startingoffsets"
  private[kafka010] val ENDING_OFFSETS_OPTION_KEY = "endingoffsets"
  private val FAIL_ON_DATA_LOSS_OPTION_KEY = "failondataloss"
  private[kafka010] val MIN_PARTITIONS_OPTION_KEY = "minpartitions"
  private[kafka010] val MAX_OFFSET_PER_TRIGGER = "maxOffsetsPerTrigger"
  private[kafka010] val FETCH_OFFSET_NUM_RETRY = "fetchOffset.numRetries"
  private[kafka010] val FETCH_OFFSET_RETRY_INTERVAL_MS = "fetchOffset.retryIntervalMs"
  private[kafka010] val CONSUMER_POLL_TIMEOUT = "kafkaConsumer.pollTimeoutMs"
  private val GROUP_ID_PREFIX = "groupidprefix"

  val TOPIC_OPTION_KEY = "topic"

  val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE =
    """
      |Some data may have been lost because they are not available in Kafka any more; either the
      | data was aged out by Kafka or the topic may have been deleted before all the data in the
      | topic was processed. If you want your streaming query to fail on such cases, set the source
      | option "failOnDataLoss" to "true".
    """.stripMargin

  val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE =
    """
      |Some data may have been lost because they are not available in Kafka any more; either the
      | data was aged out by Kafka or the topic may have been deleted before all the data in the
      | topic was processed. If you don't want your streaming query to fail on such cases, set the
      | source option "failOnDataLoss" to "false".
    """.stripMargin

  val CUSTOM_GROUP_ID_ERROR_MESSAGE =
    s"""Kafka option 'kafka.${ConsumerConfig.GROUP_ID_CONFIG}' has been set on this query, it is
       | not recommended to set this option. This option is unsafe to use since multiple concurrent
       | queries or sources using the same group id will interfere with each other as they are part
       | of the same consumer group. Restarted queries may also suffer interference from the
       | previous run having the same group id. The user should have only one query per group id,
       | and/or set the option 'kafka.session.timeout.ms' to be very small so that the Kafka
       | consumers from the previous query are marked dead by the Kafka group coordinator before the
       | restarted query starts running.
    """.stripMargin

  private val serClassName = classOf[ByteArraySerializer].getName
  private val deserClassName = classOf[ByteArrayDeserializer].getName

  def getKafkaOffsetRangeLimit(
      params: Map[String, String],
      offsetOptionKey: String,
      defaultOffsets: KafkaOffsetRangeLimit): KafkaOffsetRangeLimit = {
    params.get(offsetOptionKey).map(_.trim) match {
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "latest" =>
        LatestOffsetRangeLimit
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "earliest" =>
        EarliestOffsetRangeLimit
      case Some(json) => SpecificOffsetRangeLimit(JsonUtils.partitionOffsets(json))
      case None => defaultOffsets
    }
  }

  def kafkaParamsForDriver(specifiedKafkaParams: Map[String, String]): ju.Map[String, Object] =
    KafkaConfigUpdater("source", specifiedKafkaParams)
      .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
      .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)

      // Set to "earliest" to avoid exceptions. However, KafkaSource will fetch the initial
      // offsets by itself instead of counting on KafkaConsumer.
      .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      // So that consumers in the driver does not commit offsets unnecessarily
      .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      // So that the driver does not pull too much data
      .set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, java.lang.Integer.valueOf(1))

      // If buffer config is not set, set it to reasonable value to work around
      // buffer issues (see KAFKA-3135)
      .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
      .build()

  def kafkaParamsForExecutors(
      specifiedKafkaParams: Map[String, String],
      uniqueGroupId: String): ju.Map[String, Object] =
    KafkaConfigUpdater("executor", specifiedKafkaParams)
      .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
      .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)

      // Make sure executors do only what the driver tells them.
      .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

      // So that consumers in executors do not mess with any existing group id
      .setIfUnset(ConsumerConfig.GROUP_ID_CONFIG, s"$uniqueGroupId-executor")

      // So that consumers in executors does not commit offsets unnecessarily
      .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      // If buffer config is not set, set it to reasonable value to work around
      // buffer issues (see KAFKA-3135)
      .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
      .build()

  /**
   * Returns a unique consumer group (group.id), allowing the user to set the prefix of
   * the consumer group
   */
  private def streamingUniqueGroupId(
      parameters: Map[String, String],
      metadataPath: String): String = {
    val groupIdPrefix = parameters
      .getOrElse(GROUP_ID_PREFIX, "spark-kafka-source")
    s"${groupIdPrefix}-${UUID.randomUUID}-${metadataPath.hashCode}"
  }

  private[kafka010] def kafkaParamsForProducer(
      parameters: Map[String, String]): ju.Map[String, Object] = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    if (caseInsensitiveParams.contains(s"kafka.${ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG}' is not supported as keys "
          + "are serialized with ByteArraySerializer.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG}' is not supported as "
          + "value are serialized with ByteArraySerializer.")
    }

    val specifiedKafkaParams = convertToSpecifiedParams(parameters)

    KafkaConfigUpdater("executor", specifiedKafkaParams)
      .set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serClassName)
      .set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serClassName)
      .build()
  }

  private def convertToSpecifiedParams(parameters: Map[String, String]): Map[String, String] = {
    parameters
      .keySet
      .filter(_.toLowerCase(Locale.ROOT).startsWith("kafka."))
      .map { k => k.drop(6).toString -> parameters(k) }
      .toMap
  }
}
