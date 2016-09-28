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
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.kafka010.KafkaSource._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

/**
 * The provider class for the [[KafkaSource]]. This provider is designed such that it throws
 * IllegalArgumentException when the Kafka Dataset is created, so that it can catch
 * missing options even before the query is started.
 */
private[kafka010] class KafkaSourceProvider extends StreamSourceProvider
  with DataSourceRegister with Logging {

  import KafkaSourceProvider._

  /**
   * Returns the name and schema of the source. In addition, it also verifies whether the options
   * are correct and sufficient to create the [[KafkaSource]] when the query is started.
   */
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "Kafka source has a fixed schema and cannot be set with a custom one")
    validateOptions(parameters)
    ("kafka", KafkaSource.kafkaSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
      validateOptions(parameters)
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
    val specifiedKafkaParams =
      parameters
        .keySet
        .filter(_.toLowerCase.startsWith("kafka."))
        .map { k => k.drop(6).toString -> parameters(k) }
        .toMap

    val deserClassName = classOf[ByteArrayDeserializer].getName
    val uniqueGroupId = s"spark-kafka-source-${UUID.randomUUID}-${metadataPath.hashCode}"

    val autoOffsetResetValue = caseInsensitiveParams.get(STARTING_OFFSET_OPTION_KEY) match {
      case Some(value) => value.trim()  // same values as those supported by auto.offset.reset
      case None => "latest"
    }

    val kafkaParamsForStrategy =
      ConfigUpdater("source", specifiedKafkaParams)
        .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
        .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)

        // So that consumers in Kafka source do not mess with any existing group id
        .set(ConsumerConfig.GROUP_ID_CONFIG, s"$uniqueGroupId-driver")

        // So that consumers can start from earliest or latest
        .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetValue)

        // So that consumers in the driver does not commit offsets unnecessaribly
        .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

        // So that the driver does not pull too much data
        .set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, new java.lang.Integer(1))

        // If buffer config is not set, set it to reasonable value to work around
        // buffer issues (see KAFKA-3135)
        .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
        .build()

    val kafkaParamsForExecutors =
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

    val strategy = caseInsensitiveParams.find(x => STRATEGY_OPTION_KEYS.contains(x._1)).get match {
      case ("subscribe", value) =>
        SubscribeStrategy(
          value.split(",").map(_.trim()).filter(_.nonEmpty),
          kafkaParamsForStrategy)
      case ("subscribepattern", value) =>
        SubscribePatternStrategy(
          value.trim(),
          kafkaParamsForStrategy)
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown option")
    }

    val failOnCorruptMetadata =
      caseInsensitiveParams.getOrElse(FAIL_ON_CORRUPT_METADATA_OPTION_KEY, "false").toBoolean

    new KafkaSource(
      sqlContext,
      strategy,
      kafkaParamsForExecutors,
      parameters,
      failOnCorruptMetadata)
  }

  private def validateOptions(parameters: Map[String, String]): Unit = {

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

    caseInsensitiveParams.get(STARTING_OFFSET_OPTION_KEY) match {
      case Some(pos) if !STARTING_OFFSET_OPTION_VALUES.contains(pos.trim.toLowerCase) =>
        throw new IllegalArgumentException(
          s"Illegal value '$pos' for option '$STARTING_OFFSET_OPTION_KEY', " +
            s"acceptable values are: ${STARTING_OFFSET_OPTION_VALUES.mkString(", ")}")
      case _ =>
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
           |Instead set the source option '$STARTING_OFFSET_OPTION_KEY' to 'earliest' or 'latest' to
           |specify where to start. Structured Streaming manages which offsets are consumed
           |internally, rather than rely on the kafka Consumer to do it. This will ensure that no
           |data is missed when when new topics/partitions are dynamically subscribed. Note that
           |'$STARTING_OFFSET_OPTION_KEY' only applies when a new Streaming query is started, and
           |that resuming will always pick up from where the query left off. See the docs for more
           |details.
         """.stripMargin)
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.GROUP_ID_CONFIG}' is not supported as keys are " +
          s"deserialized as byte arrays with ByteArrayDeserializer. Use Dataframe operations to " +
          s"explicitly deserialize the keys.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}"))
    {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.GROUP_ID_CONFIG}' is not supported as value are " +
          s"deserialized as byte arrays with ByteArrayDeserializer. Use Dataframe operations to " +
          s"explicitly deserialize the values.")
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

  override def shortName(): String = "kafka"

  /** Class to conveniently update Kafka config params, while logging the changes */
  private case class ConfigUpdater(module: String, kafkaParams: Map[String, String]) {
    private val map = new ju.HashMap[String, Object](kafkaParams.asJava)

    def set(key: String, value: Object): this.type = {
      map.put(key, value)
      logInfo(s"$module: Set $key to $value, earlier value: ${kafkaParams.get(key).getOrElse("")}")
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
  private val STRATEGY_OPTION_KEYS = Set("subscribe", "subscribepattern")
  private val STARTING_OFFSET_OPTION_KEY = "startingoffset"
  private val STARTING_OFFSET_OPTION_VALUES = Set("earliest", "latest")
  private val FAIL_ON_CORRUPT_METADATA_OPTION_KEY = "failoncorruptmetadata"
}
