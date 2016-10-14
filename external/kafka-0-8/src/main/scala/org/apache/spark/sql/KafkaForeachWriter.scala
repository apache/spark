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

package org.apache.spark.sql

import java.util.Properties

import kafka.producer.OldProducer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{StringType, StructType}
/**
 * A ForeachWriter that outputs streaming query results to kafka clusters.
 * The streaming query results must be one or two [[StringType]] columns. When the output
 * is a single String column, this column will be considered as value and
 * key will be null by default. If output has two [[StringType]] columns, the fieldsName
 * of columns should be "key" and "value".
 */
class KafkaForeachWriter[T](
    kafkaParams: Properties,
    topics: Set[String],
    schema: StructType) extends ForeachWriter[T] with Logging{
  private var producer : OldProducer = null
  override def open(partitionId: Long, version: Long): Boolean = {
    verifySchema(schema)
    producer = new OldProducer(kafkaParams)
    if (producer == null) return false
    logInfo(s"Producer created. PartitionId: ${partitionId}, version: ${version}")
    true
  }

  override def process(value: T): Unit = {
    logDebug("Starting to send message")
    val data : Row = value.asInstanceOf[Row]
    verifySchema(data.schema)
    val keyIsNull = if (schema.size == 1) true else false
    if (keyIsNull) {
      logDebug(s"Send message to kafka. Message Value: ${data.getString(0)}")
      topics.map(producer.send(_, null, data.getString(0).getBytes()))
    } else {
      val messageKey: String = data.getAs[String]("key")
      val messageValue: String = data.getAs[String]("value")
      logDebug(s"Send message to kafka. Message Value: (${messageKey}, ${messageValue})")
      topics.map(producer.send(_, messageKey.getBytes(), messageValue.getBytes()))
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (errorOrNull != null) logInfo(errorOrNull.getMessage)
    logInfo("Close producer.")
    producer.close()
  }

  private def verifySchema(schema: StructType): Unit = {
    if (schema.size == 1) {
      if (schema(0).dataType != StringType) {
        throw new AnalysisException(
          s"Producer sends messages Failed! KafkaForeachWriter supports only StringType value, " +
            s"and you have ${schema(0).dataType} value.")
      }
    } else if (schema.size == 2) {
      if (schema(0).dataType != StringType || schema(1).dataType != StringType) {
        throw new AnalysisException(
          s"Producer sends messages Failed! KafkaForeachWriter supports only StringType value, " +
            s"and you have ${schema(0).dataType} and ${schema(1).dataType} value.")
      }
      if (!schema.fieldNames.contains("key") || !schema.fieldNames.contains("value")) {
        throw new AnalysisException(
          s"""Producer sends messages Failed! If there are two columns in Row, fieldsNames
              should be "key" and "value" .
           """)
      }
    } else {
      throw new AnalysisException(
        s"Producer sends messages Failed! KafkaForeachWriter supports only one or two columns, " +
          s"and you have ${schema.size} columns.")
    }
  }
}
