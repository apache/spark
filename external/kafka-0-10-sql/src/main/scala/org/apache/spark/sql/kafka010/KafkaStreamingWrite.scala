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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.kafka010.KafkaWriter.validateQuery
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.sources.v2.writer.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.StructType

/**
 * Dummy commit message. The DataSourceV2 framework requires a commit message implementation but we
 * don't need to really send one.
 */
case object KafkaWriterCommitMessage extends WriterCommitMessage

/**
 * A [[StreamingWrite]] for Kafka writing. Responsible for generating the writer factory.
 *
 * @param topic The topic this writer is responsible for. If None, topic will be inferred from
 *              a `topic` field in the incoming data.
 * @param producerParams Parameters for Kafka producers in each task.
 * @param schema The schema of the input data.
 */
class KafkaStreamingWrite(
    topic: Option[String],
    producerParams: ju.Map[String, Object],
    schema: StructType)
  extends StreamingWrite {

  validateQuery(schema.toAttributes, producerParams, topic)

  override def createStreamingWriterFactory(): KafkaStreamWriterFactory =
    KafkaStreamWriterFactory(topic, producerParams, schema)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}

/**
 * A [[StreamingDataWriterFactory]] for Kafka writing. Will be serialized and sent to executors to
 * generate the per-task data writers.
 * @param topic The topic that should be written to. If None, topic will be inferred from
 *              a `topic` field in the incoming data.
 * @param producerParams Parameters for Kafka producers in each task.
 * @param schema The schema of the input data.
 */
case class KafkaStreamWriterFactory(
    topic: Option[String],
    producerParams: ju.Map[String, Object],
    schema: StructType)
  extends StreamingDataWriterFactory {

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    new KafkaStreamDataWriter(topic, producerParams, schema.toAttributes)
  }
}

/**
 * A [[DataWriter]] for Kafka writing. One data writer will be created in each partition to
 * process incoming rows.
 *
 * @param targetTopic The topic that this data writer is targeting. If None, topic will be inferred
 *                    from a `topic` field in the incoming data.
 * @param producerParams Parameters to use for the Kafka producer.
 * @param inputSchema The attributes in the input data.
 */
class KafkaStreamDataWriter(
    targetTopic: Option[String],
    producerParams: ju.Map[String, Object],
    inputSchema: Seq[Attribute])
  extends KafkaRowWriter(inputSchema, targetTopic) with DataWriter[InternalRow] {

  private lazy val producer = CachedKafkaProducer.getOrCreate(producerParams)

  def write(row: InternalRow): Unit = {
    checkForErrors()
    sendRow(row, producer)
  }

  def commit(): WriterCommitMessage = {
    // Send is asynchronous, but we can't commit until all rows are actually in Kafka.
    // This requires flushing and then checking that no callbacks produced errors.
    // We also check for errors before to fail as soon as possible - the check is cheap.
    checkForErrors()
    producer.flush()
    checkForErrors()
    KafkaWriterCommitMessage
  }

  def abort(): Unit = {}

  def close(): Unit = {
    checkForErrors()
    if (producer != null) {
      producer.flush()
      checkForErrors()
      CachedKafkaProducer.close(producerParams)
    }
  }
}
