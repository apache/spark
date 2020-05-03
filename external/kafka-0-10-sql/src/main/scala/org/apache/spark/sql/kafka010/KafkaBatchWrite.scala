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
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.kafka010.KafkaWriter.validateQuery
import org.apache.spark.sql.types.StructType

/**
 * A [[BatchWrite]] for Kafka writing. Responsible for generating the writer factory.
 *
 * @param topic The topic this writer is responsible for. If None, topic will be inferred from
 *              a `topic` field in the incoming data.
 * @param producerParams Parameters for Kafka producers in each task.
 * @param schema The schema of the input data.
 */
private[kafka010] class KafkaBatchWrite(
    topic: Option[String],
    producerParams: ju.Map[String, Object],
    schema: StructType)
  extends BatchWrite {

  validateQuery(schema.toAttributes, producerParams, topic)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): KafkaBatchWriterFactory =
    KafkaBatchWriterFactory(topic, producerParams, schema)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

/**
 * A [[DataWriterFactory]] for Kafka writing. Will be serialized and sent to executors to
 * generate the per-task data writers.
 * @param topic The topic that should be written to. If None, topic will be inferred from
 *              a `topic` field in the incoming data.
 * @param producerParams Parameters for Kafka producers in each task.
 * @param schema The schema of the input data.
 */
private case class KafkaBatchWriterFactory(
    topic: Option[String],
    producerParams: ju.Map[String, Object],
    schema: StructType)
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new KafkaDataWriter(topic, producerParams, schema.toAttributes)
  }
}
