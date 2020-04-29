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
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.kafka010.producer.{CachedKafkaProducer, InternalKafkaProducerPool}

/**
 * Dummy commit message. The DataSourceV2 framework requires a commit message implementation but we
 * don't need to really send one.
 */
private case object KafkaDataWriterCommitMessage extends WriterCommitMessage

/**
 * A [[DataWriter]] for Kafka writing. One data writer will be created in each partition to
 * process incoming rows.
 *
 * @param targetTopic The topic that this data writer is targeting. If None, topic will be inferred
 *                    from a `topic` field in the incoming data.
 * @param producerParams Parameters to use for the Kafka producer.
 * @param inputSchema The attributes in the input data.
 */
private[kafka010] class KafkaDataWriter(
    targetTopic: Option[String],
    producerParams: ju.Map[String, Object],
    inputSchema: Seq[Attribute])
  extends KafkaRowWriter(inputSchema, targetTopic) with DataWriter[InternalRow] {

  private var producer: Option[CachedKafkaProducer] = None

  def write(row: InternalRow): Unit = {
    checkForErrors()
    if (producer.isEmpty) {
      producer = Some(InternalKafkaProducerPool.acquire(producerParams))
    }
    producer.foreach { p => sendRow(row, p.producer) }
  }

  def commit(): WriterCommitMessage = {
    // Send is asynchronous, but we can't commit until all rows are actually in Kafka.
    // This requires flushing and then checking that no callbacks produced errors.
    // We also check for errors before to fail as soon as possible - the check is cheap.
    checkForErrors()
    producer.foreach(_.producer.flush())
    checkForErrors()
    KafkaDataWriterCommitMessage
  }

  def abort(): Unit = {}

  def close(): Unit = {
    producer.foreach(InternalKafkaProducerPool.release)
    producer = None
  }
}
