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
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.cache._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.util.Utils

/**
 * A [[WriterCommitMessage]] for Kafka commit message.
 * @param transactionalId Unique transactionalId for each producer.
 * @param epoch Transactional epoch.
 * @param producerId Transactional producerId for producer, got when init transaction.
 */
private[kafka010] case class ProducerTransactionMetaData(
    transactionalId: String,
    epoch: Short,
    producerId: Long)
  extends WriterCommitMessage

/**
 * Emtpy commit message for resume transaction.
 */
private case object EmptyCommitMessage extends WriterCommitMessage

private[kafka010] case object ProducerTransactionMetaData {
  val VERSION = 1

  def toTransactionId(
      executorId: String,
      taskIndex: String,
      transactionalIdSuffix: String): String = {
    toTransactionId(toProducerIdentity(executorId, taskIndex), transactionalIdSuffix)
  }

  def toTransactionId(producerIdentity: String, transactionalIdSuffix: String): String = {
    s"$producerIdentity||$transactionalIdSuffix"
  }

  def toTransactionalIdSuffix(transactionalId: String): String = {
    transactionalId.split("\\|\\|", 2)(1)
  }

  def toProducerIdentity(transactionalId: String): String = {
    transactionalId.split("\\|\\|", 2)(0)
  }

  def toExecutorId(transactionalId: String): String = {
    val producerIdentity = toProducerIdentity(transactionalId)
    producerIdentity.split("-", 2)(0)
  }

  def toTaskIndex(transactionalId: String): String = {
    val producerIdentity = toProducerIdentity(transactionalId)
    producerIdentity.split("-", 2)(1)
  }

  def toProducerIdentity(executorId: String, taskIndex: String): String = {
    s"$executorId-$taskIndex"
  }
}

/**
 * A [[DataWriter]] for Kafka transactional writing. One data writer will be created
 * in each partition to process incoming rows.
 *
 * @param targetTopic The topic that this data writer is targeting. If None, topic will be inferred
 *                    from a `topic` field in the incoming data.
 * @param producerParams Parameters to use for the Kafka producer.
 * @param inputSchema The attributes in the input data.
 */
private[kafka010] class KafkaTransactionDataWriter(
    targetTopic: Option[String],
    producerParams: ju.Map[String, Object],
    inputSchema: Seq[Attribute])
  extends KafkaRowWriter(inputSchema, targetTopic) with DataWriter[InternalRow] {

  private lazy val producer = {
    val kafkaProducer = CachedKafkaProducer.getOrCreate(producerParams)
    if (kafkaProducer.getProducerId == -1) {
      kafkaProducer.initTransactions()
    }
    kafkaProducer.beginTransaction()
    kafkaProducer
  }

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
    ProducerTransactionMetaData(producer.getTransactionalId, producer.getEpoch,
      producer.getProducerId)
  }

  def abort(): Unit = {
    if (producer.getProducerId != -1) {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        producer.abortTransaction()
      })(catchBlock = {
        CachedKafkaProducer.close(producerParams)
      })
    }
  }

  def close(): Unit = {
    checkForErrors()
    if (producer != null) {
      producer.flush()
      checkForErrors()
      CachedKafkaProducer.close(producerParams)
    }
  }
}

/**
 * A [[DataWriter]] for resume Kafka transaction.
 *
 * @param producerParams Parameters to use for the Kafka producer.
 */
private[kafka010] class KafkaTransactionResumeDataWriter(
    targetTopic: Option[String],
    producerParams: ju.Map[String, Object],
    inputSchema: Seq[Attribute],
    metaData: ProducerTransactionMetaData)
  extends KafkaRowWriter(inputSchema, targetTopic) with DataWriter[InternalRow] {

  private val producer = CachedKafkaProducer.getOrCreate(producerParams)

  def write(row: InternalRow): Unit = {}

  def commit(): WriterCommitMessage = {
    producer.resumeTransaction(metaData.producerId, metaData.epoch)
    producer.commitTransaction()

    EmptyCommitMessage
  }

  def abort(): Unit = {}

  def close(): Unit = {
    if (producer != null) {
      CachedKafkaProducer.close(producerParams)
    }
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
private[kafka010] class KafkaDataWriter(
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
    EmptyCommitMessage
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


private object TaskIndexGenerator {
  private val generator = CacheBuilder.newBuilder().build[String, AtomicInteger](
    new CacheLoader[String, AtomicInteger] {
      override def load(key: String): AtomicInteger = {
        new AtomicInteger(0)
      }
    }
  )

  def getTaskIndex(transactionIdSuffix: String): String = {
    generator.get(transactionIdSuffix).getAndIncrement().toString
  }

  def resetTaskIndex(transactionIdSuffix: String): Unit = {
    generator.get(transactionIdSuffix).set(0)
  }
}