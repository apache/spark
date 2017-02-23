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
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.ByteArraySerializer

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.types.{BinaryType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

object KafkaWriter extends Logging {
  val TOPIC_ATTRIBUTE_NAME: String = "topic"
  val KEY_ATTRIBUTE_NAME: String = "key"
  val VALUE_ATTRIBUTE_NAME: String = "value"
  val MAX_OUTSTANDING_WRITES: String = "maxOutstandingWrites"
  val DEFAULT_MAX_OUTSTANDING_WRITES: Int = 1000
  val WAIT_FOR_CONFIRMED_WRITE_MS = "waitForConfirmedWriteMs"
  val DEFAULT_WAIT_FOR_CONFIRMED_WRITE_MS = 1000

  private case class TaskCommitMessage(
    sparkStageId: Int,
    sparkPartitionId: Int,
    writeCommitted: Boolean) extends Serializable

  def write(sparkSession: SparkSession,
    queryExecution: QueryExecution,
    kafkaParameters: ju.Map[String, Object],
    defaultTopic: Option[String] = None): Unit = {

    val schema = queryExecution.logical.output
    schema.find(p => p.name == TOPIC_ATTRIBUTE_NAME).getOrElse(
      if (defaultTopic == None) {
        throw new IllegalArgumentException(s"Default topic required when no " +
          s"'$TOPIC_ATTRIBUTE_NAME' attribute is present")
      } else {
        Literal(defaultTopic.get, StringType)
      }
    ).dataType match {
      case StringType => // good
      case _ =>
        throw new IllegalArgumentException(s"Topic type must be a String")
    }
    schema.find(p => p.name == KEY_ATTRIBUTE_NAME).getOrElse(
      Literal(null, StringType)
    ).dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new IllegalArgumentException(s"$KEY_ATTRIBUTE_NAME attribute type " +
          s"must be a String or BinaryType")
    }
    val valueField = schema.find(p => p.name == VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalArgumentException(s"Required attribute '$VALUE_ATTRIBUTE_NAME' not found")
    ).dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new IllegalArgumentException(s"$VALUE_ATTRIBUTE_NAME attribute type " +
          s"must be a String or BinaryType")
    }
    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      try {
        val ret = sparkSession.sparkContext.runJob(queryExecution.toRdd,
          (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
            executeTask(
              iterator = iter,
              producerConfiguration = kafkaParameters,
              sparkStageId = taskContext.stageId(),
              sparkPartitionId = taskContext.partitionId(),
              sparkAttemptNumber = taskContext.attemptNumber(),
              inputSchema = schema,
              defaultTopic = defaultTopic)
          })

        // logInfo(s"Job ${job.getJobID} committed.")
      } catch {
        case cause: Throwable =>
          // logError(s"Aborting job ${job.getJobID}.", cause)
          throw new SparkException("Job aborted.", cause)
      }
    }
  }

  /** Writes data out in a single Spark task. */
  private def executeTask(
      iterator: Iterator[InternalRow],
      producerConfiguration: ju.Map[String, Object],
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      inputSchema: Seq[Attribute],
      defaultTopic: Option[String]): TaskCommitMessage = {
    val writeTask = new KafkaWriteTask(
      producerConfiguration, inputSchema, defaultTopic)
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Execute the task to write rows out and commit the task.
        writeTask.execute(iterator)
        writeTask.releaseResources()
      })(catchBlock = {
        // If there is an error, release resource and then abort the task
        try {
          writeTask.releaseResources()
        } finally {
          logError(s"Stage $sparkStageId, task $sparkPartitionId aborted.")
        }
      })
    } catch {
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }

    if (writeTask.failedWrites.size == 0) {
      TaskCommitMessage(sparkStageId, sparkPartitionId, writeCommitted = true)
    } else {
      TaskCommitMessage(sparkStageId, sparkPartitionId, writeCommitted = false)
    }
  }

  /**
   * Used to reference an outstanding write. Kafka will call onCompletion when
   * the write has been confirmed, after which we remove it from the outstanding
   * write set.
   * @param uuid used to differentiate writes
   * @param outstandingWriteSet to remove itself from after write is confirmed
   */
  private case class KafkaCallback(uuid: ju.UUID)(
      val outstandingWriteSet: mutable.HashSet[KafkaCallback]) extends Callback {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      outstandingWriteSet.synchronized {
        outstandingWriteSet.remove(this)
        outstandingWriteSet.notifyAll()
      }
    }
  }

  /**
   * A simple trait for writing out data in a single Spark task, without any concerns about how
   * to commit or abort tasks. Exceptions thrown by the implementation of this trait will
   * automatically trigger task aborts.
   */
  private class KafkaWriteTask(
      producerConfiguration: ju.Map[String, Object],
      inputSchema: Seq[Attribute],
      defaultTopic: Option[String]) {
    val waitForConfirmedWriteMs =
      if (producerConfiguration.containsKey(WAIT_FOR_CONFIRMED_WRITE_MS)) {
        producerConfiguration.get(WAIT_FOR_CONFIRMED_WRITE_MS).asInstanceOf[Int]
      } else {
        DEFAULT_WAIT_FOR_CONFIRMED_WRITE_MS
      }
    val maxOutstandingWrites =
      if (producerConfiguration.containsKey(MAX_OUTSTANDING_WRITES)) {
        producerConfiguration.get(MAX_OUTSTANDING_WRITES).asInstanceOf[Int]
      } else {
        DEFAULT_MAX_OUTSTANDING_WRITES
      }
    val outstandingWriteSet = new mutable.HashSet[KafkaCallback]
    var failedWrites = ListBuffer.empty[Throwable]
    val topicExpression = inputSchema.find(p => p.name == TOPIC_ATTRIBUTE_NAME).getOrElse(
      if (defaultTopic == None) {
        throw new IllegalStateException(s"Default topic required when no " +
          s"'$TOPIC_ATTRIBUTE_NAME' attribute is present")
      } else {
        Literal(null, StringType)
      }
    ).map{c =>
      if (defaultTopic == None) {
        c   // return null if we can't fall back on a default value
      } else {
        // fall back on a default value in case we evaluate c to null
        If(IsNull(c), Literal(UTF8String.fromString(defaultTopic.get), StringType), c)
      }
    }
    val keyExpression = inputSchema.find(p => p.name == KEY_ATTRIBUTE_NAME).getOrElse(
      Literal(null, BinaryType)
    )
    keyExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(s"$KEY_ATTRIBUTE_NAME attribute unsupported type $t")
    }
    val valueExpression = inputSchema.find(p => p.name == VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException(s"Required attribute '$VALUE_ATTRIBUTE_NAME' not found")
    )
    valueExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(s"$VALUE_ATTRIBUTE_NAME attribute unsupported type $t")
    }
    val projection = UnsafeProjection.create(topicExpression ++
      Seq(Cast(keyExpression, BinaryType), Cast(valueExpression, BinaryType)), inputSchema)

    // Create a Kafka Producer
    producerConfiguration.put("key.serializer", classOf[ByteArraySerializer].getName)
    producerConfiguration.put("value.serializer", classOf[ByteArraySerializer].getName)
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfiguration)

    /**
     * Writes key value data out to topics.
     */
    def execute(iterator: Iterator[InternalRow]): Unit = {
      while (iterator.hasNext) {
        val currentRow = iterator.next()
        val projectedRow = projection(currentRow)
        val topic = projectedRow.get(0, StringType).toString
        val key = projectedRow.get(1, BinaryType).asInstanceOf[Array[Byte]]
        val value = projectedRow.get(2, BinaryType).asInstanceOf[Array[Byte]]
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
        val callback = outstandingWriteSet.synchronized {
          if (outstandingWriteSet.size >= maxOutstandingWrites) {
            outstandingWriteSet.wait(waitForConfirmedWriteMs)
          }
          if (outstandingWriteSet.size >= maxOutstandingWrites) {
            throw new SparkException(s"Outstanding Kafka writes not draining")
          }
          // okay we have head room now, create the callback and add to set
          val callback = new KafkaCallback(ju.UUID.randomUUID())(outstandingWriteSet)
          outstandingWriteSet.add(callback)
          callback
        }
        producer.send(record, callback)
      }
      producer.flush()
    }

    def releaseResources(): Unit = {
      producer.close(waitForConfirmedWriteMs, TimeUnit.MILLISECONDS)
      /* Ensure that all writes are confirmed */
      outstandingWriteSet.synchronized {
        if (outstandingWriteSet.size > 0) {
          throw new SparkException(s"Unable to confirm ${outstandingWriteSet.size} " +
            s"record writes to Kafka.")
        }
      }
    }
  }
}
