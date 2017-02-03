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
import java.nio.ByteBuffer

import scala.collection.mutable.ListBuffer
import scala.concurrent.{blocking, Future}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

object KafkaWriter extends Logging {
  val TOPIC_ATTRIBUTE_NAME: String = "topic"
  val KEY_ATTRIBUTE_NAME: String = "key"
  val VALUE_ATTRIBUTE_NAME: String = "value"

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
      }
    )
    schema.find(p => p.name == KEY_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalArgumentException(s"Required attribute '$KEY_ATTRIBUTE_NAME' not found")
    )
    schema.find(p => p.name == VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalArgumentException(s"Required attribute '$VALUE_ATTRIBUTE_NAME' not found")
    )
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
      assert(writeTask.confirmedWrites == writeTask.producerRecordCount,
        s"Confirmed writes ${writeTask.confirmedWrites} != " +
        s"records written ${writeTask.producerRecordCount}")
      TaskCommitMessage(sparkStageId, sparkPartitionId, writeCommitted = true)
    } else {
      TaskCommitMessage(sparkStageId, sparkPartitionId, writeCommitted = false)
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
    var producerRecordCount = 0
    var confirmedWrites = 0
    var failedWrites = ListBuffer.empty[Throwable]

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfiguration)
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
      }}
    //  Use to extract the topic from either the Row or default value
    val getTopic = UnsafeProjection.create(topicExpression, inputSchema)

    val keyExpression = inputSchema.find(p => p.name == KEY_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException(s"Required attribute '$KEY_ATTRIBUTE_NAME' not found")
    )
    // Use to extract the key from a Row
    val getKey = UnsafeProjection.create(Seq(keyExpression), inputSchema)

    val valueExpression = inputSchema.find(p => p.name == VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException(s"Required attribute '$VALUE_ATTRIBUTE_NAME' not found")
    )
    // Use to extract the value from a Row
    val getValue = UnsafeProjection.create(Seq(valueExpression), inputSchema)

    /**
     * Writes key value data out to topics.
     */
    def execute(iterator: Iterator[InternalRow]): Unit = {
      import scala.concurrent.ExecutionContext.Implicits.global
      while (iterator.hasNext) {
        val currentRow = iterator.next()
        val topic = getTopic(currentRow).get(0, StringType).toString
        val key = getKey(currentRow).getBytes
        val value = getValue(currentRow).getBytes
        println(s"topic $topic, key ${ByteBuffer.wrap(key).getInt}, value $value")
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
        val future = Future[RecordMetadata] {
          blocking {
            producer.send(record).get()
          }
        }
        future.onSuccess {
          case rm => confirmedWrites += 1
        }
        future.onFailure {
          case e => failedWrites += e
        }
      }
      producer.flush()
    }

    def releaseResources(): Unit = {
      producer.close()
    }
  }
}
