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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.types.{BinaryType, StringType}
import org.apache.spark.util.Utils

private[kafka010] object KafkaWriter extends Logging {
  val TOPIC_ATTRIBUTE_NAME: String = "topic"
  val KEY_ATTRIBUTE_NAME: String = "key"
  val VALUE_ATTRIBUTE_NAME: String = "value"

  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      kafkaParameters: ju.Map[String, Object],
      defaultTopic: Option[String] = None): Unit = {
    val schema = queryExecution.logical.output
    schema.find(p => p.name == TOPIC_ATTRIBUTE_NAME).getOrElse(
      if (defaultTopic == None) {
        throw new IllegalArgumentException(s"Default topic required when no " +
          s"'$TOPIC_ATTRIBUTE_NAME' attribute is present. Use the " +
          s"${KafkaSourceProvider.DEFAULT_TOPIC_KEY} option for setting a default topic.")
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
    schema.find(p => p.name == VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalArgumentException(s"Required attribute '$VALUE_ATTRIBUTE_NAME' not found")
    ).dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new IllegalArgumentException(s"$VALUE_ATTRIBUTE_NAME attribute type " +
          s"must be a String or BinaryType")
    }
    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      sparkSession.sparkContext.runJob(queryExecution.toRdd,
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
      defaultTopic: Option[String]): Unit = {
    val writeTask = new KafkaWriteTask(
      producerConfiguration, inputSchema, defaultTopic)
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      // Execute the task to write rows out and commit the task.
      writeTask.execute(iterator)
    })(catchBlock = {
      logError(s"Stage $sparkStageId, task $sparkPartitionId aborted.")
    }, finallyBlock = {
      writeTask.close()
    })
  }
}
