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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * The logical plan for writing data to a micro-batch stream.
 *
 * Note that this logical plan does not have a corresponding physical plan, as it will be converted
 * to [[WriteMicroBatch]] with epoch id for each micro-batch.
 */
case class WriteToMicroBatchDataSource(
    table: SupportsWrite,
    query: LogicalPlan,
    queryId: String,
    querySchema: StructType,
    outputMode: OutputMode,
    options: Map[String, String])
  extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil

  def createPlan(epochId: Long): WriteMicroBatch = {
    WriteMicroBatch(table, query, queryId, querySchema, outputMode, options, epochId)
  }
}

case class WriteMicroBatch(
    table: SupportsWrite,
    query: LogicalPlan,
    queryId: String,
    querySchema: StructType,
    outputMode: OutputMode,
    options: Map[String, String],
    epochId: Long) extends UnaryNode {
  override def child: LogicalPlan = query
  override def output: Seq[Attribute] = Nil
}

case class WriteMicroBatchExec(
    table: SupportsWrite,
    query: SparkPlan,
    queryId: String,
    querySchema: StructType,
    outputMode: OutputMode,
    options: Map[String, String],
    epochId: Long) extends BaseStreamingWriteExec with V2TableWriteExec {

  override protected def doExecute(): RDD[InternalRow] = {
    val batchWrite = new MicroBatchWrite(epochId, streamWrite)
    writeWithV2(batchWrite)
  }
}

/**
 * A [[BatchWrite]] used to hook V2 stream writers into a microbatch plan. It implements
 * the non-streaming interface, forwarding the epoch ID determined at construction to a wrapped
 * streaming write support.
 */
class MicroBatchWrite(epochId: Long, streamWrite: StreamingWrite) extends BatchWrite {

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    streamWrite.commit(epochId, messages)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    streamWrite.abort(epochId, messages)
  }

  override def createBatchWriterFactory(): DataWriterFactory = {
    new MicroBatchWriterFactory(epochId, streamWrite.createStreamingWriterFactory())
  }
}

class MicroBatchWriterFactory(epochId: Long, streamingWriterFactory: StreamingDataWriterFactory)
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    streamingWriterFactory.createWriter(partitionId, taskId, epochId)
  }
}
