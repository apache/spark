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

package org.apache.spark.sql.execution.streaming.sources

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory

/**
 * A simple [[org.apache.spark.sql.connector.write.DataWriterFactory]] whose tasks just pack rows
 * into the commit message for delivery to a
 * [[org.apache.spark.sql.connector.write.BatchWrite]] on the driver.
 *
 * Note that, because it sends all rows to the driver, this factory will generally be unsuitable
 * for production-quality sinks. It's intended for use in tests.
 */
case object PackedRowWriterFactory extends StreamingDataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    new PackedRowDataWriter()
  }
}

/**
 * Commit message for a [[PackedRowDataWriter]], containing all the rows written in the most
 * recent interval.
 */
case class PackedRowCommitMessage(rows: Array[InternalRow]) extends WriterCommitMessage

/**
 * A simple [[DataWriter]] that just sends all the rows it's received as a commit message.
 */
class PackedRowDataWriter() extends DataWriter[InternalRow] with Logging {
  private val data = mutable.Buffer[InternalRow]()

  // Spark reuses the same `InternalRow` instance, here we copy it before buffer it.
  override def write(row: InternalRow): Unit = data.append(row.copy())

  override def commit(): PackedRowCommitMessage = {
    PackedRowCommitMessage(data.toArray)
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
    data.clear()
  }
}
