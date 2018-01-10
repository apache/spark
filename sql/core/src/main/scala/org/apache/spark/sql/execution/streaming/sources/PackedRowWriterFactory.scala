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
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}

/**
 * A simple [[DataWriterFactory]] whose tasks just pack rows into the commit message for delivery
 * to the [[org.apache.spark.sql.sources.v2.writer.DataSourceV2Writer]] on the driver.
 */
case object PackedRowWriterFactory extends DataWriterFactory[Row] {
  def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new PackedRowDataWriter()
  }
}

case class PackedRowCommitMessage(rows: Array[Row]) extends WriterCommitMessage

class PackedRowDataWriter() extends DataWriter[Row] with Logging {
  private val data = mutable.Buffer[Row]()

  override def write(row: Row): Unit = data.append(row)

  override def commit(): PackedRowCommitMessage = {
    val msg = PackedRowCommitMessage(data.clone().toArray)
    data.clear()
    msg
  }

  override def abort(): Unit = data.clear()
}
