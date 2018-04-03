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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, FilePartitionUtil, PartitionedFile}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}

case class FileReaderFactory[T](
    file: FilePartition,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    ignoreCorruptFiles: Boolean = false,
    ignoreMissingFiles: Boolean = false)
  extends DataReaderFactory[T] {
  override def createDataReader(): DataReader[T] = {
    val taskContext = TaskContext.get()
    val iter = FilePartitionUtil.compute(file, taskContext, readFunction,
      ignoreCorruptFiles, ignoreMissingFiles)
    InternalRowDataReader[T](iter)
  }

  override def preferredLocations(): Array[String] = {
    FilePartitionUtil.getPreferredLocations(file)
  }
}

case class InternalRowDataReader[T](iter: Iterator[InternalRow])
  extends DataReader[T] {
  override def next(): Boolean = iter.hasNext

  override def get(): T = iter.next().asInstanceOf[T]

  override def close(): Unit = {}
}
