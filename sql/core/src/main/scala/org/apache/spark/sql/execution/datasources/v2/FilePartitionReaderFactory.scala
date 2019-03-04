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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class FilePartitionReaderFactory(
    readSchema: StructType,
    partitionSchema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    val iter = filePartition.files.toIterator.map { file =>
      new PartitionedFileReader(file, buildReaderWithPartitionValues(file))
    }
    new FilePartitionReader[InternalRow](iter)
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    val iter = filePartition.files.toIterator.map { file =>
      new PartitionedFileReader(file, buildColumnarReaderWithPartitionValues(file))
    }
    new FilePartitionReader[ColumnarBatch](iter)
  }

  /**
   * Returns a row-based partition reader that can be used to read a single file.
   */
  protected def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow]

  /**
   * Exactly the same as [[buildReader]] except that the reader returned by this method appends
   * partition values to [[InternalRow]]s produced by the reader [[buildReader]] returns.
   */
  def buildReaderWithPartitionValues(
      partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    val reader = buildReader(partitionedFile)
    val fullSchema = readSchema.toAttributes ++ partitionSchema.toAttributes
    val appendPartitionColumns = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
    val rowConverter = () => if (partitionSchema.isEmpty) {
      appendPartitionColumns(reader.get())}
    else {
      val joinedRow = new JoinedRow()
      appendPartitionColumns(joinedRow(reader.get(), partitionedFile.partitionValues))
    }

    new PartitionReader[InternalRow] {
      override def next(): Boolean = reader.next()

      override def get(): InternalRow = rowConverter()

      override def close(): Unit = reader.close()
    }
  }

  /**
   * Returns a columnar partition reader that can be used to read a [[PartitionedFile]] including
   * its partition values.
   */
  def buildColumnarReaderWithPartitionValues(
      partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    throw new UnsupportedOperationException("Cannot create columnar reader.")
  }
}

// A compound class for combining file and its corresponding reader.
private[v2] class PartitionedFileReader[T](
    file: PartitionedFile,
    reader: PartitionReader[T]) extends PartitionReader[T] {
  override def next(): Boolean = reader.next()

  override def get(): T = reader.get()

  override def close(): Unit = reader.close()

  override def toString: String = file.toString
}
