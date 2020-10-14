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
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

/**
 * A wrapper reader that always appends partition values to [[InternalRow]]s produced by the input
 * reader [[fileReader]].
 */
class PartitionReaderWithPartitionValues(
    fileReader: PartitionReader[InternalRow],
    readDataSchema: StructType,
    partitionSchema: StructType,
    partitionValues: InternalRow) extends PartitionReader[InternalRow] {
  private val fullSchema = readDataSchema.toAttributes ++ partitionSchema.toAttributes
  private val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
  // Note that we have to apply the converter even though `file.partitionValues` is empty.
  // This is because the converter is also responsible for converting safe `InternalRow`s into
  // `UnsafeRow`s
  private val rowConverter = {
    if (partitionSchema.isEmpty) {
      () => unsafeProjection(fileReader.get())}
    else {
      val joinedRow = new JoinedRow()
      () => unsafeProjection(joinedRow(fileReader.get(), partitionValues))
    }
  }

  override def next(): Boolean = fileReader.next()

  override def get(): InternalRow = rowConverter()

  override def close(): Unit = fileReader.close()
}
