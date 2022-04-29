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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.ColumnIOUtil
import org.apache.parquet.io.GroupColumnIO
import org.apache.parquet.io.PrimitiveColumnIO
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.types.DataType

/**
 * Rich information for a Parquet column together with its SparkSQL type.
 */
case class ParquetColumn(
    sparkType: DataType,
    descriptor: Option[ColumnDescriptor], // only set when this is a primitive column
    repetitionLevel: Int,
    definitionLevel: Int,
    required: Boolean,
    path: Seq[String],
    children: Seq[ParquetColumn]) {

  def isPrimitive: Boolean = descriptor.nonEmpty
}

object ParquetColumn {
  def apply(sparkType: DataType, io: PrimitiveColumnIO): ParquetColumn = {
    this(sparkType, Some(io.getColumnDescriptor), ColumnIOUtil.getRepetitionLevel(io),
      ColumnIOUtil.getDefinitionLevel(io), io.getType.isRepetition(Repetition.REQUIRED),
      ColumnIOUtil.getFieldPath(io), Seq.empty)
  }

  def apply(sparkType: DataType, io: GroupColumnIO, children: Seq[ParquetColumn]): ParquetColumn = {
    this(sparkType, None, ColumnIOUtil.getRepetitionLevel(io),
      ColumnIOUtil.getDefinitionLevel(io), io.getType.isRepetition(Repetition.REQUIRED),
      ColumnIOUtil.getFieldPath(io), children)
  }
}
