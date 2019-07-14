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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{PartitioningAwareFileIndex, PartitioningUtils}
import org.apache.spark.sql.sources.v2.reader.{ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

abstract class FileScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType) extends ScanBuilder with SupportsPushDownRequiredColumns {
  private val partitionSchema = fileIndex.partitionSchema
  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
  protected var requiredSchema = StructType(dataSchema.fields ++ partitionSchema.fields)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  protected def readDataSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val fields = dataSchema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName) && !partitionNameSet.contains(colName)
    }
    StructType(fields)
  }

  protected def readPartitionSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val fields = partitionSchema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName)
    }
    StructType(fields)
  }

  private def createRequiredNameSet(): Set[String] =
    requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet

  private val partitionNameSet: Set[String] =
    partitionSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet
}
