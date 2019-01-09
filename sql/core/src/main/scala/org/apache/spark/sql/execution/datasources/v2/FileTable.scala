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

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.v2.{SupportsBatchRead, Table}
import org.apache.spark.sql.types.StructType

abstract class FileTable(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    userSpecifiedSchema: Option[StructType]) extends Table with SupportsBatchRead {
  def getFileIndex: PartitioningAwareFileIndex = this.fileIndex

  lazy val dataSchema: StructType = userSpecifiedSchema.orElse {
    inferSchema(fileIndex.allFiles())
  }.getOrElse {
    throw new AnalysisException(
      s"Unable to infer schema for $name. It must be specified manually.")
  }.asNullable

  override def schema(): StructType = {
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    PartitioningUtils.mergeDataAndPartitionSchema(dataSchema,
      fileIndex.partitionSchema, caseSensitive)._1
  }

  /**
   * When possible, this method should return the schema of the given `files`.  When the format
   * does not support inference, or no valid files are given should return None.  In these cases
   * Spark will require that user specify the schema manually.
   */
  def inferSchema(files: Seq[FileStatus]): Option[StructType]
}
