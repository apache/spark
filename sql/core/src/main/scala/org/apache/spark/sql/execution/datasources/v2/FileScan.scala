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

import java.util.{Locale, OptionalLong}

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

abstract class FileScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    readDataSchema: StructType,
    readPartitionSchema: StructType) extends Scan with Batch with SupportsReportStatistics {
  /**
   * Returns whether a file with `path` could be split or not.
   */
  def isSplitable(path: Path): Boolean = {
    false
  }

  protected def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(Seq.empty, Seq.empty)
    val maxSplitBytes = FilePartition.maxSplitBytes(sparkSession, selectedPartitions)
    val partitionAttributes = fileIndex.partitionSchema.toAttributes
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.get(normalizeName(readField.name)).getOrElse {
        throw new AnalysisException(s"Can't find required partition column ${readField.name} " +
          s"in partition schema ${fileIndex.partitionSchema}")
      }
    }
    lazy val partitionValueProject =
      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    val splitFiles = selectedPartitions.flatMap { partition =>
      // Prune partition values if part of the partition columns are not required.
      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
        partitionValueProject(partition.values).copy()
      } else {
        partition.values
      }
      partition.files.flatMap { file =>
        val filePath = file.getPath
        PartitionedFileUtil.splitFiles(
          sparkSession = sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = isSplitable(filePath),
          maxSplitBytes = maxSplitBytes,
          partitionValues = partitionValues
        )
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }
    FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions.toArray
  }

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        val compressionFactor = sparkSession.sessionState.conf.fileCompressionFactor
        val size = (compressionFactor * fileIndex.sizeInBytes).toLong
        OptionalLong.of(size)
      }

      override def numRows(): OptionalLong = OptionalLong.empty()
    }
  }

  override def toBatch: Batch = this

  override def readSchema(): StructType =
    StructType(readDataSchema.fields ++ readPartitionSchema.fields)

  // Returns whether the two given arrays of [[Filter]]s are equivalent.
  protected def equivalentFilters(a: Array[Filter], b: Array[Filter]): Boolean = {
    a.sortBy(_.hashCode()).sameElements(b.sortBy(_.hashCode()))
  }

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }
}
