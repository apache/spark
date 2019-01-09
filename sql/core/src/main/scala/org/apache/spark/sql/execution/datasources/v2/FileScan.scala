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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.v2.reader.{Batch, InputPartition, Scan}
import org.apache.spark.sql.types.StructType

abstract class FileScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex) extends Scan with Batch {
  /**
   * Returns whether a file with `path` could be split or not.
   */
  def isSplitable(path: Path): Boolean = {
    false
  }

  protected def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(Seq.empty, Seq.empty)
    val maxSplitBytes = PartitionedFileUtil.maxSplitBytes(sparkSession, selectedPartitions)
    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        val filePath = file.getPath
        PartitionedFileUtil.splitFiles(
          sparkSession = sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = isSplitable(filePath),
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }
    FilePartitionUtil.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions.toArray
  }

  override def toBatch: Batch = this
}
