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

package org.apache.spark.sql.execution.streaming

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileCatalog, Partition, PartitionSpec}
import org.apache.spark.sql.types.StructType

class StreamFileCatalog(sparkSession: SparkSession, path: Path) extends FileCatalog with Logging {
  val metadataDirectory = new Path(path, FileStreamSink.metadataDir)
  logInfo(s"Reading streaming file log from $metadataDirectory")
  val metadataLog = new FileStreamSinkLog(sparkSession, metadataDirectory.toUri.toString)
  val fs = path.getFileSystem(sparkSession.sessionState.newHadoopConf())

  override def paths: Seq[Path] = path :: Nil

  override def partitionSpec(): PartitionSpec = PartitionSpec(StructType(Nil), Nil)

  /**
   * Returns all valid files grouped into partitions when the data is partitioned. If the data is
   * unpartitioned, this will return a single partition with not partition values.
   *
   * @param filters the filters used to prune which partitions are returned.  These filters must
   *                only refer to partition columns and this method will only return files
   *                where these predicates are guaranteed to evaluate to `true`.  Thus, these
   *                filters will not need to be evaluated again on the returned data.
   */
  override def listFiles(filters: Seq[Expression]): Seq[Partition] =
    Partition(InternalRow.empty, allFiles()) :: Nil

  override def getStatus(path: Path): Array[FileStatus] = fs.listStatus(path)

  override def refresh(): Unit = {}

  override def allFiles(): Seq[FileStatus] = {
    metadataLog.allFiles().map(_.toFileStatus)
  }
}
