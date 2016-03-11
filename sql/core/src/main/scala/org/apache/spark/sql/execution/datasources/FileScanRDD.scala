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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow

/**
 * A single file that should be read, along with partition column values that
 * need to be prepended to each row.  The reading should start at the first
 * valid record found after `offset`.
 */
case class PartitionedFile(
    partitionValues: InternalRow,
    filePath: String,
    start: Long,
    length: Long)

/**
 * A collection of files that should be read as a single task possibly from multiple partitioned
 * directories.
 *
 * IMPLEMENT ME: This is just a placeholder for a future implementation.
 * TODO: This currently does not take locality information about the files into account.
 */
case class FilePartition(val index: Int, files: Seq[PartitionedFile]) extends Partition

class FileScanRDD(
    @transient val sqlContext: SQLContext,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition])
    extends RDD[InternalRow](sqlContext.sparkContext, Nil) {


  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    throw new NotImplementedError("Not Implemented Yet")
  }

  override protected def getPartitions: Array[Partition] = Array.empty
}
