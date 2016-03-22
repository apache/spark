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

import java.util.UUID

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.sources.{Partition, FileCatalog, FileFormat}

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.collection.OpenHashSet

object FileStreamSink {
  val metadataDir = "_spark_metadata"
}

class StreamFileCatalog(sqlContext: SQLContext, path: Path) extends FileCatalog with Logging {
  val metadataDirectory = new Path(path, FileStreamSink.metadataDir)
  logInfo(s"Reading log from $metadataDirectory")
  val metadataLog = new HDFSMetadataLog[Seq[String]](sqlContext, metadataDirectory.toUri.toString)
  val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

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
    fs.listStatus(metadataLog.get(None, None).flatMap(_._2).map(new Path(_)))
  }
}

/**
 *
 */
class FileStreamSink(
    sqlContext: SQLContext,
    path: String,
    fileFormat: FileFormat) extends Sink with Logging {

  val basePath = new Path(path)
  val logPath = new Path(basePath, FileStreamSink.metadataDir)
  logInfo(s"Logging to $logPath")
  val fileLog = new HDFSMetadataLog[Seq[String]](sqlContext, logPath.toUri.toString)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    logInfo(s"STARTING BATCH COMMIT $batchId")
    if (fileLog.get(batchId).isDefined) {
      logInfo(s"Skipping already commited batch $batchId")
    } else {
      val files = writeFiles(data)

      println(s"Wrote ${files.size}")

      if (fileLog.add(batchId, files)) {
        logInfo(s"Wrote batch $batchId")
      } else {
        logInfo(s"Someone beat us to batch $batchId")
      }
    }
    logInfo(s"DONE BATCH COMMIT $batchId")
  }

  private def writeFiles(data: DataFrame): Seq[String] = {
    val ctx = sqlContext
    val outputDir = path
    val format = fileFormat
    val schema = data.schema

    /*
    data.queryExecution.executedPlan.execute().mapPartitions { rows =>
      val basePath = new Path(outputDir)
      val file = new Path(basePath, UUID.randomUUID().toString)
      val writer = format.openWriter(ctx, file.toUri.toString, schema)
      rows.foreach(writer.write)

      Iterator(file.toString)
    }.collect().toSeq
    */

    val file = new Path(basePath, UUID.randomUUID().toString).toUri.toString
    println(s"writing to uuid at $file")
    data.write.parquet(file)
    sqlContext.read
        .schema(data.schema)
        .parquet(file).inputFiles
  }
}
