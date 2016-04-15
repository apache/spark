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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.sources.{FileFormat, OutputWriter}
import org.apache.spark.sql.types.{StringType, StructType}

object FileStreamSink {
  // The name of the subdirectory that is used to store metadata about which files are valid.
  val metadataDir = "_spark_metadata"
}

/**
 * A sink that writes out results to parquet files.  Each batch is written out using
 * [[FileStreamSinkWriter]] to a unique set of files without using an OutputCommitter. Instead,
 * after all of the files in a batch have been successfully written, the list of
 * file paths is appended to the log atomically. In the case of partial failures, some duplicate
 * data may be present in the target directory, but only one copy of each file will be present
 * in the log.
 */
class FileStreamSink(
    sqlContext: SQLContext,
    path: String,
    fileFormat: FileFormat,
    partitionColumnNames: Seq[String],
    options: Map[String, String]) extends Sink with Logging {

  private val basePath = new Path(path)
  private val logPath = new Path(basePath, FileStreamSink.metadataDir)
  private val fileLog = new HDFSMetadataLog[Seq[String]](sqlContext, logPath.toUri.toString)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    if (fileLog.get(batchId).isDefined) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val writer = new FileStreamSinkWriter(data, fileFormat, path, partitionColumnNames, options)
      val files = writer.write()
      if (fileLog.add(batchId, files)) {
        logInfo(s"Committed batch $batchId")
      } else {
        logWarning(s"Race while writing batch $batchId")
      }
    }
  }

  override def toString: String = s"FileSink[$path]"
}

class FileStreamSinkWriter(
    data: DataFrame,
    fileFormat: FileFormat,
    basePath: String,
    partitionColumnNames: Seq[String],
    options: Map[String, String]) extends Serializable with Logging {

  PartitioningUtils.validatePartitionColumnDataTypes(
    data.schema, partitionColumnNames, data.sqlContext.conf.caseSensitiveAnalysis)

  private val dataSchema = data.schema
  private val dataColumns = data.logicalPlan.output

  private val partitionColumns = partitionColumnNames.map { col =>
    val nameEquality = if (data.sqlContext.conf.caseSensitiveAnalysis) {
      org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
    } else {
      org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
    }
    data.logicalPlan.output.find(f => nameEquality(f.name, col)).getOrElse {
      throw new RuntimeException(s"Partition column $col not found in schema $dataSchema")
    }
  }

  private val writeColumns = {
    val partitionSet = AttributeSet(partitionColumns)
    dataColumns.filterNot(partitionSet.contains)
  }

  private val outputWriterFactory =
    fileFormat.buildWriter(data.sqlContext, writeColumns.toStructType, options)

  // Expressions that given a partition key build a string like: col1=val/col2=val/...
  private def partitionStringExpression: Seq[Expression] = {
    partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val escaped =
        ScalaUDF(
          PartitioningUtils.escapePathName _,
          StringType,
          Seq(Cast(c, StringType)),
          Seq(StringType))
      val str = If(IsNull(c), Literal(PartitioningUtils.DEFAULT_PARTITION_NAME), escaped)
      val partitionName = Literal(c.name + "=") :: str :: Nil
      if (i == 0) partitionName else Literal(Path.SEPARATOR) :: partitionName
    }
  }

  /** Generate a new output writer from the writer factory */
  private def newOutputWriter(path: Path): OutputWriter = {
    val newWriter = outputWriterFactory.newWriter(path.toString)
    newWriter.initConverter(dataSchema)
    newWriter
  }

  /** Write the dataframe to a files */
  def write(): Array[String] = {
    data.sqlContext.sparkContext.runJob(
      data.queryExecution.toRdd,
      (taskContext: TaskContext, iterator: Iterator[InternalRow]) => {
        if (partitionColumns.isEmpty) {
          Seq(writePartitionToSingleFile(iterator))
        } else {
          writePartitionToPartitionedFiles(iterator)
        }
      }).flatten
  }

  /** Writes a RDD partition to a single file without dynamic partitioning. */
  def writePartitionToSingleFile(iterator: Iterator[InternalRow]): String = {
    var writer: OutputWriter = null
    try {
      val path = new Path(basePath, UUID.randomUUID.toString)
      writer = newOutputWriter(path)
      while(iterator.hasNext) {
        writer.writeInternal(iterator.next)
      }
      writer.close()
      writer = null
      path.toString
    } catch {
      case cause: Throwable =>
        logError("Aborting task.", cause)
        // call failure callbacks first, so we could have a chance to cleanup the writer.
        TaskContext.get().asInstanceOf[TaskContextImpl].markTaskFailed(cause)
        throw new SparkException("Task failed while writing rows.", cause)
    } finally {
      if (writer != null) {
        writer.close()
      }
    }
  }

  /** Writes a RDD partition to multiple dynamically partitioned files. */
  def writePartitionToPartitionedFiles(iterator: Iterator[InternalRow]): Seq[String] = {

    // Returns the partitioning columns for sorting
    val getSortingKey = UnsafeProjection.create(partitionColumns, dataColumns)

    // Returns the data columns to be written given an input row
    val getOutputRow = UnsafeProjection.create(writeColumns, dataColumns)

    // Returns the partition path given a partition key
    val getPartitionString =
      UnsafeProjection.create(Concat(partitionStringExpression) :: Nil, partitionColumns)

    // Sort the data before write, so that we only need one writer at the same time.
    val sorter = new UnsafeKVExternalSorter(
      partitionColumns.toStructType,
      StructType.fromAttributes(writeColumns),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      TaskContext.get().taskMemoryManager().pageSizeBytes)

    while (iterator.hasNext) {
      val currentRow = iterator.next()
      sorter.insertKV(getSortingKey(currentRow), getOutputRow(currentRow))
    }
    logDebug(s"Sorting complete. Writing out partition files one at a time.")

    val sortedIterator = sorter.sortedIterator()
    val paths = new ArrayBuffer[String]

    // Writes the sorted data to partitioned files, one for each unique key
    var currentWriter: OutputWriter = null
    try {
      var currentKey: UnsafeRow = null
      while (sortedIterator.next()) {
        val nextKey = sortedIterator.getKey

        // If key changes, close current writer, and open a new writer to a new partitioned file
        if (currentKey != nextKey) {
          if (currentWriter != null) {
            currentWriter.close()
            currentWriter = null
          }
          currentKey = nextKey.copy()
          val partitionPath = getPartitionString(currentKey).getString(0)
          val path = new Path(new Path(basePath, partitionPath), UUID.randomUUID.toString)
          paths += path.toString
          currentWriter = newOutputWriter(path)
          logDebug(s"Writing partition $currentKey to $path")
        }
        currentWriter.writeInternal(sortedIterator.getValue)
      }
      if (currentWriter != null) {
        currentWriter.close()
        currentWriter = null
      }
      paths
    } catch {
      case cause: Throwable =>
        logError("Aborting task.", cause)
        // call failure callbacks first, so we could have a chance to cleanup the writer.
        TaskContext.get().asInstanceOf[TaskContextImpl].markTaskFailed(cause)
        throw new SparkException("Task failed while writing rows.", cause)
    } finally {
      if (currentWriter != null) {
        currentWriter.close()
      }
    }
  }
}

