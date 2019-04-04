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

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.SerializableConfiguration

/**
 * Abstract class for writing out data in a single Spark task.
 * Exceptions thrown by the implementation of this trait will automatically trigger task aborts.
 */
abstract class FileFormatDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol) {
  /**
   * Max number of files a single task writes out due to file size. In most cases the number of
   * files written should be very small. This is just a safe guard to protect some really bad
   * settings, e.g. maxRecordsPerFile = 1.
   */
  protected val MAX_FILE_COUNTER: Int = 1000 * 1000
  protected val updatedPartitions: mutable.Set[String] = mutable.Set[String]()
  protected var currentWriter: OutputWriter = _

  /** Trackers for computing various statistics on the data as it's being written out. */
  protected val statsTrackers: Seq[WriteTaskStatsTracker] =
    description.statsTrackers.map(_.newTaskInstance())

  protected def releaseResources(): Unit = {
    if (currentWriter != null) {
      try {
        currentWriter.close()
      } finally {
        currentWriter = null
      }
    }
  }

  /** Writes a record */
  def write(record: InternalRow): Unit

  /**
   * Returns the summary of relative information which
   * includes the list of partition strings written out. The list of partitions is sent back
   * to the driver and used to update the catalog. Other information will be sent back to the
   * driver too and used to e.g. update the metrics in UI.
   */
  def commit(): WriteTaskResult = {
    releaseResources()
    val summary = ExecutedWriteSummary(
      updatedPartitions = updatedPartitions.toSet,
      stats = statsTrackers.map(_.getFinalStats()))
    WriteTaskResult(committer.commitTask(taskAttemptContext), summary)
  }

  def abort(): Unit = {
    try {
      releaseResources()
    } finally {
      committer.abortTask(taskAttemptContext)
    }
  }
}

/** FileFormatWriteTask for empty partitions */
class EmptyDirectoryDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol
) extends FileFormatDataWriter(description, taskAttemptContext, committer) {
  override def write(record: InternalRow): Unit = {}
}

/** Writes data to a single directory (used for non-dynamic-partition writes). */
class SingleDirectoryDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol)
  extends FileFormatDataWriter(description, taskAttemptContext, committer) {
  private var fileCounter: Int = _
  private var recordsInFile: Long = _
  // Initialize currentWriter and statsTrackers
  newOutputWriter()

  private def newOutputWriter(): Unit = {
    recordsInFile = 0
    releaseResources()

    val ext = description.outputWriterFactory.getFileExtension(taskAttemptContext)
    val currentPath = committer.newTaskTempFile(
      taskAttemptContext,
      None,
      f"-c$fileCounter%03d" + ext)

    currentWriter = description.outputWriterFactory.newInstance(
      path = currentPath,
      dataSchema = description.dataColumns.toStructType,
      context = taskAttemptContext)

    statsTrackers.foreach(_.newFile(currentPath))
  }

  override def write(record: InternalRow): Unit = {
    if (description.maxRecordsPerFile > 0 && recordsInFile >= description.maxRecordsPerFile) {
      fileCounter += 1
      assert(fileCounter < MAX_FILE_COUNTER,
        s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")

      newOutputWriter()
    }

    currentWriter.write(record)
    statsTrackers.foreach(_.newRow(record))
    recordsInFile += 1
  }
}

/**
 * Writes data to using dynamic partition writes, meaning this single function can write to
 * multiple directories (partitions) or files (bucketing).
 */
class DynamicPartitionDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol)
  extends FileFormatDataWriter(description, taskAttemptContext, committer) {

  /** Flag saying whether or not the data to be written out is partitioned. */
  private val isPartitioned = description.partitionColumns.nonEmpty

  /** Flag saying whether or not the data to be written out is bucketed. */
  private val isBucketed = description.bucketIdExpression.isDefined

  assert(isPartitioned || isBucketed,
    s"""DynamicPartitionWriteTask should be used for writing out data that's either
         |partitioned or bucketed. In this case neither is true.
         |WriteJobDescription: $description
       """.stripMargin)

  private var fileCounter: Int = _
  private var recordsInFile: Long = _
  private var currentPartionValues: Option[UnsafeRow] = None
  private var currentBucketId: Option[Int] = None

  /** Extracts the partition values out of an input row. */
  private lazy val getPartitionValues: InternalRow => UnsafeRow = {
    val proj = UnsafeProjection.create(description.partitionColumns, description.allColumns)
    row => proj(row)
  }

  /** Expression that given partition columns builds a path string like: col1=val/col2=val/... */
  private lazy val partitionPathExpression: Expression = Concat(
    description.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val partitionName = ScalaUDF(
        ExternalCatalogUtils.getPartitionPathString _,
        StringType,
        Seq(Literal(c.name), Cast(c, StringType, Option(description.timeZoneId))),
        Seq(true, true))
      if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
    })

  /** Evaluates the `partitionPathExpression` above on a row of `partitionValues` and returns
   * the partition string. */
  private lazy val getPartitionPath: InternalRow => String = {
    val proj = UnsafeProjection.create(Seq(partitionPathExpression), description.partitionColumns)
    row => proj(row).getString(0)
  }

  /** Given an input row, returns the corresponding `bucketId` */
  private lazy val getBucketId: InternalRow => Int = {
    val proj =
      UnsafeProjection.create(description.bucketIdExpression.toSeq, description.allColumns)
    row => proj(row).getInt(0)
  }

  /** Returns the data columns to be written given an input row */
  private val getOutputRow =
    UnsafeProjection.create(description.dataColumns, description.allColumns)

  /**
   * Opens a new OutputWriter given a partition key and/or a bucket id.
   * If bucket id is specified, we will append it to the end of the file name, but before the
   * file extension, e.g. part-r-00009-ea518ad4-455a-4431-b471-d24e03814677-00002.gz.parquet
   *
   * @param partitionValues the partition which all tuples being written by this `OutputWriter`
   *                        belong to
   * @param bucketId the bucket which all tuples being written by this `OutputWriter` belong to
   */
  private def newOutputWriter(partitionValues: Option[InternalRow], bucketId: Option[Int]): Unit = {
    recordsInFile = 0
    releaseResources()

    val partDir = partitionValues.map(getPartitionPath(_))
    partDir.foreach(updatedPartitions.add)

    val bucketIdStr = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")

    // This must be in a form that matches our bucketing format. See BucketingUtils.
    val ext = f"$bucketIdStr.c$fileCounter%03d" +
      description.outputWriterFactory.getFileExtension(taskAttemptContext)

    val customPath = partDir.flatMap { dir =>
      description.customPartitionLocations.get(PartitioningUtils.parsePathFragment(dir))
    }
    val currentPath = if (customPath.isDefined) {
      committer.newTaskTempFileAbsPath(taskAttemptContext, customPath.get, ext)
    } else {
      committer.newTaskTempFile(taskAttemptContext, partDir, ext)
    }

    currentWriter = description.outputWriterFactory.newInstance(
      path = currentPath,
      dataSchema = description.dataColumns.toStructType,
      context = taskAttemptContext)

    statsTrackers.foreach(_.newFile(currentPath))
  }

  override def write(record: InternalRow): Unit = {
    val nextPartitionValues = if (isPartitioned) Some(getPartitionValues(record)) else None
    val nextBucketId = if (isBucketed) Some(getBucketId(record)) else None

    if (currentPartionValues != nextPartitionValues || currentBucketId != nextBucketId) {
      // See a new partition or bucket - write to a new partition dir (or a new bucket file).
      if (isPartitioned && currentPartionValues != nextPartitionValues) {
        currentPartionValues = Some(nextPartitionValues.get.copy())
        statsTrackers.foreach(_.newPartition(currentPartionValues.get))
      }
      if (isBucketed) {
        currentBucketId = nextBucketId
        statsTrackers.foreach(_.newBucket(currentBucketId.get))
      }

      fileCounter = 0
      newOutputWriter(currentPartionValues, currentBucketId)
    } else if (description.maxRecordsPerFile > 0 &&
      recordsInFile >= description.maxRecordsPerFile) {
      // Exceeded the threshold in terms of the number of records per file.
      // Create a new file by increasing the file counter.
      fileCounter += 1
      assert(fileCounter < MAX_FILE_COUNTER,
        s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")

      newOutputWriter(currentPartionValues, currentBucketId)
    }
    val outputRow = getOutputRow(record)
    currentWriter.write(outputRow)
    statsTrackers.foreach(_.newRow(outputRow))
    recordsInFile += 1
  }
}

/** A shared job description for all the write tasks. */
class WriteJobDescription(
    val uuid: String, // prevent collision between different (appending) write jobs
    val serializableHadoopConf: SerializableConfiguration,
    val outputWriterFactory: OutputWriterFactory,
    val allColumns: Seq[Attribute],
    val dataColumns: Seq[Attribute],
    val partitionColumns: Seq[Attribute],
    val bucketIdExpression: Option[Expression],
    val path: String,
    val customPartitionLocations: Map[TablePartitionSpec, String],
    val maxRecordsPerFile: Long,
    val timeZoneId: String,
    val statsTrackers: Seq[WriteJobStatsTracker])
  extends Serializable {

  assert(AttributeSet(allColumns) == AttributeSet(partitionColumns ++ dataColumns),
    s"""
         |All columns: ${allColumns.mkString(", ")}
         |Partition columns: ${partitionColumns.mkString(", ")}
         |Data columns: ${dataColumns.mkString(", ")}
       """.stripMargin)
}

/** The result of a successful write task. */
case class WriteTaskResult(commitMsg: TaskCommitMessage, summary: ExecutedWriteSummary)

/**
 * Wrapper class for the metrics of writing data out.
 *
 * @param updatedPartitions the partitions updated during writing data out. Only valid
 *                          for dynamic partition.
 * @param stats one `WriteTaskStats` object for every `WriteJobStatsTracker` that the job had.
 */
case class ExecutedWriteSummary(
    updatedPartitions: Set[String],
    stats: Seq[WriteTaskStats])
