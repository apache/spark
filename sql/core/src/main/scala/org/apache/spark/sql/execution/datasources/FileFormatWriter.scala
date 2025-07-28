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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.annotation.Unstable
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.{SparkPlan, UnsafeExternalRowSorter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * A writer interface for V1 file writing.
 */
@Unstable
trait FileFormatWriter {

  // scalastyle:off argcount
  /**
   * Writes out all data output from a given Spark query plan.
   *
   * @param sparkSession The Spark session this writing operation belongs to.
   * @param plan A query plan that generates data to write.
   * @param fileFormat The target file format.
   * @param committer A committer protocol used to commit tasks.
   * @param outputSpec An output specification describing how the data should be placed
   *                   in the file system.
   * @param hadoopConf Hadoop configuration.
   * @param partitionColumns A list of partition columns in the output data. This includes
   *                         both static partition columns and dynamic partition columns.
   *                         When both are present (numStaticPartitionCols > 0), the static
   *                         partition columns will be placed at the head of this list.
   * @param bucketSpec A specification for data bucketing.
   * @param statsTrackers A list of statistic trackers that should be called during writing.
   * @param options Configuration options used when writing data.
   * @param numStaticPartitionCols Number of static partition columns in the output schema.
   *
   * @return The set of all partition paths that were updated during this write job.
   */
  def write(
    sparkSession: SparkSession,
    plan: SparkPlan,
    fileFormat: FileFormat,
    committer: FileCommitProtocol,
    outputSpec: FileFormatWriter.OutputSpec,
    hadoopConf: Configuration,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    statsTrackers: Seq[WriteJobStatsTracker],
    options: Map[String, String],
    numStaticPartitionCols: Int = 0): Set[String]
  // scalastyle:on argcount

  /**
   * Writes out all data output from a given iterator of internal rows.
   *
   * @param description The description of the write job.
   * @param jobTrackerID A tracker ID for the job.
   * @param sparkStageId ID of the Spark stage where this writing operation executes.
   * @param sparkPartitionId ID of the Spark partition this write operation handles.
   * @param sparkAttemptNumber The attempt number of the Spark task.
   * @param committer A committer protocol used to commit tasks.
   * @param iterator The iterator that outputs data to write.
   * @param concurrentOutputWriterSpec A specification for concurrent writing. This is only
   *                                   used when concurrent writing is instructed.
   *
   * @return The result of this write operation.
   */
  private[spark] def executeTask(
    description: WriteJobDescription,
    jobTrackerID: String,
    sparkStageId: Int,
    sparkPartitionId: Int,
    sparkAttemptNumber: Int,
    committer: FileCommitProtocol,
    iterator: Iterator[InternalRow],
    concurrentOutputWriterSpec: Option[FileFormatWriter.ConcurrentOutputWriterSpec]):
  WriteTaskResult
}

object FileFormatWriter {
  /** Describes how output files should be placed in the filesystem. */
  case class OutputSpec(
    outputPath: String,
    customPartitionLocations: Map[TablePartitionSpec, String],
    outputColumns: Seq[Attribute])

  /** Describes how concurrent output writers should be executed. */
  case class ConcurrentOutputWriterSpec(
    maxWriters: Int,
    createSorter: () => UnsafeExternalRowSorter)

  /**
   * Creates a file format writer instance using given SQL config. If
   * the option [[SQLConf.FILE_FORMAT_WRITER_CLASS]] is present
   * in the config, the default constructor of the writer class will be called to
   * create a custom writer instance. Otherwise, [[DefaultFileFormatWriter]] will
   * be returned.
   */
  def create(conf: SQLConf): FileFormatWriter = {
    conf.fileFormatWriterClass
      .map {
        fileFormatWriterClass =>
          if (!Utils.classIsLoadableAndAssignableFrom(
            fileFormatWriterClass, classOf[FileFormatWriter])) {
            throw new UnsupportedOperationException(s"Class must inherit " +
              s"${classOf[FileFormatWriter].getName}, but it doesn't: $fileFormatWriterClass")
          }
          val clazz = Utils.classForName[FileFormatWriter](fileFormatWriterClass)
          clazz.getDeclaredConstructor().newInstance()
      }
      .getOrElse(DefaultFileFormatWriter)
  }
}
