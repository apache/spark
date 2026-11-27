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

package org.apache.spark.sql.graph

import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.classic
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.graph.GraphCheckpointer.Checkpoint
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * A `GraphCheckpointer` that stores each checkpoint as Parquet files in its own subdirectory of
 * `dir`. A checkpoint survives the loss of executors.
 *
 * Corner cases:
 *   - Reading a removed checkpoint fails with `FAILED_READ_FILE.FILE_NOT_EXIST`.
 *   - Types that Parquet does not support, such as `CalendarIntervalType`, fail the push.
 *   - `close()` deletes `dir` recursively, so `dir` must belong to this instance only.
 *   - After a hard crash of the driver, the files stay in `dir`.
 *   - Data from `pushPartitioned` has no column pruning and no filter pushdown: every read
 *     decodes all stored columns and all rows, and filters run after the read. Only the size
 *     statistic is kept. Data from `push` has neither limitation.
 *
 * @param spark
 *   the session of the processed graph
 * @param dir
 *   the directory of this run
 */
private[spark] class ParquetCheckpointer(spark: SparkSession, dir: Path)
    extends GraphCheckpointer(spark) {

  private val fs = dir.getFileSystem(spark.sessionState.newHadoopConf())
  private val root = fs.makeQualified(dir)
  private var nextId = 0

  override protected def store(
      df: DataFrame,
      name: String,
      layout: Option[BucketSpec]): Checkpoint = {
    // The id keeps paths unique when a name is reused, for example by replaceLast.
    val path = new Path(root, f"$nextId%06d-$name")
    nextId += 1

    val command = InsertIntoHadoopFsRelationCommand(
      outputPath = path,
      staticPartitions = Map.empty,
      ifPartitionNotExists = false,
      partitionColumns = Nil,
      bucketSpec = layout,
      fileFormat = new ParquetFileFormat,
      // A bucket split into several files would lose its reported sort order.
      options = layout.fold(Map.empty[String, String])(_ => Map("maxRecordsPerFile" -> "0")),
      query = df.logicalPlan,
      mode = SaveMode.ErrorIfExists,
      catalogTable = None,
      fileIndex = None,
      outputColumnNames = df.columns.toSeq)

    spark.sessionState.executePlan(command).assertCommandExecuted()
    Checkpoint(name, read(path, df.schema, layout), () => fs.delete(path, true))
  }

  // Also removes the files of failed writes, which are not tracked.
  override protected def cleanup(): Unit = fs.delete(root, true)

  private def read(path: Path, schema: StructType, layout: Option[BucketSpec]): DataFrame = {
    if (layout.isEmpty) {
      spark.baseRelationToDataFrame(relation(spark, path, schema, layout))
    } else {
      readPartitioned(path, schema, layout)
    }
  }

  // Wraps the bucketed scan into a LogicalRDD with the partitioning and the ordering that the scan
  // itself reports after checking the bucket files, because by default it does not report the
  // ordering. The settings apply to a cloned session, so the session of the user does not change.
  private def readPartitioned(
      path: Path,
      schema: StructType,
      layout: Option[BucketSpec]): DataFrame = {
    val session = spark.cloneSession()
    val conf = session.sessionState.conf
    conf.setConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED, false)
    conf.setConf(SQLConf.BUCKETING_ENABLED, true)
    conf.setConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED, false)
    conf.setConf(SQLConf.LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING, true)
    val logicalRDD = session.withActive {
      val scanRelation = LogicalRelation(relation(session, path, schema, layout))
      val qe = session.sessionState.executePlan(scanRelation)
      val scan = qe.executedPlan
      // otherwise it will be deafultSizeInBytes
      val stats = Statistics(sizeInBytes = qe.optimizedPlan.stats.sizeInBytes)
      LogicalRDD(scan.output, scan.execute(), scan.outputPartitioning, scan.outputOrdering)(
        spark,
        Some(stats))
    }
    classic.Dataset.ofRows(spark, logicalRDD)
  }

  // Uses the original schema: spark.read.parquet would make all columns nullable, and it cannot
  // infer the schema of an empty bucketed write, which has no files.
  private def relation(
      session: classic.SparkSession,
      path: Path,
      schema: StructType,
      layout: Option[BucketSpec]): HadoopFsRelation = {
    HadoopFsRelation(
      location = new InMemoryFileIndex(session, Seq(path), Map.empty, Some(schema)),
      partitionSchema = new StructType(),
      dataSchema = schema,
      bucketSpec = layout,
      fileFormat = new ParquetFileFormat,
      options = Map.empty)(session)
  }
}

private[spark] object ParquetCheckpointer {

  /**
   * Creates a checkpointer in a new directory `<base>/<algorithm>-<uuid>`. The base is
   * `spark.graph.workDir` or, if it is not set, the checkpoint directory of the `SparkContext`.
   *
   * @param spark
   *   the session of the processed graph
   * @param algorithm
   *   the algorithm name, used in the directory name
   * @return
   *   a new checkpointer
   * @throws org.apache.spark.SparkException
   *   `CHECKPOINT_DIRECTORY_NOT_SET` if neither directory is set
   */
  def apply(spark: SparkSession, algorithm: String): ParquetCheckpointer = {
    val baseDir = GraphConf
      .workDir(spark)
      .orElse(spark.sparkContext.getCheckpointDir)
      .getOrElse(throw SparkCoreErrors.checkpointDirectoryHasNotBeenSetInSparkContextError())
    new ParquetCheckpointer(spark, new Path(baseDir, s"$algorithm-${UUID.randomUUID()}"))
  }
}
