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

import java.io.IOException

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand

/**
 * A command for writing data to a [[HadoopFsRelation]].  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.  Each [[InsertIntoHadoopFsRelationCommand]]
 * issues a single write job, and owns a UUID that identifies this job.  Each concrete
 * implementation of [[HadoopFsRelation]] should use this UUID together with task id to generate
 * unique file path for each task output file.  This UUID is passed to executor side via a
 * property named `spark.sql.sources.writeJobUUID`.
 */
case class InsertIntoHadoopFsRelationCommand(
    outputPath: Path,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    refreshFunction: () => Unit,
    options: Map[String, String],
    @transient query: LogicalPlan,
    mode: SaveMode)
  extends RunnableCommand {

  override protected def innerChildren: Seq[LogicalPlan] = query :: Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Most formats don't do well with duplicate columns, so lets not allow that
    if (query.schema.fieldNames.length != query.schema.fieldNames.distinct.length) {
      val duplicateColumns = query.schema.fieldNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }.mkString(", ")
      throw new AnalysisException(s"Duplicate column(s) : $duplicateColumns found, " +
          s"cannot save to file.")
    }

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val pathExists = fs.exists(qualifiedOutputPath)
    val doInsertion = (mode, pathExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        throw new AnalysisException(s"path $qualifiedOutputPath already exists.")
      case (SaveMode.Overwrite, true) =>
        if (!fs.delete(qualifiedOutputPath, true /* recursively */)) {
          throw new IOException(s"Unable to clear output " +
            s"directory $qualifiedOutputPath prior to writing to it")
        }
        true
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        true
      case (SaveMode.Ignore, exists) =>
        !exists
      case (s, exists) =>
        throw new IllegalStateException(s"unsupported save mode $s ($exists)")
    }
    // If we are appending data to an existing dir.
    val isAppend = pathExists && (mode == SaveMode.Append)

    if (doInsertion) {
      WriteOutput.write(
        sparkSession,
        query,
        fileFormat,
        qualifiedOutputPath,
        hadoopConf,
        partitionColumns,
        bucketSpec,
        refreshFunction,
        options,
        isAppend)
    } else {
      logInfo("Skipping insertion into a relation that already exists.")
    }

    Seq.empty[Row]
  }
}
