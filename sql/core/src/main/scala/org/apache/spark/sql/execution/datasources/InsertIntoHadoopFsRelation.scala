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
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{RunnableCommand, SQLExecution}
import org.apache.spark.sql.sources._
import org.apache.spark.util.Utils


/**
 * A command for writing data to a [[HadoopFsRelation]].  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.  Each [[InsertIntoHadoopFsRelation]] issues a
 * single write job, and owns a UUID that identifies this job.  Each concrete implementation of
 * [[HadoopFsRelation]] should use this UUID together with task id to generate unique file path for
 * each task output file.  This UUID is passed to executor side via a property named
 * `spark.sql.sources.writeJobUUID`.
 *
 * Different writer containers, [[DefaultWriterContainer]] and [[DynamicPartitionWriterContainer]]
 * are used to write to normal tables and tables with dynamic partitions.
 *
 * Basic work flow of this command is:
 *
 *   1. Driver side setup, including output committer initialization and data source specific
 *      preparation work for the write job to be issued.
 *   2. Issues a write job consists of one or more executor side tasks, each of which writes all
 *      rows within an RDD partition.
 *   3. If no exception is thrown in a task, commits that task, otherwise aborts that task;  If any
 *      exception is thrown during task commitment, also aborts that task.
 *   4. If all tasks are committed, commit the job, otherwise aborts the job;  If any exception is
 *      thrown during job commitment, also aborts the job.
 */
private[sql] case class InsertIntoHadoopFsRelation(
    @transient relation: HadoopFsRelation,
    @transient query: LogicalPlan,
    mode: SaveMode)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    require(
      relation.paths.length == 1,
      s"Cannot write to multiple destinations: ${relation.paths.mkString(",")}")

    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val outputPath = new Path(relation.paths.head)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val pathExists = fs.exists(qualifiedOutputPath)
    val doInsertion = (mode, pathExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        throw new AnalysisException(s"path $qualifiedOutputPath already exists.")
      case (SaveMode.Overwrite, true) =>
        Utils.tryOrIOException {
          if (!fs.delete(qualifiedOutputPath, true /* recursively */)) {
            throw new IOException(s"Unable to clear output " +
              s"directory $qualifiedOutputPath prior to writing to it")
          }
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
      val job = new Job(hadoopConf)
      job.setOutputKeyClass(classOf[Void])
      job.setOutputValueClass(classOf[InternalRow])
      FileOutputFormat.setOutputPath(job, qualifiedOutputPath)

      // A partitioned relation schema's can be different from the input logicalPlan, since
      // partition columns are all moved after data column. We Project to adjust the ordering.
      // TODO: this belongs in the analyzer.
      val project = Project(
        relation.schema.map(field => UnresolvedAttribute.quoted(field.name)), query)
      val queryExecution = DataFrame(sqlContext, project).queryExecution

      SQLExecution.withNewExecutionId(sqlContext, queryExecution) {
        val df = sqlContext.internalCreateDataFrame(queryExecution.toRdd, relation.schema)
        val partitionColumns = relation.partitionColumns.fieldNames

        // Some pre-flight checks.
        require(
          df.schema == relation.schema,
          s"""DataFrame must have the same schema as the relation to which is inserted.
             |DataFrame schema: ${df.schema}
             |Relation schema: ${relation.schema}
          """.stripMargin)
        val partitionColumnsInSpec = relation.partitionColumns.fieldNames
        require(
          partitionColumnsInSpec.sameElements(partitionColumns),
          s"""Partition columns mismatch.
             |Expected: ${partitionColumnsInSpec.mkString(", ")}
             |Actual: ${partitionColumns.mkString(", ")}
          """.stripMargin)

        val writerContainer = if (partitionColumns.isEmpty) {
          new DefaultWriterContainer(relation, job, isAppend)
        } else {
          val output = df.queryExecution.executedPlan.output
          val (partitionOutput, dataOutput) =
            output.partition(a => partitionColumns.contains(a.name))

          new DynamicPartitionWriterContainer(
            relation,
            job,
            partitionOutput,
            dataOutput,
            output,
            PartitioningUtils.DEFAULT_PARTITION_NAME,
            sqlContext.conf.getConf(SQLConf.PARTITION_MAX_FILES),
            isAppend)
        }

        // This call shouldn't be put into the `try` block below because it only initializes and
        // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
        writerContainer.driverSideSetup()

        try {
          sqlContext.sparkContext.runJob(df.queryExecution.toRdd, writerContainer.writeRows _)
          writerContainer.commitJob()
          relation.refresh()
        } catch { case cause: Throwable =>
          logError("Aborting job.", cause)
          writerContainer.abortJob()
          throw new SparkException("Job aborted.", cause)
        }
      }
    } else {
      logInfo("Skipping insertion into a relation that already exists.")
    }

    Seq.empty[Row]
  }
}
