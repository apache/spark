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

package org.apache.spark.sql.execution.command

import java.net.URI

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{CTEInChildren, LogicalPlan, UnaryCommand}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SerializableConfiguration

/**
 * A special `Command` which writes data out and updates metrics.
 */
trait DataWritingCommand extends UnaryCommand with CTEInChildren {
  /**
   * The input query plan that produces the data to be written.
   * IMPORTANT: the input query plan MUST be analyzed, so that we can carry its output columns
   *            to [[org.apache.spark.sql.execution.datasources.FileFormatWriter]].
   */
  def query: LogicalPlan

  override final def child: LogicalPlan = query

  // Output column names of the analyzed input query plan.
  def outputColumnNames: Seq[String]

  // Output columns of the analyzed input query plan.
  def outputColumns: Seq[Attribute] =
    DataWritingCommand.logicalPlanOutputWithNames(query, outputColumnNames)

  lazy val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics

  def basicWriteJobStatsTracker(hadoopConf: Configuration): BasicWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)
  }

  def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row]
}

object DataWritingCommand {
  /**
   * Returns output attributes with provided names.
   * The length of provided names should be the same of the length of [[LogicalPlan.output]].
   */
  def logicalPlanOutputWithNames(
      query: LogicalPlan,
      names: Seq[String]): Seq[Attribute] = {
    // Save the output attributes to a variable to avoid duplicated function calls.
    val outputAttributes = query.output
    assert(outputAttributes.length == names.length,
      "The length of provided names doesn't match the length of output attributes.")
    outputAttributes.zip(names).map { case (attr, outputName) =>
      attr.withName(outputName)
    }
  }

  /**
   * When execute CTAS operators, Spark will use [[InsertIntoHadoopFsRelationCommand]]
   * or [[InsertIntoHiveTable]] command to write data, they both inherit metrics from
   * [[DataWritingCommand]], but after running [[InsertIntoHadoopFsRelationCommand]]
   * or [[InsertIntoHiveTable]], we only update metrics in these two command through
   * [[BasicWriteJobStatsTracker]], we also need to propogate metrics to the command
   * that actually calls [[InsertIntoHadoopFsRelationCommand]] or [[InsertIntoHiveTable]].
   *
   * @param sparkContext Current SparkContext.
   * @param command Command to execute writing data.
   * @param metrics Metrics of real DataWritingCommand.
   */
  def propogateMetrics(
      sparkContext: SparkContext,
      command: DataWritingCommand,
      metrics: Map[String, SQLMetric]): Unit = {
    command.metrics.foreach { case (key, metric) => metrics(key).set(metric.value) }
    SQLMetrics.postDriverMetricUpdates(sparkContext,
      sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      metrics.values.toSeq)
  }
  /**
   * When execute CTAS operators, and the location is not empty, throw [[AnalysisException]].
   * For CTAS, the SaveMode is always [[ErrorIfExists]]
   *
   * @param tablePath Table location.
   * @param saveMode  Save mode of the table.
   * @param hadoopConf Configuration.
   */
  def assertEmptyRootPath(tablePath: URI, saveMode: SaveMode, hadoopConf: Configuration): Unit = {
    if (saveMode == SaveMode.ErrorIfExists && !SQLConf.get.allowNonEmptyLocationInCTAS) {
      val filePath = new org.apache.hadoop.fs.Path(tablePath)
      val fs = filePath.getFileSystem(hadoopConf)
      if (fs.exists(filePath) &&
          fs.getFileStatus(filePath).isDirectory &&
          fs.listStatus(filePath).length != 0) {
        throw QueryCompilationErrors.createTableAsSelectWithNonEmptyDirectoryError(
          tablePath.toString)
      }
    }
  }
}
