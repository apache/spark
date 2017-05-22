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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, WriteDataFileOut}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.ExecutedWriteSummary
import org.apache.spark.sql.execution.debug._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.streaming.{IncrementalExecution, OffsetSeqMetadata}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
 * A logical command that is executed for its side-effects. `RunnableCommand`s are
 * wrapped in `ExecutedCommand` during execution.
 */
trait RunnableCommand extends logical.Command {
  def run(sparkSession: SparkSession): Seq[Row]
}

/**
 * A logical command specialized for writing data out. `WriteDataOutCommand`s are
 * wrapped in `WriteDataOutCommand` during execution.
 */
trait WriteDataOutCommand extends logical.Command {
  // The query plan that represents the data going to write out.
  val query: LogicalPlan

  def run(sparkSession: SparkSession, queryExecution: QueryExecution,
    callback: (Seq[ExecutedWriteSummary]) => Unit): Seq[Row]

  override protected def innerChildren: Seq[LogicalPlan] = query :: Nil
}

trait CommandExec extends SparkPlan {

  val cmd: logical.Command

  /**
   * A concrete command should override this lazy field to wrap up any side effects caused by the
   * command or any other computation that should be evaluated exactly once. The value of this field
   * can be used as the contents of the corresponding RDD generated from the physical plan of this
   * command.
   *
   * The `execute()` method of all the physical command classes should reference `sideEffectResult`
   * so that the command can be executed eagerly right after the command query is created.
   */
  protected[sql] val sideEffectResult: Seq[InternalRow]

  override protected def innerChildren: Seq[QueryPlan[_]] = cmd :: Nil

  override def output: Seq[Attribute] = cmd.output

  override def children: Seq[SparkPlan] = Nil

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  override def executeToIterator: Iterator[InternalRow] = sideEffectResult.toIterator

  override def executeTake(limit: Int): Array[InternalRow] = sideEffectResult.take(limit).toArray

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
  }
}

/**
 * A physical operator that executes the run method of a `RunnableCommand` and
 * saves the result to prevent multiple executions.
 */
case class ExecutedCommandExec(cmd: RunnableCommand) extends CommandExec {
  override protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    cmd.run(sqlContext.sparkSession).map(converter(_).asInstanceOf[InternalRow])
  }
}

/**
 * A physical operator specialized to execute the run method of a `WriteDataOutCommand` and
 * saves the result to prevent multiple executions.
 */
case class WrittenDataCommandExec(cmd: WriteDataOutCommand) extends CommandExec {

  private def updateDriverMetrics(
      queryExecution: QueryExecution,
      writeTaskSummary: Seq[ExecutedWriteSummary]): Unit = {
    var partitionNum = 0
    var fileNum = 0
    var fileBytes: Long = 0L

    writeTaskSummary.foreach { summary =>
      partitionNum += summary.updatedPartitions.size
      fileNum += summary.writtenFileNum
      fileBytes += summary.writtenBytes
    }

    val writeOutPlan = queryExecution.executedPlan.collect {
      case w: WriteDataFileOutExec => w
    }.head

    val partitionMetric = writeOutPlan.metrics("dynamicPartNum")
    val fileNumMetric = writeOutPlan.metrics("fileNum")
    val fileBytesMetric = writeOutPlan.metrics("fileBytes")
    partitionMetric.add(partitionNum)
    fileNumMetric.add(fileNum)
    fileBytesMetric.add(fileBytes)

    val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sqlContext.sparkContext, executionId,
      partitionMetric :: fileNumMetric :: fileBytesMetric :: Nil)
  }

  override protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    // Wraps the query plan with an operator to track its metrics. The commands will actually use
    // this wrapped query plan when writing data out.
    val writeDataOutQuery: LogicalPlan = WriteDataFileOut(cmd.query)

    val queryExecution = Dataset.ofRows(sqlContext.sparkSession, writeDataOutQuery).queryExecution

    // Associate the query execution with a SQL execution id
    SQLExecution.withNewExecutionId(sqlContext.sparkSession, queryExecution) {
      val startTime = System.nanoTime()

      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      val results = cmd.run(sqlContext.sparkSession, queryExecution,
        updateDriverMetrics(queryExecution, _))
          .map(converter(_).asInstanceOf[InternalRow])

      val timeTakenMs = (System.nanoTime() - startTime) / 1000 / 1000
      val writingTimeMetrics = queryExecution.executedPlan.collect {
        case w: WriteDataFileOutExec => w
      }.head.metrics("writingTime")
      writingTimeMetrics.add(timeTakenMs)

      val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(sqlContext.sparkContext, executionId,
        writingTimeMetrics :: Nil)

      results
    }
  }
}

/**
 * A physical operator represents the action of writing out data files. This operator doesn't change
 * the data but associates metrics with data writes for visibility.
 */
case class WriteDataFileOutExec(child: SparkPlan) extends SparkPlan {

  override def output: Seq[Attribute] = child.output
  override def children: Seq[SparkPlan] = child :: Nil

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sqlContext.sparkContext, "number of output rows"),
    "writingTime" -> SQLMetrics.createMetric(sqlContext.sparkContext, "writing data out time (ms)"),
    "dynamicPartNum" -> SQLMetrics.createMetric(sqlContext.sparkContext, "number of dynamic part"),
    "fileNum" -> SQLMetrics.createMetric(sqlContext.sparkContext, "number of written files"),
    "fileBytes" -> SQLMetrics.createMetric(sqlContext.sparkContext, "bytes of written files"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsInternal { iter =>
      iter.map{ row =>
        numOutputRows += 1
        row
      }
    }
  }
}

/**
 * An explain command for users to see how a command will be executed.
 *
 * Note that this command takes in a logical plan, runs the optimizer on the logical plan
 * (but do NOT actually execute it).
 *
 * {{{
 *   EXPLAIN (EXTENDED | CODEGEN) SELECT * FROM ...
 * }}}
 *
 * @param logicalPlan plan to explain
 * @param extended whether to do extended explain or not
 * @param codegen whether to output generated code from whole-stage codegen or not
 * @param cost whether to show cost information for operators.
 */
case class ExplainCommand(
    logicalPlan: LogicalPlan,
    extended: Boolean = false,
    codegen: Boolean = false,
    cost: Boolean = false)
  extends RunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType, nullable = true)())

  // Run through the optimizer to generate the physical plan.
  override def run(sparkSession: SparkSession): Seq[Row] = try {
    val queryExecution =
      if (logicalPlan.isStreaming) {
        // This is used only by explaining `Dataset/DataFrame` created by `spark.readStream`, so the
        // output mode does not matter since there is no `Sink`.
        new IncrementalExecution(
          sparkSession, logicalPlan, OutputMode.Append(), "<unknown>", 0, OffsetSeqMetadata(0, 0))
      } else {
        sparkSession.sessionState.executePlan(logicalPlan)
      }
    val outputString =
      if (codegen) {
        codegenString(queryExecution.executedPlan)
      } else if (extended) {
        queryExecution.toString
      } else if (cost) {
        queryExecution.toStringWithStats
      } else {
        queryExecution.simpleString
      }
    Seq(Row(outputString))
  } catch { case cause: TreeNodeException[_] =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }
}

/** An explain command for users to see how a streaming batch is executed. */
case class StreamingExplainCommand(
    queryExecution: IncrementalExecution,
    extended: Boolean) extends RunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType, nullable = true)())

  // Run through the optimizer to generate the physical plan.
  override def run(sparkSession: SparkSession): Seq[Row] = try {
    val outputString =
      if (extended) {
        queryExecution.toString
      } else {
        queryExecution.simpleString
      }
    Seq(Row(outputString))
  } catch { case cause: TreeNodeException[_] =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }
}
