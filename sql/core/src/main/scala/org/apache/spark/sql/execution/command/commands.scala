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

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.debug._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.{IncrementalExecution, OffsetSeqMetadata}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
 * A logical command that is executed for its side-effects.  `RunnableCommand`s are
 * wrapped in `ExecutedCommand` during execution.
 */
trait RunnableCommand extends Command {

  // The map used to record the metrics of running the command. This will be passed to
  // `ExecutedCommand` during query planning.
  lazy val metrics: Map[String, SQLMetric] = Map.empty

  def run(sparkSession: SparkSession): Seq[Row]
}

/**
 * A physical operator that executes the run method of a `RunnableCommand` and
 * saves the result to prevent multiple executions.
 *
 * @param cmd the `RunnableCommand` this operator will run.
 */
case class ExecutedCommandExec(cmd: RunnableCommand) extends LeafExecNode {

  override lazy val metrics: Map[String, SQLMetric] = cmd.metrics

  /**
   * A concrete command should override this lazy field to wrap up any side effects caused by the
   * command or any other computation that should be evaluated exactly once. The value of this field
   * can be used as the contents of the corresponding RDD generated from the physical plan of this
   * command.
   *
   * The `execute()` method of all the physical command classes should reference `sideEffectResult`
   * so that the command can be executed eagerly right after the command query is created.
   */
  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    cmd.run(sqlContext.sparkSession).map(converter(_).asInstanceOf[InternalRow])
  }

  override protected def innerChildren: Seq[QueryPlan[_]] = cmd :: Nil

  override def output: Seq[Attribute] = cmd.output

  override def nodeName: String = "Execute " + cmd.nodeName

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  override def executeToIterator: Iterator[InternalRow] = sideEffectResult.toIterator

  override def executeTake(limit: Int): Array[InternalRow] = sideEffectResult.take(limit).toArray

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
  }
}

/**
 * A physical operator that executes the run method of a `DataWritingCommand` and
 * saves the result to prevent multiple executions.
 *
 * @param cmd the `DataWritingCommand` this operator will run.
 * @param child the physical plan child ran by the `DataWritingCommand`.
 */
case class DataWritingCommandExec(cmd: DataWritingCommand, child: SparkPlan)
  extends SparkPlan {

  override lazy val metrics: Map[String, SQLMetric] = cmd.metrics

  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val rows = cmd.run(sqlContext.sparkSession, child)

    rows.map(converter(_).asInstanceOf[InternalRow])
  }

  override def children: Seq[SparkPlan] = child :: Nil

  override def output: Seq[Attribute] = cmd.output

  override def nodeName: String = "Execute " + cmd.nodeName

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  override def executeToIterator: Iterator[InternalRow] = sideEffectResult.toIterator

  override def executeTake(limit: Int): Array[InternalRow] = sideEffectResult.take(limit).toArray

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
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
          sparkSession, logicalPlan, OutputMode.Append(), "<unknown>",
          UUID.randomUUID, 0, OffsetSeqMetadata(0, 0))
      } else {
        sparkSession.sessionState.executePlan(logicalPlan)
      }
    val outputString =
      if (codegen) {
        codegenString(queryExecution.executedPlan)
      } else if (extended) {
        queryExecution.toString
      } else if (cost) {
        queryExecution.stringWithStats
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
