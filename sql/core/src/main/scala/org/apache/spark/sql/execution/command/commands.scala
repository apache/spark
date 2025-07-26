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

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Command, ExecutableDuringAnalysis, LogicalPlan, SupervisingCommand}
import org.apache.spark.sql.catalyst.trees.{LeafLike, UnaryLike}
import org.apache.spark.sql.connector.ExternalCommandRunner
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.{CommandExecutionMode, ExplainMode, LeafExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.IncrementalExecution
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * A logical command that is executed for its side-effects.  `RunnableCommand`s are
 * wrapped in `ExecutedCommand` during execution.
 */
trait RunnableCommand extends Command {

  override def children: Seq[LogicalPlan] = Nil

  // The map used to record the metrics of running the command. This will be passed to
  // `ExecutedCommand` during query planning.
  lazy val metrics: Map[String, SQLMetric] = Map.empty

  def run(sparkSession: SparkSession): Seq[Row]
}

trait LeafRunnableCommand extends RunnableCommand with LeafLike[LogicalPlan]
trait UnaryRunnableCommand extends RunnableCommand with UnaryLike[LogicalPlan]

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
    cmd.run(session).map(converter(_).asInstanceOf[InternalRow])
  }

  override def innerChildren: Seq[QueryPlan[_]] = cmd :: Nil

  override def output: Seq[Attribute] = cmd.output

  override def nodeName: String = "Execute " + cmd.nodeName

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  override def executeToIterator(): Iterator[InternalRow] = sideEffectResult.iterator

  override def executeTake(limit: Int): Array[InternalRow] = sideEffectResult.take(limit).toArray

  override def executeTail(limit: Int): Array[InternalRow] = {
    sideEffectResult.takeRight(limit).toArray
  }

  protected override def doExecute(): RDD[InternalRow] = {
    sparkContext.parallelize(sideEffectResult, 1)
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
  extends UnaryExecNode {

  override lazy val metrics: Map[String, SQLMetric] = cmd.metrics

  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val rows = cmd.run(session, child)

    rows.map(converter(_).asInstanceOf[InternalRow])
  }

  override def output: Seq[Attribute] = cmd.output

  override def nodeName: String = "Execute " + cmd.nodeName

  // override the default one, otherwise the `cmd.nodeName` will appear twice from simpleString
  override def argString(maxFields: Int): String = cmd.argString(maxFields)

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  override def executeToIterator(): Iterator[InternalRow] = sideEffectResult.iterator

  override def executeTake(limit: Int): Array[InternalRow] = sideEffectResult.take(limit).toArray

  override def executeTail(limit: Int): Array[InternalRow] = {
    sideEffectResult.takeRight(limit).toArray
  }

  protected override def doExecute(): RDD[InternalRow] = {
    sparkContext.parallelize(sideEffectResult, 1)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DataWritingCommandExec =
    copy(child = newChild)
}

/**
 * An explain command for users to see how a command will be executed.
 *
 * Note that this command takes in a logical plan, runs the optimizer on the logical plan
 * (but do NOT actually execute it).
 *
 * {{{
 *   EXPLAIN (EXTENDED | CODEGEN | COST | FORMATTED) SELECT * FROM ...
 * }}}
 *
 * @param logicalPlan plan to explain
 * @param mode explain mode
 */
case class ExplainCommand(
    logicalPlan: LogicalPlan,
    mode: ExplainMode)
  extends RunnableCommand with SupervisingCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType, nullable = true)())

  // Run through the optimizer to generate the physical plan.
  override def run(sparkSession: SparkSession): Seq[Row] = try {
    val stagedLogicalPlan = stageForAnalysis(logicalPlan)
    val qe = sparkSession.sessionState.executePlan(stagedLogicalPlan, CommandExecutionMode.SKIP)
    val outputString = qe.explainString(mode)
    Seq(Row(outputString))
  } catch { case NonFatal(cause) =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n")
      .map(Row(_)).toImmutableArraySeq
  }

  private def stageForAnalysis(plan: LogicalPlan): LogicalPlan = plan transform {
    case p: ExecutableDuringAnalysis => p.stageForExplain()
  }

  def withTransformedSupervisedPlan(transformer: LogicalPlan => LogicalPlan): LogicalPlan =
    copy(logicalPlan = transformer(logicalPlan))
}

/** An explain command for users to see how a streaming batch is executed. */
case class StreamingExplainCommand(
    queryExecution: IncrementalExecution,
    extended: Boolean) extends LeafRunnableCommand {

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
  } catch { case NonFatal(cause) =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n")
      .map(Row(_)).toImmutableArraySeq
  }
}

/**
 * Used to execute an arbitrary string command inside an external execution engine
 * rather than Spark. Please check [[ExternalCommandRunner]] for more details.
 */
case class ExternalCommandExecutor(
    runner: ExternalCommandRunner,
    command: String,
    options: Map[String, String]) extends LeafRunnableCommand {

  override def output: Seq[Attribute] =
    Seq(AttributeReference("command_output", StringType)())

  def execute(): Seq[String] = {
    runner.executeCommand(command, new CaseInsensitiveStringMap(options.asJava)).toIndexedSeq
  }

  override def run(sparkSession: SparkSession): Seq[Row] = execute().map(Row(_))
}

object ExternalCommandExecutor {
  def apply(
      session: SparkSession,
      runner: String,
      command: String,
      options: Map[String, String]): ExternalCommandExecutor = {
    DataSource.lookupDataSource(runner, session.sessionState.conf) match {
      case source if classOf[ExternalCommandRunner].isAssignableFrom(source) =>
        ExternalCommandExecutor(
          source.getDeclaredConstructor().newInstance().asInstanceOf[ExternalCommandRunner],
          command,
          options)

      case _ =>
        throw QueryCompilationErrors.commandExecutionInRunnerUnsupportedError(runner)
    }
  }
}
