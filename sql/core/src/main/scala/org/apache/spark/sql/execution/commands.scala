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

package org.apache.spark.sql.execution

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, SQLConf, SQLContext}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Row, Attribute}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A logical command that is executed for its side-effects.  `RunnableCommand`s are
 * wrapped in `ExecutedCommand` during execution.
 */
trait RunnableCommand extends logical.Command {
  self: Product =>

  def run(sqlContext: SQLContext): Seq[Row]
}

/**
 * A physical operator that executes the run method of a `RunnableCommand` and
 * saves the result to prevent multiple executions.
 */
case class ExecutedCommand(cmd: RunnableCommand) extends SparkPlan {
  /**
   * A concrete command should override this lazy field to wrap up any side effects caused by the
   * command or any other computation that should be evaluated exactly once. The value of this field
   * can be used as the contents of the corresponding RDD generated from the physical plan of this
   * command.
   *
   * The `execute()` method of all the physical command classes should reference `sideEffectResult`
   * so that the command can be executed eagerly right after the command query is created.
   */
  protected[sql] lazy val sideEffectResult: Seq[Row] = cmd.run(sqlContext)

  override def output = cmd.output

  override def children = Nil

  override def executeCollect(): Array[Row] = sideEffectResult.toArray

  override def execute(): RDD[Row] = sqlContext.sparkContext.parallelize(sideEffectResult, 1)
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class SetCommand(
    kv: Option[(String, Option[String])],
    override val output: Seq[Attribute]) extends RunnableCommand with Logging {

  override def run(sqlContext: SQLContext) = kv match {
    // Configures the deprecated "mapred.reduce.tasks" property.
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, Some(value))) =>
      logWarning(
        s"Property ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} is deprecated, " +
          s"automatically converted to ${SQLConf.SHUFFLE_PARTITIONS} instead.")
      sqlContext.setConf(SQLConf.SHUFFLE_PARTITIONS, value)
      Seq(Row(s"${SQLConf.SHUFFLE_PARTITIONS}=$value"))

    // Configures a single property.
    case Some((key, Some(value))) =>
      sqlContext.setConf(key, value)
      Seq(Row(s"$key=$value"))

    // Queries all key-value pairs that are set in the SQLConf of the sqlContext.
    // Notice that different from Hive, here "SET -v" is an alias of "SET".
    // (In Hive, "SET" returns all changed properties while "SET -v" returns all properties.)
    case Some(("-v", None)) | None =>
      sqlContext.getAllConfs.map { case (k, v) => Row(s"$k=$v") }.toSeq

    // Queries the deprecated "mapred.reduce.tasks" property.
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, None)) =>
      logWarning(
        s"Property ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} is deprecated, " +
          s"showing ${SQLConf.SHUFFLE_PARTITIONS} instead.")
      Seq(Row(s"${SQLConf.SHUFFLE_PARTITIONS}=${sqlContext.conf.numShufflePartitions}"))

    // Queries a single property.
    case Some((key, None)) =>
      Seq(Row(s"$key=${sqlContext.getConf(key, "<undefined>")}"))
  }
}

/**
 * An explain command for users to see how a command will be executed.
 *
 * Note that this command takes in a logical plan, runs the optimizer on the logical plan
 * (but do NOT actually execute it).
 *
 * :: DeveloperApi ::
 */
@DeveloperApi
case class ExplainCommand(
    logicalPlan: LogicalPlan,
    override val output: Seq[Attribute], extended: Boolean = false) extends RunnableCommand {

  // Run through the optimizer to generate the physical plan.
  override def run(sqlContext: SQLContext) = try {
    // TODO in Hive, the "extended" ExplainCommand prints the AST as well, and detailed properties.
    val queryExecution = sqlContext.executePlan(logicalPlan)
    val outputString = if (extended) queryExecution.toString else queryExecution.simpleString

    outputString.split("\n").map(Row(_))
  } catch { case cause: TreeNodeException[_] =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class CacheTableCommand(
    tableName: String,
    plan: Option[LogicalPlan],
    isLazy: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    plan.foreach(p => new SchemaRDD(sqlContext, p).registerTempTable(tableName))
    sqlContext.cacheTable(tableName)

    if (!isLazy) {
      // Performs eager caching
      sqlContext.table(tableName).count()
    }

    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}


/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class UncacheTableCommand(tableName: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    sqlContext.table(tableName).unpersist()
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class DescribeCommand(
    child: SparkPlan,
    override val output: Seq[Attribute]) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    child.output.map(field => Row(field.name, field.dataType.toString, null))
  }
}
