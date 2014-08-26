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
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Row, SQLConf, SQLContext}

trait Command {
  /**
   * A concrete command should override this lazy field to wrap up any side effects caused by the
   * command or any other computation that should be evaluated exactly once. The value of this field
   * can be used as the contents of the corresponding RDD generated from the physical plan of this
   * command.
   *
   * The `execute()` method of all the physical command classes should reference `sideEffectResult`
   * so that the command can be executed eagerly right after the command query is created.
   */
  protected[sql] lazy val sideEffectResult: Seq[Any] = Seq.empty[Any]
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class SetCommand(
    key: Option[String], value: Option[String], output: Seq[Attribute])(
    @transient context: SQLContext)
  extends LeafNode with Command with Logging {

  override protected[sql] lazy val sideEffectResult: Seq[String] = (key, value) match {
    // Set value for key k.
    case (Some(k), Some(v)) =>
      if (k == SQLConf.Deprecated.MAPRED_REDUCE_TASKS) {
        logWarning(s"Property ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} is deprecated, " +
          s"automatically converted to ${SQLConf.SHUFFLE_PARTITIONS} instead.")
        context.setConf(SQLConf.SHUFFLE_PARTITIONS, v)
        Array(s"${SQLConf.SHUFFLE_PARTITIONS}=$v")
      } else {
        context.setConf(k, v)
        Array(s"$k=$v")
      }

    // Query the value bound to key k.
    case (Some(k), _) =>
      // TODO (lian) This is just a workaround to make the Simba ODBC driver work.
      // Should remove this once we get the ODBC driver updated.
      if (k == "-v") {
        val hiveJars = Seq(
          "hive-exec-0.12.0.jar",
          "hive-service-0.12.0.jar",
          "hive-common-0.12.0.jar",
          "hive-hwi-0.12.0.jar",
          "hive-0.12.0.jar").mkString(":")

        Array(
          "system:java.class.path=" + hiveJars,
          "system:sun.java.command=shark.SharkServer2")
      }
      else {
        Array(s"$k=${context.getConf(k, "<undefined>")}")
      }

    // Query all key-value pairs that are set in the SQLConf of the context.
    case (None, None) =>
      context.getAllConfs.map { case (k, v) =>
        s"$k=$v"
      }.toSeq

    case _ =>
      throw new IllegalArgumentException()
  }

  def execute(): RDD[Row] = {
    val rows = sideEffectResult.map { line => new GenericRow(Array[Any](line)) }
    context.sparkContext.parallelize(rows, 1)
  }

  override def otherCopyArgs = context :: Nil
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
    logicalPlan: LogicalPlan, output: Seq[Attribute], extended: Boolean)(
    @transient context: SQLContext)
  extends LeafNode with Command {

  // Run through the optimizer to generate the physical plan.
  override protected[sql] lazy val sideEffectResult: Seq[String] = try {
    // TODO in Hive, the "extended" ExplainCommand prints the AST as well, and detailed properties.
    val queryExecution = context.executePlan(logicalPlan)
    val outputString = if (extended) queryExecution.toString else queryExecution.simpleString

    outputString.split("\n")
  } catch { case cause: TreeNodeException[_] =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n")
  }

  def execute(): RDD[Row] = {
    val explanation = sideEffectResult.map(row => new GenericRow(Array[Any](row)))
    context.sparkContext.parallelize(explanation, 1)
  }

  override def otherCopyArgs = context :: Nil
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class CacheCommand(tableName: String, doCache: Boolean)(@transient context: SQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult = {
    if (doCache) {
      context.cacheTable(tableName)
    } else {
      context.uncacheTable(tableName)
    }
    Seq.empty[Any]
  }

  override def execute(): RDD[Row] = {
    sideEffectResult
    context.emptyResult
  }

  override def output: Seq[Attribute] = Seq.empty
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class DescribeCommand(child: SparkPlan, output: Seq[Attribute])(
    @transient context: SQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult: Seq[(String, String, String)] = {
    Seq(("# Registered as a temporary table", null, null)) ++
      child.output.map(field => (field.name, field.dataType.toString, null))
  }

  override def execute(): RDD[Row] = {
    val rows = sideEffectResult.map {
      case (name, dataType, comment) => new GenericRow(Array[Any](name, dataType, comment))
    }
    context.sparkContext.parallelize(rows, 1)
  }
}
