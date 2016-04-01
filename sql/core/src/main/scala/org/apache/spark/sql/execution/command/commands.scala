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
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SQLContext}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.debug._
import org.apache.spark.sql.types._

/**
 * A logical command that is executed for its side-effects.  `RunnableCommand`s are
 * wrapped in `ExecutedCommand` during execution.
 */
private[sql] trait RunnableCommand extends LogicalPlan with logical.Command {
  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
  def run(sparkSession: SparkSession): Seq[Row]
}

/**
 * A physical operator that executes the run method of a `RunnableCommand` and
 * saves the result to prevent multiple executions.
 */
private[sql] case class ExecutedCommandExec(cmd: RunnableCommand) extends SparkPlan {
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

  override def output: Seq[Attribute] = cmd.output

  override def children: Seq[SparkPlan] = Nil

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  override def executeTake(limit: Int): Array[InternalRow] = sideEffectResult.take(limit).toArray

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
  }

  override def argString: String = cmd.toString
}


/**
 * An explain command for users to see how a command will be executed.
 *
 * Note that this command takes in a logical plan, runs the optimizer on the logical plan
 * (but do NOT actually execute it).
 *
 * {{{
 *   EXPLAIN (EXTENDED|CODEGEN) SELECT * FROM ...
 * }}}
 *
 * @param logicalPlan plan to explain
 * @param output output schema
 * @param extended whether to do extended explain or not
 * @param codegen whether to output generated code from whole-stage codegen or not
 */
case class ExplainCommand(
    logicalPlan: LogicalPlan,
    override val output: Seq[Attribute] =
      Seq(AttributeReference("plan", StringType, nullable = true)()),
    extended: Boolean = false,
    codegen: Boolean = false)
  extends RunnableCommand {

  // Run through the optimizer to generate the physical plan.
  override def run(sparkSession: SparkSession): Seq[Row] = try {
    val queryExecution = sparkSession.executePlan(logicalPlan)
    val outputString =
      if (codegen) {
        codegenString(queryExecution.executedPlan)
      } else if (extended) {
        queryExecution.toString
      } else {
        queryExecution.simpleString
      }
    Seq(Row(outputString))
  } catch { case cause: TreeNodeException[_] =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }
}

/**
 * A command for users to list the column names for a table. This function creates a
 * [[ShowColumnsCommand]] logical plan.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW COLUMNS (FROM | IN) table_identifier [(FROM | IN) database];
 * }}}
 */
case class ShowColumnsCommand(table: TableIdentifier) extends RunnableCommand {
  // The result of SHOW COLUMNS has one column called 'result'
  override val output: Seq[Attribute] = {
    AttributeReference("result", StringType, nullable = false)() :: Nil
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = sqlContext.sessionState.catalog.lookupRelation(table, None)
    relation.schema.fields.map { field =>
      Row(field.name)
    }
  }
}

/**
 * A command for users to list the partition names of a table. If the partition spec is specified,
 * partitions that match the spec are returned. [[AnalysisException]] exception is thrown under
 * the following conditions:
 *
 * 1. If the command is called for a non partitioned table.
 * 2. If the partition spec refers to the columns that are not defined as partitioning columns.
 *
 * This function creates a [[ShowPartitionsCommand]] logical plan
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW PARTITIONS [db_name.]table_name [PARTITION(partition_spec)]
 * }}}
 */
case class ShowPartitionsCommand(
    table: TableIdentifier,
    partitionSpec: Option[Map[String, String]]) extends RunnableCommand {
  // The result of SHOW PARTITIONS has one column called 'result'
  override val output: Seq[Attribute] = {
    AttributeReference("result", StringType, nullable = false)() :: Nil
  }

  /**
   * This function validates the partitioning spec by making sure all the referenced columns are
   * defined as partitioning columns in table definition. An AnalysisException exception is
   * thrown if the partitioning spec is invalid.
   */
  private def validatePartitionSpec(table: CatalogTable, spec: Map[String, String]): Unit = {
    if (!spec.keySet.forall(table.partitionColumns.map(_.name).contains)) {
      throw new AnalysisException(s"Partition spec ${spec.mkString("(", ", ", ")")} contains " +
        s"non-partition columns")
    }
  }

  /**
   * Validates and throws an [[AnalysisException]] exception under the following conditions:
   * 1. If the table is not partitioned.
   * 2. If it is a datasource table.
   * 3. If it is a view or index table.
   */
  private def checkRequirements(table: CatalogTable): Unit = {
    if (table.tableType == CatalogTableType.VIRTUAL_VIEW ||
      table.tableType == CatalogTableType.INDEX_TABLE) {
      throw new AnalysisException("Operation not allowed: view or index table")
    } else if (!DDLUtils.isTablePartitioned(table)) {
      throw new AnalysisException(s"Table ${table.qualifiedName} is not a partitioned table")
    } else if (DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException("Operation not allowed: datasource table")
    }
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.sessionState.catalog
    val db = table.database.getOrElse(catalog.getCurrentDatabase)
    if (catalog.isTemporaryTable(table)) {
      Seq.empty[Row]
    } else {
      val tab = catalog.getTable(table)
      checkRequirements(tab)
      val partNames = partitionSpec match {
        case None => catalog.getPartitionNames(db, table.identifier, -1.asInstanceOf[Short])
        case Some(spec) =>
          validatePartitionSpec(tab, spec)
          catalog.getPartitionNames(db, table.identifier, spec, -1.asInstanceOf[Short])
      }
      partNames.map { p => Row(p) }
    }
  }
}
