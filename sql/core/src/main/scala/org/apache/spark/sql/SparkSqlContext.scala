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

package org.apache.spark.sql

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

import catalyst.analysis._
import catalyst.dsl
import catalyst.expressions._
import catalyst.optimizer.Optimizer
import catalyst.planning.QueryPlanner
import catalyst.plans.logical.{LogicalPlan, NativeCommand}
import catalyst.rules.RuleExecutor

import execution._

/**
 * The result of executing a query using SparkSQL.  This class acts as an RDD of the through
 * implicit conversions.  It also allows access to the executed plan for DML queries, similar to an
 * EXPLAIN command in standard SQL.
 */
case class ExecutedQuery(
    sql: String,
    logicalPlan: LogicalPlan,
    executedPlan: Option[SparkPlan],
    rdd: RDD[Row]) {

  def schema = logicalPlan.output

  override def toString() =
    s"$sql\n${executedPlan.map(p => s"=== Query Plan ===\n$p").getOrElse("")}"
}

object TestSqlContext
  extends SqlContext(new SparkContext("local", "TestSqlContext", new SparkConf()))

/**
 * The entry point for running relational queries using Spark.  Uses the provided spark context
 * to execute relational operators.
 */
class SqlContext(val sparkContext: SparkContext) extends Logging with dsl.ExpressionConversions {
  self =>

  protected[sql] lazy val catalog: Catalog = new SimpleCatalog
  protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, EmptyFunctionRegistry, caseSensitive = true)
  protected[sql] val optimizer = Optimizer
  protected[sql] val parser = new catalyst.SqlParser

  protected[sql] def parseSql(sql: String): LogicalPlan = parser(sql)
  protected[sql] def executeSql(sql: String): this.QueryExecution = executePlan(parseSql(sql))
  protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  implicit def logicalPlanToSparkQuery(plan: LogicalPlan) = executePlan(plan)

  implicit def logicalDsl(q: ExecutedQuery) = new DslLogicalPlan(q.logicalPlan)

  /** Allows the results of sql queries to be used as RDDs */
  implicit def toRdd(q: ExecutedQuery) = q.rdd

  // Expression implicits.  Copied from dsl package object.

  implicit class DslLogicalPlan(val logicalPlan: LogicalPlan) extends dsl.LogicalPlanFunctions

  /**
   * Implicitly adds a `registerAsTable` to RDDs of case classes and allows the Query DSL to be
   * used on them.
   */
  implicit class TableRdd[A <: Product: TypeTag](rdd: RDD[A]) extends dsl.LogicalPlanFunctions {
    def logicalPlan = SparkLogicalPlan(ExistingRdd.fromProductRdd(rdd))

    def registerAsTable(tableName: String) = {
      catalog.registerTable(None, tableName, logicalPlan)
    }
  }

  /**
   * Executes a SQL query using Spark, returning the result as an RDD as well as the plan used
   * for execution.
   */
  def sql(sqlText: String): ExecutedQuery = {
    val queryWorkflow = executeSql(sqlText)
    val executedPlan = queryWorkflow.analyzed match {
      case _: NativeCommand => None
      case other => Some(queryWorkflow.executedPlan)
    }
    ExecutedQuery(sqlText, queryWorkflow.analyzed, executedPlan, queryWorkflow.toRdd)
  }

  protected[sql] class SparkPlanner extends SparkStrategies {
    val sparkContext = self.sparkContext

    val strategies: Seq[Strategy] =
      TopK ::
      PartialAggregation ::
      SparkEquiInnerJoin ::
      BasicOperators ::
      CartesianProduct ::
      BroadcastNestedLoopJoin :: Nil
  }

  protected[sql] val planner = new SparkPlanner

  /**
   * Prepares a planned SparkPlan for execution by binding references to specific ordinals, and
   * inserting shuffle operations as needed.
   */
  protected[sql] object PrepareForExecution extends RuleExecutor[SparkPlan] {
    val batches =
      Batch("Add exchange", Once, AddExchange) ::
      Batch("Prepare Expressions", Once, new BindReferences[SparkPlan]) :: Nil
  }

  /**
   * The primary workflow for executing relational queries using Spark.  Designed to allow easy
   * access to the intermediate phases of query execution for developers.  Most users should
   * use [[ExecutedQuery]] to interact with query results.
   */
  protected abstract class QueryExecution {
    def logical: LogicalPlan

    lazy val analyzed = analyzer(logical)
    lazy val optimizedPlan = optimizer(analyzed)
    // TODO: Don't just pick the first one...
    lazy val sparkPlan = planner(optimizedPlan).next()
    lazy val executedPlan: SparkPlan = PrepareForExecution(sparkPlan)

    // TODO: We are loosing schema here.
    lazy val toRdd: RDD[Row] = executedPlan.execute().map(_.copy())

    protected def stringOrError[A](f: => A): String =
      try f.toString catch { case e: Throwable => e.toString }

    override def toString: String =
      s"""== Logical Plan ==
         |${stringOrError(analyzed)}
         |== Optimized Logical Plan
         |${stringOrError(optimizedPlan)}
         |== Physical Plan ==
         |${stringOrError(executedPlan)}
      """.stripMargin.trim

    /**
     * Runs the query after interposing operators that print the result of each intermediate step.
     */
    def debugExec() = DebugQuery(executedPlan).execute().collect()
  }
}
