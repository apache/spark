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
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, NativeCommand, WriteToFile}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution._

/** A SQLContext that can be used for local testing. */
object TestSQLContext
  extends SQLContext(new SparkContext("local", "TestSQLContext", new SparkConf()))

/**
 * <span class="badge" style="float: right; background-color: darkblue;">ALPHA COMPONENT</span>
 *
 * The entry point for running relational queries using Spark.  Allows the creation of [[SchemaRDD]]
 * objects and the execution of SQL queries.
 *
 * @groupname userf Spark SQL Functions
 * @groupname Ungrouped Support functions for language integrated queries.
 */
class SQLContext(val sparkContext: SparkContext) extends Logging with dsl.ExpressionConversions {
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

  implicit class DslLogicalPlan(val logicalPlan: LogicalPlan) extends dsl.LogicalPlanFunctions {
    def registerAsTable(tableName: String): Unit = {
       catalog.registerTable(None, tableName, logicalPlan)
     }
  }

  /**
   * Creates a SchemaRDD from an RDD of case classes.
   *
   * @group userf
   */
  implicit def createSchemaRDD[A <: Product: TypeTag](rdd: RDD[A]) =
    new SchemaRDD(this, SparkLogicalPlan(ExistingRdd.fromProductRdd(rdd)))

  /**
   * Loads a parequet file, returning the result as a [[SchemaRDD]].
   *
   * @group userf
   */
  def parquetFile(path: String): SchemaRDD =
    new SchemaRDD(this, parquet.ParquetRelation("ParquetFile", path))

  /**
   * Writes the given [[SchemaRDD]] to a file using Parquet.
   *
   * @group userf
   */
  def saveRDDAsParquetFile(rdd: SchemaRDD, path: String): Unit = {
    rdd.saveAsParquetFile(path)
  }

  /**
   * Registers the given RDD as a table in the catalog.
   *
   * @group userf
   */
  def registerRDDAsTable(rdd: SchemaRDD, tableName: String): Unit = {
    catalog.registerTable(None, tableName, rdd.logicalPlan)
  }

  /**
   * Executes a SQL query using Spark, returning the result as an RDD as well as the plan used
   * for execution.
   *
   * @group userf
   */
  def sql(sqlText: String): SchemaRDD = {
    new SchemaRDD(this, parseSql(sqlText))
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
   * access to the intermediate phases of query execution for developers.
   */
  protected abstract class QueryExecution {
    def logical: LogicalPlan

    lazy val analyzed = analyzer(logical)
    lazy val optimizedPlan = optimizer(analyzed)
    // TODO: Don't just pick the first one...
    lazy val sparkPlan = planner(optimizedPlan).next()
    lazy val executedPlan: SparkPlan = PrepareForExecution(sparkPlan)

    /** Internal version of the RDD. Avoids copies and has no schema */
    lazy val toRdd: RDD[Row] = executedPlan.execute()

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
