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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{AlphaComponent, DeveloperApi, Experimental}
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.{ScalaReflection, dsl}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{SetCommand, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

import org.apache.spark.sql.columnar.InMemoryColumnarTableScan

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.SparkStrategies

import org.apache.spark.sql.parquet.ParquetRelation

/**
 * :: AlphaComponent ::
 * The entry point for running relational queries using Spark.  Allows the creation of [[SchemaRDD]]
 * objects and the execution of SQL queries.
 *
 * @groupname userf Spark SQL Functions
 * @groupname Ungrouped Support functions for language integrated queries.
 */
@AlphaComponent
class SQLContext(@transient val sparkContext: SparkContext)
  extends Logging
  with SQLConf
  with dsl.ExpressionConversions
  with Serializable {

  self =>

  @transient
  protected[sql] lazy val catalog: Catalog = new SimpleCatalog
  @transient
  protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, EmptyFunctionRegistry, caseSensitive = true)
  @transient
  protected[sql] val optimizer = Optimizer
  @transient
  protected[sql] val parser = new catalyst.SqlParser

  protected[sql] def parseSql(sql: String): LogicalPlan = parser(sql)
  protected[sql] def executeSql(sql: String): this.QueryExecution = executePlan(parseSql(sql))
  protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  /**
   * :: DeveloperApi ::
   * Allows catalyst LogicalPlans to be executed as a SchemaRDD.  Note that the LogicalPlan
   * interface is considered internal, and thus not guaranteed to be stable.  As a result, using
   * them directly is not recommended.
   */
  @DeveloperApi
  implicit def logicalPlanToSparkQuery(plan: LogicalPlan): SchemaRDD = new SchemaRDD(this, plan)

  /**
   * Creates a SchemaRDD from an RDD of case classes.
   *
   * @group userf
   */
  implicit def createSchemaRDD[A <: Product: TypeTag](rdd: RDD[A]) =
    new SchemaRDD(this, SparkLogicalPlan(ExistingRdd.fromProductRdd(rdd)))

  /**
   * Loads a Parquet file, returning the result as a [[SchemaRDD]].
   *
   * @group userf
   */
  def parquetFile(path: String): SchemaRDD =
    new SchemaRDD(this, parquet.ParquetRelation(path))

  /**
   * :: Experimental ::
   * Creates an empty parquet file with the schema of class `A`, which can be registered as a table.
   * This registered table can be used as the target of future `insertInto` operations.
   *
   * {{{
   *   val sqlContext = new SQLContext(...)
   *   import sqlContext._
   *
   *   case class Person(name: String, age: Int)
   *   createParquetFile[Person]("path/to/file.parquet").registerAsTable("people")
   *   sql("INSERT INTO people SELECT 'michael', 29")
   * }}}
   *
   * @tparam A A case class type that describes the desired schema of the parquet file to be
   *           created.
   * @param path The path where the directory containing parquet metadata should be created.
   *             Data inserted into this table will also be stored at this location.
   * @param allowExisting When false, an exception will be thrown if this directory already exists.
   * @param conf A Hadoop configuration object that can be used to specify options to the parquet
   *             output format.
   *
   * @group userf
   */
  @Experimental
  def createParquetFile[A <: Product : TypeTag](
      path: String,
      allowExisting: Boolean = true,
      conf: Configuration = new Configuration()): SchemaRDD = {
    new SchemaRDD(
      this,
      ParquetRelation.createEmpty(path, ScalaReflection.attributesFor[A], allowExisting, conf))
  }

  /**
   * Registers the given RDD as a temporary table in the catalog.  Temporary tables exist only
   * during the lifetime of this instance of SQLContext.
   *
   * @group userf
   */
  def registerRDDAsTable(rdd: SchemaRDD, tableName: String): Unit = {
    catalog.registerTable(None, tableName, rdd.logicalPlan)
  }

  /**
   * Executes a SQL query using Spark, returning the result as a SchemaRDD.
   *
   * @group userf
   */
  def sql(sqlText: String): SchemaRDD = {
    val result = new SchemaRDD(this, parseSql(sqlText))
    // We force query optimization to happen right away instead of letting it happen lazily like
    // when using the query DSL.  This is so DDL commands behave as expected.  This is only
    // generates the RDD lineage for DML queries, but do not perform any execution.
    result.queryExecution.toRdd
    result
  }

  /** Returns the specified table as a SchemaRDD */
  def table(tableName: String): SchemaRDD =
    new SchemaRDD(this, catalog.lookupRelation(None, tableName))

  /** Caches the specified table in-memory. */
  def cacheTable(tableName: String): Unit = {
    val currentTable = catalog.lookupRelation(None, tableName)
    val useCompression =
      sparkContext.conf.getBoolean("spark.sql.inMemoryColumnarStorage.compressed", false)
    val asInMemoryRelation =
      InMemoryColumnarTableScan(
        currentTable.output, executePlan(currentTable).executedPlan, useCompression)

    catalog.registerTable(None, tableName, SparkLogicalPlan(asInMemoryRelation))
  }

  /** Removes the specified table from the in-memory cache. */
  def uncacheTable(tableName: String): Unit = {
    EliminateAnalysisOperators(catalog.lookupRelation(None, tableName)) match {
      // This is kind of a hack to make sure that if this was just an RDD registered as a table,
      // we reregister the RDD as a table.
      case SparkLogicalPlan(inMem @ InMemoryColumnarTableScan(_, e: ExistingRdd, _)) =>
        inMem.cachedColumnBuffers.unpersist()
        catalog.unregisterTable(None, tableName)
        catalog.registerTable(None, tableName, SparkLogicalPlan(e))
      case SparkLogicalPlan(inMem: InMemoryColumnarTableScan) =>
        inMem.cachedColumnBuffers.unpersist()
        catalog.unregisterTable(None, tableName)
      case plan => throw new IllegalArgumentException(s"Table $tableName is not cached: $plan")
    }
  }

  protected[sql] class SparkPlanner extends SparkStrategies {
    val sparkContext = self.sparkContext

    def numPartitions = self.numShufflePartitions

    val strategies: Seq[Strategy] =
      CommandStrategy(self) ::
      TakeOrdered ::
      PartialAggregation ::
      LeftSemiJoin ::
      HashJoin ::
      ParquetOperations ::
      BasicOperators ::
      CartesianProduct ::
      BroadcastNestedLoopJoin :: Nil

    /**
     * Used to build table scan operators where complex projection and filtering are done using
     * separate physical operators.  This function returns the given scan operator with Project and
     * Filter nodes added only when needed.  For example, a Project operator is only used when the
     * final desired output requires complex expressions to be evaluated or when columns can be
     * further eliminated out after filtering has been done.
     *
     * The `prunePushedDownFilters` parameter is used to remove those filters that can be optimized
     * away by the filter pushdown optimization.
     *
     * The required attributes for both filtering and expression evaluation are passed to the
     * provided `scanBuilder` function so that it can avoid unnecessary column materialization.
     */
    def pruneFilterProject(
        projectList: Seq[NamedExpression],
        filterPredicates: Seq[Expression],
        prunePushedDownFilters: Seq[Expression] => Seq[Expression],
        scanBuilder: Seq[Attribute] => SparkPlan): SparkPlan = {

      val projectSet = projectList.flatMap(_.references).toSet
      val filterSet = filterPredicates.flatMap(_.references).toSet
      val filterCondition = prunePushedDownFilters(filterPredicates).reduceLeftOption(And)

      // Right now we still use a projection even if the only evaluation is applying an alias
      // to a column.  Since this is a no-op, it could be avoided. However, using this
      // optimization with the current implementation would change the output schema.
      // TODO: Decouple final output schema from expression evaluation so this copy can be
      // avoided safely.

      if (projectList.toSet == projectSet && filterSet.subsetOf(projectSet)) {
        // When it is possible to just use column pruning to get the right projection and
        // when the columns of this projection are enough to evaluate all filter conditions,
        // just do a scan followed by a filter, with no extra project.
        val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
        filterCondition.map(Filter(_, scan)).getOrElse(scan)
      } else {
        val scan = scanBuilder((projectSet ++ filterSet).toSeq)
        Project(projectList, filterCondition.map(Filter(_, scan)).getOrElse(scan))
      }
    }
  }

  @transient
  protected[sql] val planner = new SparkPlanner

  @transient
  protected[sql] lazy val emptyResult =
    sparkContext.parallelize(Seq(new GenericRow(Array[Any]()): Row), 1)

  /**
   * Prepares a planned SparkPlan for execution by binding references to specific ordinals, and
   * inserting shuffle operations as needed.
   */
  @transient
  protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches =
      Batch("Add exchange", Once, AddExchange(self)) ::
      Batch("Prepare Expressions", Once, new BindReferences[SparkPlan]) :: Nil
  }

  /**
   * The primary workflow for executing relational queries using Spark.  Designed to allow easy
   * access to the intermediate phases of query execution for developers.
   */
  protected abstract class QueryExecution {
    def logical: LogicalPlan

    def eagerlyProcess(plan: LogicalPlan): RDD[Row] = plan match {
      case SetCommand(key, value) =>
        // Only this case needs to be executed eagerly. The other cases will
        // be taken care of when the actual results are being extracted.
        // In the case of HiveContext, sqlConf is overridden to also pass the
        // pair into its HiveConf.
        if (key.isDefined && value.isDefined) {
          set(key.get, value.get)
        }
        // It doesn't matter what we return here, since this is only used
        // to force the evaluation to happen eagerly.  To query the results,
        // one must use SchemaRDD operations to extract them.
        emptyResult
      case _ => executedPlan.execute()
    }

    lazy val analyzed = analyzer(logical)
    lazy val optimizedPlan = optimizer(analyzed)
    // TODO: Don't just pick the first one...
    lazy val sparkPlan = planner(optimizedPlan).next()
    lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

    /** Internal version of the RDD. Avoids copies and has no schema */
    lazy val toRdd: RDD[Row] = {
      logical match {
        case s: SetCommand => eagerlyProcess(s)
        case _ => executedPlan.execute()
      }
    }

    protected def stringOrError[A](f: => A): String =
      try f.toString catch { case e: Throwable => e.toString }

    def simpleString: String = stringOrError(executedPlan)

    override def toString: String =
      s"""== Logical Plan ==
         |${stringOrError(analyzed)}
         |== Optimized Logical Plan ==
         |${stringOrError(optimizedPlan)}
         |== Physical Plan ==
         |${stringOrError(executedPlan)}
      """.stripMargin.trim
  }

  /**
   * Peek at the first row of the RDD and infer its schema.
   * TODO: We only support primitive types, add support for nested types.
   */
  private[sql] def inferSchema(rdd: RDD[Map[String, _]]): SchemaRDD = {
    val schema = rdd.first.map { case (fieldName, obj) =>
      val dataType = obj.getClass match {
        case c: Class[_] if c == classOf[java.lang.String] => StringType
        case c: Class[_] if c == classOf[java.lang.Integer] => IntegerType
        case c: Class[_] if c == classOf[java.lang.Long] => LongType
        case c: Class[_] if c == classOf[java.lang.Double] => DoubleType
        case c: Class[_] if c == classOf[java.lang.Boolean] => BooleanType
        case c => throw new Exception(s"Object of type $c cannot be used")
      }
      AttributeReference(fieldName, dataType, true)()
    }.toSeq

    val rowRdd = rdd.mapPartitions { iter =>
      iter.map { map =>
        new GenericRow(map.values.toArray.asInstanceOf[Array[Any]]): Row
      }
    }
    new SchemaRDD(this, SparkLogicalPlan(ExistingRdd(schema, rowRdd)))
  }

}
