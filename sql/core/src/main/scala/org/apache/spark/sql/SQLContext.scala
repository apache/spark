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

import org.apache.spark.annotation.{AlphaComponent, DeveloperApi, Experimental}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.dsl.ExpressionConversions
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.columnar.InMemoryRelation
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.SparkStrategies
import org.apache.spark.sql.json._
import org.apache.spark.sql.parquet.ParquetRelation
import org.apache.spark.SparkContext

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
  with ExpressionConversions
  with Serializable {

  self =>

  @transient
  protected[sql] lazy val catalog: Catalog = new SimpleCatalog(true)
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
    new SchemaRDD(this, parquet.ParquetRelation(path, Some(sparkContext.hadoopConfiguration)))

  /**
   * Loads a JSON file (one object per line), returning the result as a [[SchemaRDD]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group userf
   */
  def jsonFile(path: String): SchemaRDD = jsonFile(path, 1.0)

  /**
   * :: Experimental ::
   */
  @Experimental
  def jsonFile(path: String, samplingRatio: Double): SchemaRDD = {
    val json = sparkContext.textFile(path)
    jsonRDD(json, samplingRatio)
  }

  /**
   * Loads an RDD[String] storing JSON objects (one object per record), returning the result as a
   * [[SchemaRDD]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group userf
   */
  def jsonRDD(json: RDD[String]): SchemaRDD = jsonRDD(json, 1.0)

  /**
   * :: Experimental ::
   */
  @Experimental
  def jsonRDD(json: RDD[String], samplingRatio: Double): SchemaRDD =
    new SchemaRDD(this, JsonRDD.inferSchema(json, samplingRatio))

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
    val name = tableName
    val newPlan = rdd.logicalPlan transform {
      case s @ SparkLogicalPlan(ExistingRdd(_, _), _) => s.copy(tableName = name)
    }
    catalog.registerTable(None, tableName, newPlan)
  }

  /**
   * Executes a SQL query using Spark, returning the result as a SchemaRDD.
   *
   * @group userf
   */
  def sql(sqlText: String): SchemaRDD = new SchemaRDD(this, parseSql(sqlText))

  /** Returns the specified table as a SchemaRDD */
  def table(tableName: String): SchemaRDD =
    new SchemaRDD(this, catalog.lookupRelation(None, tableName))

  /** Caches the specified table in-memory. */
  def cacheTable(tableName: String): Unit = {
    val currentTable = table(tableName).queryExecution.analyzed
    val asInMemoryRelation = currentTable match {
      case _: InMemoryRelation =>
        currentTable.logicalPlan

      case _ =>
        val useCompression =
          sparkContext.conf.getBoolean("spark.sql.inMemoryColumnarStorage.compressed", false)
        InMemoryRelation(useCompression, executePlan(currentTable).executedPlan)
    }

    catalog.registerTable(None, tableName, asInMemoryRelation)
  }

  /** Removes the specified table from the in-memory cache. */
  def uncacheTable(tableName: String): Unit = {
    table(tableName).queryExecution.analyzed match {
      // This is kind of a hack to make sure that if this was just an RDD registered as a table,
      // we reregister the RDD as a table.
      case inMem @ InMemoryRelation(_, _, e: ExistingRdd) =>
        inMem.cachedColumnBuffers.unpersist()
        catalog.unregisterTable(None, tableName)
        catalog.registerTable(None, tableName, SparkLogicalPlan(e))
      case inMem: InMemoryRelation =>
        inMem.cachedColumnBuffers.unpersist()
        catalog.unregisterTable(None, tableName)
      case plan => throw new IllegalArgumentException(s"Table $tableName is not cached: $plan")
    }
  }

  /** Returns true if the table is currently cached in-memory. */
  def isCached(tableName: String): Boolean = {
    val relation = table(tableName).queryExecution.analyzed
    relation match {
      case _: InMemoryRelation => true
      case _ => false
    }
  }

  protected[sql] class SparkPlanner extends SparkStrategies {
    val sparkContext: SparkContext = self.sparkContext

    val sqlContext: SQLContext = self

    def numPartitions = self.numShufflePartitions

    val strategies: Seq[Strategy] =
      CommandStrategy(self) ::
      TakeOrdered ::
      PartialAggregation ::
      LeftSemiJoin ::
      HashJoin ::
      InMemoryScans ::
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
  protected[sql] lazy val emptyResult = sparkContext.parallelize(Seq.empty[Row], 1)

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

    lazy val analyzed = analyzer(logical)
    lazy val optimizedPlan = optimizer(analyzed)
    // TODO: Don't just pick the first one...
    lazy val sparkPlan = planner(optimizedPlan).next()
    // executedPlan should not be used to initialize any SparkPlan. It should be
    // only used for execution.
    lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

    /** Internal version of the RDD. Avoids copies and has no schema */
    lazy val toRdd: RDD[Row] = executedPlan.execute()

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
   * TODO: consolidate this with the type system developed in SPARK-2060.
   */
  private[sql] def inferSchema(rdd: RDD[Map[String, _]]): SchemaRDD = {
    import scala.collection.JavaConversions._
    def typeFor(obj: Any): DataType = obj match {
      case c: java.lang.String => StringType
      case c: java.lang.Integer => IntegerType
      case c: java.lang.Long => LongType
      case c: java.lang.Double => DoubleType
      case c: java.lang.Boolean => BooleanType
      case c: java.util.List[_] => ArrayType(typeFor(c.head))
      case c: java.util.Set[_] => ArrayType(typeFor(c.head))
      case c: java.util.Map[_, _] =>
        val (key, value) = c.head
        MapType(typeFor(key), typeFor(value))
      case c if c.getClass.isArray =>
        val elem = c.asInstanceOf[Array[_]].head
        ArrayType(typeFor(elem))
      case c => throw new Exception(s"Object of type $c cannot be used")
    }
    val schema = rdd.first().map { case (fieldName, obj) =>
      AttributeReference(fieldName, typeFor(obj), true)()
    }.toSeq

    val rowRdd = rdd.mapPartitions { iter =>
      iter.map { map =>
        new GenericRow(map.values.toArray.asInstanceOf[Array[Any]]): Row
      }
    }
    new SchemaRDD(this, SparkLogicalPlan(ExistingRdd(schema, rowRdd)))
  }

}
