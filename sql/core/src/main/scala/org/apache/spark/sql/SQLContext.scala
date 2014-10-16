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
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.ExpressionConversions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.types.DataType
import org.apache.spark.sql.columnar.InMemoryRelation
import org.apache.spark.sql.execution.{SparkStrategies, _}
import org.apache.spark.sql.json._
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
  extends org.apache.spark.Logging
  with SQLConf
  with CacheManager
  with ExpressionConversions
  with UDFRegistration
  with Serializable {

  self =>

  @transient
  protected[sql] lazy val catalog: Catalog = new SimpleCatalog(true)

  @transient
  protected[sql] lazy val functionRegistry: FunctionRegistry = new SimpleFunctionRegistry

  @transient
  protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, caseSensitive = true)

  @transient
  protected[sql] val optimizer = Optimizer

  @transient
  protected[sql] val sqlParser = {
    val fallback = new catalyst.SqlParser
    new catalyst.SparkSQLParser(fallback(_))
  }

  protected[sql] def parseSql(sql: String): LogicalPlan = sqlParser(sql)
  protected[sql] def executeSql(sql: String): this.QueryExecution = executePlan(parseSql(sql))
  protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  sparkContext.getConf.getAll.foreach {
    case (key, value) if key.startsWith("spark.sql") => setConf(key, value)
    case _ =>
  }

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
  implicit def createSchemaRDD[A <: Product: TypeTag](rdd: RDD[A]) = {
    SparkPlan.currentContext.set(self)
    new SchemaRDD(this,
      LogicalRDD(ScalaReflection.attributesFor[A], RDDConversions.productToRowRdd(rdd))(self))
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[SchemaRDD]] from an [[RDD]] containing [[Row]]s by applying a schema to this RDD.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   * Example:
   * {{{
   *  import org.apache.spark.sql._
   *  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   *
   *  val schema =
   *    StructType(
   *      StructField("name", StringType, false) ::
   *      StructField("age", IntegerType, true) :: Nil)
   *
   *  val people =
   *    sc.textFile("examples/src/main/resources/people.txt").map(
   *      _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *  val peopleSchemaRDD = sqlContext. applySchema(people, schema)
   *  peopleSchemaRDD.printSchema
   *  // root
   *  // |-- name: string (nullable = false)
   *  // |-- age: integer (nullable = true)
   *
   *    peopleSchemaRDD.registerTempTable("people")
   *  sqlContext.sql("select name from people").collect.foreach(println)
   * }}}
   *
   * @group userf
   */
  @DeveloperApi
  def applySchema(rowRDD: RDD[Row], schema: StructType): SchemaRDD = {
    // TODO: use MutableProjection when rowRDD is another SchemaRDD and the applied
    // schema differs from the existing schema on any field data type.
    val logicalPlan = LogicalRDD(schema.toAttributes, rowRDD)(self)
    new SchemaRDD(this, logicalPlan)
  }

  /**
   * Loads a Parquet file, returning the result as a [[SchemaRDD]].
   *
   * @group userf
   */
  def parquetFile(path: String): SchemaRDD =
    new SchemaRDD(this, parquet.ParquetRelation(path, Some(sparkContext.hadoopConfiguration), this))

  /**
   * Loads a JSON file (one object per line), returning the result as a [[SchemaRDD]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group userf
   */
  def jsonFile(path: String): SchemaRDD = jsonFile(path, 1.0)

  /**
   * :: Experimental ::
   * Loads a JSON file (one object per line) and applies the given schema,
   * returning the result as a [[SchemaRDD]].
   *
   * @group userf
   */
  @Experimental
  def jsonFile(path: String, schema: StructType): SchemaRDD = {
    val json = sparkContext.textFile(path)
    jsonRDD(json, schema)
  }

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
   * Loads an RDD[String] storing JSON objects (one object per record) and applies the given schema,
   * returning the result as a [[SchemaRDD]].
   *
   * @group userf
   */
  @Experimental
  def jsonRDD(json: RDD[String], schema: StructType): SchemaRDD = {
    val columnNameOfCorruptJsonRecord = columnNameOfCorruptRecord
    val appliedSchema =
      Option(schema).getOrElse(
        JsonRDD.nullTypeToStringType(
          JsonRDD.inferSchema(json, 1.0, columnNameOfCorruptJsonRecord)))
    val rowRDD = JsonRDD.jsonStringToRow(json, appliedSchema, columnNameOfCorruptJsonRecord)
    applySchema(rowRDD, appliedSchema)
  }

  /**
   * :: Experimental ::
   */
  @Experimental
  def jsonRDD(json: RDD[String], samplingRatio: Double): SchemaRDD = {
    val columnNameOfCorruptJsonRecord = columnNameOfCorruptRecord
    val appliedSchema =
      JsonRDD.nullTypeToStringType(
        JsonRDD.inferSchema(json, samplingRatio, columnNameOfCorruptJsonRecord))
    val rowRDD = JsonRDD.jsonStringToRow(json, appliedSchema, columnNameOfCorruptJsonRecord)
    applySchema(rowRDD, appliedSchema)
  }

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
   *   createParquetFile[Person]("path/to/file.parquet").registerTempTable("people")
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
      ParquetRelation.createEmpty(
        path, ScalaReflection.attributesFor[A], allowExisting, conf, this))
  }

  /**
   * Registers the given RDD as a temporary table in the catalog.  Temporary tables exist only
   * during the lifetime of this instance of SQLContext.
   *
   * @group userf
   */
  def registerRDDAsTable(rdd: SchemaRDD, tableName: String): Unit = {
    catalog.registerTable(None, tableName, rdd.queryExecution.logical)
  }

  /**
   * Executes a SQL query using Spark, returning the result as a SchemaRDD.  The dialect that is
   * used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
   * @group userf
   */
  def sql(sqlText: String): SchemaRDD = {
    if (dialect == "sql") {
      new SchemaRDD(this, parseSql(sqlText))
    } else {
      sys.error(s"Unsupported SQL dialect: $dialect")
    }
  }

  /** Returns the specified table as a SchemaRDD */
  def table(tableName: String): SchemaRDD =
    new SchemaRDD(this, catalog.lookupRelation(None, tableName))

  protected[sql] class SparkPlanner extends SparkStrategies {
    val sparkContext: SparkContext = self.sparkContext

    val sqlContext: SQLContext = self

    def codegenEnabled = self.codegenEnabled

    def numPartitions = self.numShufflePartitions

    val strategies: Seq[Strategy] =
      CommandStrategy(self) ::
      TakeOrdered ::
      HashAggregation ::
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

      val projectSet = AttributeSet(projectList.flatMap(_.references))
      val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
      val filterCondition = prunePushedDownFilters(filterPredicates).reduceLeftOption(And)

      // Right now we still use a projection even if the only evaluation is applying an alias
      // to a column.  Since this is a no-op, it could be avoided. However, using this
      // optimization with the current implementation would change the output schema.
      // TODO: Decouple final output schema from expression evaluation so this copy can be
      // avoided safely.

      if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
          filterSet.subsetOf(projectSet)) {
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
   * Prepares a planned SparkPlan for execution by inserting shuffle operations as needed.
   */
  @transient
  protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches =
      Batch("Add exchange", Once, AddExchange(self)) :: Nil
  }

  /**
   * :: DeveloperApi ::
   * The primary workflow for executing relational queries using Spark.  Designed to allow easy
   * access to the intermediate phases of query execution for developers.
   */
  @DeveloperApi
  protected abstract class QueryExecution {
    def logical: LogicalPlan

    lazy val analyzed = ExtractPythonUdfs(analyzer(logical))
    lazy val optimizedPlan = optimizer(analyzed)
    lazy val withCachedData = useCachedData(optimizedPlan)

    // TODO: Don't just pick the first one...
    lazy val sparkPlan = {
      SparkPlan.currentContext.set(self)
      planner(withCachedData).next()
    }
    // executedPlan should not be used to initialize any SparkPlan. It should be
    // only used for execution.
    lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

    /** Internal version of the RDD. Avoids copies and has no schema */
    lazy val toRdd: RDD[Row] = executedPlan.execute()

    protected def stringOrError[A](f: => A): String =
      try f.toString catch { case e: Throwable => e.toString }

    def simpleString: String =
      s"""== Physical Plan ==
         |${stringOrError(executedPlan)}
      """.stripMargin.trim

    override def toString: String =
      // TODO previously will output RDD details by run (${stringOrError(toRdd.toDebugString)})
      // however, the `toRdd` will cause the real execution, which is not what we want.
      // We need to think about how to avoid the side effect.
      s"""== Parsed Logical Plan ==
         |${stringOrError(logical)}
         |== Analyzed Logical Plan ==
         |${stringOrError(analyzed)}
         |== Optimized Logical Plan ==
         |${stringOrError(optimizedPlan)}
         |== Physical Plan ==
         |${stringOrError(executedPlan)}
         |Code Generation: ${executedPlan.codegenEnabled}
         |== RDD ==
      """.stripMargin.trim
  }

  /**
   * Parses the data type in our internal string representation. The data type string should
   * have the same format as the one generated by `toString` in scala.
   * It is only used by PySpark.
   */
  private[sql] def parseDataType(dataTypeString: String): DataType = {
    DataType.fromJson(dataTypeString)
  }

  /**
   * Apply a schema defined by the schemaString to an RDD. It is only used by PySpark.
   */
  private[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schemaString: String): SchemaRDD = {
    val schema = parseDataType(schemaString).asInstanceOf[StructType]
    applySchemaToPythonRDD(rdd, schema)
  }

  /**
   * Apply a schema defined by the schema to an RDD. It is only used by PySpark.
   */
  private[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schema: StructType): SchemaRDD = {
    import scala.collection.JavaConversions._

    def needsConversion(dataType: DataType): Boolean = dataType match {
      case ByteType => true
      case ShortType => true
      case FloatType => true
      case TimestampType => true
      case ArrayType(_, _) => true
      case MapType(_, _, _) => true
      case StructType(_) => true
      case other => false
    }

    // Converts value to the type specified by the data type.
    // Because Python does not have data types for TimestampType, FloatType, ShortType, and
    // ByteType, we need to explicitly convert values in columns of these data types to the desired
    // JVM data types.
    def convert(obj: Any, dataType: DataType): Any = (obj, dataType) match {
      // TODO: We should check nullable
      case (null, _) => null

      case (c: java.util.List[_], ArrayType(elementType, _)) =>
        c.map { e => convert(e, elementType)}: Seq[Any]

      case (c, ArrayType(elementType, _)) if c.getClass.isArray =>
        c.asInstanceOf[Array[_]].map(e => convert(e, elementType)): Seq[Any]

      case (c: java.util.Map[_, _], MapType(keyType, valueType, _)) => c.map {
          case (key, value) => (convert(key, keyType), convert(value, valueType))
        }.toMap

      case (c, StructType(fields)) if c.getClass.isArray =>
        new GenericRow(c.asInstanceOf[Array[_]].zip(fields).map {
          case (e, f) => convert(e, f.dataType)
        }): Row

      case (c: java.util.Calendar, TimestampType) =>
        new java.sql.Timestamp(c.getTime().getTime())

      case (c: Int, ByteType) => c.toByte
      case (c: Long, ByteType) => c.toByte
      case (c: Int, ShortType) => c.toShort
      case (c: Long, ShortType) => c.toShort
      case (c: Long, IntegerType) => c.toInt
      case (c: Double, FloatType) => c.toFloat
      case (c, StringType) if !c.isInstanceOf[String] => c.toString

      case (c, _) => c
    }

    val convertedRdd = if (schema.fields.exists(f => needsConversion(f.dataType))) {
      rdd.map(m => m.zip(schema.fields).map {
        case (value, field) => convert(value, field.dataType)
      })
    } else {
      rdd
    }

    val rowRdd = convertedRdd.mapPartitions { iter =>
      iter.map { m => new GenericRow(m): Row}
    }

    new SchemaRDD(this, LogicalRDD(schema.toAttributes, rowRdd)(self))
  }
}
