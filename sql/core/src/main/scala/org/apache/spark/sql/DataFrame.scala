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

import java.util.{List => JList}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.collection.JavaConversions._

import com.fasterxml.jackson.core.JsonFactory

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{LogicalRDD, EvaluatePython}
import org.apache.spark.sql.json.JsonRDD
import org.apache.spark.sql.types.{NumericType, StructType}
import org.apache.spark.util.Utils


/**
 * A collection of rows that have the same columns.
 *
 * A [[DataFrame]] is equivalent to a relational table in Spark SQL, and can be created using
 * various functions in [[SQLContext]].
 * {{{
 *   val people = sqlContext.parquetFile("...")
 * }}}
 *
 * Once created, it can be manipulated using the various domain-specific-language (DSL) functions
 * defined in: [[DataFrame]] (this class), [[Column]], [[Dsl]] for the DSL.
 *
 * To select a column from the data frame, use the apply method:
 * {{{
 *   val ageCol = people("age")  // in Scala
 *   Column ageCol = people.apply("age")  // in Java
 * }}}
 *
 * Note that the [[Column]] type can also be manipulated through its various functions.
 * {{
 *   // The following creates a new column that increases everybody's age by 10.
 *   people("age") + 10  // in Scala
 * }}
 *
 * A more concrete example:
 * {{{
 *   // To create DataFrame using SQLContext
 *   val people = sqlContext.parquetFile("...")
 *   val department = sqlContext.parquetFile("...")
 *
 *   people.filter("age" > 30)
 *     .join(department, people("deptId") === department("id"))
 *     .groupBy(department("name"), "gender")
 *     .agg(avg(people("salary")), max(people("age")))
 * }}}
 */
// TODO: Improve documentation.
class DataFrame protected[sql](
    val sqlContext: SQLContext,
    private val baseLogicalPlan: LogicalPlan,
    operatorsEnabled: Boolean)
  extends DataFrameSpecificApi with RDDApi[Row] {

  protected[sql] def this(sqlContext: Option[SQLContext], plan: Option[LogicalPlan]) =
    this(sqlContext.orNull, plan.orNull, sqlContext.isDefined && plan.isDefined)

  protected[sql] def this(sqlContext: SQLContext, plan: LogicalPlan) = this(sqlContext, plan, true)

  @transient protected[sql] lazy val queryExecution = sqlContext.executePlan(baseLogicalPlan)

  @transient protected[sql] val logicalPlan: LogicalPlan = baseLogicalPlan match {
    // For various commands (like DDL) and queries with side effects, we force query optimization to
    // happen right away to let these side effects take place eagerly.
    case _: Command | _: InsertIntoTable | _: CreateTableAsSelect[_] |_: WriteToFile =>
      LogicalRDD(queryExecution.analyzed.output, queryExecution.toRdd)(sqlContext)
    case _ =>
      baseLogicalPlan
  }

  /**
   * An implicit conversion function internal to this class for us to avoid doing
   * "new DataFrame(...)" everywhere.
   */
  private implicit def logicalPlanToDataFrame(logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan, true)
  }

  /** Returns the list of numeric columns, useful for doing aggregation. */
  protected[sql] def numericColumns: Seq[Expression] = {
    schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map { n =>
      logicalPlan.resolve(n.name, sqlContext.analyzer.resolver).get
    }
  }

  /** Resolves a column name into a Catalyst [[NamedExpression]]. */
  protected[sql] def resolve(colName: String): NamedExpression = {
    logicalPlan.resolve(colName, sqlContext.analyzer.resolver).getOrElse(throw new RuntimeException(
      s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})"""))
  }

  /** Left here for compatibility reasons. */
  @deprecated("1.3.0", "use toDataFrame")
  def toSchemaRDD: DataFrame = this

  /**
   * Returns the object itself. Used to force an implicit conversion from RDD to DataFrame in Scala.
   */
  def toDataFrame: DataFrame = this

  /**
   * Returns a new [[DataFrame]] with columns renamed. This can be quite convenient in conversion
   * from a RDD of tuples into a [[DataFrame]] with meaningful names. For example:
   * {{{
   *   val rdd: RDD[(Int, String)] = ...
   *   rdd.toDataFrame  // this implicit conversion creates a DataFrame with column name _1 and _2
   *   rdd.toDataFrame("id", "name")  // this creates a DataFrame with column name "id" and "name"
   * }}}
   */
  @scala.annotation.varargs
  def toDataFrame(colName: String, colNames: String*): DataFrame = {
    val newNames = colName +: colNames
    require(schema.size == newNames.size,
      "The number of columns doesn't match.\n" +
      "Old column names: " + schema.fields.map(_.name).mkString(", ") + "\n" +
      "New column names: " + newNames.mkString(", "))

    val newCols = schema.fieldNames.zip(newNames).map { case (oldName, newName) =>
      apply(oldName).as(newName)
    }
    select(newCols :_*)
  }

  /** Returns the schema of this [[DataFrame]]. */
  override def schema: StructType = queryExecution.analyzed.schema

  /** Returns all column names and their data types as an array. */
  override def dtypes: Array[(String, String)] = schema.fields.map { field =>
    (field.name, field.dataType.toString)
  }

  /** Returns all column names as an array. */
  override def columns: Array[String] = schema.fields.map(_.name)

  /** Prints the schema to the console in a nice tree format. */
  override def printSchema(): Unit = println(schema.treeString)

  /**
   * Cartesian join with another [[DataFrame]].
   *
   * Note that cartesian joins are very expensive without an extra filter that can be pushed down.
   *
   * @param right Right side of the join operation.
   */
  override def join(right: DataFrame): DataFrame = {
    Join(logicalPlan, right.logicalPlan, joinType = Inner, None)
  }

  /**
   * Inner join with another [[DataFrame]], using the given join expression.
   *
   * {{{
   *   // The following two are equivalent:
   *   df1.join(df2, $"df1Key" === $"df2Key")
   *   df1.join(df2).where($"df1Key" === $"df2Key")
   * }}}
   */
  override def join(right: DataFrame, joinExprs: Column): DataFrame = {
    Join(logicalPlan, right.logicalPlan, Inner, Some(joinExprs.expr))
  }

  /**
   * Join with another [[DataFrame]], usin  g the given join expression. The following performs
   * a full outer join between `df1` and `df2`.
   *
   * {{{
   *   df1.join(df2, "outer", $"df1Key" === $"df2Key")
   * }}}
   *
   * @param right Right side of the join.
   * @param joinExprs Join expression.
   * @param joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `semijoin`.
   */
  override def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame = {
    Join(logicalPlan, right.logicalPlan, JoinType(joinType), Some(joinExprs.expr))
  }

  /**
   * Returns a new [[DataFrame]] sorted by the specified column, all in ascending order.
   * {{{
   *   // The following 3 are equivalent
   *   df.sort("sortcol")
   *   df.sort($"sortcol")
   *   df.sort($"sortcol".asc)
   * }}}
   */
  @scala.annotation.varargs
  override def sort(sortCol: String, sortCols: String*): DataFrame = {
    orderBy(apply(sortCol), sortCols.map(apply) :_*)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions. For example:
   * {{{
   *   df.sort($"col1", $"col2".desc)
   * }}}
   */
  @scala.annotation.varargs
  override def sort(sortExpr: Column, sortExprs: Column*): DataFrame = {
    val sortOrder: Seq[SortOrder] = (sortExpr +: sortExprs).map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    Sort(sortOrder, global = true, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
   * This is an alias of the `sort` function.
   */
  @scala.annotation.varargs
  override def orderBy(sortCol: String, sortCols: String*): DataFrame = {
    sort(sortCol, sortCols :_*)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
   * This is an alias of the `sort` function.
   */
  @scala.annotation.varargs
  override def orderBy(sortExpr: Column, sortExprs: Column*): DataFrame = {
    sort(sortExpr, sortExprs :_*)
  }

  /**
   * Selects column based on the column name and return it as a [[Column]].
   */
  override def apply(colName: String): Column = colName match {
    case "*" =>
      Column("*")
    case _ =>
      val expr = resolve(colName)
      new Column(Some(sqlContext), Some(Project(Seq(expr), logicalPlan)), expr)
  }

  /**
   * Selects a set of expressions, wrapped in a Product.
   * {{{
   *   // The following two are equivalent:
   *   df.apply(($"colA", $"colB" + 1))
   *   df.select($"colA", $"colB" + 1)
   * }}}
   */
  override def apply(projection: Product): DataFrame = {
    require(projection.productArity >= 1)
    select(projection.productIterator.map {
      case c: Column => c
      case o: Any => new Column(Some(sqlContext), None, Literal(o))
    }.toSeq :_*)
  }

  /**
   * Returns a new [[DataFrame]] with an alias set.
   */
  override def as(name: String): DataFrame = Subquery(name, logicalPlan)

  /**
   * Selects a set of expressions.
   * {{{
   *   df.select($"colA", $"colB" + 1)
   * }}}
   */
  @scala.annotation.varargs
  override def select(cols: Column*): DataFrame = {
    val exprs = cols.zipWithIndex.map {
      case (Column(expr: NamedExpression), _) =>
        expr
      case (Column(expr: Expression), _) =>
        Alias(expr, expr.toString)()
    }
    Project(exprs.toSeq, logicalPlan)
  }

  /**
   * Selects a set of columns. This is a variant of `select` that can only select
   * existing columns using column names (i.e. cannot construct expressions).
   *
   * {{{
   *   // The following two are equivalent:
   *   df.select("colA", "colB")
   *   df.select($"colA", $"colB")
   * }}}
   */
  @scala.annotation.varargs
  override def select(col: String, cols: String*): DataFrame = {
    select((col +: cols).map(new Column(_)) :_*)
  }

  /**
   * Filters rows using the given condition.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   *   peopleDf($"age" > 15)
   * }}}
   */
  override def filter(condition: Column): DataFrame = {
    Filter(condition.expr, logicalPlan)
  }

  /**
   * Filters rows using the given condition. This is an alias for `filter`.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   *   peopleDf($"age" > 15)
   * }}}
   */
  override def where(condition: Column): DataFrame = filter(condition)

  /**
   * Filters rows using the given condition. This is a shorthand meant for Scala.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   *   peopleDf($"age" > 15)
   * }}}
   */
  override def apply(condition: Column): DataFrame = filter(condition)

  /**
   * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
   * See [[GroupedDataFrame]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns grouped by department.
   *   df.groupBy($"department").avg()
   *
   *   // Compute the max age and average salary, grouped by department and gender.
   *   df.groupBy($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   */
  @scala.annotation.varargs
  override def groupBy(cols: Column*): GroupedDataFrame = {
    new GroupedDataFrame(this, cols.map(_.expr))
  }

  /**
   * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
   * See [[GroupedDataFrame]] for all the available aggregate functions.
   *
   * This is a variant of groupBy that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns grouped by department.
   *   df.groupBy("department").avg()
   *
   *   // Compute the max age and average salary, grouped by department and gender.
   *   df.groupBy($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   */
  @scala.annotation.varargs
  override def groupBy(col1: String, cols: String*): GroupedDataFrame = {
    val colNames: Seq[String] = col1 +: cols
    new GroupedDataFrame(this, colNames.map(colName => resolve(colName)))
  }

  /**
   * Aggregates on the entire [[DataFrame]] without groups.
   * {{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}
   */
  override def agg(exprs: Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * Aggregates on the entire [[DataFrame]] without groups.
   * {{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}
   */
  override def agg(exprs: java.util.Map[String, String]): DataFrame = agg(exprs.toMap)

  /**
   * Aggregates on the entire [[DataFrame]] without groups.
   * {{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(max($"age"), avg($"salary"))
   *   df.groupBy().agg(max($"age"), avg($"salary"))
   * }}
   */
  @scala.annotation.varargs
  override def agg(expr: Column, exprs: Column*): DataFrame = groupBy().agg(expr, exprs :_*)

  /**
   * Returns a new [[DataFrame]] by taking the first `n` rows. The difference between this function
   * and `head` is that `head` returns an array while `limit` returns a new [[DataFrame]].
   */
  override def limit(n: Int): DataFrame = Limit(Literal(n), logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing union of rows in this frame and another frame.
   * This is equivalent to `UNION ALL` in SQL.
   */
  override def unionAll(other: DataFrame): DataFrame = Union(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing rows only in both this frame and another frame.
   * This is equivalent to `INTERSECT` in SQL.
   */
  override def intersect(other: DataFrame): DataFrame = Intersect(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing rows in this frame but not in another frame.
   * This is equivalent to `EXCEPT` in SQL.
   */
  override def except(other: DataFrame): DataFrame = Except(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   * @param seed Seed for sampling.
   */
  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame = {
    Sample(fraction, withReplacement, seed, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows, using a random seed.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   */
  override def sample(withReplacement: Boolean, fraction: Double): DataFrame = {
    sample(withReplacement, fraction, Utils.random.nextLong)
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a new [[DataFrame]] by adding a column.
   */
  override def addColumn(colName: String, col: Column): DataFrame = {
    select(Column("*"), col.as(colName))
  }

  /**
   * Returns the first `n` rows.
   */
  override def head(n: Int): Array[Row] = limit(n).collect()

  /**
   * Returns the first row.
   */
  override def head(): Row = head(1).head

  /**
   * Returns the first row. Alias for head().
   */
  override def first(): Row = head()

  /**
   * Returns a new RDD by applying a function to all rows of this DataFrame.
   */
  override def map[R: ClassTag](f: Row => R): RDD[R] = {
    rdd.map(f)
  }

  /**
   * Returns a new RDD by first applying a function to all rows of this [[DataFrame]],
   * and then flattening the results.
   */
  override def flatMap[R: ClassTag](f: Row => TraversableOnce[R]): RDD[R] = rdd.flatMap(f)

  /**
   * Returns a new RDD by applying a function to each partition of this DataFrame.
   */
  override def mapPartitions[R: ClassTag](f: Iterator[Row] => Iterator[R]): RDD[R] = {
    rdd.mapPartitions(f)
  }

  /**
   * Applies a function `f` to all rows.
   */
  override def foreach(f: Row => Unit): Unit = rdd.foreach(f)

  /**
   * Applies a function f to each partition of this [[DataFrame]].
   */
  override def foreachPartition(f: Iterator[Row] => Unit): Unit = rdd.foreachPartition(f)

  /**
   * Returns the first `n` rows in the [[DataFrame]].
   */
  override def take(n: Int): Array[Row] = head(n)

  /**
   * Returns an array that contains all of [[Row]]s in this [[DataFrame]].
   */
  override def collect(): Array[Row] = rdd.collect()

  /**
   * Returns a Java list that contains all of [[Row]]s in this [[DataFrame]].
   */
  override def collectAsList(): java.util.List[Row] = java.util.Arrays.asList(rdd.collect() :_*)

  /**
   * Returns the number of rows in the [[DataFrame]].
   */
  override def count(): Long = groupBy().count().rdd.collect().head.getLong(0)

  /**
   * Returns a new [[DataFrame]] that has exactly `numPartitions` partitions.
   */
  override def repartition(numPartitions: Int): DataFrame = {
    sqlContext.applySchema(rdd.repartition(numPartitions), schema)
  }

  override def persist(): this.type = {
    sqlContext.cacheManager.cacheQuery(this)
    this
  }

  override def persist(newLevel: StorageLevel): this.type = {
    sqlContext.cacheManager.cacheQuery(this, None, newLevel)
    this
  }

  override def unpersist(blocking: Boolean): this.type = {
    sqlContext.cacheManager.tryUncacheQuery(this, blocking)
    this
  }

  /////////////////////////////////////////////////////////////////////////////
  // I/O
  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the content of the [[DataFrame]] as an [[RDD]] of [[Row]]s.
   */
  override def rdd: RDD[Row] = {
    val schema = this.schema
    queryExecution.executedPlan.execute().map(ScalaReflection.convertRowToScala(_, schema))
  }

  /**
   * Registers this RDD as a temporary table using the given name.  The lifetime of this temporary
   * table is tied to the [[SQLContext]] that was used to create this DataFrame.
   *
   * @group schema
   */
  override def registerTempTable(tableName: String): Unit = {
    sqlContext.registerRDDAsTable(this, tableName)
  }

  /**
   * Saves the contents of this [[DataFrame]] as a parquet file, preserving the schema.
   * Files that are written out using this method can be read back in as a [[DataFrame]]
   * using the `parquetFile` function in [[SQLContext]].
   */
  override def saveAsParquetFile(path: String): Unit = {
    sqlContext.executePlan(WriteToFile(path, logicalPlan)).toRdd
  }

  /**
   * :: Experimental ::
   * Creates a table from the the contents of this DataFrame.  This will fail if the table already
   * exists.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   */
  @Experimental
  override def saveAsTable(tableName: String): Unit = {
    sqlContext.executePlan(
      CreateTableAsSelect(None, tableName, logicalPlan, allowExisting = false)).toRdd
  }

  /**
   * :: Experimental ::
   * Adds the rows from this RDD to the specified table, optionally overwriting the existing data.
   */
  @Experimental
  override def insertInto(tableName: String, overwrite: Boolean): Unit = {
    sqlContext.executePlan(InsertIntoTable(UnresolvedRelation(Seq(tableName)),
      Map.empty, logicalPlan, overwrite)).toRdd
  }

  /**
   * Returns the content of the [[DataFrame]] as a RDD of JSON strings.
   */
  override def toJSON: RDD[String] = {
    val rowSchema = this.schema
    this.mapPartitions { iter =>
      val jsonFactory = new JsonFactory()
      iter.map(JsonRDD.rowToJSON(rowSchema, jsonFactory))
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  // for Python API
  ////////////////////////////////////////////////////////////////////////////
  /**
   * A helpful function for Py4j, convert a list of Column to an array
   */
  protected[sql] def toColumnArray(cols: JList[Column]): Array[Column] = {
    cols.toList.toArray
  }

  /**
   * Converts a JavaRDD to a PythonRDD.
   */
  protected[sql] def javaToPython: JavaRDD[Array[Byte]] = {
    val fieldTypes = schema.fields.map(_.dataType)
    val jrdd = rdd.map(EvaluatePython.rowToArray(_, fieldTypes)).toJavaRDD()
    SerDeUtil.javaToPython(jrdd)
  }
}
