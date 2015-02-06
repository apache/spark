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

import scala.reflect.ClassTag

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


private[sql] object DataFrame {
  def apply(sqlContext: SQLContext, logicalPlan: LogicalPlan): DataFrame = {
    new DataFrameImpl(sqlContext, logicalPlan)
  }
}


/**
 * :: Experimental ::
 * A distributed collection of data organized into named columns.
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
 * {{{
 *   // The following creates a new column that increases everybody's age by 10.
 *   people("age") + 10  // in Scala
 * }}}
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
@Experimental
trait DataFrame extends RDDApi[Row] {

  val sqlContext: SQLContext

  @DeveloperApi
  def queryExecution: SQLContext#QueryExecution

  protected[sql] def logicalPlan: LogicalPlan

  /** Left here for backward compatibility. */
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
  def toDataFrame(colName: String, colNames: String*): DataFrame

  /** Returns the schema of this [[DataFrame]]. */
  def schema: StructType

  /** Returns all column names and their data types as an array. */
  def dtypes: Array[(String, String)]

  /** Returns all column names as an array. */
  def columns: Array[String] = schema.fields.map(_.name)

  /** Prints the schema to the console in a nice tree format. */
  def printSchema(): Unit

  /**
   * Cartesian join with another [[DataFrame]].
   *
   * Note that cartesian joins are very expensive without an extra filter that can be pushed down.
   *
   * @param right Right side of the join operation.
   */
  def join(right: DataFrame): DataFrame

  /**
   * Inner join with another [[DataFrame]], using the given join expression.
   *
   * {{{
   *   // The following two are equivalent:
   *   df1.join(df2, $"df1Key" === $"df2Key")
   *   df1.join(df2).where($"df1Key" === $"df2Key")
   * }}}
   */
  def join(right: DataFrame, joinExprs: Column): DataFrame

  /**
   * Join with another [[DataFrame]], usin  g the given join expression. The following performs
   * a full outer join between `df1` and `df2`.
   *
   * {{{
   *   // Scala:
   *   import org.apache.spark.sql.dsl._
   *   df1.join(df2, "outer", $"df1Key" === $"df2Key")
   *
   *   // Java:
   *   import static org.apache.spark.sql.Dsl.*;
   *   df1.join(df2, "outer", col("df1Key") === col("df2Key"));
   * }}}
   *
   * @param right Right side of the join.
   * @param joinExprs Join expression.
   * @param joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `semijoin`.
   */
  def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame

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
  def sort(sortCol: String, sortCols: String*): DataFrame

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions. For example:
   * {{{
   *   df.sort($"col1", $"col2".desc)
   * }}}
   */
  @scala.annotation.varargs
  def sort(sortExprs: Column*): DataFrame

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
   * This is an alias of the `sort` function.
   */
  @scala.annotation.varargs
  def orderBy(sortCol: String, sortCols: String*): DataFrame

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
   * This is an alias of the `sort` function.
   */
  @scala.annotation.varargs
  def orderBy(sortExprs: Column*): DataFrame

  /**
   * Selects column based on the column name and return it as a [[Column]].
   */
  def apply(colName: String): Column = col(colName)

  /**
   * Selects column based on the column name and return it as a [[Column]].
   */
  def col(colName: String): Column

  /**
   * Selects a set of expressions, wrapped in a Product.
   * {{{
   *   // The following two are equivalent:
   *   df.apply(($"colA", $"colB" + 1))
   *   df.select($"colA", $"colB" + 1)
   * }}}
   */
  def apply(projection: Product): DataFrame

  /**
   * Returns a new [[DataFrame]] with an alias set.
   */
  def as(name: String): DataFrame

  /**
   * Selects a set of expressions.
   * {{{
   *   df.select($"colA", $"colB" + 1)
   * }}}
   */
  @scala.annotation.varargs
  def select(cols: Column*): DataFrame

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
  def select(col: String, cols: String*): DataFrame

  /**
   * Selects a set of SQL expressions. This is a variant of `select` that accepts
   * SQL expressions.
   *
   * {{{
   *   df.selectExpr("colA", "colB as newName", "abs(colC)")
   * }}}
   */
  @scala.annotation.varargs
  def selectExpr(exprs: String*): DataFrame

  /**
   * Filters rows using the given condition.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   *   peopleDf($"age" > 15)
   * }}}
   */
  def filter(condition: Column): DataFrame

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDf.filter("age > 15")
   * }}}
   */
  def filter(conditionExpr: String): DataFrame

  /**
   * Filters rows using the given condition. This is an alias for `filter`.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   *   peopleDf($"age" > 15)
   * }}}
   */
  def where(condition: Column): DataFrame

  /**
   * Filters rows using the given condition. This is a shorthand meant for Scala.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   *   peopleDf($"age" > 15)
   * }}}
   */
  def apply(condition: Column): DataFrame

  /**
   * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
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
  def groupBy(cols: Column*): GroupedData

  /**
   * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
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
  def groupBy(col1: String, cols: String*): GroupedData

  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(
   *     "age" -> "max",
   *     "expense" -> "sum"
   *   )
   * }}}
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    groupBy().agg(aggExpr, aggExprs :_*)
  }

  /**
   * (Scala-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}
   */
  def agg(exprs: Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * (Java-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}
   */
  def agg(exprs: java.util.Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * Aggregates on the entire [[DataFrame]] without groups.
   * {{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(max($"age"), avg($"salary"))
   *   df.groupBy().agg(max($"age"), avg($"salary"))
   * }}
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = groupBy().agg(expr, exprs :_*)

  /**
   * Returns a new [[DataFrame]] by taking the first `n` rows. The difference between this function
   * and `head` is that `head` returns an array while `limit` returns a new [[DataFrame]].
   */
  def limit(n: Int): DataFrame

  /**
   * Returns a new [[DataFrame]] containing union of rows in this frame and another frame.
   * This is equivalent to `UNION ALL` in SQL.
   */
  def unionAll(other: DataFrame): DataFrame

  /**
   * Returns a new [[DataFrame]] containing rows only in both this frame and another frame.
   * This is equivalent to `INTERSECT` in SQL.
   */
  def intersect(other: DataFrame): DataFrame

  /**
   * Returns a new [[DataFrame]] containing rows in this frame but not in another frame.
   * This is equivalent to `EXCEPT` in SQL.
   */
  def except(other: DataFrame): DataFrame

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   * @param seed Seed for sampling.
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows, using a random seed.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   */
  def sample(withReplacement: Boolean, fraction: Double): DataFrame = {
    sample(withReplacement, fraction, Utils.random.nextLong)
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a new [[DataFrame]] by adding a column.
   */
  def addColumn(colName: String, col: Column): DataFrame

  /**
   * Returns a new [[DataFrame]] with a column renamed.
   */
  def renameColumn(existingName: String, newName: String): DataFrame

  /**
   * Returns the first `n` rows.
   */
  def head(n: Int): Array[Row]

  /**
   * Returns the first row.
   */
  def head(): Row

  /**
   * Returns the first row. Alias for head().
   */
  override def first(): Row

  /**
   * Returns a new RDD by applying a function to all rows of this DataFrame.
   */
  override def map[R: ClassTag](f: Row => R): RDD[R]

  /**
   * Returns a new RDD by first applying a function to all rows of this [[DataFrame]],
   * and then flattening the results.
   */
  override def flatMap[R: ClassTag](f: Row => TraversableOnce[R]): RDD[R]

  /**
   * Returns a new RDD by applying a function to each partition of this DataFrame.
   */
  override def mapPartitions[R: ClassTag](f: Iterator[Row] => Iterator[R]): RDD[R]
  /**
   * Applies a function `f` to all rows.
   */
  override def foreach(f: Row => Unit): Unit

  /**
   * Applies a function f to each partition of this [[DataFrame]].
   */
  override def foreachPartition(f: Iterator[Row] => Unit): Unit

  /**
   * Returns the first `n` rows in the [[DataFrame]].
   */
  override def take(n: Int): Array[Row]

  /**
   * Returns an array that contains all of [[Row]]s in this [[DataFrame]].
   */
  override def collect(): Array[Row]

  /**
   * Returns a Java list that contains all of [[Row]]s in this [[DataFrame]].
   */
  override def collectAsList(): java.util.List[Row]

  /**
   * Returns the number of rows in the [[DataFrame]].
   */
  override def count(): Long

  /**
   * Returns a new [[DataFrame]] that has exactly `numPartitions` partitions.
   */
  override def repartition(numPartitions: Int): DataFrame

  override def persist(): this.type

  override def persist(newLevel: StorageLevel): this.type

  override def unpersist(blocking: Boolean): this.type

  /////////////////////////////////////////////////////////////////////////////
  // I/O
  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the content of the [[DataFrame]] as an [[RDD]] of [[Row]]s.
   */
  def rdd: RDD[Row]

  /**
   * Returns the content of the [[DataFrame]] as a [[JavaRDD]] of [[Row]]s.
   */
  def toJavaRDD: JavaRDD[Row] = rdd.toJavaRDD()

  /**
   * Returns the content of the [[DataFrame]] as a [[JavaRDD]] of [[Row]]s.
   */
  def javaRDD: JavaRDD[Row] = toJavaRDD

  /**
   * Registers this RDD as a temporary table using the given name.  The lifetime of this temporary
   * table is tied to the [[SQLContext]] that was used to create this DataFrame.
   *
   * @group schema
   */
  def registerTempTable(tableName: String): Unit

  /**
   * Saves the contents of this [[DataFrame]] as a parquet file, preserving the schema.
   * Files that are written out using this method can be read back in as a [[DataFrame]]
   * using the `parquetFile` function in [[SQLContext]].
   */
  def saveAsParquetFile(path: String): Unit

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
  def saveAsTable(tableName: String): Unit

  /**
   * :: Experimental ::
   * Creates a table from the the contents of this DataFrame based on a given data source and
   * a set of options. This will fail if the table already exists.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   */
  @Experimental
  def saveAsTable(
      tableName: String,
      dataSourceName: String,
      option: (String, String),
      options: (String, String)*): Unit

  /**
   * :: Experimental ::
   * Creates a table from the the contents of this DataFrame based on a given data source and
   * a set of options. This will fail if the table already exists.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   */
  @Experimental
  def saveAsTable(
      tableName: String,
      dataSourceName: String,
      options: java.util.Map[String, String]): Unit

  @Experimental
  def save(path: String): Unit

  @Experimental
  def save(
      dataSourceName: String,
      option: (String, String),
      options: (String, String)*): Unit

  @Experimental
  def save(
      dataSourceName: String,
      options: java.util.Map[String, String]): Unit

  /**
   * :: Experimental ::
   * Adds the rows from this RDD to the specified table, optionally overwriting the existing data.
   */
  @Experimental
  def insertInto(tableName: String, overwrite: Boolean): Unit

  /**
   * :: Experimental ::
   * Adds the rows from this RDD to the specified table.
   * Throws an exception if the table already exists.
   */
  @Experimental
  def insertInto(tableName: String): Unit = insertInto(tableName, overwrite = false)

  /**
   * Returns the content of the [[DataFrame]] as a RDD of JSON strings.
   */
  def toJSON: RDD[String]

  ////////////////////////////////////////////////////////////////////////////
  // for Python API
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Converts a JavaRDD to a PythonRDD.
   */
  protected[sql] def javaToPython: JavaRDD[Array[Byte]]
}
