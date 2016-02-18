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

import java.io.CharArrayWriter

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import com.fasterxml.jackson.core.JsonFactory

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.PythonRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{FileRelation, QueryExecution}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.json.JacksonGenerator
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.types._

private[sql] object DataFrame {
  def apply(sqlContext: SQLContext, logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }
}

/**
 * :: Experimental ::
 * A distributed collection of data organized into named columns.
 *
 * A [[DataFrame]] is equivalent to a relational table in Spark SQL. The following example creates
 * a [[DataFrame]] by pointing Spark SQL to a Parquet data set.
 * {{{
 *   val people = sqlContext.read.parquet("...")  // in Scala
 *   DataFrame people = sqlContext.read().parquet("...")  // in Java
 * }}}
 *
 * Once created, it can be manipulated using the various domain-specific-language (DSL) functions
 * defined in: [[DataFrame]] (this class), [[Column]], and [[functions]].
 *
 * To select a column from the data frame, use `apply` method in Scala and `col` in Java.
 * {{{
 *   val ageCol = people("age")  // in Scala
 *   Column ageCol = people.col("age")  // in Java
 * }}}
 *
 * Note that the [[Column]] type can also be manipulated through its various functions.
 * {{{
 *   // The following creates a new column that increases everybody's age by 10.
 *   people("age") + 10  // in Scala
 *   people.col("age").plus(10);  // in Java
 * }}}
 *
 * A more concrete example in Scala:
 * {{{
 *   // To create DataFrame using SQLContext
 *   val people = sqlContext.read.parquet("...")
 *   val department = sqlContext.read.parquet("...")
 *
 *   people.filter("age > 30")
 *     .join(department, people("deptId") === department("id"))
 *     .groupBy(department("name"), "gender")
 *     .agg(avg(people("salary")), max(people("age")))
 * }}}
 *
 * and in Java:
 * {{{
 *   // To create DataFrame using SQLContext
 *   DataFrame people = sqlContext.read().parquet("...");
 *   DataFrame department = sqlContext.read().parquet("...");
 *
 *   people.filter("age".gt(30))
 *     .join(department, people.col("deptId").equalTo(department("id")))
 *     .groupBy(department.col("name"), "gender")
 *     .agg(avg(people.col("salary")), max(people.col("age")));
 * }}}
 *
 * @groupname basic Basic DataFrame functions
 * @groupname dfops Language Integrated Queries
 * @groupname rdd RDD Operations
 * @groupname output Output Operations
 * @groupname action Actions
 * @since 1.3.0
 */
@Experimental
class DataFrame private[sql](
    sqlContext: SQLContext,
    queryExecution: QueryExecution)
  extends Dataset[Row](
    sqlContext,
    queryExecution,
    RowEncoder(queryExecution.analyzed.schema)) {

  // Note for Spark contributors: if adding or updating any action in `DataFrame`, please make sure
  // you wrap it with `withNewExecutionId` if this actions doesn't call other action.

  /**
   * A constructor that automatically analyzes the logical plan.
   *
   * This reports error eagerly as the [[DataFrame]] is constructed, unless
   * [[SQLConf.dataFrameEagerAnalysis]] is turned off.
   */
  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan) = {
    this(sqlContext, {
      val qe = sqlContext.executePlan(logicalPlan)
      if (sqlContext.conf.dataFrameEagerAnalysis) {
        qe.assertAnalyzed()  // This should force analysis and throw errors if there are any
      }
      qe
    })
  }

  /**
   * Returns the object itself.
   * @group basic
   * @since 1.3.0
   */
  // This is declared with parentheses to prevent the Scala compiler from treating
  // `rdd.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  override def toDF(): DataFrame = this

  /**
   * Returns a new [[DataFrame]] with columns renamed. This can be quite convenient in conversion
   * from a RDD of tuples into a [[DataFrame]] with meaningful names. For example:
   * {{{
   *   val rdd: RDD[(Int, String)] = ...
   *   rdd.toDF()  // this implicit conversion creates a DataFrame with column name _1 and _2
   *   rdd.toDF("id", "name")  // this creates a DataFrame with column name "id" and "name"
   * }}}
   * @group basic
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def toDF(colNames: String*): DataFrame = {
    require(schema.size == colNames.size,
      "The number of columns doesn't match.\n" +
        s"Old column names (${schema.size}): " + schema.fields.map(_.name).mkString(", ") + "\n" +
        s"New column names (${colNames.size}): " + colNames.mkString(", "))

    val newCols = logicalPlan.output.zip(colNames).map { case (oldAttribute, newName) =>
      Column(oldAttribute).as(newName)
    }
    select(newCols : _*)
  }

  /**
   * Returns a [[DataFrameNaFunctions]] for working with missing data.
   * {{{
   *   // Dropping rows containing any null values.
   *   df.na.drop()
   * }}}
   *
   * @group dfops
   * @since 1.3.1
   */
  def na: DataFrameNaFunctions = new DataFrameNaFunctions(this)

  /**
   * Returns a [[DataFrameStatFunctions]] for working statistic functions support.
   * {{{
   *   // Finding frequent items in column with name 'a'.
   *   df.stat.freqItems(Seq("a"))
   * }}}
   *
   * @group dfops
   * @since 1.4.0
   */
  def stat: DataFrameStatFunctions = new DataFrameStatFunctions(this)

  /**
   * Cartesian join with another [[DataFrame]].
   *
   * Note that cartesian joins are very expensive without an extra filter that can be pushed down.
   *
   * @param right Right side of the join operation.
   * @group dfops
   * @since 1.3.0
   */
  def join(right: DataFrame): DataFrame = withPlan {
    Join(logicalPlan, right.logicalPlan, joinType = Inner, None)
  }

  /**
   * Inner equi-join with another [[DataFrame]] using the given column.
   *
   * Different from other join functions, the join column will only appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * {{{
   *   // Joining df1 and df2 using the column "user_id"
   *   df1.join(df2, "user_id")
   * }}}
   *
   * Note that if you perform a self-join using this function without aliasing the input
   * [[DataFrame]]s, you will NOT be able to reference any columns after the join, since
   * there is no way to disambiguate which side of the join you would like to reference.
   *
   * @param right Right side of the join operation.
   * @param usingColumn Name of the column to join on. This column must exist on both sides.
   * @group dfops
   * @since 1.4.0
   */
  def join(right: DataFrame, usingColumn: String): DataFrame = {
    join(right, Seq(usingColumn))
  }

  /**
   * Inner equi-join with another [[DataFrame]] using the given columns.
   *
   * Different from other join functions, the join columns will only appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * {{{
   *   // Joining df1 and df2 using the columns "user_id" and "user_name"
   *   df1.join(df2, Seq("user_id", "user_name"))
   * }}}
   *
   * Note that if you perform a self-join using this function without aliasing the input
   * [[DataFrame]]s, you will NOT be able to reference any columns after the join, since
   * there is no way to disambiguate which side of the join you would like to reference.
   *
   * @param right Right side of the join operation.
   * @param usingColumns Names of the columns to join on. This columns must exist on both sides.
   * @group dfops
   * @since 1.4.0
   */
  def join(right: DataFrame, usingColumns: Seq[String]): DataFrame = {
    join(right, usingColumns, "inner")
  }

  /**
   * Equi-join with another [[DataFrame]] using the given columns.
   *
   * Different from other join functions, the join columns will only appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * Note that if you perform a self-join using this function without aliasing the input
   * [[DataFrame]]s, you will NOT be able to reference any columns after the join, since
   * there is no way to disambiguate which side of the join you would like to reference.
   *
   * @param right Right side of the join operation.
   * @param usingColumns Names of the columns to join on. This columns must exist on both sides.
   * @param joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
   * @group dfops
   * @since 1.6.0
   */
  def join(right: DataFrame, usingColumns: Seq[String], joinType: String): DataFrame = {
    super.join(right, usingColumns, joinType).toDF()
  }

  /**
   * Inner join with another [[DataFrame]], using the given join expression.
   *
   * {{{
   *   // The following two are equivalent:
   *   df1.join(df2, $"df1Key" === $"df2Key")
   *   df1.join(df2).where($"df1Key" === $"df2Key")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def join(right: DataFrame, joinExprs: Column): DataFrame = join(right, joinExprs, "inner")

  /**
   * Join with another [[DataFrame]], using the given join expression. The following performs
   * a full outer join between `df1` and `df2`.
   *
   * {{{
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df1.join(df2, $"df1Key" === $"df2Key", "outer")
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df1.join(df2, col("df1Key").equalTo(col("df2Key")), "outer");
   * }}}
   *
   * @param right Right side of the join.
   * @param joinExprs Join expression.
   * @param joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
   * @group dfops
   * @since 1.3.0
   */
  def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame = {
    super.join(right, joinExprs, joinType).toDF()
  }

  /**
   * Returns a new [[DataFrame]] with each partition sorted by the given expressions.
   *
   * This is the same operation as "SORT BY" in SQL (Hive QL).
   *
   * @group dfops
   * @since 1.6.0
   */
  @scala.annotation.varargs
  override def sortWithinPartitions(sortCol: String, sortCols: String*): DataFrame = {
    super.sortWithinPartitions(sortCol, sortCols: _*).toDF()
  }

  /**
   * Returns a new [[DataFrame]] with each partition sorted by the given expressions.
   *
   * This is the same operation as "SORT BY" in SQL (Hive QL).
   *
   * @group dfops
   * @since 1.6.0
   */
  @scala.annotation.varargs
  override def sortWithinPartitions(sortExprs: Column*): DataFrame = {
    super.sortWithinPartitions(sortExprs: _*).toDF()
  }

  /**
   * Returns a new [[DataFrame]] sorted by the specified column, all in ascending order.
   * {{{
   *   // The following 3 are equivalent
   *   df.sort("sortcol")
   *   df.sort($"sortcol")
   *   df.sort($"sortcol".asc)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  override def sort(sortCol: String, sortCols: String*): DataFrame = {
    super.sort(sortCol, sortCols: _*).toDF()
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions. For example:
   * {{{
   *   df.sort($"col1", $"col2".desc)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  override def sort(sortExprs: Column*): DataFrame = {
    super.sort(sortExprs: _*).toDF()
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
   * This is an alias of the `sort` function.
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  override def orderBy(sortCol: String, sortCols: String*): DataFrame = {
    sort(sortCol, sortCols : _*)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
   * This is an alias of the `sort` function.
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  override def orderBy(sortExprs: Column*): DataFrame = sort(sortExprs : _*)

  /**
   * Returns a new [[DataFrame]] with an alias set.
   * @group dfops
   * @since 1.3.0
   */
  override def as(alias: String): DataFrame = super.as(alias).toDF()

  /**
   * (Scala-specific) Returns a new [[DataFrame]] with an alias set.
   * @group dfops
   * @since 1.3.0
   */
  def as(alias: Symbol): DataFrame = as(alias.name)

  /**
   * Returns a new [[DataFrame]] with an alias set. Same as `as`.
   * @group dfops
   * @since 1.6.0
   */
  def alias(alias: String): DataFrame = as(alias)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] with an alias set. Same as `as`.
   * @group dfops
   * @since 1.6.0
   */
  def alias(alias: Symbol): DataFrame = as(alias)

  /**
   * Selects a set of column based expressions.
   * {{{
   *   df.select($"colA", $"colB" + 1)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  override def select(cols: Column*): DataFrame = super.select(cols: _*).toDF()

  /**
   * Selects a set of columns. This is a variant of `select` that can only select
   * existing columns using column names (i.e. cannot construct expressions).
   *
   * {{{
   *   // The following two are equivalent:
   *   df.select("colA", "colB")
   *   df.select($"colA", $"colB")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  override def select(col: String, cols: String*): DataFrame = super.select(col, cols: _*).toDF()

  /**
   * Selects a set of SQL expressions. This is a variant of `select` that accepts
   * SQL expressions.
   *
   * {{{
   *   // The following are equivalent:
   *   df.selectExpr("colA", "colB as newName", "abs(colC)")
   *   df.select(expr("colA"), expr("colB as newName"), expr("abs(colC)"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  override def selectExpr(exprs: String*): DataFrame = super.selectExpr(exprs: _*).toDF()

  /**
   * Filters rows using the given condition.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  override def filter(condition: Column): DataFrame = super.filter(condition).toDF()

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDf.filter("age > 15")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  override def filter(conditionExpr: String): DataFrame = super.filter(conditionExpr).toDF()

  /**
   * Filters rows using the given condition. This is an alias for `filter`.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  override def where(condition: Column): DataFrame = filter(condition)

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDf.where("age > 15")
   * }}}
   * @group dfops
   * @since 1.5.0
   */
  override def where(conditionExpr: String): DataFrame = filter(conditionExpr)

  /**
   * (Scala-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg("age" -> "max", "salary" -> "avg")
   *   df.groupBy().agg("age" -> "max", "salary" -> "avg")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  override def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    super.agg(aggExpr, aggExprs: _*).toDF()
  }

  /**
   * (Scala-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  override def agg(exprs: Map[String, String]): DataFrame = super.agg(exprs).toDF()

  /**
   * (Java-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  override def agg(exprs: java.util.Map[String, String]): DataFrame = super.agg(exprs).toDF()

  /**
   * Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(max($"age"), avg($"salary"))
   *   df.groupBy().agg(max($"age"), avg($"salary"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  override def agg(expr: Column, exprs: Column*): DataFrame = super.agg(expr, exprs: _*).toDF()

  /**
   * Returns a new [[DataFrame]] by taking the first `n` rows. The difference between this function
   * and `head` is that `head` returns an array while `limit` returns a new [[DataFrame]].
   * @group dfops
   * @since 1.3.0
   */
  override def limit(n: Int): DataFrame = super.limit(n).toDF()

  /**
   * Returns a new [[DataFrame]] containing union of rows in this frame and another frame.
   * This is equivalent to `UNION ALL` in SQL.
   * @group dfops
   * @since 1.3.0
   */
  def unionAll(other: DataFrame): DataFrame = withPlan {
    // This breaks caching, but it's usually ok because it addresses a very specific use case:
    // using union to union many files or partitions.
    CombineUnions(Union(logicalPlan, other.logicalPlan))
  }

  /**
   * Returns a new [[DataFrame]] containing rows only in both this frame and another frame.
   * This is equivalent to `INTERSECT` in SQL.
   * @group dfops
   * @since 1.3.0
   */
  def intersect(other: DataFrame): DataFrame = withPlan {
    Intersect(logicalPlan, other.logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] containing rows in this frame but not in another frame.
   * This is equivalent to `EXCEPT` in SQL.
   * @group dfops
   * @since 1.3.0
   */
  def except(other: DataFrame): DataFrame = withPlan {
    Except(logicalPlan, other.logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   * @param seed Seed for sampling.
   * @group dfops
   * @since 1.3.0
   */
  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame = {
    super.sample(withReplacement, fraction, seed).toDF()
  }

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows, using a random seed.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   * @group dfops
   * @since 1.3.0
   */
  override def sample(withReplacement: Boolean, fraction: Double): DataFrame = {
    super.sample(withReplacement, fraction).toDF()
  }

  /**
   * (Scala-specific) Returns a new [[DataFrame]] where each row has been expanded to zero or more
   * rows by the provided function.  This is similar to a `LATERAL VIEW` in HiveQL. The columns of
   * the input row are implicitly joined with each row that is output by the function.
   *
   * The following example uses this function to count the number of books which contain
   * a given word:
   *
   * {{{
   *   case class Book(title: String, words: String)
   *   val df: RDD[Book]
   *
   *   case class Word(word: String)
   *   val allWords = df.explode('words) {
   *     case Row(words: String) => words.split(" ").map(Word(_))
   *   }
   *
   *   val bookCountPerWord = allWords.groupBy("word").agg(countDistinct("title"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  override def explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A])
    : DataFrame = {
    super.explode(input: _*)(f).toDF()
  }

  /**
   * (Scala-specific) Returns a new [[DataFrame]] where a single column has been expanded to zero
   * or more rows by the provided function.  This is similar to a `LATERAL VIEW` in HiveQL. All
   * columns of the input row are implicitly joined with each value that is output by the function.
   *
   * {{{
   *   df.explode("words", "word"){words: String => words.split(" ")}
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  override def explode[A, B : TypeTag](
      inputColumn: String, outputColumn: String)(
      f: A => TraversableOnce[B]): DataFrame = {
    super.explode(inputColumn, outputColumn)(f).toDF()
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a new [[DataFrame]] by adding a column or replacing the existing column that has
   * the same name.
   * @group dfops
   * @since 1.3.0
   */
  override def withColumn(colName: String, col: Column): DataFrame = {
    super.withColumn(colName, col).toDF()
  }

  /**
   * Returns a new [[DataFrame]] by adding a column with metadata.
   */
  private[spark] override def withColumn(
      colName: String, col: Column, metadata: Metadata): DataFrame = {
    super.withColumn(colName, col, metadata).toDF()
  }

  /**
   * Returns a new [[DataFrame]] with a column renamed.
   * This is a no-op if schema doesn't contain existingName.
   * @group dfops
   * @since 1.3.0
   */
  override def withColumnRenamed(existingName: String, newName: String): DataFrame = {
    super.withColumnRenamed(existingName, newName).toDF()
  }

  /**
   * Returns a new [[DataFrame]] with a column dropped.
   * This is a no-op if schema doesn't contain column name.
   * @group dfops
   * @since 1.4.0
   */
  override def drop(colName: String): DataFrame = drop(Seq(colName) : _*).toDF()

  /**
   * Returns a new [[DataFrame]] with columns dropped.
   * This is a no-op if schema doesn't contain column name(s).
   * @group dfops
   * @since 1.6.0
   */
  @scala.annotation.varargs
  override def drop(colNames: String*): DataFrame = super.drop(colNames: _*).toDF()

  /**
   * Returns a new [[DataFrame]] with a column dropped.
   * This version of drop accepts a Column rather than a name.
   * This is a no-op if the DataFrame doesn't have a column
   * with an equivalent expression.
   * @group dfops
   * @since 1.4.1
   */
  override def drop(col: Column): DataFrame = super.drop(col).toDF()

  /**
   * (Scala-specific) Returns a new [[DataFrame]] with duplicate rows removed, considering only
   * the subset of columns.
   *
   * @group dfops
   * @since 1.4.0
   */
  override def dropDuplicates(colNames: Seq[String]): DataFrame = {
    super.dropDuplicates(colNames).toDF()
  }

  /**
   * Returns a new [[DataFrame]] with duplicate rows removed, considering only
   * the subset of columns.
   *
   * @group dfops
   * @since 1.4.0
   */
  override def dropDuplicates(colNames: Array[String]): DataFrame = {
    super.dropDuplicates(colNames).toDF()
  }

  /**
   * Computes statistics for numeric columns, including count, mean, stddev, min, and max.
   * If no columns are given, this function computes statistics for all numerical columns.
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting [[DataFrame]]. If you want to
   * programmatically compute summary statistics, use the `agg` function instead.
   *
   * {{{
   *   df.describe("age", "height").show()
   *
   *   // output:
   *   // summary age   height
   *   // count   10.0  10.0
   *   // mean    53.3  178.05
   *   // stddev  11.6  15.7
   *   // min     18.0  163.0
   *   // max     92.0  192.0
   * }}}
   *
   * @group action
   * @since 1.3.1
   */
  @scala.annotation.varargs
  def describe(cols: String*): DataFrame = withPlan {

    // The list of summary statistics to compute, in the form of expressions.
    val statistics = List[(String, Expression => Expression)](
      "count" -> ((child: Expression) => Count(child).toAggregateExpression()),
      "mean" -> ((child: Expression) => Average(child).toAggregateExpression()),
      "stddev" -> ((child: Expression) => StddevSamp(child).toAggregateExpression()),
      "min" -> ((child: Expression) => Min(child).toAggregateExpression()),
      "max" -> ((child: Expression) => Max(child).toAggregateExpression()))

    val outputCols = (if (cols.isEmpty) numericColumns.map(_.prettyString) else cols).toList

    val ret: Seq[Row] = if (outputCols.nonEmpty) {
      val aggExprs = statistics.flatMap { case (_, colToAgg) =>
        outputCols.map(c => Column(Cast(colToAgg(Column(c).expr), StringType)).as(c))
      }

      val row = agg(aggExprs.head, aggExprs.tail: _*).head().toSeq

      // Pivot the data so each summary is one row
      row.grouped(outputCols.size).toSeq.zip(statistics).map { case (aggregation, (statistic, _)) =>
        Row(statistic :: aggregation.toList: _*)
      }
    } else {
      // If there are no output columns, just output a single column that contains the stats.
      statistics.map { case (name, _) => Row(name) }
    }

    // All columns are string type
    val schema = StructType(
      StructField("summary", StringType) :: outputCols.map(StructField(_, StringType))).toAttributes
    LocalRelation.fromExternalRows(schema, ret)
  }

  /**
   * Concise syntax for chaining custom transformations.
   * {{{
   *   def featurize(ds: DataFrame) = ...
   *
   *   df
   *     .transform(featurize)
   *     .transform(...)
   * }}}
   * @since 1.6.0
   */
  def transform[U](t: DataFrame => DataFrame): DataFrame = t(this)

  /**
   * Returns a new RDD by applying a function to all rows of this DataFrame.
   * @group rdd
   * @since 1.3.0
   */
  def mapRows[R: ClassTag](f: Row => R): RDD[R] = rdd.map(f)

  /**
   * Returns a new RDD by first applying a function to all rows of this [[DataFrame]],
   * and then flattening the results.
   * @group rdd
   * @since 1.3.0
   */
  def flatMapRows[R: ClassTag](f: Row => TraversableOnce[R]): RDD[R] = rdd.flatMap(f)

  /**
   * Returns a new RDD by applying a function to each partition of this DataFrame.
   * @group rdd
   * @since 1.3.0
   */
  def mapPartitions[R: ClassTag](f: Iterator[Row] => Iterator[R]): RDD[R] = {
    rdd.mapPartitions(f)
  }

  /**
   * Applies a function `f` to all rows.
   * @group rdd
   * @since 1.3.0
   */
  def foreachRows(f: Row => Unit): Unit = withNewExecutionId {
    rdd.foreach(f)
  }

  /**
   * Returns a new [[DataFrame]] that has exactly `numPartitions` partitions.
   * @group dfops
   * @since 1.3.0
   */
  override def repartition(numPartitions: Int): DataFrame = {
    super.repartition(numPartitions).toDF()
  }

  /**
   * Returns a new [[DataFrame]] partitioned by the given partitioning expressions into
   * `numPartitions`. The resulting DataFrame is hash partitioned.
   *
   * This is the same operation as "DISTRIBUTE BY" in SQL (Hive QL).
   *
   * @group dfops
   * @since 1.6.0
   */
  @scala.annotation.varargs
  override def repartition(numPartitions: Int, partitionExprs: Column*): DataFrame = {
    super.repartition(numPartitions, partitionExprs: _*).toDF()
  }

  /**
   * Returns a new [[DataFrame]] partitioned by the given partitioning expressions preserving
   * the existing number of partitions. The resulting DataFrame is hash partitioned.
   *
   * This is the same operation as "DISTRIBUTE BY" in SQL (Hive QL).
   *
   * @group dfops
   * @since 1.6.0
   */
  @scala.annotation.varargs
  override def repartition(partitionExprs: Column*): DataFrame = {
    super.repartition(partitionExprs: _*).toDF()
  }

  /**
   * Returns a new [[DataFrame]] that has exactly `numPartitions` partitions.
   * Similar to coalesce defined on an [[RDD]], this operation results in a narrow dependency, e.g.
   * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
   * the 100 new partitions will claim 10 of the current partitions.
   * @group rdd
   * @since 1.4.0
   */
  override def coalesce(numPartitions: Int): DataFrame = super.coalesce(numPartitions).toDF()

  /**
   * Returns a new [[DataFrame]] that contains only the unique rows from this [[DataFrame]].
   * This is an alias for `dropDuplicates`.
   * @group dfops
   * @since 1.3.0
   */
  override def distinct: DataFrame = super.distinct.toDF()

  /////////////////////////////////////////////////////////////////////////////
  // I/O
  /////////////////////////////////////////////////////////////////////////////

  /**
   * :: Experimental ::
   * Interface for saving the content of the [[DataFrame]] out into external storage or streams.
   *
   * @group output
   * @since 1.4.0
   */
  @Experimental
  def write: DataFrameWriter = new DataFrameWriter(this)

  /**
   * Returns the content of the [[DataFrame]] as a RDD of JSON strings.
   * @group rdd
   * @since 1.3.0
   */
  def toJSON: RDD[String] = {
    val rowSchema = this.schema
    queryExecution.toRdd.mapPartitions { iter =>
      val writer = new CharArrayWriter()
      // create the Generator without separator inserted between 2 records
      val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)

      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): String = {
          JacksonGenerator(rowSchema, gen)(iter.next())
          gen.flush()

          val json = writer.toString
          if (hasNext) {
            writer.reset()
          } else {
            gen.close()
          }

          json
        }
      }
    }
  }

  /**
   * Returns a best-effort snapshot of the files that compose this DataFrame. This method simply
   * asks each constituent BaseRelation for its respective files and takes the union of all results.
   * Depending on the source relations, this may not find all input files. Duplicates are removed.
   */
  def inputFiles: Array[String] = {
    val files: Seq[String] = logicalPlan.collect {
      case LogicalRelation(fsBasedRelation: FileRelation, _, _) =>
        fsBasedRelation.inputFiles
      case fr: FileRelation =>
        fr.inputFiles
    }.flatten
    files.toSet.toArray
  }

  ////////////////////////////////////////////////////////////////////////////
  // for Python API
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Converts a JavaRDD to a PythonRDD.
   */
  protected[sql] def javaToPython: JavaRDD[Array[Byte]] = {
    val structType = schema  // capture it for closure
    val rdd = queryExecution.toRdd.map(EvaluatePython.toJava(_, structType))
    EvaluatePython.javaToPython(rdd)
  }

  protected[sql] def collectToPython(): Int = {
    withNewExecutionId {
      PythonRDD.collectAndServe(javaToPython.rdd)
    }
  }

  /** A convenient function to wrap a logical plan and produce a DataFrame. */
  @inline private def withPlan(logicalPlan: => LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }

}
