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
import java.sql.DriverManager

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonFactory

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.catalyst.{ScalaReflection, SqlParser}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, ResolvedStar}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{EvaluatePython, ExplainCommand, LogicalRDD}
import org.apache.spark.sql.jdbc.JDBCWriteDetails
import org.apache.spark.sql.json.JsonRDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.{ResolvedDataSource, CreateTableUsingAsSelect}
import org.apache.spark.util.Utils


private[sql] object DataFrame {
  def apply(sqlContext: SQLContext, logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }
}


/**
 * :: Experimental ::
 * A distributed collection of data organized into named columns.
 *
 * A [[DataFrame]] is equivalent to a relational table in Spark SQL. There are multiple ways
 * to create a [[DataFrame]]:
 * {{{
 *   // Create a DataFrame from Parquet files
 *   val people = sqlContext.parquetFile("...")
 *
 *   // Create a DataFrame from data sources
 *   val df = sqlContext.load("...", "json")
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
 *   val people = sqlContext.parquetFile("...")
 *   val department = sqlContext.parquetFile("...")
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
 *   DataFrame people = sqlContext.parquetFile("...");
 *   DataFrame department = sqlContext.parquetFile("...");
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
 */
// TODO: Improve documentation.
@Experimental
class DataFrame private[sql](
    @transient val sqlContext: SQLContext,
    @DeveloperApi @transient val queryExecution: SQLContext#QueryExecution)
  extends RDDApi[Row] with Serializable {

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

  @transient protected[sql] val logicalPlan: LogicalPlan = queryExecution.logical match {
    // For various commands (like DDL) and queries with side effects, we force query optimization to
    // happen right away to let these side effects take place eagerly.
    case _: Command |
         _: InsertIntoTable |
         _: CreateTableAsSelect[_] |
         _: CreateTableUsingAsSelect |
         _: WriteToFile =>
      LogicalRDD(queryExecution.analyzed.output, queryExecution.toRdd)(sqlContext)
    case _ =>
      queryExecution.logical
  }

  /**
   * An implicit conversion function internal to this class for us to avoid doing
   * "new DataFrame(...)" everywhere.
   */
  @inline private implicit def logicalPlanToDataFrame(logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }

  protected[sql] def resolve(colName: String): NamedExpression = {
    queryExecution.analyzed.resolve(colName, sqlContext.analyzer.resolver).getOrElse {
      throw new AnalysisException(
        s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
    }
  }

  protected[sql] def numericColumns: Seq[Expression] = {
    schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map { n =>
      queryExecution.analyzed.resolve(n.name, sqlContext.analyzer.resolver).get
    }
  }

  /**
   * Internal API for Python
   * @param numRows Number of rows to show
   */
  private[sql] def showString(numRows: Int): String = {
    val data = take(numRows)
    val numCols = schema.fieldNames.length

    // For cells that are beyond 20 characters, replace it with the first 17 and "..."
    val rows: Seq[Seq[String]] = schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        val str = if (cell == null) "null" else cell.toString
        if (str.length > 20) str.substring(0, 17) + "..." else str
      }: Seq[String]
    }

    // Compute the width of each column
    val colWidths = Array.fill(numCols)(0)
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Pad the cells
    rows.map { row =>
      row.zipWithIndex.map { case (cell, i) =>
        String.format(s"%-${colWidths(i)}s", cell)
      }.mkString(" ")
    }.mkString("\n")
  }

  override def toString: String = {
    try {
      schema.map(f => s"${f.name}: ${f.dataType.simpleString}").mkString("[", ", ", "]")
    } catch {
      case NonFatal(e) =>
        s"Invalid tree; ${e.getMessage}:\n$queryExecution"
    }
  }

  /** Left here for backward compatibility. */
  @deprecated("1.3.0", "use toDF")
  def toSchemaRDD: DataFrame = this

  /**
   * Returns the object itself.
   * @group basic
   */
  // This is declared with parentheses to prevent the Scala compiler from treating
  // `rdd.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  def toDF(): DataFrame = this

  /**
   * Returns a new [[DataFrame]] with columns renamed. This can be quite convenient in conversion
   * from a RDD of tuples into a [[DataFrame]] with meaningful names. For example:
   * {{{
   *   val rdd: RDD[(Int, String)] = ...
   *   rdd.toDF()  // this implicit conversion creates a DataFrame with column name _1 and _2
   *   rdd.toDF("id", "name")  // this creates a DataFrame with column name "id" and "name"
   * }}}
   * @group basic
   */
  @scala.annotation.varargs
  def toDF(colNames: String*): DataFrame = {
    require(schema.size == colNames.size,
      "The number of columns doesn't match.\n" +
        "Old column names: " + schema.fields.map(_.name).mkString(", ") + "\n" +
        "New column names: " + colNames.mkString(", "))

    val newCols = schema.fieldNames.zip(colNames).map { case (oldName, newName) =>
      apply(oldName).as(newName)
    }
    select(newCols :_*)
  }

  /**
   * Returns the schema of this [[DataFrame]].
   * @group basic
   */
  def schema: StructType = queryExecution.analyzed.schema

  /**
   * Returns all column names and their data types as an array.
   * @group basic
   */
  def dtypes: Array[(String, String)] = schema.fields.map { field =>
    (field.name, field.dataType.toString)
  }

  /**
   * Returns all column names as an array.
   * @group basic
   */
  def columns: Array[String] = schema.fields.map(_.name)

  /**
   * Prints the schema to the console in a nice tree format.
   * @group basic
   */
  def printSchema(): Unit = println(schema.treeString)

  /**
   * Prints the plans (logical and physical) to the console for debugging purpose.
   * @group basic
   */
  def explain(extended: Boolean): Unit = {
    ExplainCommand(
      queryExecution.logical,
      extended = extended).queryExecution.executedPlan.executeCollect().map {
      r => println(r.getString(0))
    }
  }

  /**
   * Only prints the physical plan to the console for debugging purpose.
   * @group basic
   */
  def explain(): Unit = explain(extended = false)

  /**
   * Returns true if the `collect` and `take` methods can be run locally
   * (without any Spark executors).
   * @group basic
   */
  def isLocal: Boolean = logicalPlan.isInstanceOf[LocalRelation]

  /**
   * Displays the [[DataFrame]] in a tabular form. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   * @param numRows Number of rows to show
   *
   * @group action
   */
  def show(numRows: Int): Unit = println(showString(numRows))

  /**
   * Displays the top 20 rows of [[DataFrame]] in a tabular form.
   * @group action
   */
  def show(): Unit = show(20)

  /**
   * Cartesian join with another [[DataFrame]].
   *
   * Note that cartesian joins are very expensive without an extra filter that can be pushed down.
   *
   * @param right Right side of the join operation.
   * @group dfops
   */
  def join(right: DataFrame): DataFrame = {
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
   * @group dfops
   */
  def join(right: DataFrame, joinExprs: Column): DataFrame = {
    Join(logicalPlan, right.logicalPlan, joinType = Inner, Some(joinExprs.expr))
  }

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
   * @param joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `semijoin`.
   * @group dfops
   */
  def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame = {
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
   * @group dfops
   */
  @scala.annotation.varargs
  def sort(sortCol: String, sortCols: String*): DataFrame = {
    sort((sortCol +: sortCols).map(apply) :_*)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions. For example:
   * {{{
   *   df.sort($"col1", $"col2".desc)
   * }}}
   * @group dfops
   */
  @scala.annotation.varargs
  def sort(sortExprs: Column*): DataFrame = {
    val sortOrder: Seq[SortOrder] = sortExprs.map { col =>
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
   * @group dfops
   */
  @scala.annotation.varargs
  def orderBy(sortCol: String, sortCols: String*): DataFrame = sort(sortCol, sortCols :_*)

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
   * This is an alias of the `sort` function.
   * @group dfops
   */
  @scala.annotation.varargs
  def orderBy(sortExprs: Column*): DataFrame = sort(sortExprs :_*)

  /**
   * Selects column based on the column name and return it as a [[Column]].
   * @group dfops
   */
  def apply(colName: String): Column = col(colName)

  /**
   * Selects column based on the column name and return it as a [[Column]].
   * @group dfops
   */
  def col(colName: String): Column = colName match {
    case "*" =>
      Column(ResolvedStar(schema.fieldNames.map(resolve)))
    case _ =>
      val expr = resolve(colName)
      Column(expr)
  }

  /**
   * Returns a new [[DataFrame]] with an alias set.
   * @group dfops
   */
  def as(alias: String): DataFrame = Subquery(alias, logicalPlan)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] with an alias set.
   * @group dfops
   */
  def as(alias: Symbol): DataFrame = as(alias.name)

  /**
   * Selects a set of expressions.
   * {{{
   *   df.select($"colA", $"colB" + 1)
   * }}}
   * @group dfops
   */
  @scala.annotation.varargs
  def select(cols: Column*): DataFrame = {
    val namedExpressions = cols.map {
      case Column(expr: NamedExpression) => expr
      case Column(expr: Expression) => Alias(expr, expr.prettyString)()
    }
    Project(namedExpressions.toSeq, logicalPlan)
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
   * @group dfops
   */
  @scala.annotation.varargs
  def select(col: String, cols: String*): DataFrame = select((col +: cols).map(Column(_)) :_*)

  /**
   * Selects a set of SQL expressions. This is a variant of `select` that accepts
   * SQL expressions.
   *
   * {{{
   *   df.selectExpr("colA", "colB as newName", "abs(colC)")
   * }}}
   * @group dfops
   */
  @scala.annotation.varargs
  def selectExpr(exprs: String*): DataFrame = {
    select(exprs.map { expr =>
      Column(new SqlParser().parseExpression(expr))
    }: _*)
  }

  /**
   * Filters rows using the given condition.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   *   peopleDf($"age" > 15)
   * }}}
   * @group dfops
   */
  def filter(condition: Column): DataFrame = Filter(condition.expr, logicalPlan)

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDf.filter("age > 15")
   * }}}
   * @group dfops
   */
  def filter(conditionExpr: String): DataFrame = {
    filter(Column(new SqlParser().parseExpression(conditionExpr)))
  }

  /**
   * Filters rows using the given condition. This is an alias for `filter`.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   *   peopleDf($"age" > 15)
   * }}}
   * @group dfops
   */
  def where(condition: Column): DataFrame = filter(condition)

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
   * @group dfops
   */
  @scala.annotation.varargs
  def groupBy(cols: Column*): GroupedData = new GroupedData(this, cols.map(_.expr))

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
   * @group dfops
   */
  @scala.annotation.varargs
  def groupBy(col1: String, cols: String*): GroupedData = {
    val colNames: Seq[String] = col1 +: cols
    new GroupedData(this, colNames.map(colName => resolve(colName)))
  }

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
   * @group dfops
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
   * @group dfops
   */
  def agg(exprs: Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * (Java-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}
   * @group dfops
   */
  def agg(exprs: java.util.Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * Aggregates on the entire [[DataFrame]] without groups.
   * {{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(max($"age"), avg($"salary"))
   *   df.groupBy().agg(max($"age"), avg($"salary"))
   * }}
   * @group dfops
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = groupBy().agg(expr, exprs :_*)

  /**
   * Returns a new [[DataFrame]] by taking the first `n` rows. The difference between this function
   * and `head` is that `head` returns an array while `limit` returns a new [[DataFrame]].
   * @group dfops
   */
  def limit(n: Int): DataFrame = Limit(Literal(n), logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing union of rows in this frame and another frame.
   * This is equivalent to `UNION ALL` in SQL.
   * @group dfops
   */
  def unionAll(other: DataFrame): DataFrame = Union(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing rows only in both this frame and another frame.
   * This is equivalent to `INTERSECT` in SQL.
   * @group dfops
   */
  def intersect(other: DataFrame): DataFrame = Intersect(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing rows in this frame but not in another frame.
   * This is equivalent to `EXCEPT` in SQL.
   * @group dfops
   */
  def except(other: DataFrame): DataFrame = Except(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   * @param seed Seed for sampling.
   * @group dfops
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame = {
    Sample(fraction, withReplacement, seed, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows, using a random seed.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   * @group dfops
   */
  def sample(withReplacement: Boolean, fraction: Double): DataFrame = {
    sample(withReplacement, fraction, Utils.random.nextLong)
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
   */
  def explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A]): DataFrame = {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributes = schema.toAttributes
    val rowFunction =
      f.andThen(_.map(ScalaReflection.convertToCatalyst(_, schema).asInstanceOf[Row]))
    val generator = UserDefinedGenerator(attributes, rowFunction, input.map(_.expr))

    Generate(generator, join = true, outer = false, None, logicalPlan)
  }

  /**
   * (Scala-specific) Returns a new [[DataFrame]] where a single column has been expanded to zero
   * or more rows by the provided function.  This is similar to a `LATERAL VIEW` in HiveQL. All
   * columns of the input row are implicitly joined with each value that is output by the function.
   *
   * {{{
   *   df.explode("words", "word")(words: String => words.split(" "))
   * }}}
   * @group dfops
   */
  def explode[A, B : TypeTag](inputColumn: String, outputColumn: String)(f: A => TraversableOnce[B])
    : DataFrame = {
    val dataType = ScalaReflection.schemaFor[B].dataType
    val attributes = AttributeReference(outputColumn, dataType)() :: Nil
    def rowFunction(row: Row): TraversableOnce[Row] = {
      f(row(0).asInstanceOf[A]).map(o => Row(ScalaReflection.convertToCatalyst(o, dataType)))
    }
    val generator = UserDefinedGenerator(attributes, rowFunction, apply(inputColumn).expr :: Nil)

    Generate(generator, join = true, outer = false, None, logicalPlan)
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a new [[DataFrame]] by adding a column.
   * @group dfops
   */
  def withColumn(colName: String, col: Column): DataFrame = select(Column("*"), col.as(colName))

  /**
   * Returns a new [[DataFrame]] with a column renamed.
   * @group dfops
   */
  def withColumnRenamed(existingName: String, newName: String): DataFrame = {
    val resolver = sqlContext.analyzer.resolver
    val colNames = schema.map { field =>
      val name = field.name
      if (resolver(name, existingName)) Column(name).as(newName) else Column(name)
    }
    select(colNames :_*)
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
   */
  @scala.annotation.varargs
  def describe(cols: String*): DataFrame = {

    // TODO: Add stddev as an expression, and remove it from here.
    def stddevExpr(expr: Expression): Expression =
      Sqrt(Subtract(Average(Multiply(expr, expr)), Multiply(Average(expr), Average(expr))))

    // The list of summary statistics to compute, in the form of expressions.
    val statistics = List[(String, Expression => Expression)](
      "count" -> Count,
      "mean" -> Average,
      "stddev" -> stddevExpr,
      "min" -> Min,
      "max" -> Max)

    val outputCols = (if (cols.isEmpty) numericColumns.map(_.prettyString) else cols).toList

    val ret: Seq[Row] = if (outputCols.nonEmpty) {
      val aggExprs = statistics.flatMap { case (_, colToAgg) =>
        outputCols.map(c => Column(colToAgg(Column(c).expr)).as(c))
      }

      val row = agg(aggExprs.head, aggExprs.tail: _*).head().toSeq

      // Pivot the data so each summary is one row
      row.grouped(outputCols.size).toSeq.zip(statistics).map {
        case (aggregation, (statistic, _)) => Row(statistic :: aggregation.toList: _*)
      }
    } else {
      // If there are no output columns, just output a single column that contains the stats.
      statistics.map { case (name, _) => Row(name) }
    }

    // The first column is string type, and the rest are double type.
    val schema = StructType(
      StructField("summary", StringType) :: outputCols.map(StructField(_, DoubleType))).toAttributes
    LocalRelation(schema, ret)
  }

  /**
   * Returns the first `n` rows.
   * @group action
   */
  def head(n: Int): Array[Row] = limit(n).collect()

  /**
   * Returns the first row.
   * @group action
   */
  def head(): Row = head(1).head

  /**
   * Returns the first row. Alias for head().
   * @group action
   */
  override def first(): Row = head()

  /**
   * Returns a new RDD by applying a function to all rows of this DataFrame.
   * @group rdd
   */
  override def map[R: ClassTag](f: Row => R): RDD[R] = rdd.map(f)

  /**
   * Returns a new RDD by first applying a function to all rows of this [[DataFrame]],
   * and then flattening the results.
   * @group rdd
   */
  override def flatMap[R: ClassTag](f: Row => TraversableOnce[R]): RDD[R] = rdd.flatMap(f)

  /**
   * Returns a new RDD by applying a function to each partition of this DataFrame.
   * @group rdd
   */
  override def mapPartitions[R: ClassTag](f: Iterator[Row] => Iterator[R]): RDD[R] = {
    rdd.mapPartitions(f)
  }

  /**
   * Applies a function `f` to all rows.
   * @group rdd
   */
  override def foreach(f: Row => Unit): Unit = rdd.foreach(f)

  /**
   * Applies a function f to each partition of this [[DataFrame]].
   * @group rdd
   */
  override def foreachPartition(f: Iterator[Row] => Unit): Unit = rdd.foreachPartition(f)

  /**
   * Returns the first `n` rows in the [[DataFrame]].
   * @group action
   */
  override def take(n: Int): Array[Row] = head(n)

  /**
   * Returns an array that contains all of [[Row]]s in this [[DataFrame]].
   * @group action
   */
  override def collect(): Array[Row] = queryExecution.executedPlan.executeCollect()

  /**
   * Returns a Java list that contains all of [[Row]]s in this [[DataFrame]].
   * @group action
   */
  override def collectAsList(): java.util.List[Row] = java.util.Arrays.asList(rdd.collect() :_*)

  /**
   * Returns the number of rows in the [[DataFrame]].
   * @group action
   */
  override def count(): Long = groupBy().count().collect().head.getLong(0)

  /**
   * Returns a new [[DataFrame]] that has exactly `numPartitions` partitions.
   * @group rdd
   */
  override def repartition(numPartitions: Int): DataFrame = {
    sqlContext.createDataFrame(
      queryExecution.toRdd.map(_.copy()).repartition(numPartitions), schema)
  }

  /**
   * Returns a new [[DataFrame]] that contains only the unique rows from this [[DataFrame]].
   * @group dfops
   */
  override def distinct: DataFrame = Distinct(logicalPlan)

  /**
   * @group basic
   */
  override def persist(): this.type = {
    sqlContext.cacheManager.cacheQuery(this)
    this
  }

  /**
   * @group basic
   */
  override def cache(): this.type = persist()

  /**
   * @group basic
   */
  override def persist(newLevel: StorageLevel): this.type = {
    sqlContext.cacheManager.cacheQuery(this, None, newLevel)
    this
  }

  /**
   * @group basic
   */
  override def unpersist(blocking: Boolean): this.type = {
    sqlContext.cacheManager.tryUncacheQuery(this, blocking)
    this
  }

  /**
   * @group basic
   */
  override def unpersist(): this.type = unpersist(blocking = false)

  /////////////////////////////////////////////////////////////////////////////
  // I/O
  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the content of the [[DataFrame]] as an [[RDD]] of [[Row]]s.
   * @group rdd
   */
  def rdd: RDD[Row] = {
    // use a local variable to make sure the map closure doesn't capture the whole DataFrame
    val schema = this.schema
    queryExecution.executedPlan.execute().map(ScalaReflection.convertRowToScala(_, schema))
  }

  /**
   * Returns the content of the [[DataFrame]] as a [[JavaRDD]] of [[Row]]s.
   * @group rdd
   */
  def toJavaRDD: JavaRDD[Row] = rdd.toJavaRDD()

  /**
   * Returns the content of the [[DataFrame]] as a [[JavaRDD]] of [[Row]]s.
   * @group rdd
   */
  def javaRDD: JavaRDD[Row] = toJavaRDD

  /**
   * Registers this RDD as a temporary table using the given name.  The lifetime of this temporary
   * table is tied to the [[SQLContext]] that was used to create this DataFrame.
   *
   * @group basic
   */
  def registerTempTable(tableName: String): Unit = {
    sqlContext.registerDataFrameAsTable(this, tableName)
  }

  /**
   * Saves the contents of this [[DataFrame]] as a parquet file, preserving the schema.
   * Files that are written out using this method can be read back in as a [[DataFrame]]
   * using the `parquetFile` function in [[SQLContext]].
   * @group output
   */
  def saveAsParquetFile(path: String): Unit = {
    if (sqlContext.conf.parquetUseDataSourceApi) {
      save("org.apache.spark.sql.parquet", SaveMode.ErrorIfExists, Map("path" -> path))
    } else {
      sqlContext.executePlan(WriteToFile(path, logicalPlan)).toRdd
    }
  }

  /**
   * :: Experimental ::
   * Creates a table from the the contents of this DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   * This will fail if the table already exists.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   * @group output
   */
  @Experimental
  def saveAsTable(tableName: String): Unit = {
    saveAsTable(tableName, SaveMode.ErrorIfExists)
  }

  /**
   * :: Experimental ::
   * Creates a table from the the contents of this DataFrame, using the default data source
   * configured by spark.sql.sources.default and [[SaveMode.ErrorIfExists]] as the save mode.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   * @group output
   */
  @Experimental
  def saveAsTable(tableName: String, mode: SaveMode): Unit = {
    if (sqlContext.catalog.tableExists(Seq(tableName)) && mode == SaveMode.Append) {
      // If table already exists and the save mode is Append,
      // we will just call insertInto to append the contents of this DataFrame.
      insertInto(tableName, overwrite = false)
    } else {
      val dataSourceName = sqlContext.conf.defaultDataSourceName
      saveAsTable(tableName, dataSourceName, mode)
    }
  }

  /**
   * :: Experimental ::
   * Creates a table at the given path from the the contents of this DataFrame
   * based on a given data source and a set of options,
   * using [[SaveMode.ErrorIfExists]] as the save mode.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   * @group output
   */
  @Experimental
  def saveAsTable(tableName: String, source: String): Unit = {
    saveAsTable(tableName, source, SaveMode.ErrorIfExists)
  }

  /**
   * :: Experimental ::
   * Creates a table at the given path from the the contents of this DataFrame
   * based on a given data source, [[SaveMode]] specified by mode, and a set of options.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   * @group output
   */
  @Experimental
  def saveAsTable(tableName: String, source: String, mode: SaveMode): Unit = {
    saveAsTable(tableName, source, mode, Map.empty[String, String])
  }

  /**
   * :: Experimental ::
   * Creates a table at the given path from the the contents of this DataFrame
   * based on a given data source, [[SaveMode]] specified by mode, and a set of options.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   * @group output
   */
  @Experimental
  def saveAsTable(
      tableName: String,
      source: String,
      mode: SaveMode,
      options: java.util.Map[String, String]): Unit = {
    saveAsTable(tableName, source, mode, options.toMap)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Creates a table from the the contents of this DataFrame based on a given data source,
   * [[SaveMode]] specified by mode, and a set of options.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   * @group output
   */
  @Experimental
  def saveAsTable(
      tableName: String,
      source: String,
      mode: SaveMode,
      options: Map[String, String]): Unit = {
    val cmd =
      CreateTableUsingAsSelect(
        tableName,
        source,
        temporary = false,
        mode,
        options,
        logicalPlan)

    sqlContext.executePlan(cmd).toRdd
  }

  /**
   * :: Experimental ::
   * Saves the contents of this DataFrame to the given path,
   * using the default data source configured by spark.sql.sources.default and
   * [[SaveMode.ErrorIfExists]] as the save mode.
   * @group output
   */
  @Experimental
  def save(path: String): Unit = {
    save(path, SaveMode.ErrorIfExists)
  }

  /**
   * :: Experimental ::
   * Saves the contents of this DataFrame to the given path and [[SaveMode]] specified by mode,
   * using the default data source configured by spark.sql.sources.default.
   * @group output
   */
  @Experimental
  def save(path: String, mode: SaveMode): Unit = {
    val dataSourceName = sqlContext.conf.defaultDataSourceName
    save(path, dataSourceName, mode)
  }

  /**
   * :: Experimental ::
   * Saves the contents of this DataFrame to the given path based on the given data source,
   * using [[SaveMode.ErrorIfExists]] as the save mode.
   * @group output
   */
  @Experimental
  def save(path: String, source: String): Unit = {
    save(source, SaveMode.ErrorIfExists, Map("path" -> path))
  }

  /**
   * :: Experimental ::
   * Saves the contents of this DataFrame to the given path based on the given data source and
   * [[SaveMode]] specified by mode.
   * @group output
   */
  @Experimental
  def save(path: String, source: String, mode: SaveMode): Unit = {
    save(source, mode, Map("path" -> path))
  }

  /**
   * :: Experimental ::
   * Saves the contents of this DataFrame based on the given data source,
   * [[SaveMode]] specified by mode, and a set of options.
   * @group output
   */
  @Experimental
  def save(
      source: String,
      mode: SaveMode,
      options: java.util.Map[String, String]): Unit = {
    save(source, mode, options.toMap)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Saves the contents of this DataFrame based on the given data source,
   * [[SaveMode]] specified by mode, and a set of options
   * @group output
   */
  @Experimental
  def save(
      source: String,
      mode: SaveMode,
      options: Map[String, String]): Unit = {
    ResolvedDataSource(sqlContext, source, mode, options, this)
  }

  /**
   * :: Experimental ::
   * Adds the rows from this RDD to the specified table, optionally overwriting the existing data.
   * @group output
   */
  @Experimental
  def insertInto(tableName: String, overwrite: Boolean): Unit = {
    sqlContext.executePlan(InsertIntoTable(UnresolvedRelation(Seq(tableName)),
      Map.empty, logicalPlan, overwrite)).toRdd
  }

  /**
   * :: Experimental ::
   * Adds the rows from this RDD to the specified table.
   * Throws an exception if the table already exists.
   * @group output
   */
  @Experimental
  def insertInto(tableName: String): Unit = insertInto(tableName, overwrite = false)

  /**
   * Returns the content of the [[DataFrame]] as a RDD of JSON strings.
   * @group rdd
   */
  def toJSON: RDD[String] = {
    val rowSchema = this.schema
    this.mapPartitions { iter =>
      val writer = new CharArrayWriter()
      // create the Generator without separator inserted between 2 records
      val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)

      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): String = {
          JsonRDD.rowToJSON(rowSchema, gen)(iter.next())
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

  ////////////////////////////////////////////////////////////////////////////
  // JDBC Write Support
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Save this RDD to a JDBC database at `url` under the table name `table`.
   * This will run a `CREATE TABLE` and a bunch of `INSERT INTO` statements.
   * If you pass `true` for `allowExisting`, it will drop any table with the
   * given name; if you pass `false`, it will throw if the table already
   * exists.
   * @group output
   */
  def createJDBCTable(url: String, table: String, allowExisting: Boolean): Unit = {
    val conn = DriverManager.getConnection(url)
    try {
      if (allowExisting) {
        val sql = s"DROP TABLE IF EXISTS $table"
        conn.prepareStatement(sql).executeUpdate()
      }
      val schema = JDBCWriteDetails.schemaString(this, url)
      val sql = s"CREATE TABLE $table ($schema)"
      conn.prepareStatement(sql).executeUpdate()
    } finally {
      conn.close()
    }
    JDBCWriteDetails.saveTable(this, url, table)
  }

  /**
   * Save this RDD to a JDBC database at `url` under the table name `table`.
   * Assumes the table already exists and has a compatible schema.  If you
   * pass `true` for `overwrite`, it will `TRUNCATE` the table before
   * performing the `INSERT`s.
   *
   * The table must already exist on the database.  It must have a schema
   * that is compatible with the schema of this RDD; inserting the rows of
   * the RDD in order via the simple statement
   * `INSERT INTO table VALUES (?, ?, ..., ?)` should not fail.
   * @group output
   */
  def insertIntoJDBC(url: String, table: String, overwrite: Boolean): Unit = {
    if (overwrite) {
      val conn = DriverManager.getConnection(url)
      try {
        val sql = s"TRUNCATE TABLE $table"
        conn.prepareStatement(sql).executeUpdate()
      } finally {
        conn.close()
      }
    }
    JDBCWriteDetails.saveTable(this, url, table)
  }

  ////////////////////////////////////////////////////////////////////////////
  // for Python API
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Converts a JavaRDD to a PythonRDD.
   */
  protected[sql] def javaToPython: JavaRDD[Array[Byte]] = {
    val fieldTypes = schema.fields.map(_.dataType)
    val jrdd = rdd.map(EvaluatePython.rowToArray(_, fieldTypes)).toJavaRDD()
    SerDeUtil.javaToPython(jrdd)
  }
}
