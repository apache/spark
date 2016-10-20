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

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils

import org.apache.spark.annotation.{DeveloperApi, Experimental, InterfaceStability}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function._
import org.apache.spark.api.python.{PythonRDD, SerDeUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.usePrettyExpression
import org.apache.spark.sql.execution.{FileRelation, LogicalRDD, QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.command.{CreateViewCommand, ExplainCommand, GlobalTempView, LocalTempView}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.json.JacksonGenerator
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

private[sql] object Dataset {
  def apply[T: Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[T] = {
    new Dataset(sparkSession, logicalPlan, implicitly[Encoder[T]])
  }

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }
}

/**
 * A Dataset is a strongly typed collection of domain-specific objects that can be transformed
 * in parallel using functional or relational operations. Each Dataset also has an untyped view
 * called a [[DataFrame]], which is a Dataset of [[Row]].
 *
 * Operations available on Datasets are divided into transformations and actions. Transformations
 * are the ones that produce new Datasets, and actions are the ones that trigger computation and
 * return results. Example transformations include map, filter, select, and aggregate (`groupBy`).
 * Example actions count, show, or writing data out to file systems.
 *
 * Datasets are "lazy", i.e. computations are only triggered when an action is invoked. Internally,
 * a Dataset represents a logical plan that describes the computation required to produce the data.
 * When an action is invoked, Spark's query optimizer optimizes the logical plan and generates a
 * physical plan for efficient execution in a parallel and distributed manner. To explore the
 * logical plan as well as optimized physical plan, use the `explain` function.
 *
 * To efficiently support domain-specific objects, an [[Encoder]] is required. The encoder maps
 * the domain specific type `T` to Spark's internal type system. For example, given a class `Person`
 * with two fields, `name` (string) and `age` (int), an encoder is used to tell Spark to generate
 * code at runtime to serialize the `Person` object into a binary structure. This binary structure
 * often has much lower memory footprint as well as are optimized for efficiency in data processing
 * (e.g. in a columnar format). To understand the internal binary representation for data, use the
 * `schema` function.
 *
 * There are typically two ways to create a Dataset. The most common way is by pointing Spark
 * to some files on storage systems, using the `read` function available on a `SparkSession`.
 * {{{
 *   val people = spark.read.parquet("...").as[Person]  // Scala
 *   Dataset<Person> people = spark.read().parquet("...").as(Encoders.bean(Person.class)); // Java
 * }}}
 *
 * Datasets can also be created through transformations available on existing Datasets. For example,
 * the following creates a new Dataset by applying a filter on the existing one:
 * {{{
 *   val names = people.map(_.name)  // in Scala; names is a Dataset[String]
 *   Dataset<String> names = people.map((Person p) -> p.name, Encoders.STRING)); // in Java 8
 * }}}
 *
 * Dataset operations can also be untyped, through various domain-specific-language (DSL)
 * functions defined in: Dataset (this class), [[Column]], and [[functions]]. These operations
 * are very similar to the operations available in the data frame abstraction in R or Python.
 *
 * To select a column from the Dataset, use `apply` method in Scala and `col` in Java.
 * {{{
 *   val ageCol = people("age")  // in Scala
 *   Column ageCol = people.col("age"); // in Java
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
 *   // To create Dataset[Row] using SparkSession
 *   val people = spark.read.parquet("...")
 *   val department = spark.read.parquet("...")
 *
 *   people.filter("age > 30")
 *     .join(department, people("deptId") === department("id"))
 *     .groupBy(department("name"), "gender")
 *     .agg(avg(people("salary")), max(people("age")))
 * }}}
 *
 * and in Java:
 * {{{
 *   // To create Dataset<Row> using SparkSession
 *   Dataset<Row> people = spark.read().parquet("...");
 *   Dataset<Row> department = spark.read().parquet("...");
 *
 *   people.filter("age".gt(30))
 *     .join(department, people.col("deptId").equalTo(department("id")))
 *     .groupBy(department.col("name"), "gender")
 *     .agg(avg(people.col("salary")), max(people.col("age")));
 * }}}
 *
 * @groupname basic Basic Dataset functions
 * @groupname action Actions
 * @groupname untypedrel Untyped transformations
 * @groupname typedrel Typed transformations
 *
 * @since 1.6.0
 */
@InterfaceStability.Stable
class Dataset[T] private[sql](
    @transient val sparkSession: SparkSession,
    @DeveloperApi @InterfaceStability.Unstable @transient val queryExecution: QueryExecution,
    encoder: Encoder[T])
  extends Serializable {

  queryExecution.assertAnalyzed()

  // Note for Spark contributors: if adding or updating any action in `Dataset`, please make sure
  // you wrap it with `withNewExecutionId` if this actions doesn't call other action.

  def this(sparkSession: SparkSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(sparkSession, sparkSession.sessionState.executePlan(logicalPlan), encoder)
  }

  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(sqlContext.sparkSession, logicalPlan, encoder)
  }

  @transient private[sql] val logicalPlan: LogicalPlan = {
    def hasSideEffects(plan: LogicalPlan): Boolean = plan match {
      case _: Command |
           _: InsertIntoTable => true
      case _ => false
    }

    queryExecution.analyzed match {
      // For various commands (like DDL) and queries with side effects, we force query execution
      // to happen right away to let these side effects take place eagerly.
      case p if hasSideEffects(p) =>
        LogicalRDD(queryExecution.analyzed.output, queryExecution.toRdd)(sparkSession)
      case Union(children) if children.forall(hasSideEffects) =>
        LogicalRDD(queryExecution.analyzed.output, queryExecution.toRdd)(sparkSession)
      case _ =>
        queryExecution.analyzed
    }
  }

  /**
   * Currently [[ExpressionEncoder]] is the only implementation of [[Encoder]], here we turn the
   * passed in encoder to [[ExpressionEncoder]] explicitly, and mark it implicit so that we can use
   * it when constructing new Dataset objects that have the same object type (that will be
   * possibly resolved to a different schema).
   */
  private[sql] implicit val exprEnc: ExpressionEncoder[T] = encoderFor(encoder)

  /**
   * Encoder is used mostly as a container of serde expressions in Dataset.  We build logical
   * plans by these serde expressions and execute it within the query framework.  However, for
   * performance reasons we may want to use encoder as a function to deserialize internal rows to
   * custom objects, e.g. collect.  Here we resolve and bind the encoder so that we can call its
   * `fromRow` method later.
   */
  private val boundEnc =
    exprEnc.resolveAndBind(logicalPlan.output, sparkSession.sessionState.analyzer)

  private implicit def classTag = exprEnc.clsTag

  // sqlContext must be val because a stable identifier is expected when you import implicits
  @transient lazy val sqlContext: SQLContext = sparkSession.sqlContext

  private[sql] def resolve(colName: String): NamedExpression = {
    queryExecution.analyzed.resolveQuoted(colName, sparkSession.sessionState.analyzer.resolver)
      .getOrElse {
        throw new AnalysisException(
          s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
      }
  }

  private[sql] def numericColumns: Seq[Expression] = {
    schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map { n =>
      queryExecution.analyzed.resolveQuoted(n.name, sparkSession.sessionState.analyzer.resolver).get
    }
  }

  private def aggregatableColumns: Seq[Expression] = {
    schema.fields
      .filter(f => f.dataType.isInstanceOf[NumericType] || f.dataType.isInstanceOf[StringType])
      .map { n =>
        queryExecution.analyzed.resolveQuoted(n.name, sparkSession.sessionState.analyzer.resolver)
          .get
      }
  }

  /**
   * Compose the string representing rows for output
   *
   * @param _numRows Number of rows to show
   * @param truncate If set to more than 0, truncates strings to `truncate` characters and
   *                   all cells will be aligned right.
   */
  private[sql] def showString(_numRows: Int, truncate: Int = 20): String = {
    val numRows = _numRows.max(0)
    val takeResult = toDF().take(numRows + 1)
    val hasMoreData = takeResult.length > numRows
    val data = takeResult.take(numRows)

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond `truncate` characters, replace it with the
    // first `truncate-3` and "..."
    val rows: Seq[Seq[String]] = schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }: Seq[String]
    }

    val sb = new StringBuilder
    val numCols = schema.fieldNames.length

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    rows.head.zipWithIndex.map { case (cell, i) =>
      if (truncate > 0) {
        StringUtils.leftPad(cell, colWidths(i))
      } else {
        StringUtils.rightPad(cell, colWidths(i))
      }
    }.addString(sb, "|", "|", "|\n")

    sb.append(sep)

    // data
    rows.tail.map {
      _.zipWithIndex.map { case (cell, i) =>
        if (truncate > 0) {
          StringUtils.leftPad(cell.toString, colWidths(i))
        } else {
          StringUtils.rightPad(cell.toString, colWidths(i))
        }
      }.addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)

    // For Data that has more than "numRows" records
    if (hasMoreData) {
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }

  override def toString: String = {
    try {
      val builder = new StringBuilder
      val fields = schema.take(2).map {
        case f => s"${f.name}: ${f.dataType.simpleString(2)}"
      }
      builder.append("[")
      builder.append(fields.mkString(", "))
      if (schema.length > 2) {
        if (schema.length - fields.size == 1) {
          builder.append(" ... 1 more field")
        } else {
          builder.append(" ... " + (schema.length - 2) + " more fields")
        }
      }
      builder.append("]").toString()
    } catch {
      case NonFatal(e) =>
        s"Invalid tree; ${e.getMessage}:\n$queryExecution"
    }
  }

  /**
   * Converts this strongly typed collection of data to generic Dataframe. In contrast to the
   * strongly typed objects that Dataset operations work on, a Dataframe returns generic [[Row]]
   * objects that allow fields to be accessed by ordinal or name.
   *
   * @group basic
   * @since 1.6.0
   */
  // This is declared with parentheses to prevent the Scala compiler from treating
  // `ds.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  def toDF(): DataFrame = new Dataset[Row](sparkSession, queryExecution, RowEncoder(schema))

  /**
   * :: Experimental ::
   * Returns a new Dataset where each record has been mapped on to the specified type. The
   * method used to map columns depend on the type of `U`:
   *  - When `U` is a class, fields for the class will be mapped to columns of the same name
   *    (case sensitivity is determined by `spark.sql.caseSensitive`).
   *  - When `U` is a tuple, the columns will be be mapped by ordinal (i.e. the first column will
   *    be assigned to `_1`).
   *  - When `U` is a primitive type (i.e. String, Int, etc), then the first column of the
   *    [[DataFrame]] will be used.
   *
   * If the schema of the Dataset does not match the desired `U` type, you can use `select`
   * along with `alias` or `as` to rearrange or rename as required.
   *
   * @group basic
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def as[U : Encoder]: Dataset[U] = Dataset[U](sparkSession, logicalPlan)

  /**
   * Converts this strongly typed collection of data to generic `DataFrame` with columns renamed.
   * This can be quite convenient in conversion from a RDD of tuples into a [[DataFrame]] with
   * meaningful names. For example:
   * {{{
   *   val rdd: RDD[(Int, String)] = ...
   *   rdd.toDF()  // this implicit conversion creates a DataFrame with column name `_1` and `_2`
   *   rdd.toDF("id", "name")  // this creates a DataFrame with column name "id" and "name"
   * }}}
   *
   * @group basic
   * @since 2.0.0
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
   * Returns the schema of this Dataset.
   *
   * @group basic
   * @since 1.6.0
   */
  def schema: StructType = queryExecution.analyzed.schema

  /**
   * Prints the schema to the console in a nice tree format.
   *
   * @group basic
   * @since 1.6.0
   */
  // scalastyle:off println
  def printSchema(): Unit = println(schema.treeString)
  // scalastyle:on println

  /**
   * Prints the plans (logical and physical) to the console for debugging purposes.
   *
   * @group basic
   * @since 1.6.0
   */
  def explain(extended: Boolean): Unit = {
    val explain = ExplainCommand(queryExecution.logical, extended = extended)
    sparkSession.sessionState.executePlan(explain).executedPlan.executeCollect().foreach {
      // scalastyle:off println
      r => println(r.getString(0))
      // scalastyle:on println
    }
  }

  /**
   * Prints the physical plan to the console for debugging purposes.
   *
   * @group basic
   * @since 1.6.0
   */
  def explain(): Unit = explain(extended = false)

  /**
   * Returns all column names and their data types as an array.
   *
   * @group basic
   * @since 1.6.0
   */
  def dtypes: Array[(String, String)] = schema.fields.map { field =>
    (field.name, field.dataType.toString)
  }

  /**
   * Returns all column names as an array.
   *
   * @group basic
   * @since 1.6.0
   */
  def columns: Array[String] = schema.fields.map(_.name)

  /**
   * Returns true if the `collect` and `take` methods can be run locally
   * (without any Spark executors).
   *
   * @group basic
   * @since 1.6.0
   */
  def isLocal: Boolean = logicalPlan.isInstanceOf[LocalRelation]

  /**
   * Returns true if this Dataset contains one or more sources that continuously
   * return data as it arrives. A Dataset that reads data from a streaming source
   * must be executed as a [[StreamingQuery]] using the `start()` method in
   * [[DataStreamWriter]]. Methods that return a single answer, e.g. `count()` or
   * `collect()`, will throw an [[AnalysisException]] when there is a streaming
   * source present.
   *
   * @group basic
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def isStreaming: Boolean = logicalPlan.isStreaming

  /**
   * Displays the Dataset in a tabular form. Strings more than 20 characters will be truncated,
   * and all cells will be aligned right. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   *
   * @param numRows Number of rows to show
   *
   * @group action
   * @since 1.6.0
   */
  def show(numRows: Int): Unit = show(numRows, truncate = true)

  /**
   * Displays the top 20 rows of Dataset in a tabular form. Strings more than 20 characters
   * will be truncated, and all cells will be aligned right.
   *
   * @group action
   * @since 1.6.0
   */
  def show(): Unit = show(20)

  /**
   * Displays the top 20 rows of Dataset in a tabular form.
   *
   * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
   *                 be truncated and all cells will be aligned right
   *
   * @group action
   * @since 1.6.0
   */
  def show(truncate: Boolean): Unit = show(20, truncate)

  /**
   * Displays the Dataset in a tabular form. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   * @param numRows Number of rows to show
   * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
   *              be truncated and all cells will be aligned right
   *
   * @group action
   * @since 1.6.0
   */
  // scalastyle:off println
  def show(numRows: Int, truncate: Boolean): Unit = if (truncate) {
    println(showString(numRows, truncate = 20))
  } else {
    println(showString(numRows, truncate = 0))
  }
  // scalastyle:on println

  /**
   * Displays the Dataset in a tabular form. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
 *
   * @param numRows Number of rows to show
   * @param truncate If set to more than 0, truncates strings to `truncate` characters and
   *                    all cells will be aligned right.
   * @group action
   * @since 1.6.0
   */
  // scalastyle:off println
  def show(numRows: Int, truncate: Int): Unit = println(showString(numRows, truncate))
  // scalastyle:on println

  /**
   * Returns a [[DataFrameNaFunctions]] for working with missing data.
   * {{{
   *   // Dropping rows containing any null values.
   *   ds.na.drop()
   * }}}
   *
   * @group untypedrel
   * @since 1.6.0
   */
  def na: DataFrameNaFunctions = new DataFrameNaFunctions(toDF())

  /**
   * Returns a [[DataFrameStatFunctions]] for working statistic functions support.
   * {{{
   *   // Finding frequent items in column with name 'a'.
   *   ds.stat.freqItems(Seq("a"))
   * }}}
   *
   * @group untypedrel
   * @since 1.6.0
   */
  def stat: DataFrameStatFunctions = new DataFrameStatFunctions(toDF())

  /**
   * Join with another [[DataFrame]].
   *
   * Behaves as an INNER JOIN and requires a subsequent join predicate.
   *
   * @param right Right side of the join operation.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_]): DataFrame = withPlan {
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
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], usingColumn: String): DataFrame = {
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
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], usingColumns: Seq[String]): DataFrame = {
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
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], usingColumns: Seq[String], joinType: String): DataFrame = {
    // Analyze the self join. The assumption is that the analyzer will disambiguate left vs right
    // by creating a new instance for one of the branch.
    val joined = sparkSession.sessionState.executePlan(
      Join(logicalPlan, right.logicalPlan, joinType = JoinType(joinType), None))
      .analyzed.asInstanceOf[Join]

    withPlan {
      Join(
        joined.left,
        joined.right,
        UsingJoin(JoinType(joinType), usingColumns.map(UnresolvedAttribute(_))),
        None)
    }
  }

  /**
   * Inner join with another [[DataFrame]], using the given join expression.
   *
   * {{{
   *   // The following two are equivalent:
   *   df1.join(df2, $"df1Key" === $"df2Key")
   *   df1.join(df2).where($"df1Key" === $"df2Key")
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], joinExprs: Column): DataFrame = join(right, joinExprs, "inner")

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
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame = {
    // Note that in this function, we introduce a hack in the case of self-join to automatically
    // resolve ambiguous join conditions into ones that might make sense [SPARK-6231].
    // Consider this case: df.join(df, df("key") === df("key"))
    // Since df("key") === df("key") is a trivially true condition, this actually becomes a
    // cartesian join. However, most likely users expect to perform a self join using "key".
    // With that assumption, this hack turns the trivially true condition into equality on join
    // keys that are resolved to both sides.

    // Trigger analysis so in the case of self-join, the analyzer will clone the plan.
    // After the cloning, left and right side will have distinct expression ids.
    val plan = withPlan(
      Join(logicalPlan, right.logicalPlan, JoinType(joinType), Some(joinExprs.expr)))
      .queryExecution.analyzed.asInstanceOf[Join]

    // If auto self join alias is disabled, return the plan.
    if (!sparkSession.sessionState.conf.dataFrameSelfJoinAutoResolveAmbiguity) {
      return withPlan(plan)
    }

    // If left/right have no output set intersection, return the plan.
    val lanalyzed = withPlan(this.logicalPlan).queryExecution.analyzed
    val ranalyzed = withPlan(right.logicalPlan).queryExecution.analyzed
    if (lanalyzed.outputSet.intersect(ranalyzed.outputSet).isEmpty) {
      return withPlan(plan)
    }

    // Otherwise, find the trivially true predicates and automatically resolves them to both sides.
    // By the time we get here, since we have already run analysis, all attributes should've been
    // resolved and become AttributeReference.
    val cond = plan.condition.map { _.transform {
      case catalyst.expressions.EqualTo(a: AttributeReference, b: AttributeReference)
          if a.sameRef(b) =>
        catalyst.expressions.EqualTo(
          withPlan(plan.left).resolve(a.name),
          withPlan(plan.right).resolve(b.name))
    }}

    withPlan {
      plan.copy(condition = cond)
    }
  }

  /**
   * Explicit cartesian join with another [[DataFrame]].
   *
   * Note that cartesian joins are very expensive without an extra filter that can be pushed down.
   *
   * @param right Right side of the join operation.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def crossJoin(right: Dataset[_]): DataFrame = withPlan {
    Join(logicalPlan, right.logicalPlan, joinType = Cross, None)
  }

  /**
   * :: Experimental ::
   * Joins this Dataset returning a [[Tuple2]] for each pair where `condition` evaluates to
   * true.
   *
   * This is similar to the relation `join` function with one important difference in the
   * result schema. Since `joinWith` preserves objects present on either side of the join, the
   * result schema is similarly nested into a tuple under the column names `_1` and `_2`.
   *
   * This type of join can be useful both for preserving type-safety with the original object
   * types as well as working with relational data where either side of the join has column
   * names in common.
   *
   * @param other Right side of the join.
   * @param condition Join expression.
   * @param joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[(T, U)] = {
    // Creates a Join node and resolve it first, to get join condition resolved, self-join resolved,
    // etc.
    val joined = sparkSession.sessionState.executePlan(
      Join(
        this.logicalPlan,
        other.logicalPlan,
        JoinType(joinType),
        Some(condition.expr))).analyzed.asInstanceOf[Join]

    // For both join side, combine all outputs into a single column and alias it with "_1" or "_2",
    // to match the schema for the encoder of the join result.
    // Note that we do this before joining them, to enable the join operator to return null for one
    // side, in cases like outer-join.
    val left = {
      val combined = if (this.exprEnc.flat) {
        assert(joined.left.output.length == 1)
        Alias(joined.left.output.head, "_1")()
      } else {
        Alias(CreateStruct(joined.left.output), "_1")()
      }
      Project(combined :: Nil, joined.left)
    }

    val right = {
      val combined = if (other.exprEnc.flat) {
        assert(joined.right.output.length == 1)
        Alias(joined.right.output.head, "_2")()
      } else {
        Alias(CreateStruct(joined.right.output), "_2")()
      }
      Project(combined :: Nil, joined.right)
    }

    // Rewrites the join condition to make the attribute point to correct column/field, after we
    // combine the outputs of each join side.
    val conditionExpr = joined.condition.get transformUp {
      case a: Attribute if joined.left.outputSet.contains(a) =>
        if (this.exprEnc.flat) {
          left.output.head
        } else {
          val index = joined.left.output.indexWhere(_.exprId == a.exprId)
          GetStructField(left.output.head, index)
        }
      case a: Attribute if joined.right.outputSet.contains(a) =>
        if (other.exprEnc.flat) {
          right.output.head
        } else {
          val index = joined.right.output.indexWhere(_.exprId == a.exprId)
          GetStructField(right.output.head, index)
        }
    }

    implicit val tuple2Encoder: Encoder[(T, U)] =
      ExpressionEncoder.tuple(this.exprEnc, other.exprEnc)

    withTypedPlan(Join(left, right, joined.joinType, Some(conditionExpr)))
  }

  /**
   * :: Experimental ::
   * Using inner equi-join to join this Dataset returning a [[Tuple2]] for each pair
   * where `condition` evaluates to true.
   *
   * @param other Right side of the join.
   * @param condition Join expression.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def joinWith[U](other: Dataset[U], condition: Column): Dataset[(T, U)] = {
    joinWith(other, condition, "inner")
  }

  /**
   * Returns a new Dataset with each partition sorted by the given expressions.
   *
   * This is the same operation as "SORT BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def sortWithinPartitions(sortCol: String, sortCols: String*): Dataset[T] = {
    sortWithinPartitions((sortCol +: sortCols).map(Column(_)) : _*)
  }

  /**
   * Returns a new Dataset with each partition sorted by the given expressions.
   *
   * This is the same operation as "SORT BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def sortWithinPartitions(sortExprs: Column*): Dataset[T] = {
    sortInternal(global = false, sortExprs)
  }

  /**
   * Returns a new Dataset sorted by the specified column, all in ascending order.
   * {{{
   *   // The following 3 are equivalent
   *   ds.sort("sortcol")
   *   ds.sort($"sortcol")
   *   ds.sort($"sortcol".asc)
   * }}}
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def sort(sortCol: String, sortCols: String*): Dataset[T] = {
    sort((sortCol +: sortCols).map(apply) : _*)
  }

  /**
   * Returns a new Dataset sorted by the given expressions. For example:
   * {{{
   *   ds.sort($"col1", $"col2".desc)
   * }}}
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def sort(sortExprs: Column*): Dataset[T] = {
    sortInternal(global = true, sortExprs)
  }

  /**
   * Returns a new Dataset sorted by the given expressions.
   * This is an alias of the `sort` function.
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def orderBy(sortCol: String, sortCols: String*): Dataset[T] = sort(sortCol, sortCols : _*)

  /**
   * Returns a new Dataset sorted by the given expressions.
   * This is an alias of the `sort` function.
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def orderBy(sortExprs: Column*): Dataset[T] = sort(sortExprs : _*)

  /**
   * Selects column based on the column name and return it as a [[Column]].
   * Note that the column name can also reference to a nested column like `a.b`.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def apply(colName: String): Column = col(colName)

  /**
   * Selects column based on the column name and return it as a [[Column]].
   * Note that the column name can also reference to a nested column like `a.b`.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def col(colName: String): Column = colName match {
    case "*" =>
      Column(ResolvedStar(queryExecution.analyzed.output))
    case _ =>
      val expr = resolve(colName)
      Column(expr)
  }

  /**
   * Returns a new Dataset with an alias set.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def as(alias: String): Dataset[T] = withTypedPlan {
    SubqueryAlias(alias, logicalPlan, None)
  }

  /**
   * (Scala-specific) Returns a new Dataset with an alias set.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def as(alias: Symbol): Dataset[T] = as(alias.name)

  /**
   * Returns a new Dataset with an alias set. Same as `as`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def alias(alias: String): Dataset[T] = as(alias)

  /**
   * (Scala-specific) Returns a new Dataset with an alias set. Same as `as`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def alias(alias: Symbol): Dataset[T] = as(alias)

  /**
   * Selects a set of column based expressions.
   * {{{
   *   ds.select($"colA", $"colB" + 1)
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def select(cols: Column*): DataFrame = withPlan {
    Project(cols.map(_.named), logicalPlan)
  }

  /**
   * Selects a set of columns. This is a variant of `select` that can only select
   * existing columns using column names (i.e. cannot construct expressions).
   *
   * {{{
   *   // The following two are equivalent:
   *   ds.select("colA", "colB")
   *   ds.select($"colA", $"colB")
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def select(col: String, cols: String*): DataFrame = select((col +: cols).map(Column(_)) : _*)

  /**
   * Selects a set of SQL expressions. This is a variant of `select` that accepts
   * SQL expressions.
   *
   * {{{
   *   // The following are equivalent:
   *   ds.selectExpr("colA", "colB as newName", "abs(colC)")
   *   ds.select(expr("colA"), expr("colB as newName"), expr("abs(colC)"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def selectExpr(exprs: String*): DataFrame = {
    select(exprs.map { expr =>
      Column(sparkSession.sessionState.sqlParser.parseExpression(expr))
    }: _*)
  }

  /**
   * :: Experimental ::
   * Returns a new Dataset by computing the given [[Column]] expression for each element.
   *
   * {{{
   *   val ds = Seq(1, 2, 3).toDS()
   *   val newDS = ds.select(expr("value + 1").as[Int])
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def select[U1](c1: TypedColumn[T, U1]): Dataset[U1] = {
    implicit val encoder = c1.encoder
    val project = Project(c1.withInputType(exprEnc, logicalPlan.output).named :: Nil,
      logicalPlan)

    if (encoder.flat) {
      new Dataset[U1](sparkSession, project, encoder)
    } else {
      // Flattens inner fields of U1
      new Dataset[Tuple1[U1]](sparkSession, project, ExpressionEncoder.tuple(encoder)).map(_._1)
    }
  }

  /**
   * Internal helper function for building typed selects that return tuples. For simplicity and
   * code reuse, we do this without the help of the type system and then use helper functions
   * that cast appropriately for the user facing interface.
   */
  protected def selectUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    val encoders = columns.map(_.encoder)
    val namedColumns =
      columns.map(_.withInputType(exprEnc, logicalPlan.output).named)
    val execution = new QueryExecution(sparkSession, Project(namedColumns, logicalPlan))
    new Dataset(sparkSession, execution, ExpressionEncoder.tuple(encoders))
  }

  /**
   * :: Experimental ::
   * Returns a new Dataset by computing the given [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)] =
    selectUntyped(c1, c2).asInstanceOf[Dataset[(U1, U2)]]

  /**
   * :: Experimental ::
   * Returns a new Dataset by computing the given [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def select[U1, U2, U3](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3]): Dataset[(U1, U2, U3)] =
    selectUntyped(c1, c2, c3).asInstanceOf[Dataset[(U1, U2, U3)]]

  /**
   * :: Experimental ::
   * Returns a new Dataset by computing the given [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def select[U1, U2, U3, U4](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4]): Dataset[(U1, U2, U3, U4)] =
    selectUntyped(c1, c2, c3, c4).asInstanceOf[Dataset[(U1, U2, U3, U4)]]

  /**
   * :: Experimental ::
   * Returns a new Dataset by computing the given [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def select[U1, U2, U3, U4, U5](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4],
      c5: TypedColumn[T, U5]): Dataset[(U1, U2, U3, U4, U5)] =
    selectUntyped(c1, c2, c3, c4, c5).asInstanceOf[Dataset[(U1, U2, U3, U4, U5)]]

  /**
   * Filters rows using the given condition.
   * {{{
   *   // The following are equivalent:
   *   peopleDs.filter($"age" > 15)
   *   peopleDs.where($"age" > 15)
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def filter(condition: Column): Dataset[T] = withTypedPlan {
    Filter(condition.expr, logicalPlan)
  }

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDs.filter("age > 15")
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def filter(conditionExpr: String): Dataset[T] = {
    filter(Column(sparkSession.sessionState.sqlParser.parseExpression(conditionExpr)))
  }

  /**
   * Filters rows using the given condition. This is an alias for `filter`.
   * {{{
   *   // The following are equivalent:
   *   peopleDs.filter($"age" > 15)
   *   peopleDs.where($"age" > 15)
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def where(condition: Column): Dataset[T] = filter(condition)

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDs.where("age > 15")
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def where(conditionExpr: String): Dataset[T] = {
    filter(Column(sparkSession.sessionState.sqlParser.parseExpression(conditionExpr)))
  }

  /**
   * Groups the Dataset using the specified columns, so we can run aggregation on them. See
   * [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns grouped by department.
   *   ds.groupBy($"department").avg()
   *
   *   // Compute the max age and average salary, grouped by department and gender.
   *   ds.groupBy($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def groupBy(cols: Column*): RelationalGroupedDataset = {
    RelationalGroupedDataset(toDF(), cols.map(_.expr), RelationalGroupedDataset.GroupByType)
  }

  /**
   * Create a multi-dimensional rollup for the current Dataset using the specified columns,
   * so we can run aggregation on them.
   * See [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns rolluped by department and group.
   *   ds.rollup($"department", $"group").avg()
   *
   *   // Compute the max age and average salary, rolluped by department and gender.
   *   ds.rollup($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def rollup(cols: Column*): RelationalGroupedDataset = {
    RelationalGroupedDataset(toDF(), cols.map(_.expr), RelationalGroupedDataset.RollupType)
  }

  /**
   * Create a multi-dimensional cube for the current Dataset using the specified columns,
   * so we can run aggregation on them.
   * See [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns cubed by department and group.
   *   ds.cube($"department", $"group").avg()
   *
   *   // Compute the max age and average salary, cubed by department and gender.
   *   ds.cube($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def cube(cols: Column*): RelationalGroupedDataset = {
    RelationalGroupedDataset(toDF(), cols.map(_.expr), RelationalGroupedDataset.CubeType)
  }

  /**
   * Groups the Dataset using the specified columns, so that we can run aggregation on them.
   * See [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * This is a variant of groupBy that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns grouped by department.
   *   ds.groupBy("department").avg()
   *
   *   // Compute the max age and average salary, grouped by department and gender.
   *   ds.groupBy($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def groupBy(col1: String, cols: String*): RelationalGroupedDataset = {
    val colNames: Seq[String] = col1 +: cols
    RelationalGroupedDataset(
      toDF(), colNames.map(colName => resolve(colName)), RelationalGroupedDataset.GroupByType)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Reduces the elements of this Dataset using the specified binary function. The given `func`
   * must be commutative and associative or the result may be non-deterministic.
   *
   * @group action
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def reduce(func: (T, T) => T): T = rdd.reduce(func)

  /**
   * :: Experimental ::
   * (Java-specific)
   * Reduces the elements of this Dataset using the specified binary function. The given `func`
   * must be commutative and associative or the result may be non-deterministic.
   *
   * @group action
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def reduce(func: ReduceFunction[T]): T = reduce(func.call(_, _))

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Returns a [[KeyValueGroupedDataset]] where the data is grouped by the given key `func`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def groupByKey[K: Encoder](func: T => K): KeyValueGroupedDataset[K, T] = {
    val inputPlan = logicalPlan
    val withGroupingKey = AppendColumns(func, inputPlan)
    val executed = sparkSession.sessionState.executePlan(withGroupingKey)

    new KeyValueGroupedDataset(
      encoderFor[K],
      encoderFor[T],
      executed,
      inputPlan.output,
      withGroupingKey.newColumns)
  }

  /**
   * :: Experimental ::
   * (Java-specific)
   * Returns a [[KeyValueGroupedDataset]] where the data is grouped by the given key `func`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def groupByKey[K](func: MapFunction[T, K], encoder: Encoder[K]): KeyValueGroupedDataset[K, T] =
    groupByKey(func.call(_))(encoder)

  /**
   * Create a multi-dimensional rollup for the current Dataset using the specified columns,
   * so we can run aggregation on them.
   * See [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * This is a variant of rollup that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns rolluped by department and group.
   *   ds.rollup("department", "group").avg()
   *
   *   // Compute the max age and average salary, rolluped by department and gender.
   *   ds.rollup($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def rollup(col1: String, cols: String*): RelationalGroupedDataset = {
    val colNames: Seq[String] = col1 +: cols
    RelationalGroupedDataset(
      toDF(), colNames.map(colName => resolve(colName)), RelationalGroupedDataset.RollupType)
  }

  /**
   * Create a multi-dimensional cube for the current Dataset using the specified columns,
   * so we can run aggregation on them.
   * See [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * This is a variant of cube that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns cubed by department and group.
   *   ds.cube("department", "group").avg()
   *
   *   // Compute the max age and average salary, cubed by department and gender.
   *   ds.cube($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def cube(col1: String, cols: String*): RelationalGroupedDataset = {
    val colNames: Seq[String] = col1 +: cols
    RelationalGroupedDataset(
      toDF(), colNames.map(colName => resolve(colName)), RelationalGroupedDataset.CubeType)
  }

  /**
   * (Scala-specific) Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg("age" -> "max", "salary" -> "avg")
   *   ds.groupBy().agg("age" -> "max", "salary" -> "avg")
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    groupBy().agg(aggExpr, aggExprs : _*)
  }

  /**
   * (Scala-specific) Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg(Map("age" -> "max", "salary" -> "avg"))
   *   ds.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def agg(exprs: Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * (Java-specific) Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg(Map("age" -> "max", "salary" -> "avg"))
   *   ds.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def agg(exprs: java.util.Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg(max($"age"), avg($"salary"))
   *   ds.groupBy().agg(max($"age"), avg($"salary"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = groupBy().agg(expr, exprs : _*)

  /**
   * Returns a new Dataset by taking the first `n` rows. The difference between this function
   * and `head` is that `head` is an action and returns an array (by triggering query execution)
   * while `limit` returns a new Dataset.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def limit(n: Int): Dataset[T] = withTypedPlan {
    Limit(Literal(n), logicalPlan)
  }

  /**
   * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
   * This is equivalent to `UNION ALL` in SQL.
   *
   * To do a SQL-style set union (that does deduplication of elements), use this function followed
   * by a [[distinct]].
   *
   * @group typedrel
   * @since 2.0.0
   */
  @deprecated("use union()", "2.0.0")
  def unionAll(other: Dataset[T]): Dataset[T] = union(other)

  /**
   * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
   * This is equivalent to `UNION ALL` in SQL.
   *
   * To do a SQL-style set union (that does deduplication of elements), use this function followed
   * by a [[distinct]].
   *
   * @group typedrel
   * @since 2.0.0
   */
  def union(other: Dataset[T]): Dataset[T] = withTypedPlan {
    // This breaks caching, but it's usually ok because it addresses a very specific use case:
    // using union to union many files or partitions.
    CombineUnions(Union(logicalPlan, other.logicalPlan))
  }

  /**
   * Returns a new Dataset containing rows only in both this Dataset and another Dataset.
   * This is equivalent to `INTERSECT` in SQL.
   *
   * Note that, equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def intersect(other: Dataset[T]): Dataset[T] = withTypedPlan {
    Intersect(logicalPlan, other.logicalPlan)
  }

  /**
   * Returns a new Dataset containing rows in this Dataset but not in another Dataset.
   * This is equivalent to `EXCEPT` in SQL.
   *
   * Note that, equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def except(other: Dataset[T]): Dataset[T] = withTypedPlan {
    Except(logicalPlan, other.logicalPlan)
  }

  /**
   * Returns a new Dataset by sampling a fraction of rows.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   * @param seed Seed for sampling.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] = {
    require(fraction >= 0,
      s"Fraction must be nonnegative, but got ${fraction}")

    withTypedPlan {
      Sample(0.0, fraction, withReplacement, seed, logicalPlan)()
    }
  }

  /**
   * Returns a new Dataset by sampling a fraction of rows, using a random seed.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def sample(withReplacement: Boolean, fraction: Double): Dataset[T] = {
    sample(withReplacement, fraction, Utils.random.nextLong)
  }

  /**
   * Randomly splits this Dataset with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   *
   * For Java API, use [[randomSplitAsList]].
   *
   * @group typedrel
   * @since 2.0.0
   */
  def randomSplit(weights: Array[Double], seed: Long): Array[Dataset[T]] = {
    require(weights.forall(_ >= 0),
      s"Weights must be nonnegative, but got ${weights.mkString("[", ",", "]")}")
    require(weights.sum > 0,
      s"Sum of weights must be positive, but got ${weights.mkString("[", ",", "]")}")

    // It is possible that the underlying dataframe doesn't guarantee the ordering of rows in its
    // constituent partitions each time a split is materialized which could result in
    // overlapping splits. To prevent this, we explicitly sort each input partition to make the
    // ordering deterministic.
    // MapType cannot be sorted.
    val sorted = Sort(logicalPlan.output.filterNot(_.dataType.isInstanceOf[MapType])
      .map(SortOrder(_, Ascending)), global = false, logicalPlan)
    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights.sliding(2).map { x =>
      new Dataset[T](
        sparkSession, Sample(x(0), x(1), withReplacement = false, seed, sorted)(), encoder)
    }.toArray
  }

  /**
   * Returns a Java list that contains randomly split Dataset with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def randomSplitAsList(weights: Array[Double], seed: Long): java.util.List[Dataset[T]] = {
    val values = randomSplit(weights, seed)
    java.util.Arrays.asList(values : _*)
  }

  /**
   * Randomly splits this Dataset with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @group typedrel
   * @since 2.0.0
   */
  def randomSplit(weights: Array[Double]): Array[Dataset[T]] = {
    randomSplit(weights, Utils.random.nextLong)
  }

  /**
   * Randomly splits this Dataset with the provided weights. Provided for the Python Api.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   */
  private[spark] def randomSplit(weights: List[Double], seed: Long): Array[Dataset[T]] = {
    randomSplit(weights.toArray, seed)
  }

  /**
   * (Scala-specific) Returns a new Dataset where each row has been expanded to zero or more
   * rows by the provided function. This is similar to a `LATERAL VIEW` in HiveQL. The columns of
   * the input row are implicitly joined with each row that is output by the function.
   *
   * Given that this is deprecated, as an alternative, you can explode columns either using
   * `functions.explode()` or `flatMap()`. The following example uses these alternatives to count
   * the number of books that contain a given word:
   *
   * {{{
   *   case class Book(title: String, words: String)
   *   val ds: Dataset[Book]
   *
   *   val allWords = ds.select('title, explode(split('words, " ")).as("word"))
   *
   *   val bookCountPerWord = allWords.groupBy("word").agg(countDistinct("title"))
   * }}}
   *
   * Using `flatMap()` this can similarly be exploded as:
   *
   * {{{
   *   ds.flatMap(_.words.split(" "))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @deprecated("use flatMap() or select() with functions.explode() instead", "2.0.0")
  def explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A]): DataFrame = {
    val elementSchema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]

    val convert = CatalystTypeConverters.createToCatalystConverter(elementSchema)

    val rowFunction =
      f.andThen(_.map(convert(_).asInstanceOf[InternalRow]))
    val generator = UserDefinedGenerator(elementSchema, rowFunction, input.map(_.expr))

    withPlan {
      Generate(generator, join = true, outer = false,
        qualifier = None, generatorOutput = Nil, logicalPlan)
    }
  }

  /**
   * (Scala-specific) Returns a new Dataset where a single column has been expanded to zero
   * or more rows by the provided function. This is similar to a `LATERAL VIEW` in HiveQL. All
   * columns of the input row are implicitly joined with each value that is output by the function.
   *
   * Given that this is deprecated, as an alternative, you can explode columns either using
   * `functions.explode()`:
   *
   * {{{
   *   ds.select(explode(split('words, " ")).as("word"))
   * }}}
   *
   * or `flatMap()`:
   *
   * {{{
   *   ds.flatMap(_.words.split(" "))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @deprecated("use flatMap() or select() with functions.explode() instead", "2.0.0")
  def explode[A, B : TypeTag](inputColumn: String, outputColumn: String)(f: A => TraversableOnce[B])
    : DataFrame = {
    val dataType = ScalaReflection.schemaFor[B].dataType
    val attributes = AttributeReference(outputColumn, dataType)() :: Nil
    // TODO handle the metadata?
    val elementSchema = attributes.toStructType

    def rowFunction(row: Row): TraversableOnce[InternalRow] = {
      val convert = CatalystTypeConverters.createToCatalystConverter(dataType)
      f(row(0).asInstanceOf[A]).map(o => InternalRow(convert(o)))
    }
    val generator = UserDefinedGenerator(elementSchema, rowFunction, apply(inputColumn).expr :: Nil)

    withPlan {
      Generate(generator, join = true, outer = false,
        qualifier = None, generatorOutput = Nil, logicalPlan)
    }
  }

  /**
   * Returns a new Dataset by adding a column or replacing the existing column that has
   * the same name.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def withColumn(colName: String, col: Column): DataFrame = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    val output = queryExecution.analyzed.output
    val shouldReplace = output.exists(f => resolver(f.name, colName))
    if (shouldReplace) {
      val columns = output.map { field =>
        if (resolver(field.name, colName)) {
          col.as(colName)
        } else {
          Column(field)
        }
      }
      select(columns : _*)
    } else {
      select(Column("*"), col.as(colName))
    }
  }

  /**
   * Returns a new Dataset by adding a column with metadata.
   */
  private[spark] def withColumn(colName: String, col: Column, metadata: Metadata): DataFrame = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    val output = queryExecution.analyzed.output
    val shouldReplace = output.exists(f => resolver(f.name, colName))
    if (shouldReplace) {
      val columns = output.map { field =>
        if (resolver(field.name, colName)) {
          col.as(colName, metadata)
        } else {
          Column(field)
        }
      }
      select(columns : _*)
    } else {
      select(Column("*"), col.as(colName, metadata))
    }
  }

  /**
   * Returns a new Dataset with a column renamed.
   * This is a no-op if schema doesn't contain existingName.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def withColumnRenamed(existingName: String, newName: String): DataFrame = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    val output = queryExecution.analyzed.output
    val shouldRename = output.exists(f => resolver(f.name, existingName))
    if (shouldRename) {
      val columns = output.map { col =>
        if (resolver(col.name, existingName)) {
          Column(col).as(newName)
        } else {
          Column(col)
        }
      }
      select(columns : _*)
    } else {
      toDF()
    }
  }

  /**
   * Returns a new Dataset with a column dropped. This is a no-op if schema doesn't contain
   * column name.
   *
   * This method can only be used to drop top level columns. the colName string is treated
   * literally without further interpretation.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def drop(colName: String): DataFrame = {
    drop(Seq(colName) : _*)
  }

  /**
   * Returns a new Dataset with columns dropped.
   * This is a no-op if schema doesn't contain column name(s).
   *
   * This method can only be used to drop top level columns. the colName string is treated literally
   * without further interpretation.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def drop(colNames: String*): DataFrame = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    val allColumns = queryExecution.analyzed.output
    val remainingCols = allColumns.filter { attribute =>
      colNames.forall(n => !resolver(attribute.name, n))
    }.map(attribute => Column(attribute))
    if (remainingCols.size == allColumns.size) {
      toDF()
    } else {
      this.select(remainingCols: _*)
    }
  }

  /**
   * Returns a new Dataset with a column dropped.
   * This version of drop accepts a [[Column]] rather than a name.
   * This is a no-op if the Dataset doesn't have a column
   * with an equivalent expression.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def drop(col: Column): DataFrame = {
    val expression = col match {
      case Column(u: UnresolvedAttribute) =>
        queryExecution.analyzed.resolveQuoted(
          u.name, sparkSession.sessionState.analyzer.resolver).getOrElse(u)
      case Column(expr: Expression) => expr
    }
    val attrs = this.logicalPlan.output
    val colsAfterDrop = attrs.filter { attr =>
      attr != expression
    }.map(attr => Column(attr))
    select(colsAfterDrop : _*)
  }

  /**
   * Returns a new Dataset that contains only the unique rows from this Dataset.
   * This is an alias for `distinct`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def dropDuplicates(): Dataset[T] = dropDuplicates(this.columns)

  /**
   * (Scala-specific) Returns a new Dataset with duplicate rows removed, considering only
   * the subset of columns.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def dropDuplicates(colNames: Seq[String]): Dataset[T] = withTypedPlan {
    val resolver = sparkSession.sessionState.analyzer.resolver
    val allColumns = queryExecution.analyzed.output
    val groupCols = colNames.flatMap { colName =>
      // It is possibly there are more than one columns with the same name,
      // so we call filter instead of find.
      val cols = allColumns.filter(col => resolver(col.name, colName))
      if (cols.isEmpty) {
        throw new AnalysisException(
          s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
      }
      cols
    }
    val groupColExprIds = groupCols.map(_.exprId)
    val aggCols = logicalPlan.output.map { attr =>
      if (groupColExprIds.contains(attr.exprId)) {
        attr
      } else {
        // Removing duplicate rows should not change output attributes. We should keep
        // the original exprId of the attribute. Otherwise, to select a column in original
        // dataset will cause analysis exception due to unresolved attribute.
        Alias(new First(attr).toAggregateExpression(), attr.name)(exprId = attr.exprId)
      }
    }
    Aggregate(groupCols, aggCols, logicalPlan)
  }

  /**
   * Returns a new Dataset with duplicate rows removed, considering only
   * the subset of columns.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def dropDuplicates(colNames: Array[String]): Dataset[T] = dropDuplicates(colNames.toSeq)

  /**
   * Returns a new [[Dataset]] with duplicate rows removed, considering only
   * the subset of columns.
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def dropDuplicates(col1: String, cols: String*): Dataset[T] = {
    val colNames: Seq[String] = col1 +: cols
    dropDuplicates(colNames)
  }

  /**
   * Computes statistics for numeric and string columns, including count, mean, stddev, min, and
   * max. If no columns are given, this function computes statistics for all numerical or string
   * columns.
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting Dataset. If you want to
   * programmatically compute summary statistics, use the `agg` function instead.
   *
   * {{{
   *   ds.describe("age", "height").show()
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
   * @since 1.6.0
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

    val outputCols =
      (if (cols.isEmpty) aggregatableColumns.map(usePrettyExpression(_).sql) else cols).toList

    val ret: Seq[Row] = if (outputCols.nonEmpty) {
      val aggExprs = statistics.flatMap { case (_, colToAgg) =>
        outputCols.map(c => Column(Cast(colToAgg(Column(c).expr), StringType)).as(c))
      }

      val row = groupBy().agg(aggExprs.head, aggExprs.tail: _*).head().toSeq

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
    // `toArray` forces materialization to make the seq serializable
    LocalRelation.fromExternalRows(schema, ret.toArray.toSeq)
  }

  /**
   * Returns the first `n` rows.
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   *
   * @group action
   * @since 1.6.0
   */
  def head(n: Int): Array[T] = withTypedCallback("head", limit(n)) { df =>
    df.collect(needCallback = false)
  }

  /**
   * Returns the first row.
   * @group action
   * @since 1.6.0
   */
  def head(): T = head(1).head

  /**
   * Returns the first row. Alias for head().
   * @group action
   * @since 1.6.0
   */
  def first(): T = head()

  /**
   * Concise syntax for chaining custom transformations.
   * {{{
   *   def featurize(ds: Dataset[T]): Dataset[U] = ...
   *
   *   ds
   *     .transform(featurize)
   *     .transform(...)
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = t(this)

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Returns a new Dataset that only contains elements where `func` returns `true`.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def filter(func: T => Boolean): Dataset[T] = {
    withTypedPlan(TypedFilter(func, logicalPlan))
  }

  /**
   * :: Experimental ::
   * (Java-specific)
   * Returns a new Dataset that only contains elements where `func` returns `true`.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def filter(func: FilterFunction[T]): Dataset[T] = {
    withTypedPlan(TypedFilter(func, logicalPlan))
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Returns a new Dataset that contains the result of applying `func` to each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def map[U : Encoder](func: T => U): Dataset[U] = withTypedPlan {
    MapElements[T, U](func, logicalPlan)
  }

  /**
   * :: Experimental ::
   * (Java-specific)
   * Returns a new Dataset that contains the result of applying `func` to each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def map[U](func: MapFunction[T, U], encoder: Encoder[U]): Dataset[U] = {
    implicit val uEnc = encoder
    withTypedPlan(MapElements[T, U](func, logicalPlan))
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Returns a new Dataset that contains the result of applying `func` to each partition.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def mapPartitions[U : Encoder](func: Iterator[T] => Iterator[U]): Dataset[U] = {
    new Dataset[U](
      sparkSession,
      MapPartitions[T, U](func, logicalPlan),
      implicitly[Encoder[U]])
  }

  /**
   * :: Experimental ::
   * (Java-specific)
   * Returns a new Dataset that contains the result of applying `f` to each partition.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def mapPartitions[U](f: MapPartitionsFunction[T, U], encoder: Encoder[U]): Dataset[U] = {
    val func: (Iterator[T]) => Iterator[U] = x => f.call(x.asJava).asScala
    mapPartitions(func)(encoder)
  }

  /**
   * Returns a new [[DataFrame]] that contains the result of applying a serialized R function
   * `func` to each partition.
   */
  private[sql] def mapPartitionsInR(
      func: Array[Byte],
      packageNames: Array[Byte],
      broadcastVars: Array[Broadcast[Object]],
      schema: StructType): DataFrame = {
    val rowEncoder = encoder.asInstanceOf[ExpressionEncoder[Row]]
    Dataset.ofRows(
      sparkSession,
      MapPartitionsInR(func, packageNames, broadcastVars, schema, rowEncoder, logicalPlan))
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Returns a new Dataset by first applying a function to all elements of this Dataset,
   * and then flattening the results.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def flatMap[U : Encoder](func: T => TraversableOnce[U]): Dataset[U] =
    mapPartitions(_.flatMap(func))

  /**
   * :: Experimental ::
   * (Java-specific)
   * Returns a new Dataset by first applying a function to all elements of this Dataset,
   * and then flattening the results.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def flatMap[U](f: FlatMapFunction[T, U], encoder: Encoder[U]): Dataset[U] = {
    val func: (T) => Iterator[U] = x => f.call(x).asScala
    flatMap(func)(encoder)
  }

  /**
   * Applies a function `f` to all rows.
   *
   * @group action
   * @since 1.6.0
   */
  def foreach(f: T => Unit): Unit = withNewExecutionId {
    rdd.foreach(f)
  }

  /**
   * (Java-specific)
   * Runs `func` on each element of this Dataset.
   *
   * @group action
   * @since 1.6.0
   */
  def foreach(func: ForeachFunction[T]): Unit = foreach(func.call(_))

  /**
   * Applies a function `f` to each partition of this Dataset.
   *
   * @group action
   * @since 1.6.0
   */
  def foreachPartition(f: Iterator[T] => Unit): Unit = withNewExecutionId {
    rdd.foreachPartition(f)
  }

  /**
   * (Java-specific)
   * Runs `func` on each partition of this Dataset.
   *
   * @group action
   * @since 1.6.0
   */
  def foreachPartition(func: ForeachPartitionFunction[T]): Unit =
    foreachPartition(it => func.call(it.asJava))

  /**
   * Returns the first `n` rows in the Dataset.
   *
   * Running take requires moving data into the application's driver process, and doing so with
   * a very large `n` can crash the driver process with OutOfMemoryError.
   *
   * @group action
   * @since 1.6.0
   */
  def take(n: Int): Array[T] = head(n)

  /**
   * Returns the first `n` rows in the Dataset as a list.
   *
   * Running take requires moving data into the application's driver process, and doing so with
   * a very large `n` can crash the driver process with OutOfMemoryError.
   *
   * @group action
   * @since 1.6.0
   */
  def takeAsList(n: Int): java.util.List[T] = java.util.Arrays.asList(take(n) : _*)

  /**
   * Returns an array that contains all of [[Row]]s in this Dataset.
   *
   * Running collect requires moving all the data into the application's driver process, and
   * doing so on a very large dataset can crash the driver process with OutOfMemoryError.
   *
   * For Java API, use [[collectAsList]].
   *
   * @group action
   * @since 1.6.0
   */
  def collect(): Array[T] = collect(needCallback = true)

  /**
   * Returns a Java list that contains all of [[Row]]s in this Dataset.
   *
   * Running collect requires moving all the data into the application's driver process, and
   * doing so on a very large dataset can crash the driver process with OutOfMemoryError.
   *
   * @group action
   * @since 1.6.0
   */
  def collectAsList(): java.util.List[T] = withCallback("collectAsList", toDF()) { _ =>
    withNewExecutionId {
      val values = queryExecution.executedPlan.executeCollect().map(boundEnc.fromRow)
      java.util.Arrays.asList(values : _*)
    }
  }

  private def collect(needCallback: Boolean): Array[T] = {
    def execute(): Array[T] = withNewExecutionId {
      queryExecution.executedPlan.executeCollect().map(boundEnc.fromRow)
    }

    if (needCallback) {
      withCallback("collect", toDF())(_ => execute())
    } else {
      execute()
    }
  }

  /**
   * Return an iterator that contains all of [[Row]]s in this Dataset.
   *
   * The iterator will consume as much memory as the largest partition in this Dataset.
   *
   * Note: this results in multiple Spark jobs, and if the input Dataset is the result
   * of a wide transformation (e.g. join with different partitioners), to avoid
   * recomputing the input Dataset should be cached first.
   *
   * @group action
   * @since 2.0.0
   */
  def toLocalIterator(): java.util.Iterator[T] = withCallback("toLocalIterator", toDF()) { _ =>
    withNewExecutionId {
      queryExecution.executedPlan.executeToIterator().map(boundEnc.fromRow).asJava
    }
  }

  /**
   * Returns the number of rows in the Dataset.
   * @group action
   * @since 1.6.0
   */
  def count(): Long = withCallback("count", groupBy().count()) { df =>
    df.collect(needCallback = false).head.getLong(0)
  }

  /**
   * Returns a new Dataset that has exactly `numPartitions` partitions.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def repartition(numPartitions: Int): Dataset[T] = withTypedPlan {
    Repartition(numPartitions, shuffle = true, logicalPlan)
  }

  /**
   * Returns a new Dataset partitioned by the given partitioning expressions into
   * `numPartitions`. The resulting Dataset is hash partitioned.
   *
   * This is the same operation as "DISTRIBUTE BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def repartition(numPartitions: Int, partitionExprs: Column*): Dataset[T] = withTypedPlan {
    RepartitionByExpression(partitionExprs.map(_.expr), logicalPlan, Some(numPartitions))
  }

  /**
   * Returns a new Dataset partitioned by the given partitioning expressions, using
   * `spark.sql.shuffle.partitions` as number of partitions.
   * The resulting Dataset is hash partitioned.
   *
   * This is the same operation as "DISTRIBUTE BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def repartition(partitionExprs: Column*): Dataset[T] = withTypedPlan {
    RepartitionByExpression(partitionExprs.map(_.expr), logicalPlan, numPartitions = None)
  }

  /**
   * Returns a new Dataset that has exactly `numPartitions` partitions.
   * Similar to coalesce defined on an [[RDD]], this operation results in a narrow dependency, e.g.
   * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
   * the 100 new partitions will claim 10 of the current partitions.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def coalesce(numPartitions: Int): Dataset[T] = withTypedPlan {
    Repartition(numPartitions, shuffle = false, logicalPlan)
  }

  /**
   * Returns a new Dataset that contains only the unique rows from this Dataset.
   * This is an alias for `dropDuplicates`.
   *
   * Note that, equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def distinct(): Dataset[T] = dropDuplicates()

  /**
   * Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
   *
   * @group basic
   * @since 1.6.0
   */
  def persist(): this.type = {
    sparkSession.sharedState.cacheManager.cacheQuery(this)
    this
  }

  /**
   * Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
   *
   * @group basic
   * @since 1.6.0
   */
  def cache(): this.type = persist()

  def cached: Dataset[T] = {
    sparkSession.sharedState.cacheManager.cacheQuery(this)
    Dataset(sparkSession, sparkSession.sharedState.cacheManager.useCachedData(this.logicalPlan))
  }

  /**
   * Persist this Dataset with the given storage level.
   * @param newLevel One of: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`,
   *                 `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`,
   *                 `MEMORY_AND_DISK_2`, etc.
   *
   * @group basic
   * @since 1.6.0
   */
  def persist(newLevel: StorageLevel): this.type = {
    sparkSession.sharedState.cacheManager.cacheQuery(this, None, newLevel)
    this
  }

  /**
   * Mark the Dataset as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted.
   *
   * @group basic
   * @since 1.6.0
   */
  def unpersist(blocking: Boolean): this.type = {
    sparkSession.sharedState.cacheManager.uncacheQuery(this, blocking)
    this
  }

  /**
   * Mark the Dataset as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @group basic
   * @since 1.6.0
   */
  def unpersist(): this.type = unpersist(blocking = false)

  /**
   * Represents the content of the Dataset as an [[RDD]] of [[T]].
   *
   * @group basic
   * @since 1.6.0
   */
  lazy val rdd: RDD[T] = {
    val objectType = exprEnc.deserializer.dataType
    val deserialized = CatalystSerde.deserialize[T](logicalPlan)
    sparkSession.sessionState.executePlan(deserialized).toRdd.mapPartitions { rows =>
      rows.map(_.get(0, objectType).asInstanceOf[T])
    }
  }

  /**
   * Returns the content of the Dataset as a [[JavaRDD]] of [[T]]s.
   * @group basic
   * @since 1.6.0
   */
  def toJavaRDD: JavaRDD[T] = rdd.toJavaRDD()

  /**
   * Returns the content of the Dataset as a [[JavaRDD]] of [[T]]s.
   * @group basic
   * @since 1.6.0
   */
  def javaRDD: JavaRDD[T] = toJavaRDD

  /**
   * Registers this Dataset as a temporary table using the given name. The lifetime of this
   * temporary table is tied to the [[SparkSession]] that was used to create this Dataset.
   *
   * @group basic
   * @since 1.6.0
   */
  @deprecated("Use createOrReplaceTempView(viewName) instead.", "2.0.0")
  def registerTempTable(tableName: String): Unit = {
    createOrReplaceTempView(tableName)
  }

  /**
   * Creates a local temporary view using the given name. The lifetime of this
   * temporary view is tied to the [[SparkSession]] that was used to create this Dataset.
   *
   * Local temporary view is session-scoped. Its lifetime is the lifetime of the session that
   * created it, i.e. it will be automatically dropped when the session terminates. It's not
   * tied to any databases, i.e. we can't use `db1.view1` to reference a local temporary view.
   *
   * @throws AnalysisException if the view name already exists
   *
   * @group basic
   * @since 2.0.0
   */
  @throws[AnalysisException]
  def createTempView(viewName: String): Unit = withPlan {
    createTempViewCommand(viewName, replace = false, global = false)
  }



  /**
   * Creates a local temporary view using the given name. The lifetime of this
   * temporary view is tied to the [[SparkSession]] that was used to create this Dataset.
   *
   * @group basic
   * @since 2.0.0
   */
  def createOrReplaceTempView(viewName: String): Unit = withPlan {
    createTempViewCommand(viewName, replace = true, global = false)
  }

  /**
   * Creates a global temporary view using the given name. The lifetime of this
   * temporary view is tied to this Spark application.
   *
   * Global temporary view is cross-session. Its lifetime is the lifetime of the Spark application,
   * i.e. it will be automatically dropped when the application terminates. It's tied to a system
   * preserved database `_global_temp`, and we must use the qualified name to refer a global temp
   * view, e.g. `SELECT * FROM _global_temp.view1`.
   *
   * @throws AnalysisException if the view name already exists
   *
   * @group basic
   * @since 2.1.0
   */
  @throws[AnalysisException]
  def createGlobalTempView(viewName: String): Unit = withPlan {
    createTempViewCommand(viewName, replace = false, global = true)
  }

  private def createTempViewCommand(
      viewName: String,
      replace: Boolean,
      global: Boolean): CreateViewCommand = {
    val viewType = if (global) GlobalTempView else LocalTempView
    CreateViewCommand(
      name = sparkSession.sessionState.sqlParser.parseTableIdentifier(viewName),
      userSpecifiedColumns = Nil,
      comment = None,
      properties = Map.empty,
      originalText = None,
      child = logicalPlan,
      allowExisting = false,
      replace = replace,
      viewType = viewType)
  }

  /**
   * Interface for saving the content of the non-streaming Dataset out into external storage.
   *
   * @group basic
   * @since 1.6.0
   */
  def write: DataFrameWriter[T] = {
    if (isStreaming) {
      logicalPlan.failAnalysis(
        "'write' can not be called on streaming Dataset/DataFrame")
    }
    new DataFrameWriter[T](this)
  }

  /**
   * :: Experimental ::
   * Interface for saving the content of the streaming Dataset out into external storage.
   *
   * @group basic
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def writeStream: DataStreamWriter[T] = {
    if (!isStreaming) {
      logicalPlan.failAnalysis(
        "'writeStream' can be called only on streaming Dataset/DataFrame")
    }
    new DataStreamWriter[T](this)
  }


  /**
   * Returns the content of the Dataset as a Dataset of JSON strings.
   * @since 2.0.0
   */
  def toJSON: Dataset[String] = {
    val rowSchema = this.schema
    val rdd: RDD[String] = queryExecution.toRdd.mapPartitions { iter =>
      val writer = new CharArrayWriter()
      // create the Generator without separator inserted between 2 records
      val gen = new JacksonGenerator(rowSchema, writer)

      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): String = {
          gen.write(iter.next())
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
    import sparkSession.implicits.newStringEncoder
    sparkSession.createDataset(rdd)
  }

  /**
   * Returns a best-effort snapshot of the files that compose this Dataset. This method simply
   * asks each constituent BaseRelation for its respective files and takes the union of all results.
   * Depending on the source relations, this may not find all input files. Duplicates are removed.
   *
   * @group basic
   * @since 2.0.0
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
  // For Python API
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Converts a JavaRDD to a PythonRDD.
   */
  private[sql] def javaToPython: JavaRDD[Array[Byte]] = {
    val structType = schema  // capture it for closure
    val rdd = queryExecution.toRdd.map(EvaluatePython.toJava(_, structType))
    EvaluatePython.javaToPython(rdd)
  }

  private[sql] def collectToPython(): Int = {
    EvaluatePython.registerPicklers()
    withNewExecutionId {
      val toJava: (Any) => Any = EvaluatePython.toJava(_, schema)
      val iter = new SerDeUtil.AutoBatchedPickler(
        queryExecution.executedPlan.executeCollect().iterator.map(toJava))
      PythonRDD.serveIterator(iter, "serve-DataFrame")
    }
  }

  private[sql] def toPythonIterator(): Int = {
    withNewExecutionId {
      PythonRDD.toLocalIteratorAndServe(javaToPython.rdd)
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  // Private Helpers
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Wrap a Dataset action to track all Spark jobs in the body so that we can connect them with
   * an execution.
   */
  private[sql] def withNewExecutionId[U](body: => U): U = {
    SQLExecution.withNewExecutionId(sparkSession, queryExecution)(body)
  }

  /**
   * Wrap a Dataset action to track the QueryExecution and time cost, then report to the
   * user-registered callback functions.
   */
  private def withCallback[U](name: String, df: DataFrame)(action: DataFrame => U) = {
    try {
      df.queryExecution.executedPlan.foreach { plan =>
        plan.resetMetrics()
      }
      val start = System.nanoTime()
      val result = action(df)
      val end = System.nanoTime()
      sparkSession.listenerManager.onSuccess(name, df.queryExecution, end - start)
      result
    } catch {
      case e: Exception =>
        sparkSession.listenerManager.onFailure(name, df.queryExecution, e)
        throw e
    }
  }

  private def withTypedCallback[A, B](name: String, ds: Dataset[A])(action: Dataset[A] => B) = {
    try {
      ds.queryExecution.executedPlan.foreach { plan =>
        plan.resetMetrics()
      }
      val start = System.nanoTime()
      val result = action(ds)
      val end = System.nanoTime()
      sparkSession.listenerManager.onSuccess(name, ds.queryExecution, end - start)
      result
    } catch {
      case e: Exception =>
        sparkSession.listenerManager.onFailure(name, ds.queryExecution, e)
        throw e
    }
  }

  private def sortInternal(global: Boolean, sortExprs: Seq[Column]): Dataset[T] = {
    val sortOrder: Seq[SortOrder] = sortExprs.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    withTypedPlan {
      Sort(sortOrder, global = global, logicalPlan)
    }
  }

  /** A convenient function to wrap a logical plan and produce a DataFrame. */
  @inline private def withPlan(logicalPlan: => LogicalPlan): DataFrame = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  /** A convenient function to wrap a logical plan and produce a Dataset. */
  @inline private def withTypedPlan[U : Encoder](logicalPlan: => LogicalPlan): Dataset[U] = {
    Dataset(sparkSession, logicalPlan)
  }
}
