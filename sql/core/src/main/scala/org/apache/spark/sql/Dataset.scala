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

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.{ResolvedStar, UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.ExplainCommand
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.Queryable
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.CreateTableUsingAsSelect
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/**
 * :: Experimental ::
 * A [[Dataset]] is a strongly typed collection of objects that can be transformed in parallel
 * using functional or relational operations.
 *
 * A [[Dataset]] differs from an [[RDD]] in the following ways:
 *  - Internally, a [[Dataset]] is represented by a Catalyst logical plan and the data is stored
 *    in the encoded form.  This representation allows for additional logical operations and
 *    enables many operations (sorting, shuffling, etc.) to be performed without deserializing to
 *    an object.
 *  - The creation of a [[Dataset]] requires the presence of an explicit [[Encoder]] that can be
 *    used to serialize the object into a binary format.  Encoders are also capable of mapping the
 *    schema of a given object to the Spark SQL type system.  In contrast, RDDs rely on runtime
 *    reflection based serialization. Operations that change the type of object stored in the
 *    dataset also need an encoder for the new type.
 *
 * A [[Dataset]] can be thought of as a specialized DataFrame, where the elements map to a specific
 * JVM object type, instead of to a generic [[Row]] container. A DataFrame can be transformed into
 * specific Dataset by calling `df.as[ElementType]`.  Similarly you can transform a strongly-typed
 * [[Dataset]] to a generic DataFrame by calling `ds.toDF()`.
 *
 * COMPATIBILITY NOTE: Long term we plan to make [[DataFrame]] extend `Dataset[Row]`.  However,
 * making this change to the class hierarchy would break the function signatures for the existing
 * functional operations (map, flatMap, etc).  As such, this class should be considered a preview
 * of the final API.  Changes will be made to the interface after Spark 1.6.
 *
 * @since 1.6.0
 */
@Experimental
class Dataset[T] private[sql](
    @transient override val sqlContext: SQLContext,
    @transient override val queryExecution: QueryExecution,
    tEncoder: Encoder[T]) extends Queryable with Serializable with Logging {

  import sqlContext.implicits._

  @transient protected[sql] val logicalPlan: LogicalPlan = queryExecution.logical match {
    // For various commands (like DDL) and queries with side effects, we force query optimization to
    // happen right away to let these side effects take place eagerly.
    case _: logical.Command |
         _: logical.InsertIntoTable |
         _: CreateTableUsingAsSelect =>
      LogicalRDD(queryExecution.analyzed.output, queryExecution.toRdd)(sqlContext)
    case _ =>
      queryExecution.analyzed
  }

  /**
   * An unresolved version of the internal encoder for the type of this [[Dataset]].  This one is
   * marked implicit so that we can use it when constructing new [[Dataset]] objects that have the
   * same object type (that will be possibly resolved to a different schema).
   */
  private[sql] implicit val unresolvedTEncoder: ExpressionEncoder[T] = encoderFor(tEncoder)
  unresolvedTEncoder.validate(logicalPlan.output)

  /** The encoder for this [[Dataset]] that has been resolved to its output schema. */
  private[sql] val resolvedTEncoder: ExpressionEncoder[T] =
    unresolvedTEncoder.resolve(logicalPlan.output, OuterScopes.outerScopes)

  /**
   * The encoder where the expressions used to construct an object from an input row have been
   * bound to the ordinals of this [[Dataset]]'s output schema.
   */
  private[sql] val boundTEncoder = resolvedTEncoder.bind(logicalPlan.output)

  def isLocal: Boolean = logicalPlan.isInstanceOf[LocalRelation]

  private implicit def classTag = resolvedTEncoder.clsTag

  private[sql] def this(sqlContext: SQLContext, plan: LogicalPlan)(implicit encoder: Encoder[T]) =
    this(sqlContext, new QueryExecution(sqlContext, plan), encoder)

  protected[sql] def resolve(colName: String): NamedExpression = {
    queryExecution.analyzed.resolveQuoted(colName, sqlContext.analyzer.resolver).getOrElse {
      throw new AnalysisException(
        s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
    }
  }

  /**
   * Returns the schema of the encoded form of the objects in this [[Dataset]].
   * @since 1.6.0
   */
  override def schema: StructType = resolvedTEncoder.schema

  /**
   * Returns all column names and their data types as an array.
   *
   * @group basic
   * @since 2.0.0
   */
  def dtypes: Array[(String, String)] = schema.fields.map { field =>
    (field.name, field.dataType.toString)
  }

  /**
   * Returns all column names as an array.
   *
   * @group basic
   * @since 2.0.0
   */
  def columns: Array[String] = schema.fields.map(_.name)

  /**
   * Prints the schema of the underlying [[Dataset]] to the console in a nice tree format.
   * @since 1.6.0
   */
  override def printSchema(): Unit = schema.printTreeString()

  /**
   * Prints the plans (logical and physical) to the console for debugging purposes.
   * @since 1.6.0
   */
  override def explain(extended: Boolean): Unit = {
    val explain = ExplainCommand(queryExecution.logical, extended = extended)
    sqlContext.executePlan(explain).executedPlan.executeCollect().foreach {
      // scalastyle:off println
      r => println(r.getString(0))
      // scalastyle:on println
    }
  }

  /**
   * Prints the physical plan to the console for debugging purposes.
   * @since 1.6.0
   */
  override def explain(): Unit = explain(extended = false)

  /**
   * Selects column based on the column name and return it as a [[Column]].
   * Note that the column name can also reference to a nested column like `a.b`.
   *
   * @group dfops
   * @since 1.3.0
   */
  def col(colName: String): Column = colName match {
    case "*" =>
      Column(ResolvedStar(queryExecution.analyzed.output))
    case _ =>
      val expr = resolve(colName)
      Column(expr)
  }

  /* ************* *
   *  Conversions  *
   * ************* */

  /**
   * Selects column based on the column name and return it as a [[Column]].
   * Note that the column name can also reference to a nested column like `a.b`.
   *
   * @group dfops
   * @since 1.3.0
   */
  def apply(colName: String): Column = col(colName)

  /**
   * Returns a new [[Dataset]] where each record has been mapped on to the specified type.  The
   * method used to map columns depend on the type of `U`:
   *  - When `U` is a class, fields for the class will be mapped to columns of the same name
   *    (case sensitivity is determined by `spark.sql.caseSensitive`)
   *  - When `U` is a tuple, the columns will be be mapped by ordinal (i.e. the first column will
   *    be assigned to `_1`).
   *  - When `U` is a primitive type (i.e. String, Int, etc). then the first column of the
   *    [[DataFrame]] will be used.
   *
   * If the schema of the [[DataFrame]] does not match the desired `U` type, you can use `select`
   * along with `alias` or `as` to rearrange or rename as required.
   * @since 1.6.0
   */
  def as[U : Encoder]: Dataset[U] = {
    new Dataset(sqlContext, queryExecution, encoderFor[U])
  }

  /**
   * Converts this strongly typed collection of data to generic Dataframe.  In contrast to the
   * strongly typed objects that Dataset operations work on, a Dataframe returns generic [[Row]]
   * objects that allow fields to be accessed by ordinal or name.
   */
  // This is declared with parentheses to prevent the Scala compiler from treating
  // `ds.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  def toDF(): DataFrame = DataFrame(sqlContext, logicalPlan)

  /**
   * Returns this [[Dataset]].
   * @since 1.6.0
   */
  // This is declared with parentheses to prevent the Scala compiler from treating
  // `ds.toDS("1")` as invoking this toDF and then apply on the returned Dataset.
  def toDS(): Dataset[T] = this

  /**
   * Returns the number of elements in the [[Dataset]].
   * @since 1.6.0
   */
  def count(): Long = {
    asRowDataset.withCallback("count", groupBy().count().map(_._2)) { ds =>
      ds.collect(needCallback = false).head
    }
  }

  /**
   * Displays the content of this [[Dataset]] in a tabular form. Strings more than 20 characters
   * will be truncated, and all cells will be aligned right. For example:
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
   * @since 1.6.0
   */
  def show(numRows: Int): Unit = show(numRows, truncate = true)

  /**
   * Displays the top 20 rows of [[Dataset]] in a tabular form. Strings more than 20 characters
   * will be truncated, and all cells will be aligned right.
   *
   * @since 1.6.0
   */
  def show(): Unit = show(20)

  /**
   * Displays the top 20 rows of [[Dataset]] in a tabular form.
   *
   * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
   *              be truncated and all cells will be aligned right
   * @since 1.6.0
   */
  def show(truncate: Boolean): Unit = show(20, truncate)

  /**
   * Displays the [[Dataset]] in a tabular form. For example:
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
   * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
   *              be truncated and all cells will be aligned right
   * @since 1.6.0
   */
  // scalastyle:off println
  def show(numRows: Int, truncate: Boolean): Unit = println(showString(numRows, truncate))
  // scalastyle:on println

  /**
   * Compose the string representing rows for output
   *
   * @param _numRows Number of rows to show
   * @param truncate Whether truncate long strings and align cells right
   */
  override private[sql] def showString(_numRows: Int, truncate: Boolean = true): String = {
    val numRows = _numRows.max(0)
    val takeResult = take(numRows + 1)
    val hasMoreData = takeResult.length > numRows
    val data = takeResult.take(numRows)

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond 20 characters, replace it with the first 17 and "..."
    val rows: Seq[Seq[String]] = schema.fieldNames.toSeq +: (data.map {
      case r: Row => r
      case tuple: Product => Row.fromTuple(tuple)
      case o => Row(o)
    } map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
          case _ => cell.toString
        }
        if (truncate && str.length > 20) str.substring(0, 17) + "..." else str
      }: Seq[String]
    })

    formatString ( rows, numRows, hasMoreData, truncate )
  }

  /**
   * Returns a new [[Dataset]] that has exactly `numPartitions` partitions.
   * @since 1.6.0
   */
  def repartition(numPartitions: Int): Dataset[T] = withPlan {
    logical.Repartition(numPartitions, shuffle = true, _)
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
  def repartition(numPartitions: Int, partitionExprs: Column*): Dataset[T] = withPlan {
    logical.RepartitionByExpression(partitionExprs.map(_.expr), logicalPlan, Some(numPartitions))
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
  def repartition(partitionExprs: Column*): Dataset[T] = withPlan {
    logical.RepartitionByExpression(partitionExprs.map(_.expr), logicalPlan, numPartitions = None)
  }

  /**
   * Returns a new [[Dataset]] that has exactly `numPartitions` partitions.
   * Similar to coalesce defined on an [[RDD]], this operation results in a narrow dependency, e.g.
   * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
   * the 100 new partitions will claim 10 of the current partitions.
   * @since 1.6.0
   */
  def coalesce(numPartitions: Int): Dataset[T] = withPlan {
    logical.Repartition(numPartitions, shuffle = false, _)
  }

  /* *********************** *
   *  Functional Operations  *
   * *********************** */

  /**
   * Concise syntax for chaining custom transformations.
   * {{{
   *   def featurize(ds: Dataset[T]) = ...
   *
   *   dataset
   *     .transform(featurize)
   *     .transform(...)
   * }}}
   * @since 1.6.0
   */
  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = t(this)

  /**
   * (Scala-specific)
   * Returns a new [[Dataset]] that only contains elements where `func` returns `true`.
   * @since 1.6.0
   */
  def filter(func: T => Boolean): Dataset[T] = mapPartitions(_.filter(func))

  /**
   * (Java-specific)
   * Returns a new [[Dataset]] that only contains elements where `func` returns `true`.
   * @since 1.6.0
   */
  def filter(func: FilterFunction[T]): Dataset[T] = filter(t => func.call(t))

  /**
   * (Scala-specific)
   * Returns a new [[Dataset]] that contains the result of applying `func` to each element.
   * @since 1.6.0
   */
  def map[U : Encoder](func: T => U): Dataset[U] = mapPartitions(_.map(func))

  /**
   * (Java-specific)
   * Returns a new [[Dataset]] that contains the result of applying `func` to each element.
   * @since 1.6.0
   */
  def map[U](func: MapFunction[T, U], encoder: Encoder[U]): Dataset[U] =
    map(t => func.call(t))(encoder)

  /**
   * (Scala-specific)
   * Returns a new [[Dataset]] that contains the result of applying `func` to each partition.
   * @since 1.6.0
   */
  def mapPartitions[U : Encoder](func: Iterator[T] => Iterator[U]): Dataset[U] = {
    new Dataset[U](
      sqlContext,
      logical.MapPartitions[T, U](func, logicalPlan))
  }

  /**
   * (Java-specific)
   * Returns a new [[Dataset]] that contains the result of applying `func` to each partition.
   * @since 1.6.0
   */
  def mapPartitions[U](f: MapPartitionsFunction[T, U], encoder: Encoder[U]): Dataset[U] = {
    val func: (Iterator[T]) => Iterator[U] = x => f.call(x.asJava).asScala
    mapPartitions(func)(encoder)
  }

  /**
   * (Scala-specific)
   * Returns a new [[Dataset]] by first applying a function to all elements of this [[Dataset]],
   * and then flattening the results.
   * @since 1.6.0
   */
  def flatMap[U : Encoder](func: T => TraversableOnce[U]): Dataset[U] =
    mapPartitions(_.flatMap(func))

  /**
   * (Java-specific)
   * Returns a new [[Dataset]] by first applying a function to all elements of this [[Dataset]],
   * and then flattening the results.
   * @since 1.6.0
   */
  def flatMap[U](f: FlatMapFunction[T, U], encoder: Encoder[U]): Dataset[U] = {
    val func: (T) => Iterator[U] = x => f.call(x).asScala
    flatMap(func)(encoder)
  }

  /* ************** *
   *  Side effects  *
   * ************** */

  /**
   * (Scala-specific)
   * Runs `func` on each element of this [[Dataset]].
   * @since 1.6.0
   */
  def foreach(func: T => Unit): Unit = rdd.foreach(func)

  /**
   * (Java-specific)
   * Runs `func` on each element of this [[Dataset]].
   * @since 1.6.0
   */
  def foreach(func: ForeachFunction[T]): Unit = foreach(func.call(_))

  /**
   * (Scala-specific)
   * Runs `func` on each partition of this [[Dataset]].
   * @since 1.6.0
   */
  def foreachPartition(func: Iterator[T] => Unit): Unit = rdd.foreachPartition(func)

  /**
   * (Java-specific)
   * Runs `func` on each partition of this [[Dataset]].
   * @since 1.6.0
   */
  def foreachPartition(func: ForeachPartitionFunction[T]): Unit =
    foreachPartition(it => func.call(it.asJava))

  /* ************* *
   *  Aggregation  *
   * ************* */

  /**
   * (Scala-specific)
   * Reduces the elements of this [[Dataset]] using the specified binary function. The given `func`
   * must be commutative and associative or the result may be non-deterministic.
   * @since 1.6.0
   */
  def reduce(func: (T, T) => T): T = rdd.reduce(func)

  /**
   * (Java-specific)
   * Reduces the elements of this Dataset using the specified binary function.  The given `func`
   * must be commutative and associative or the result may be non-deterministic.
   * @since 1.6.0
   */
  def reduce(func: ReduceFunction[T]): T = reduce(func.call(_, _))

  /**
   * (Scala-specific)
   * Returns a [[GroupedDataset]] where the data is grouped by the given key `func`.
   * @since 1.6.0
   */
  def groupBy[K : Encoder](func: T => K): GroupedDataset[K, T] = {
    val inputPlan = logicalPlan
    val withGroupingKey = logical.AppendColumns(func, inputPlan)
    val executed = sqlContext.executePlan(withGroupingKey)

    new GroupedDataset(
      this,
      encoderFor[K],
      executed,
      inputPlan.output,
      withGroupingKey.newColumns,
      GroupedDataset.GroupByType)
  }

  /**
   * Returns a [[GroupedDataset]] where the data is grouped by the given [[Column]] expressions.
   * @since 1.6.0
   */
  @scala.annotation.varargs
  def groupBy(cols: Column*): GroupedDataset[Row, T] = {
    groupBy(cols.map(_.expr), GroupedDataset.GroupByType)
  }

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
   *
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def groupBy(col1: String, cols: String*): GroupedDataset[Row, T] = {
    groupBy((col1 +: cols).map(resolve), GroupedDataset.GroupByType)
  }

  /**
   * (Java-specific)
   * Returns a [[GroupedDataset]] where the data is grouped by the given key `func`.
   * @since 1.6.0
   */
  def groupBy[K](func: MapFunction[T, K], encoder: Encoder[K]): GroupedDataset[K, T] = {
    groupBy(func.call(_))(encoder)
  }

  /**
   * Create a multi-dimensional rollup for the current [[DataFrame]] using the specified columns,
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns rolluped by department and group.
   *   df.rollup($"department", $"group").avg()
   *
   *   // Compute the max age and average salary, rolluped by department and gender.
   *   df.rollup($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def rollup(cols: Column*): GroupedDataset[Row, T] = {
    groupBy(cols.map(_.expr), GroupedDataset.RollupType)
  }

  /**
   * Create a multi-dimensional cube for the current [[DataFrame]] using the specified columns,
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns cubed by department and group.
   *   df.cube($"department", $"group").avg()
   *
   *   // Compute the max age and average salary, cubed by department and gender.
   *   df.cube($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def cube(cols: Column*): GroupedDataset[Row, T] = {
    groupBy(cols.map(_.expr), GroupedDataset.CubeType)
  }

  /**
   * Create a multi-dimensional rollup for the current [[DataFrame]] using the specified columns,
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * This is a variant of rollup that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns rolluped by department and group.
   *   df.rollup("department", "group").avg()
   *
   *   // Compute the max age and average salary, rolluped by department and gender.
   *   df.rollup($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def rollup(col1: String, cols: String*): GroupedDataset[Row, T] = {
    val colNames: Seq[String] = col1 +: cols
    groupBy(colNames.map(resolve), GroupedDataset.RollupType)
  }

  /**
   * Create a multi-dimensional cube for the current [[DataFrame]] using the specified columns,
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * This is a variant of cube that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns cubed by department and group.
   *   df.cube("department", "group").avg()
   *
   *   // Compute the max age and average salary, cubed by department and gender.
   *   df.cube($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def cube(col1: String, cols: String*): GroupedDataset[Row, T] = {
    val colNames: Seq[String] = col1 +: cols
    groupBy(colNames.map(resolve), GroupedDataset.CubeType)
  }

  /**
   * (Scala-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg("age" -> "max", "salary" -> "avg")
   *   df.groupBy().agg("age" -> "max", "salary" -> "avg")
   * }}}
   *
   * @group dfops
   * @since 1.3.0
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): Dataset[Row] = {
    groupBy().agg(aggExpr, aggExprs : _*)
  }

  /**
   * (Scala-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   *
   * @group dfops
   * @since 1.3.0
   */
  def agg(exprs: Map[String, String]): Dataset[Row] = groupBy().agg(exprs)

  /**
   * (Java-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   *
   * @group dfops
   * @since 1.3.0
   */
  def agg(exprs: java.util.Map[String, String]): Dataset[Row] = groupBy().agg(exprs)

  /**
   * Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(max($"age"), avg($"salary"))
   *   df.groupBy().agg(max($"age"), avg($"salary"))
   * }}}
   *
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): Dataset[Row] = groupBy().agg(expr, exprs : _*)

  /* ****************** *
   *  Typed Relational  *
   * ****************** */

  /**
   * Applies a logical alias to this [[Dataset]] that can be used to disambiguate columns that have
   * the same name after two Datasets have been joined.
   * @since 1.6.0
   */
  def as(alias: String): Dataset[T] = withPlan(logical.Subquery(alias, _))

  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expression for each element.
   *
   * {{{
   *   val ds = Seq(1, 2, 3).toDS()
   *   val newDS = ds.select(expr("value + 1").as[Int])
   * }}}
   * @since 1.6.0
   */
  def select[U1: Encoder](c1: TypedColumn[T, U1]): Dataset[U1] = {
    new Dataset[U1](
      sqlContext,
      logical.Project(
        c1.withInputType(
          boundTEncoder,
          logicalPlan.output).named :: Nil,
        logicalPlan))
  }

  /**
   * Internal helper function for building typed selects that return tuples.  For simplicity and
   * code reuse, we do this without the help of the type system and then use helper functions
   * that cast appropriately for the user facing interface.
   */
  protected def selectUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    val encoders = columns.map(_.encoder)
    val namedColumns =
      columns.map(_.withInputType(resolvedTEncoder, logicalPlan.output).named)
    val execution = new QueryExecution(sqlContext, logical.Project(namedColumns, logicalPlan))

    new Dataset(sqlContext, execution, ExpressionEncoder.tuple(encoders))
  }

  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)] =
    selectUntyped(c1, c2).asInstanceOf[Dataset[(U1, U2)]]

  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2, U3](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3]): Dataset[(U1, U2, U3)] =
    selectUntyped(c1, c2, c3).asInstanceOf[Dataset[(U1, U2, U3)]]

  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2, U3, U4](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4]): Dataset[(U1, U2, U3, U4)] =
    selectUntyped(c1, c2, c3, c4).asInstanceOf[Dataset[(U1, U2, U3, U4)]]

  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2, U3, U4, U5](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4],
      c5: TypedColumn[T, U5]): Dataset[(U1, U2, U3, U4, U5)] =
    selectUntyped(c1, c2, c3, c4, c5).asInstanceOf[Dataset[(U1, U2, U3, U4, U5)]]

  /**
   * Returns a new [[Dataset]] by sampling a fraction of records.
   * @since 1.6.0
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long) : Dataset[T] =
    withPlan(logical.Sample(0.0, fraction, withReplacement, seed, _))

  /**
   * Returns a new [[Dataset]] by sampling a fraction of records, using a random seed.
   * @since 1.6.0
   */
  def sample(withReplacement: Boolean, fraction: Double) : Dataset[T] = {
    sample(withReplacement, fraction, Utils.random.nextLong)
  }

  /**
   * Filters rows using the given condition.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   * }}}
   *
   * @group dfops
   * @since 2.0.0
   */
  def filter(condition: Column): Dataset[T] = withPlan {
    logical.Filter(condition.expr, _)
  }

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDf.filter("age > 15")
   * }}}
   *
   * @group dfops
   * @since 2.0.0
   */
  def filter(conditionExpr: String): Dataset[T] = {
    filter(Column(sqlContext.sqlParser.parseExpression(conditionExpr)))
  }

  /**
   * Filters rows using the given condition. This is an alias for `filter`.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   * }}}
   *
   * @group dfops
   * @since 2.0.0
   */
  def where(condition: Column): Dataset[T] = filter(condition)

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDf.where("age > 15")
   * }}}
   *
   * @group dfops
   * @since 2.0.0
   */
  def where(conditionExpr: String): Dataset[T] = {
    filter(Column(sqlContext.sqlParser.parseExpression(conditionExpr)))
  }

  /**
   * Returns a new [[DataFrame]] by taking the first `n` rows. The difference between this function
   * and `head` is that `head` returns an array while `limit` returns a new [[DataFrame]].
   *
   * @group dfops
   * @since 2.0.0
   */
  def limit(n: Int): Dataset[T] = withPlan(logical.Limit(Literal(n), _))

  /**
   * Returns a new [[DataFrame]] with each partition sorted by the given expressions.
   *
   * This is the same operation as "SORT BY" in SQL (Hive QL).
   *
   * @group dfops
   * @since 1.6.0
   */
  @scala.annotation.varargs
  def sortWithinPartitions(sortCol: String, sortCols: String*): Dataset[T] = {
    sortWithinPartitions((sortCol +: sortCols).map(Column(_)) : _*)
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
  def sortWithinPartitions(sortExprs: Column*): Dataset[T] = {
    sortInternal(global = false, sortExprs)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the specified column, all in ascending order.
   * {{{
   *   // The following 3 are equivalent
   *   df.sort("sortcol")
   *   df.sort($"sortcol")
   *   df.sort($"sortcol".asc)
   * }}}
   *
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def sort(sortCol: String, sortCols: String*): Dataset[T] = {
    sort((sortCol +: sortCols).map(apply) : _*)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions. For example:
   * {{{
   *   df.sort($"col1", $"col2".desc)
   * }}}
   *
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def sort(sortExprs: Column*): Dataset[T] = {
    sortInternal(global = true, sortExprs)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
   * This is an alias of the `sort` function.
   *
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def orderBy(sortCol: String, sortCols: String*): Dataset[T] = sort(sortCol, sortCols : _*)

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
   * This is an alias of the `sort` function.
   *
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def orderBy(sortExprs: Column*): Dataset[T] = sort(sortExprs : _*)

  /**
   * Randomly splits this [[DataFrame]] with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   * @group dfops
   * @since 1.4.0
   */
  def randomSplit(weights: Array[Double], seed: Long): Array[Dataset[T]] = {
    // It is possible that the underlying dataframe doesn't guarantee the ordering of rows in its
    // constituent partitions each time a split is materialized which could result in
    // overlapping splits. To prevent this, we explicitly sort each input partition to make the
    // ordering deterministic.
    val sorted = logical.Sort(
      logicalPlan.output.map(SortOrder(_, Ascending)), global = false, logicalPlan)
    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights.sliding(2).map { x =>
      new Dataset[T](sqlContext, logical.Sample(x(0), x(1), withReplacement = false, seed, sorted))
    }.toArray
  }

  /**
   * Randomly splits this [[DataFrame]] with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @group dfops
   * @since 1.4.0
   */
  def randomSplit(weights: Array[Double]): Array[Dataset[T]] = {
    randomSplit(weights, Utils.random.nextLong)
  }

  /**
   * Randomly splits this [[DataFrame]] with the provided weights. Provided for the Python Api.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   * @group dfops
   */
  private[spark] def randomSplit(weights: List[Double], seed: Long): Array[Dataset[T]] = {
    randomSplit(weights.toArray, seed)
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
   *
   * @group dfops
   * @since 1.3.0
   */
  def explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A])
    : Dataset[Row] = {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val elementTypes = schema.toAttributes.map {
      attr => (attr.dataType, attr.nullable, attr.name)
    }

    val convert = CatalystTypeConverters.createToCatalystConverter(schema)
    val rowFunction = f.andThen(_.map(convert(_).asInstanceOf[InternalRow]))
    val generator = UserDefinedGenerator(elementTypes, rowFunction, input.map(_.expr))

    asRowDataset.withPlan {
      logical.Generate(generator, join = true, outer = false,
        qualifier = None, generatorOutput = Nil, logicalPlan)
    }
  }

  /**
   * (Scala-specific) Returns a new [[DataFrame]] where a single column has been expanded to zero
   * or more rows by the provided function.  This is similar to a `LATERAL VIEW` in HiveQL. All
   * columns of the input row are implicitly joined with each value that is output by the function.
   *
   * {{{
   *   df.explode("words", "word"){words: String => words.split(" ")}
   * }}}
   *
   * @group dfops
   * @since 1.3.0
   */
  def explode[A, B : TypeTag](inputColumn: String, outputColumn: String)(f: A => TraversableOnce[B])
    : Dataset[Row] = {
    val dataType = ScalaReflection.schemaFor[B].dataType
    val attributes = AttributeReference(outputColumn, dataType)() :: Nil
    // TODO handle the metadata?
    val elementTypes = attributes.map { attr => (attr.dataType, attr.nullable, attr.name) }

    def rowFunction(row: Row): TraversableOnce[InternalRow] = {
      val convert = CatalystTypeConverters.createToCatalystConverter(dataType)
      f(row(0).asInstanceOf[A]).map(o => InternalRow(convert(o)))
    }
    val generator = UserDefinedGenerator(elementTypes, rowFunction, apply(inputColumn).expr :: Nil)

    asRowDataset.withPlan {
      logical.Generate(generator, join = true, outer = false,
        qualifier = None, generatorOutput = Nil, logicalPlan)
    }
  }

  /* ******************** *
   *  Untyped Relational  *
   * ******************** */

  /**
   * Returns a new [[Dataset]] by selecting a set of column based expressions.
   * {{{
   *   df.select($"colA", $"colB" + 1)
   * }}}
   *
   * @since 1.6.0
   */
  // Copied from Dataframe to make sure we don't have invalid overloads.
  @scala.annotation.varargs
  def select(cols: Column*): Dataset[Row] = asRowDataset.withPlan {
    logical.Project(cols.map(_.named), _)
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
   *
   * @group dfops
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def select(col: String, cols: String*): Dataset[Row] = select((col +: cols).map(Column(_)) : _*)

  /**
   * Selects a set of SQL expressions. This is a variant of `select` that accepts
   * SQL expressions.
   *
   * {{{
   *   // The following are equivalent:
   *   df.selectExpr("colA", "colB as newName", "abs(colC)")
   *   df.select(expr("colA"), expr("colB as newName"), expr("abs(colC)"))
   * }}}
   *
   * @group dfops
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def selectExpr(exprs: String*): Dataset[Row] = {
    select(exprs.map { expr =>
      Column(sqlContext.sqlParser.parseExpression(expr))
    }: _*)
  }

  /* **************** *
   *  Set operations  *
   * **************** */

  /**
   * Returns a new [[Dataset]] that contains only the unique elements of this [[Dataset]].
   *
   * Note that, equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   * @since 1.6.0
   */
  def distinct: Dataset[T] = withPlan(logical.Distinct)

  /**
   * Returns a new [[Dataset]] that contains only the elements of this [[Dataset]] that are also
   * present in `other`.
   *
   * Note that, equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   * @since 1.6.0
   */
  def intersect(other: Dataset[T]): Dataset[T] = withPlan[T](other)(logical.Intersect)

  /**
   * Returns a new [[Dataset]] that contains the elements of both this and the `other` [[Dataset]]
   * combined.
   *
   * Note that, this function is not a typical set union operation, in that it does not eliminate
   * duplicate items.  As such, it is analogous to `UNION ALL` in SQL.
   * @since 1.6.0
   */
  def union(other: Dataset[T]): Dataset[T] = withPlan[T](other) { (left, right) =>
    // This breaks caching, but it's usually ok because it addresses a very specific use case:
    // using union to union many files or partitions.
    CombineUnions(logical.Union(left, right))
  }

  /**
   * Returns a new [[Dataset]] that contains the elements of both this and the `other` [[Dataset]]
   * combined.
   *
   * Note that, this function is not a typical set union operation, in that it does not eliminate
   * duplicate items.  As such, it is analogous to `UNION ALL` in SQL.
   *
   * @since 2.0.0
   */
  def unionAll(other: Dataset[T]): Dataset[T] = union(other)

  /**
   * Returns a new [[Dataset]] where any elements present in `other` have been removed.
   *
   * Note that, equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   * @since 1.6.0
   */
  def subtract(other: Dataset[T]): Dataset[T] = withPlan[T](other)(logical.Except(_, _))

  /**
   * Returns a new [[Dataset]] where any elements present in `other` have been removed.
   *
   * Note that, equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   *
   * @since 2.0.0
   */
  def except(other: Dataset[T]): Dataset[T] = subtract(other)

  /* ************* *
   *  Typed Joins  *
   * ************* */

  /**
   * Joins this [[Dataset]] returning a [[Tuple2]] for each pair where `condition` evaluates to
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
   * @since 1.6.0
   */
  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[(T, U)] = {
    val left = this.logicalPlan
    val right = other.logicalPlan

    val joined = sqlContext.executePlan(logical.Join(left, right, joinType =
      JoinType(joinType), Some(condition.expr)))
    val leftOutput = joined.analyzed.output.take(left.output.length)
    val rightOutput = joined.analyzed.output.takeRight(right.output.length)

    val leftData = this.unresolvedTEncoder match {
      case e if e.flat => Alias(leftOutput.head, "_1")()
      case _ => Alias(CreateStruct(leftOutput), "_1")()
    }
    val rightData = other.unresolvedTEncoder match {
      case e if e.flat => Alias(rightOutput.head, "_2")()
      case _ => Alias(CreateStruct(rightOutput), "_2")()
    }

    implicit val tuple2Encoder: Encoder[(T, U)] =
      ExpressionEncoder.tuple(this.unresolvedTEncoder, other.unresolvedTEncoder)
    withPlan[(T, U)](other) { (left, right) =>
      logical.Project(
        leftData :: rightData :: Nil,
        joined.analyzed)
    }
  }

  /**
   * Using inner equi-join to join this [[Dataset]] returning a [[Tuple2]] for each pair
   * where `condition` evaluates to true.
   *
   * @param other Right side of the join.
   * @param condition Join expression.
   * @since 1.6.0
   */
  def joinWith[U](other: Dataset[U], condition: Column): Dataset[(T, U)] = {
    joinWith(other, condition, "inner")
  }

  /* *************** *
   *  Untyped Joins  *
   * *************** */

  /**
   * Cartesian join with another [[DataFrame]].
   *
   * Note that cartesian joins are very expensive without an extra filter that can be pushed down.
   *
   * @param right Right side of the join operation.
   * @group dfops
   * @since 2.0.0
   */
  def join[U: Encoder](right: Dataset[U]): Dataset[Row] = asRowDataset.withPlan {
    logical.Join(_, right.logicalPlan, joinType = Inner, None)
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
  def join[U: Encoder](right: Dataset[U], usingColumn: String): Dataset[Row] = {
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
  def join[U: Encoder](right: Dataset[U], usingColumns: Seq[String]): Dataset[Row] = {
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
  def join[U: Encoder](
      right: Dataset[U], usingColumns: Seq[String], joinType: String): Dataset[Row] = {
    // Analyze the self join. The assumption is that the analyzer will disambiguate left vs right
    // by creating a new instance for one of the branch.
    val joined = sqlContext.executePlan(
      logical.Join(logicalPlan, right.logicalPlan, joinType = JoinType(joinType), None))
      .analyzed.asInstanceOf[logical.Join]

    val condition = usingColumns.map { col =>
      catalyst.expressions.EqualTo(
        withPlan(joined.left).resolve(col),
        withPlan(joined.right).resolve(col))
    }.reduceLeftOption[catalyst.expressions.BinaryExpression] { (cond, eqTo) =>
      catalyst.expressions.And(cond, eqTo)
    }

    // Project only one of the join columns.
    val joinedCols = JoinType(joinType) match {
      case Inner | LeftOuter | LeftSemi =>
        usingColumns.map(col => withPlan(joined.left).resolve(col))
      case RightOuter =>
        usingColumns.map(col => withPlan(joined.right).resolve(col))
      case FullOuter =>
        usingColumns.map { col =>
          val leftCol = withPlan(joined.left).resolve(col).toAttribute.withNullability(true)
          val rightCol = withPlan(joined.right).resolve(col).toAttribute.withNullability(true)
          Alias(Coalesce(Seq(leftCol, rightCol)), col)()
        }
      case NaturalJoin(_) => sys.error("NaturalJoin with using clause is not supported.")
    }
    // The nullability of output of joined could be different than original column,
    // so we can only compare them by exprId
    val joinRefs = AttributeSet(condition.toSeq.flatMap(_.references))
    val resultCols = joinedCols ++ joined.output.filterNot(joinRefs.contains(_))
    asRowDataset.withPlan {
      logical.Project(
        resultCols,
        logical.Join(
          joined.left,
          joined.right,
          joinType = JoinType(joinType),
          condition)
      )
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
   * @group dfops
   * @since 1.3.0
   */
  def join[U: Encoder](right: Dataset[U], joinExprs: Column): Dataset[Row] = {
    join(right, joinExprs, "inner")
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
   * @param joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
   * @group dfops
   * @since 1.3.0
   */
  def join[U: Encoder](right: Dataset[U], joinExprs: Column, joinType: String): Dataset[Row] = {
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
      logical.Join(logicalPlan, right.logicalPlan, JoinType(joinType), Some(joinExprs.expr)))
      .queryExecution.analyzed.asInstanceOf[logical.Join]

    // If auto self join alias is disabled, return the plan.
    if (!sqlContext.conf.dataFrameSelfJoinAutoResolveAmbiguity) {
      return asRowDataset.withPlan(plan)
    }

    // If left/right have no output set intersection, return the plan.
    val lanalyzed = withPlan(this.logicalPlan).queryExecution.analyzed
    val ranalyzed = withPlan(right.logicalPlan).queryExecution.analyzed
    if (lanalyzed.outputSet.intersect(ranalyzed.outputSet).isEmpty) {
      return asRowDataset.withPlan(plan)
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

    asRowDataset.withPlan {
      plan.copy(condition = cond)
    }
  }

  /* ************************** *
   *  Gather to Driver Actions  *
   * ************************** */

  /**
   * Returns the first `n` rows.
   * @group action
   * @since 1.3.0
   */
  def head(n: Int): Array[T] = withCallback("head", limit(n)) { ds =>
    ds.collect(needCallback = false)
  }

  /**
   * Returns the first row.
   * @group action
   * @since 1.3.0
   */
  def head(): T = head(1).head

  /**
   * Returns the first element in this [[Dataset]].
   * @since 1.6.0
   */
  def first(): T = take(1).head

  /**
   * Returns an array that contains all the elements in this [[Dataset]].
   *
   * Running collect requires moving all the data into the application's driver process, and
   * doing so on a very large [[Dataset]] can crash the driver process with OutOfMemoryError.
   *
   * For Java API, use [[collectAsList]].
   * @since 1.6.0
   */
  def collect(): Array[T] = collect(needCallback = false)

  protected def collect(needCallback: Boolean): Array[T] = {
    def execute(): Array[T] = withNewExecutionId {
      queryExecution.toRdd.map(_.copy()).collect().map(boundTEncoder.fromRow)
    }

    if (needCallback) {
      withCallback("collect", this)(_ => execute())
    } else {
      execute()
    }
  }

  /**
   * Returns an array that contains all the elements in this [[Dataset]].
   *
   * Running collect requires moving all the data into the application's driver process, and
   * doing so on a very large [[Dataset]] can crash the driver process with OutOfMemoryError.
   *
   * For Java API, use [[collectAsList]].
   * @since 1.6.0
   */
  def collectAsList(): java.util.List[T] = collect().toSeq.asJava

  /**
   * Returns the first `num` elements of this [[Dataset]] as an array.
   *
   * Running take requires moving data into the application's driver process, and doing so with
   * a very large `num` can crash the driver process with OutOfMemoryError.
   * @since 1.6.0
   */
  def take(num: Int): Array[T] = withPlan(logical.Limit(Literal(num), _)).collect()

  /**
   * Returns the first `num` elements of this [[Dataset]] as an array.
   *
   * Running take requires moving data into the application's driver process, and doing so with
   * a very large `num` can crash the driver process with OutOfMemoryError.
   * @since 1.6.0
   */
  def takeAsList(num: Int): java.util.List[T] = java.util.Arrays.asList(take(num) : _*)

  /**
   * Persist this [[Dataset]] with the default storage level (`MEMORY_AND_DISK`).
   * @since 1.6.0
   */
  def persist(): this.type = {
    sqlContext.cacheManager.cacheQuery(this)
    this
  }

  /**
   * Persist this [[Dataset]] with the default storage level (`MEMORY_AND_DISK`).
   * @since 1.6.0
   */
  def cache(): this.type = persist()

  /**
   * Persist this [[Dataset]] with the given storage level.
   *
   * @param newLevel One of: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`,
   *                 `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`,
   *                 `MEMORY_AND_DISK_2`, etc.
   * @group basic
   * @since 1.6.0
   */
  def persist(newLevel: StorageLevel): this.type = {
    sqlContext.cacheManager.cacheQuery(this, None, newLevel)
    this
  }

  /**
   * Mark the [[Dataset]] as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted.
   * @since 1.6.0
   */
  def unpersist(blocking: Boolean): this.type = {
    sqlContext.cacheManager.tryUncacheQuery(this, blocking)
    this
  }

  /**
   * Mark the [[Dataset]] as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @since 1.6.0
   */
  def unpersist(): this.type = unpersist(blocking = false)

  /* ******************
   *  Other Operations
   * ****************** */

  /**
   * Returns a new [[DataFrame]] by adding a column or replacing the existing column that has
   * the same name.
   * @group dfops
   * @since 1.3.0
   */
  def withColumn(colName: String, col: Column): Dataset[Row] = {
    val resolver = sqlContext.analyzer.resolver
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
   * Returns a new [[DataFrame]] by adding a column with metadata.
   */
  private[spark] def withColumn(colName: String, col: Column, metadata: Metadata): Dataset[Row] = {
    val resolver = sqlContext.analyzer.resolver
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
   * Returns a new [[DataFrame]] with a column renamed.
   * This is a no-op if schema doesn't contain existingName.
   * @group dfops
   * @since 1.3.0
   */
  def withColumnRenamed(existingName: String, newName: String): Dataset[Row] = {
    val resolver = sqlContext.analyzer.resolver
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
      this.asRowDataset
    }
  }

  /**
   * Returns a new [[DataFrame]] with a column dropped.
   * This is a no-op if schema doesn't contain column name.
   * @group dfops
   * @since 1.4.0
   */
  def drop(colName: String): Dataset[Row] = {
    drop(Seq(colName) : _*)
  }

  /**
   * Returns a new [[DataFrame]] with columns dropped.
   * This is a no-op if schema doesn't contain column name(s).
   * @group dfops
   * @since 1.6.0
   */
  @scala.annotation.varargs
  def drop(colNames: String*): Dataset[Row] = {
    val resolver = sqlContext.analyzer.resolver
    val remainingCols =
      schema.filter(f => colNames.forall(n => !resolver(f.name, n))).map(f => Column(f.name))
    if (remainingCols.size == this.schema.size) {
      this.asRowDataset
    } else {
      this.select(remainingCols: _*)
    }
  }

  /**
   * Returns a new [[DataFrame]] with a column dropped.
   * This version of drop accepts a Column rather than a name.
   * This is a no-op if the DataFrame doesn't have a column
   * with an equivalent expression.
   * @group dfops
   * @since 1.4.1
   */
  def drop(col: Column): Dataset[Row] = {
    val expression = col match {
      case Column(u: UnresolvedAttribute) =>
        queryExecution.analyzed.resolveQuoted(u.name, sqlContext.analyzer.resolver).getOrElse(u)
      case Column(expr: Expression) => expr
    }
    val attrs = this.logicalPlan.output
    val colsAfterDrop = attrs.filter { attr =>
      attr != expression
    }.map(attr => Column(attr))
    select(colsAfterDrop : _*)
  }

  /**
   * Returns a new [[DataFrame]] that contains only the unique rows from this [[DataFrame]].
   * This is an alias for `distinct`.
   * @group dfops
   * @since 1.4.0
   */
  def dropDuplicates(): Dataset[T] = dropDuplicates(this.columns)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] with duplicate rows removed, considering only
   * the subset of columns.
   *
   * @group dfops
   * @since 1.4.0
   */
  def dropDuplicates(colNames: Seq[String]): Dataset[T] = withPlan {
    val groupCols = colNames.map(resolve)
    val groupColExprIds = groupCols.map(_.exprId)
    val aggCols = logicalPlan.output.map { attr =>
      if (groupColExprIds.contains(attr.exprId)) {
        attr
      } else {
        Alias(new First(attr).toAggregateExpression(), attr.name)()
      }
    }
    logical.Aggregate(groupCols, aggCols, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] with duplicate rows removed, considering only
   * the subset of columns.
   *
   * @group dfops
   * @since 1.4.0
   */
  def dropDuplicates(colNames: Array[String]): Dataset[T] = dropDuplicates(colNames.toSeq)

  /* ***** *
   *  I/O  *
   * ***** */

  /**
   * Converts this [[Dataset]] to an [[RDD]].
   * @since 1.6.0
   */
  def rdd: RDD[T] = {
    queryExecution.toRdd.mapPartitions { iter =>
      iter.map(boundTEncoder.fromRow)
    }
  }

  /**
   * Returns the content of the [[DataFrame]] as a [[JavaRDD]] of [[Row]]s.
   * @group rdd
   * @since 1.3.0
   */
  def toJavaRDD: JavaRDD[T] = rdd.toJavaRDD()

  /**
   * Returns the content of the [[DataFrame]] as a [[JavaRDD]] of [[Row]]s.
   * @group rdd
   * @since 1.3.0
   */
  def javaRDD: JavaRDD[T] = toJavaRDD

  /**
   * Registers this [[DataFrame]] as a temporary table using the given name.  The lifetime of this
   * temporary table is tied to the [[SQLContext]] that was used to create this DataFrame.
   *
   * @group basic
   * @since 1.3.0
   */
  def registerTempTable(tableName: String): Unit = {
    sqlContext.registerDatasetAsTable(this, tableName)
  }

  /* ******************** *
   *  Internal Functions  *
   * ******************** */

  private[sql] def withPlan(plan: LogicalPlan): Dataset[T] = withPlan(_ => plan)

  private[sql] def withPlan(f: LogicalPlan => LogicalPlan): Dataset[T] =
    new Dataset[T](sqlContext, sqlContext.executePlan(f(logicalPlan)), tEncoder)

  private[sql] def withPlan[R : Encoder](
      other: Dataset[_])(
      f: (LogicalPlan, LogicalPlan) => LogicalPlan): Dataset[R] =
    new Dataset[R](sqlContext, f(logicalPlan, other.logicalPlan))

  private[sql] def asRowDataset: Dataset[Row] = as[Row](RowEncoder(schema))

  protected def sortInternal(global: Boolean, sortExprs: Seq[Column]): Dataset[T] = {
    val sortOrder: Seq[SortOrder] = sortExprs.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    withPlan {
      logical.Sort(sortOrder, global = global, _)
    }
  }

  private[this] def groupBy(
      exprs: Seq[Expression],
      groupType: GroupedDataset.GroupType): GroupedDataset[Row, T] = {
    val withKeyColumns = logicalPlan.output ++ exprs.map(UnresolvedAlias(_))
    val withKey = logical.Project(withKeyColumns, logicalPlan)
    val executed = sqlContext.executePlan(withKey)

    val dataAttributes = executed.analyzed.output.dropRight(exprs.size)
    val keyAttributes = executed.analyzed.output.takeRight(exprs.size)

    new GroupedDataset(
      this,
      RowEncoder(keyAttributes.toStructType),
      executed,
      dataAttributes,
      keyAttributes,
      groupType)
  }

  protected[sql] def numericColumns: Seq[Expression] = {
    schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map { n =>
      queryExecution.analyzed.resolveQuoted(n.name, sqlContext.analyzer.resolver).get
    }
  }

  /**
   * Wrap a DataFrame action to track all Spark jobs in the body so that we can connect them with
   * an execution.
   */
  private[sql] def withNewExecutionId[U](body: => U): U = {
    SQLExecution.withNewExecutionId(sqlContext, queryExecution)(body)
  }

  /**
   * Wrap a DataFrame action to track the QueryExecution and time cost, then report to the
   * user-registered callback functions.
   */
  protected def withCallback[A: Encoder, B](
      name: String, ds: Dataset[A])(
      action: Dataset[A] => B) = {
    try {
      ds.queryExecution.executedPlan.foreach { plan =>
        plan.metrics.valuesIterator.foreach(_.reset())
      }
      val start = System.nanoTime()
      val result = action(ds)
      val end = System.nanoTime()
      sqlContext.listenerManager.onSuccess(name, ds.queryExecution, end - start)
      result
    } catch {
      case e: Exception =>
        sqlContext.listenerManager.onFailure(name, ds.queryExecution, e)
        throw e
    }
  }
}
