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

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.function._
import org.apache.spark.sql.catalyst.analysis.{Star, UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder, OuterScopes, RowEncoder}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{NumericType, StructType}

/**
 * :: Experimental ::
 * A [[Dataset]] has been logically grouped by a user specified grouping key.  Users should not
 * construct a [[GroupedDataset]] directly, but should instead call `groupBy` on an existing
 * [[Dataset]].
 *
 * COMPATIBILITY NOTE: Long term we plan to make [[GroupedDataset)]] extend `GroupedData`.  However,
 * making this change to the class hierarchy would break some function signatures. As such, this
 * class should be considered a preview of the final API.  Changes will be made to the interface
 * after Spark 1.6.
 *
 * @since 1.6.0
 */
@Experimental
class GroupedDataset[K, V] private[sql](
    ds: Dataset[V],
    keyEncoder: Encoder[K],
    queryExecution: QueryExecution,
    val dataAttributes: Seq[Attribute],
    val groupingAttributes: Seq[Attribute],
    groupType: GroupedDataset.GroupType)
  extends Serializable {

  private val valueEncoder = ds.unresolvedTEncoder

  // Similar to [[Dataset]], we use unresolved encoders for later composition and resolved encoders
  // when constructing new logical plans that will operate on the output of the current
  // query execution.

  private implicit val unresolvedKEncoder = encoderFor(keyEncoder)
  private implicit val unresolvedVEncoder = encoderFor(valueEncoder)

  private val resolvedKEncoder =
    unresolvedKEncoder.resolve(groupingAttributes, OuterScopes.outerScopes)
  private val resolvedVEncoder =
    unresolvedVEncoder.resolve(dataAttributes, OuterScopes.outerScopes)

  private def logicalPlan = queryExecution.analyzed
  private def sqlContext = queryExecution.sqlContext

  // Wrap UnresolvedAttribute with UnresolvedAlias, as when we resolve UnresolvedAttribute, we
  // will remove intermediate Alias for ExtractValue chain, and we need to alias it again to
  // make it a NamedExpression.
  private[this] def alias(expr: Expression): NamedExpression = expr match {
    case u: UnresolvedAttribute => UnresolvedAlias(u)
    case expr: NamedExpression => expr
    case expr: Expression => Alias(expr, expr.prettyString)()
  }

  private[this] def toDS(aggExprs: Seq[Expression]): Dataset[Row] = {
    val aggregates = if (ds.sqlContext.conf.dataFrameRetainGroupColumns) {
      groupingAttributes ++ aggExprs
    } else {
      aggExprs
    }

    val rowDs = ds.asRowDataset
    val aliasesAgg = aggregates.map(alias)

    implicit val rowEncoder: Encoder[Row] =
      RowEncoder(StructType.fromAttributes(aliasesAgg.map(_.toAttribute)))

    groupType match {
      case GroupedDataset.GroupByType =>
        new Dataset[Row](
          ds.sqlContext, Aggregate(groupingAttributes, aliasesAgg, rowDs.logicalPlan))
      case GroupedDataset.RollupType =>
        new Dataset[Row](
          ds.sqlContext, Aggregate(Seq(Rollup(groupingAttributes)), aliasesAgg, rowDs.logicalPlan))
      case GroupedDataset.CubeType =>
        new Dataset[Row](
          ds.sqlContext, Aggregate(Seq(Cube(groupingAttributes)), aliasesAgg, rowDs.logicalPlan))
      case GroupedDataset.PivotType(pivotCol, values) =>
        val aliasedGrps = groupingAttributes.map(alias)
        new Dataset[Row](
          ds.sqlContext, Pivot(aliasedGrps, pivotCol, values, aggExprs, rowDs.logicalPlan))
    }
  }

  /**
   * Returns a new [[GroupedDataset]] where the type of the key has been mapped to the specified
   * type. The mapping of key columns to the type follows the same rules as `as` on [[Dataset]].
   *
   * @since 1.6.0
   */
  def keyAs[L : Encoder]: GroupedDataset[L, V] =
    new GroupedDataset(
      ds,
      encoderFor[L],
      queryExecution,
      dataAttributes,
      groupingAttributes,
      groupType)

  /**
   * Returns a [[Dataset]] that contains each unique key.
   *
   * @since 1.6.0
   */
  def keys: Dataset[K] = {
    new Dataset[K](
      sqlContext,
      Distinct(
        Project(groupingAttributes, logicalPlan)))
  }

  /**
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an iterator containing elements of an arbitrary type which will be returned
   * as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * [[org.apache.spark.sql.expressions#Aggregator Aggregator]].
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def flatMapGroups[U : Encoder](f: (K, Iterator[V]) => TraversableOnce[U]): Dataset[U] = {
    new Dataset[U](
      sqlContext,
      MapGroups(
        f,
        groupingAttributes,
        dataAttributes,
        logicalPlan))
  }

  /**
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an iterator containing elements of an arbitrary type which will be returned
   * as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * [[org.apache.spark.sql.expressions#Aggregator Aggregator]].
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def flatMapGroups[U](f: FlatMapGroupsFunction[K, V, U], encoder: Encoder[U]): Dataset[U] = {
    flatMapGroups((key, data) => f.call(key, data.asJava).asScala)(encoder)
  }

  /**
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an element of arbitrary type which will be returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * [[org.apache.spark.sql.expressions#Aggregator Aggregator]].
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def mapGroups[U : Encoder](f: (K, Iterator[V]) => U): Dataset[U] = {
    val func = (key: K, it: Iterator[V]) => Iterator(f(key, it))
    flatMapGroups(func)
  }

  /**
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an element of arbitrary type which will be returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * [[org.apache.spark.sql.expressions#Aggregator Aggregator]].
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def mapGroups[U](f: MapGroupsFunction[K, V, U], encoder: Encoder[U]): Dataset[U] = {
    mapGroups((key, data) => f.call(key, data.asJava))(encoder)
  }

  /**
   * Reduces the elements of each group of data using the specified binary function.
   * The given function must be commutative and associative or the result may be non-deterministic.
   *
   * @since 1.6.0
   */
  def reduce(f: (V, V) => V): Dataset[(K, V)] = {
    val func = (key: K, it: Iterator[V]) => Iterator((key, it.reduce(f)))

    implicit val resultEncoder = ExpressionEncoder.tuple(unresolvedKEncoder, unresolvedVEncoder)
    flatMapGroups(func)
  }

  /**
   * Reduces the elements of each group of data using the specified binary function.
   * The given function must be commutative and associative or the result may be non-deterministic.
   *
   * @since 1.6.0
   */
  def reduce(f: ReduceFunction[V]): Dataset[(K, V)] = {
    reduce(f.call _)
  }

  /**
   * Internal helper function for building typed aggregations that return tuples.  For simplicity
   * and code reuse, we do this without the help of the type system and then use helper functions
   * that cast appropriately for the user facing interface.
   * TODO: does not handle aggrecations that return nonflat results,
   */
  protected def aggUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    val encoders = columns.map(_.encoder)
    val namedColumns =
      columns.map(
        _.withInputType(resolvedVEncoder, dataAttributes).named)
    val keyColumn = if (resolvedKEncoder.flat) {
      assert(groupingAttributes.length == 1)
      groupingAttributes.head
    } else {
      Alias(CreateStruct(groupingAttributes), "key")()
    }
    val aggregate = Aggregate(groupingAttributes, keyColumn +: namedColumns, logicalPlan)
    val execution = new QueryExecution(sqlContext, aggregate)

    new Dataset(
      sqlContext,
      execution,
      ExpressionEncoder.tuple(unresolvedKEncoder +: encoders))
  }

  /**
   * Computes the given aggregation, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing this aggregation over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1](col1: TypedColumn[V, U1]): Dataset[(K, U1)] =
    aggUntyped(col1).asInstanceOf[Dataset[(K, U1)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2]): Dataset[(K, U1, U2)] =
    aggUntyped(col1, col2).asInstanceOf[Dataset[(K, U1, U2)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2, U3](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3]): Dataset[(K, U1, U2, U3)] =
    aggUntyped(col1, col2, col3).asInstanceOf[Dataset[(K, U1, U2, U3)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2, U3, U4](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4]): Dataset[(K, U1, U2, U3, U4)] =
    aggUntyped(col1, col2, col3, col4).asInstanceOf[Dataset[(K, U1, U2, U3, U4)]]

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
   *
   * @since 1.3.0
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): Dataset[Row] = {
    agg((aggExpr +: aggExprs).toMap)
  }

  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(Map(
   *     "age" -> "max",
   *     "expense" -> "sum"
   *   ))
   * }}}
   *
   * @since 1.3.0
   */
  def agg(exprs: Map[String, String]): Dataset[Row] = {
    toDS(exprs.map { case (colName, expr) =>
      strToExpr(expr)(ds(colName).expr)
    }.toSeq)
  }

  /**
   * (Java-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   import com.google.common.collect.ImmutableMap;
   *   df.groupBy("department").agg(ImmutableMap.of("age", "max", "expense", "sum"));
   * }}}
   *
   * @since 1.3.0
   */
  def agg(exprs: java.util.Map[String, String]): Dataset[Row] = {
    agg(exprs.asScala.toMap)
  }

  /**
   * Compute aggregates by specifying a series of aggregate columns. Note that this function by
   * default retains the grouping columns in its output. To not retain grouping columns, set
   * `spark.sql.retainGroupColumns` to false.
   *
   * The available aggregate methods are defined in [[org.apache.spark.sql.functions]].
   *
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df.groupBy("department").agg(max("age"), sum("expense"))
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.groupBy("department").agg(max("age"), sum("expense"));
   * }}}
   *
   * Note that before Spark 1.4, the default behavior is to NOT retain grouping columns. To change
   * to that behavior, set config variable `spark.sql.retainGroupColumns` to `false`.
   * {{{
   *   // Scala, 1.3.x:
   *   df.groupBy("department").agg($"department", max("age"), sum("expense"))
   *
   *   // Java, 1.3.x:
   *   df.groupBy("department").agg(col("department"), max("age"), sum("expense"));
   * }}}
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): Dataset[Row] = {
    toDS((expr +: exprs).map(_.expr))
  }

  /**
   * Returns a [[Dataset]] that contains a tuple with each key and the number of items present
   * for that key.
   *
   * @since 1.6.0
   */
  def count(): Dataset[(K, Long)] = agg(functions.count("*").as(ExpressionEncoder[Long]()))

  /**
   * Compute the average value for each numeric columns for each group. This is an alias for `avg`.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the average values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def mean(colNames: String*): Dataset[Row] = {
    aggregateNumericColumns(colNames : _*)(Average)
  }

  /**
   * Compute the max value for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the max values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def max(colNames: String*): Dataset[Row] = {
    aggregateNumericColumns(colNames : _*)(Max)
  }

  /**
   * Compute the mean value for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the mean values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def avg(colNames: String*): Dataset[Row] = {
    aggregateNumericColumns(colNames : _*)(Average)
  }

  /**
   * Compute the min value for each numeric column for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the min values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def min(colNames: String*): Dataset[Row] = {
    aggregateNumericColumns(colNames : _*)(Min)
  }

  /**
   * Compute the sum for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the sum for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def sum(colNames: String*): Dataset[Row] = {
    aggregateNumericColumns(colNames : _*)(Sum)
  }

  /**
   * Pivots a column of the current [[DataFrame]] and perform the specified aggregation.
   * There are two versions of pivot function: one that requires the caller to specify the list
   * of distinct values to pivot on, and one that does not. The latter is more concise but less
   * efficient, because Spark needs to first compute the list of distinct values internally.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy("year").pivot("course", Seq("dotNET", "Java")).sum("earnings")
   *
   *   // Or without specifying column values (less efficient)
   *   df.groupBy("year").pivot("course").sum("earnings")
   * }}}
   *
   * @param pivotColumn Name of the column to pivot.
   * @since 1.6.0
   */
  def pivot(pivotColumn: String): GroupedDataset[Row, V] = {
    // This is to prevent unintended OOM errors when the number of distinct values is large
    val maxValues = ds.sqlContext.conf.getConf(SQLConf.DATAFRAME_PIVOT_MAX_VALUES)
    // Get the distinct values of the column and sort them so its consistent
    val values = ds.select(pivotColumn)
      .distinct
      .sort(pivotColumn)  // ensure that the output columns are in a consistent logical order
      .rdd
      .map(_.get(0))
      .take(maxValues + 1)
      .toSeq

    if (values.length > maxValues) {
      throw new AnalysisException(
        s"The pivot column $pivotColumn has more than $maxValues distinct values, " +
          "this could indicate an error. " +
          s"If this was intended, set ${SQLConf.DATAFRAME_PIVOT_MAX_VALUES.key} " +
          "to at least the number of distinct values of the pivot column.")
    }

    pivot(pivotColumn, values)
  }

  /**
   * Pivots a column of the current [[DataFrame]] and perform the specified aggregation.
   * There are two versions of pivot function: one that requires the caller to specify the list
   * of distinct values to pivot on, and one that does not. The latter is more concise but less
   * efficient, because Spark needs to first compute the list of distinct values internally.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy("year").pivot("course", Seq("dotNET", "Java")).sum("earnings")
   *
   *   // Or without specifying column values (less efficient)
   *   df.groupBy("year").pivot("course").sum("earnings")
   * }}}
   *
   * @param pivotColumn Name of the column to pivot.
   * @param values List of values that will be translated to columns in the output DataFrame.
   * @since 1.6.0
   */
  def pivot(pivotColumn: String, values: Seq[Any]): GroupedDataset[Row, V] = {
    groupType match {
      case GroupedDataset.GroupByType =>
        new GroupedDataset(
          ds,
          RowEncoder(StructType.fromAttributes(groupingAttributes)),
          ds.queryExecution,
          dataAttributes,
          groupingAttributes,
          GroupedDataset.PivotType(ds.resolve(pivotColumn), values.map(Literal.apply)))
      case _: GroupedData.PivotType =>
        throw new UnsupportedOperationException("repeated pivots are not supported")
      case _ =>
        throw new UnsupportedOperationException("pivot is only supported after a groupBy")
    }
  }

  /**
   * Pivots a column of the current [[DataFrame]] and perform the specified aggregation.
   * There are two versions of pivot function: one that requires the caller to specify the list
   * of distinct values to pivot on, and one that does not. The latter is more concise but less
   * efficient, because Spark needs to first compute the list of distinct values internally.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy("year").pivot("course", Arrays.<Object>asList("dotNET", "Java")).sum("earnings");
   *
   *   // Or without specifying column values (less efficient)
   *   df.groupBy("year").pivot("course").sum("earnings");
   * }}}
   *
   * @param pivotColumn Name of the column to pivot.
   * @param values List of values that will be translated to columns in the output DataFrame.
   * @since 1.6.0
   */
  def pivot(pivotColumn: String, values: java.util.List[Any]): GroupedDataset[Row, V] = {
    pivot(pivotColumn, values.asScala)
  }

  /**
   * Applies the given function to each cogrouped data.  For each unique group, the function will
   * be passed the grouping key and 2 iterators containing all elements in the group from
   * [[Dataset]] `this` and `other`.  The function can return an iterator containing elements of an
   * arbitrary type which will be returned as a new [[Dataset]].
   *
   * @since 1.6.0
   */
  def cogroup[U, R : Encoder](
      other: GroupedDataset[K, U])(
      f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]): Dataset[R] = {
    implicit val uEncoder = other.unresolvedVEncoder
    new Dataset[R](
      sqlContext,
      CoGroup(
        f,
        this.groupingAttributes,
        other.groupingAttributes,
        this.dataAttributes,
        other.dataAttributes,
        this.logicalPlan,
        other.logicalPlan))
  }

  /**
   * Applies the given function to each cogrouped data.  For each unique group, the function will
   * be passed the grouping key and 2 iterators containing all elements in the group from
   * [[Dataset]] `this` and `other`.  The function can return an iterator containing elements of an
   * arbitrary type which will be returned as a new [[Dataset]].
   *
   * @since 1.6.0
   */
  def cogroup[U, R](
      other: GroupedDataset[K, U],
      f: CoGroupFunction[K, V, U, R],
      encoder: Encoder[R]): Dataset[R] = {
    cogroup(other)((key, left, right) => f.call(key, left.asJava, right.asJava).asScala)(encoder)
  }

  private[this] def strToExpr(expr: String): (Expression => Expression) = {
    val exprToFunc: (Expression => Expression) = {
      (inputExpr: Expression) => expr.toLowerCase match {
        // We special handle a few cases that have alias that are not in function registry.
        case "avg" | "average" | "mean" =>
          UnresolvedFunction("avg", inputExpr :: Nil, isDistinct = false)
        case "stddev" | "std" =>
          UnresolvedFunction("stddev", inputExpr :: Nil, isDistinct = false)
        // Also special handle count because we need to take care count(*).
        case "count" | "size" =>
          // Turn count(*) into count(1)
          inputExpr match {
            case s: Star => Count(Literal(1)).toAggregateExpression()
            case _ => Count(inputExpr).toAggregateExpression()
          }
        case name => UnresolvedFunction(name, inputExpr :: Nil, isDistinct = false)
      }
    }
    (inputExpr: Expression) => exprToFunc(inputExpr)
  }

  private[this] def aggregateNumericColumns(colNames: String*)(f: Expression => AggregateFunction)
    : Dataset[Row] = {

    val columnExprs = if (colNames.isEmpty) {
      // No columns specified. Use all numeric columns.
      ds.numericColumns
    } else {
      // Make sure all specified columns are numeric.
      colNames.map { colName =>
        val namedExpr = ds.resolve(colName)
        if (!namedExpr.dataType.isInstanceOf[NumericType]) {
          throw new AnalysisException(
            s""""$colName" is not a numeric column. """ +
              "Aggregation function can only be applied on a numeric column.")
        }
        namedExpr
      }
    }
    toDS(columnExprs.map(expr => f(expr).toAggregateExpression()))
  }
}

/**
 * Companion object for GroupedData.
 */
private[sql] object GroupedDataset {
  /**
   * The Grouping Type
   */
  private[sql] trait GroupType

  /**
   * To indicate it's the GroupBy
   */
  private[sql] object GroupByType extends GroupType

  /**
   * To indicate it's the CUBE
   */
  private[sql] object CubeType extends GroupType

  /**
   * To indicate it's the ROLLUP
   */
  private[sql] object RollupType extends GroupType

  /**
   * To indicate it's the PIVOT
   */
  private[sql] case class PivotType(pivotCol: Expression, values: Seq[Literal]) extends GroupType
}
