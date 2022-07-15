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

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.SparkRuntimeException
import org.apache.spark.annotation.Stable
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.analysis.{Star, UnresolvedAlias, UnresolvedFunction}
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression
import org.apache.spark.sql.types.{NumericType, StructType}

/**
 * A set of methods for aggregations on a `DataFrame`, created by [[Dataset#groupBy groupBy]],
 * [[Dataset#cube cube]] or [[Dataset#rollup rollup]] (and also `pivot`).
 *
 * The main method is the `agg` function, which has multiple variants. This class also contains
 * some first-order statistics such as `mean`, `sum` for convenience.
 *
 * @note This class was named `GroupedData` in Spark 1.x.
 *
 * @since 2.0.0
 */
@Stable
class RelationalGroupedDataset protected[sql](
    private[sql] val df: DataFrame,
    private[sql] val groupingExprs: Seq[Expression],
    groupType: RelationalGroupedDataset.GroupType) {

  private[this] def toDF(aggExprs: Seq[Expression]): DataFrame = {
    val aggregates = if (df.sparkSession.sessionState.conf.dataFrameRetainGroupColumns) {
      groupingExprs match {
        // call `toList` because `Stream` can't serialize in scala 2.13
        case s: Stream[Expression] => s.toList ++ aggExprs
        case other => other ++ aggExprs
      }
    } else {
      aggExprs
    }

    val aliasedAgg = aggregates.map(alias)

    groupType match {
      case RelationalGroupedDataset.GroupByType =>
        Dataset.ofRows(df.sparkSession, Aggregate(groupingExprs, aliasedAgg, df.logicalPlan))
      case RelationalGroupedDataset.RollupType =>
        Dataset.ofRows(
          df.sparkSession, Aggregate(Seq(Rollup(groupingExprs.map(Seq(_)))),
            aliasedAgg, df.logicalPlan))
      case RelationalGroupedDataset.CubeType =>
        Dataset.ofRows(
          df.sparkSession, Aggregate(Seq(Cube(groupingExprs.map(Seq(_)))),
            aliasedAgg, df.logicalPlan))
      case RelationalGroupedDataset.PivotType(pivotCol, values) =>
        val aliasedGrps = groupingExprs.map(alias)
        Dataset.ofRows(
          df.sparkSession, Pivot(Some(aliasedGrps), pivotCol, values, aggExprs, df.logicalPlan))
    }
  }

  private[this] def alias(expr: Expression): NamedExpression = expr match {
    case expr: NamedExpression => expr
    case a: AggregateExpression if a.aggregateFunction.isInstanceOf[TypedAggregateExpression] =>
      UnresolvedAlias(a, Some(Column.generateAlias))
    case expr: Expression => Alias(expr, toPrettySQL(expr))()
  }

  private[this] def aggregateNumericColumns(colNames: String*)(f: Expression => AggregateFunction)
    : DataFrame = {

    val columnExprs = if (colNames.isEmpty) {
      // No columns specified. Use all numeric columns.
      df.numericColumns
    } else {
      // Make sure all specified columns are numeric.
      colNames.map { colName =>
        val namedExpr = df.resolve(colName)
        if (!namedExpr.dataType.isInstanceOf[NumericType]) {
          throw QueryCompilationErrors.aggregationFunctionAppliedOnNonNumericColumnError(colName)
        }
        namedExpr
      }
    }
    toDF(columnExprs.map(expr => f(expr).toAggregateExpression()))
  }

  private[this] def strToExpr(expr: String): (Expression => Expression) = {
    val exprToFunc: (Expression => Expression) = {
      (inputExpr: Expression) => expr.toLowerCase(Locale.ROOT) match {
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

  /**
   * Returns a `KeyValueGroupedDataset` where the data is grouped by the grouping expressions
   * of current `RelationalGroupedDataset`.
   *
   * @since 3.0.0
   */
  def as[K: Encoder, T: Encoder]: KeyValueGroupedDataset[K, T] = {
    val keyEncoder = encoderFor[K]
    val valueEncoder = encoderFor[T]

    // Resolves grouping expressions.
    val dummyPlan = Project(groupingExprs.map(alias), LocalRelation(df.logicalPlan.output))
    val analyzedPlan = df.sparkSession.sessionState.analyzer.execute(dummyPlan)
      .asInstanceOf[Project]
    df.sparkSession.sessionState.analyzer.checkAnalysis(analyzedPlan)
    val aliasedGroupings = analyzedPlan.projectList

    // Adds the grouping expressions that are not in base DataFrame into outputs.
    val addedCols = aliasedGroupings.filter(g => !df.logicalPlan.outputSet.contains(g.toAttribute))
    val qe = Dataset.ofRows(
      df.sparkSession,
      Project(df.logicalPlan.output ++ addedCols, df.logicalPlan)).queryExecution

    new KeyValueGroupedDataset(
      keyEncoder,
      valueEncoder,
      qe,
      df.logicalPlan.output,
      aliasedGroupings.map(_.toAttribute))
  }

  /**
   * (Scala-specific) Compute aggregates by specifying the column names and
   * aggregate methods. The resulting `DataFrame` will also contain the grouping columns.
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
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    toDF((aggExpr +: aggExprs).map { case (colName, expr) =>
      strToExpr(expr)(df(colName).expr)
    })
  }

  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting `DataFrame` will also contain the grouping columns.
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
  def agg(exprs: Map[String, String]): DataFrame = {
    toDF(exprs.map { case (colName, expr) =>
      strToExpr(expr)(df(colName).expr)
    }.toSeq)
  }

  /**
   * (Java-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting `DataFrame` will also contain the grouping columns.
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
  def agg(exprs: java.util.Map[String, String]): DataFrame = {
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
  def agg(expr: Column, exprs: Column*): DataFrame = {
    toDF((expr +: exprs).map {
      case typed: TypedColumn[_, _] =>
        typed.withInputType(df.exprEnc, df.logicalPlan.output).expr
      case c => c.expr
    })
  }

  /**
   * Count the number of rows for each group.
   * The resulting `DataFrame` will also contain the grouping columns.
   *
   * @since 1.3.0
   */
  def count(): DataFrame = toDF(Seq(Alias(Count(Literal(1)).toAggregateExpression(), "count")()))

  /**
   * Compute the average value for each numeric columns for each group. This is an alias for `avg`.
   * The resulting `DataFrame` will also contain the grouping columns.
   * When specified columns are given, only compute the average values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def mean(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Average(_))
  }

  /**
   * Compute the max value for each numeric columns for each group.
   * The resulting `DataFrame` will also contain the grouping columns.
   * When specified columns are given, only compute the max values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def max(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Max)
  }

  /**
   * Compute the mean value for each numeric columns for each group.
   * The resulting `DataFrame` will also contain the grouping columns.
   * When specified columns are given, only compute the mean values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def avg(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Average(_))
  }

  /**
   * Compute the min value for each numeric column for each group.
   * The resulting `DataFrame` will also contain the grouping columns.
   * When specified columns are given, only compute the min values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def min(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Min)
  }

  /**
   * Compute the sum for each numeric columns for each group.
   * The resulting `DataFrame` will also contain the grouping columns.
   * When specified columns are given, only compute the sum for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def sum(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Sum(_))
  }

  /**
   * Pivots a column of the current `DataFrame` and performs the specified aggregation.
   *
   * There are two versions of `pivot` function: one that requires the caller to specify the list
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
   * @see `org.apache.spark.sql.Dataset.unpivot` for the reverse operation,
   *      except for the aggregation.
   *
   * @param pivotColumn Name of the column to pivot.
   * @since 1.6.0
   */
  def pivot(pivotColumn: String): RelationalGroupedDataset = pivot(Column(pivotColumn))

  /**
   * Pivots a column of the current `DataFrame` and performs the specified aggregation.
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
   * From Spark 3.0.0, values can be literal columns, for instance, struct. For pivoting by
   * multiple columns, use the `struct` function to combine the columns and values:
   *
   * {{{
   *   df.groupBy("year")
   *     .pivot("trainingCourse", Seq(struct(lit("java"), lit("Experts"))))
   *     .agg(sum($"earnings"))
   * }}}
   *
   * @see `org.apache.spark.sql.Dataset.unpivot` for the reverse operation,
   *      except for the aggregation.
   *
   * @param pivotColumn Name of the column to pivot.
   * @param values List of values that will be translated to columns in the output DataFrame.
   * @since 1.6.0
   */
  def pivot(pivotColumn: String, values: Seq[Any]): RelationalGroupedDataset = {
    pivot(Column(pivotColumn), values)
  }

  /**
   * (Java-specific) Pivots a column of the current `DataFrame` and performs the specified
   * aggregation.
   *
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
   * @see `org.apache.spark.sql.Dataset.unpivot` for the reverse operation,
   *      except for the aggregation.
   *
   * @param pivotColumn Name of the column to pivot.
   * @param values List of values that will be translated to columns in the output DataFrame.
   * @since 1.6.0
   */
  def pivot(pivotColumn: String, values: java.util.List[Any]): RelationalGroupedDataset = {
    pivot(Column(pivotColumn), values)
  }

  /**
   * Pivots a column of the current `DataFrame` and performs the specified aggregation.
   * This is an overloaded version of the `pivot` method with `pivotColumn` of the `String` type.
   *
   * {{{
   *   // Or without specifying column values (less efficient)
   *   df.groupBy($"year").pivot($"course").sum($"earnings");
   * }}}
   *
   * @see `org.apache.spark.sql.Dataset.unpivot` for the reverse operation,
   *      except for the aggregation.
   *
   * @param pivotColumn he column to pivot.
   * @since 2.4.0
   */
  def pivot(pivotColumn: Column): RelationalGroupedDataset = {
    // This is to prevent unintended OOM errors when the number of distinct values is large
    val maxValues = df.sparkSession.sessionState.conf.dataFramePivotMaxValues
    // Get the distinct values of the column and sort them so its consistent
    val values = df.select(pivotColumn)
      .distinct()
      .limit(maxValues + 1)
      .sort(pivotColumn)  // ensure that the output columns are in a consistent logical order
      .collect()
      .map(_.get(0))
      .toSeq

    if (values.length > maxValues) {
      throw QueryCompilationErrors.aggregationFunctionAppliedOnNonNumericColumnError(
        pivotColumn.toString, maxValues)
    }

    pivot(pivotColumn, values)
  }

  /**
   * Pivots a column of the current `DataFrame` and performs the specified aggregation.
   * This is an overloaded version of the `pivot` method with `pivotColumn` of the `String` type.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy($"year").pivot($"course", Seq("dotNET", "Java")).sum($"earnings")
   * }}}
   *
   * @see `org.apache.spark.sql.Dataset.unpivot` for the reverse operation,
   *      except for the aggregation.
   *
   * @param pivotColumn the column to pivot.
   * @param values List of values that will be translated to columns in the output DataFrame.
   * @since 2.4.0
   */
  def pivot(pivotColumn: Column, values: Seq[Any]): RelationalGroupedDataset = {
    groupType match {
      case RelationalGroupedDataset.GroupByType =>
        val valueExprs = values.map(_ match {
          case c: Column => c.expr
          case v =>
            try {
              Literal.apply(v)
            } catch {
              case _: SparkRuntimeException =>
                throw QueryExecutionErrors.pivotColumnUnsupportedError(v, pivotColumn.expr.dataType)
            }
        })
        new RelationalGroupedDataset(
          df,
          groupingExprs,
          RelationalGroupedDataset.PivotType(pivotColumn.expr, valueExprs))
      case _: RelationalGroupedDataset.PivotType =>
        throw QueryExecutionErrors.repeatedPivotsUnsupportedError()
      case _ =>
        throw QueryExecutionErrors.pivotNotAfterGroupByUnsupportedError()
    }
  }

  /**
   * (Java-specific) Pivots a column of the current `DataFrame` and performs the specified
   * aggregation. This is an overloaded version of the `pivot` method with `pivotColumn` of
   * the `String` type.
   *
   * @see `org.apache.spark.sql.Dataset.unpivot` for the reverse operation,
   *      except for the aggregation.
   *
   * @param pivotColumn the column to pivot.
   * @param values List of values that will be translated to columns in the output DataFrame.
   * @since 2.4.0
   */
  def pivot(pivotColumn: Column, values: java.util.List[Any]): RelationalGroupedDataset = {
    pivot(pivotColumn, values.asScala.toSeq)
  }

  /**
   * Applies the given serialized R function `func` to each group of data. For each unique group,
   * the function will be passed the group key and an iterator that contains all of the elements in
   * the group. The function can return an iterator containing elements of an arbitrary type which
   * will be returned as a new `DataFrame`.
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 2.0.0
   */
  private[sql] def flatMapGroupsInR(
      f: Array[Byte],
      packageNames: Array[Byte],
      broadcastVars: Array[Broadcast[Object]],
      outputSchema: StructType): DataFrame = {
      val groupingNamedExpressions = groupingExprs.map(alias)
      val groupingCols = groupingNamedExpressions.map(Column(_))
      val groupingDataFrame = df.select(groupingCols : _*)
      val groupingAttributes = groupingNamedExpressions.map(_.toAttribute)
      Dataset.ofRows(
        df.sparkSession,
        FlatMapGroupsInR(
          f,
          packageNames,
          broadcastVars,
          outputSchema,
          groupingDataFrame.exprEnc.deserializer,
          df.exprEnc.deserializer,
          df.exprEnc.schema,
          groupingAttributes,
          df.logicalPlan.output,
          df.logicalPlan))
  }

  /**
   * Applies a grouped vectorized python user-defined function to each group of data.
   * The user-defined function defines a transformation: `pandas.DataFrame` -> `pandas.DataFrame`.
   * For each group, all elements in the group are passed as a `pandas.DataFrame` and the results
   * for all groups are combined into a new [[DataFrame]].
   *
   * This function does not support partial aggregation, and requires shuffling all the data in
   * the [[DataFrame]].
   *
   * This function uses Apache Arrow as serialization format between Java executors and Python
   * workers.
   */
  private[sql] def flatMapGroupsInPandas(expr: PythonUDF): DataFrame = {
    require(expr.evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
      "Must pass a grouped map udf")
    require(expr.dataType.isInstanceOf[StructType],
      s"The returnType of the udf must be a ${StructType.simpleString}")

    val groupingNamedExpressions = groupingExprs.map {
      case ne: NamedExpression => ne
      case other => Alias(other, other.toString)()
    }
    val child = df.logicalPlan
    val project = df.sparkSession.sessionState.executePlan(
      Project(groupingNamedExpressions ++ child.output, child)).analyzed
    val groupingAttributes = project.output.take(groupingNamedExpressions.length)
    val output = expr.dataType.asInstanceOf[StructType].toAttributes
    val plan = FlatMapGroupsInPandas(groupingAttributes, expr, output, project)

    Dataset.ofRows(df.sparkSession, plan)
  }

  /**
   * Applies a vectorized python user-defined function to each cogrouped data.
   * The user-defined function defines a transformation:
   * `pandas.DataFrame`, `pandas.DataFrame` -> `pandas.DataFrame`.
   *  For each group in the cogrouped data, all elements in the group are passed as a
   * `pandas.DataFrame` and the results for all cogroups are combined into a new [[DataFrame]].
   *
   * This function uses Apache Arrow as serialization format between Java executors and Python
   * workers.
   */
  private[sql] def flatMapCoGroupsInPandas(
      r: RelationalGroupedDataset,
      expr: PythonUDF): DataFrame = {
    require(expr.evalType == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
      "Must pass a cogrouped map udf")
    require(expr.dataType.isInstanceOf[StructType],
      s"The returnType of the udf must be a ${StructType.simpleString}")

    val leftGroupingNamedExpressions = groupingExprs.map {
      case ne: NamedExpression => ne
      case other => Alias(other, other.toString)()
    }

    val rightGroupingNamedExpressions = r.groupingExprs.map {
      case ne: NamedExpression => ne
      case other => Alias(other, other.toString)()
    }

    val leftChild = df.logicalPlan
    val rightChild = r.df.logicalPlan

    val left = df.sparkSession.sessionState.executePlan(
      Project(leftGroupingNamedExpressions ++ leftChild.output, leftChild)).analyzed
    val right = r.df.sparkSession.sessionState.executePlan(
      Project(rightGroupingNamedExpressions ++ rightChild.output, rightChild)).analyzed

    val output = expr.dataType.asInstanceOf[StructType].toAttributes
    val plan = FlatMapCoGroupsInPandas(
      leftGroupingNamedExpressions.length, rightGroupingNamedExpressions.length,
      expr, output, left, right)
    Dataset.ofRows(df.sparkSession, plan)
  }

  override def toString: String = {
    val builder = new StringBuilder
    builder.append("RelationalGroupedDataset: [grouping expressions: [")
    val kFields = groupingExprs.collect {
      case expr: NamedExpression if expr.resolved =>
        s"${expr.name}: ${expr.dataType.simpleString(2)}"
      case expr: NamedExpression => expr.name
      case o => o.toString
    }
    builder.append(kFields.take(2).mkString(", "))
    if (kFields.length > 2) {
      builder.append(" ... " + (kFields.length - 2) + " more field(s)")
    }
    builder.append(s"], value: ${df.toString}, type: $groupType]").toString()
  }
}

private[sql] object RelationalGroupedDataset {

  def apply(
      df: DataFrame,
      groupingExprs: Seq[Expression],
      groupType: GroupType): RelationalGroupedDataset = {
    new RelationalGroupedDataset(df, groupingExprs, groupType: GroupType)
  }

  /**
   * The Grouping Type
   */
  private[sql] trait GroupType {
    override def toString: String = getClass.getSimpleName.stripSuffix("$").stripSuffix("Type")
  }

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
  private[sql] case class PivotType(pivotCol: Expression, values: Seq[Expression]) extends GroupType
}
