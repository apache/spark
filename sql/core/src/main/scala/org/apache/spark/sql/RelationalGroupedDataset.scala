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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.annotation.Stable
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.ExpressionUtils.{column, generateAlias}
import org.apache.spark.sql.internal.TypedAggUtils.withInputType
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{NumericType, StructType}
import org.apache.spark.util.ArrayImplicits._

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
    protected[sql] val df: DataFrame,
    private[sql] val groupingExprs: Seq[Expression],
    groupType: RelationalGroupedDataset.GroupType)
  extends api.RelationalGroupedDataset[Dataset] {
  type RGD = RelationalGroupedDataset
  import RelationalGroupedDataset._
  import df.sparkSession._

  override protected def toDF(aggCols: Seq[Column]): DataFrame = {
    val aggExprs = aggCols.map(expression).map { e =>
      withInputType(e, df.exprEnc, df.logicalPlan.output)
    }

    @scala.annotation.nowarn("cat=deprecation")
    val aggregates = if (df.sparkSession.sessionState.conf.dataFrameRetainGroupColumns) {
      groupingExprs match {
        // call `toList` because `Stream` and `LazyList` can't serialize in scala 2.13
        case s: LazyList[Expression] => s.toList ++ aggExprs
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
      case RelationalGroupedDataset.GroupingSetsType(groupingSets) =>
        Dataset.ofRows(
          df.sparkSession,
          Aggregate(Seq(GroupingSets(groupingSets, groupingExprs)),
            aliasedAgg, df.logicalPlan))
      case RelationalGroupedDataset.PivotType(pivotCol, values) =>
        val aliasedGrps = groupingExprs.map(alias)
        Dataset.ofRows(
          df.sparkSession, Pivot(Some(aliasedGrps), pivotCol, values, aggExprs, df.logicalPlan))
    }
  }

  override protected def selectNumericColumns(colNames: Seq[String]): Seq[Column] = {
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
    columnExprs.map(column)
  }

  /** @inheritdoc */
  def as[K: Encoder, T: Encoder]: KeyValueGroupedDataset[K, T] = {
    val keyEncoder = encoderFor[K]
    val valueEncoder = encoderFor[T]

    val (qe, groupingAttributes) =
      handleGroupingExpression(df.logicalPlan, df.sparkSession, groupingExprs)

    new KeyValueGroupedDataset(
      keyEncoder,
      valueEncoder,
      qe,
      df.logicalPlan.output,
      groupingAttributes)
  }

  /** @inheritdoc */
  override def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame =
    super.agg(aggExpr, aggExprs: _*)

  /** @inheritdoc */
  override def agg(exprs: Map[String, String]): DataFrame = super.agg(exprs)

  /** @inheritdoc */
  override def agg(exprs: java.util.Map[String, String]): DataFrame = super.agg(exprs)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def agg(expr: Column, exprs: Column*): DataFrame = super.agg(expr, exprs: _*)

  /** @inheritdoc */
  override def count(): DataFrame = super.count()

  /** @inheritdoc */
  @scala.annotation.varargs
  override def mean(colNames: String*): DataFrame = super.mean(colNames: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def max(colNames: String*): DataFrame = super.max(colNames: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def avg(colNames: String*): DataFrame = super.avg(colNames: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def min(colNames: String*): DataFrame = super.min(colNames: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def sum(colNames: String*): DataFrame = super.sum(colNames: _*)

  /** @inheritdoc */
  override def pivot(pivotColumn: String): RelationalGroupedDataset = super.pivot(pivotColumn)

  /** @inheritdoc */
  override def pivot(pivotColumn: String, values: Seq[Any]): RelationalGroupedDataset =
    super.pivot(pivotColumn, values)

  /** @inheritdoc */
  override def pivot(pivotColumn: String, values: java.util.List[Any]): RelationalGroupedDataset =
    super.pivot(pivotColumn, values)

  /** @inheritdoc */
  override def pivot(pivotColumn: Column, values: java.util.List[Any]): RelationalGroupedDataset = {
    super.pivot(pivotColumn, values)
  }

  /** @inheritdoc */
  override def pivot(pivotColumn: Column): RelationalGroupedDataset =
    pivot(pivotColumn, collectPivotValues(df, pivotColumn))

  /** @inheritdoc */
  def pivot(pivotColumn: Column, values: Seq[Any]): RelationalGroupedDataset = {
    groupType match {
      case RelationalGroupedDataset.GroupByType =>
        val valueExprs = values.map {
          case c: Column => c.expr
          case v =>
            try {
              Literal.apply(v)
            } catch {
              case _: SparkRuntimeException =>
                throw QueryExecutionErrors.pivotColumnUnsupportedError(v, pivotColumn.expr)
            }
        }
        new RelationalGroupedDataset(
          df,
          groupingExprs,
          RelationalGroupedDataset.PivotType(pivotColumn.expr, valueExprs))
      case _: RelationalGroupedDataset.PivotType =>
        throw QueryExecutionErrors.repeatedPivotsUnsupportedError(
          clause = "PIVOT", operation = "SUBQUERY"
        )
      case _ =>
        throw QueryExecutionErrors.pivotNotAfterGroupByUnsupportedError()
    }
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
      val groupingCols = groupingNamedExpressions.map(column)
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
  private[sql] def flatMapGroupsInPandas(column: Column): DataFrame = {
    val expr = column.expr.asInstanceOf[PythonUDF]
    require(expr.evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
      "Must pass a grouped map pandas udf")
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
    val output = toAttributes(expr.dataType.asInstanceOf[StructType])
    val plan = FlatMapGroupsInPandas(groupingAttributes, expr, output, project)

    Dataset.ofRows(df.sparkSession, plan)
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
  private[sql] def flatMapGroupsInArrow(column: Column): DataFrame = {
    val expr = column.expr.asInstanceOf[PythonUDF]
    require(expr.evalType == PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF,
      "Must pass a grouped map arrow udf")
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
    val output = toAttributes(expr.dataType.asInstanceOf[StructType])
    val plan = FlatMapGroupsInArrow(groupingAttributes, expr, output, project)

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
      column: Column): DataFrame = {
    val expr = column.expr.asInstanceOf[PythonUDF]
    require(expr.evalType == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
      "Must pass a cogrouped map pandas udf")
    require(this.groupingExprs.length == r.groupingExprs.length,
      "Cogroup keys must have same size: " +
        s"${this.groupingExprs.length} != ${r.groupingExprs.length}")
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

    val output = toAttributes(expr.dataType.asInstanceOf[StructType])
    val plan = FlatMapCoGroupsInPandas(
      leftGroupingNamedExpressions.length, rightGroupingNamedExpressions.length,
      expr, output, left, right)
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
  private[sql] def flatMapCoGroupsInArrow(
      r: RelationalGroupedDataset,
      column: Column): DataFrame = {
    val expr = column.expr.asInstanceOf[PythonUDF]
    require(expr.evalType == PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF,
      "Must pass a cogrouped map arrow udf")
    require(this.groupingExprs.length == r.groupingExprs.length,
      "Cogroup keys must have same size: " +
        s"${this.groupingExprs.length} != ${r.groupingExprs.length}")
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

    val output = toAttributes(expr.dataType.asInstanceOf[StructType])
    val plan = FlatMapCoGroupsInArrow(
      leftGroupingNamedExpressions.length, rightGroupingNamedExpressions.length,
      expr, output, left, right)
    Dataset.ofRows(df.sparkSession, plan)
  }

  /**
   * Applies a grouped vectorized python user-defined function to each group of data.
   * The user-defined function defines a transformation: iterator of `pandas.DataFrame` ->
   * iterator of `pandas.DataFrame`.
   * For each group, all elements in the group are passed as an iterator of `pandas.DataFrame`
   * along with corresponding state, and the results for all groups are combined into a new
   * [[DataFrame]].
   *
   * This function does not support partial aggregation, and requires shuffling all the data in
   * the [[DataFrame]].
   *
   * This function uses Apache Arrow as serialization format between Java executors and Python
   * workers.
   */
  private[sql] def applyInPandasWithState(
      func: Column,
      outputStructType: StructType,
      stateStructType: StructType,
      outputModeStr: String,
      timeoutConfStr: String): DataFrame = {
    val timeoutConf = org.apache.spark.sql.execution.streaming
      .GroupStateImpl.groupStateTimeoutFromString(timeoutConfStr)
    val outputMode = InternalOutputModes(outputModeStr)
    if (outputMode != OutputMode.Append && outputMode != OutputMode.Update) {
      throw new IllegalArgumentException("The output mode of function should be append or update")
    }
    val groupingNamedExpressions = groupingExprs.map {
      case ne: NamedExpression => ne
      case other => Alias(other, other.toString)()
    }
    val groupingAttrs = groupingNamedExpressions.map(_.toAttribute)
    val outputAttrs = toAttributes(outputStructType)
    val plan = FlatMapGroupsInPandasWithState(
      func.expr,
      groupingAttrs,
      outputAttrs,
      stateStructType,
      outputMode,
      timeoutConf,
      child = df.logicalPlan)
    Dataset.ofRows(df.sparkSession, plan)
  }

  /**
   * Applies a grouped vectorized python user-defined function to each group of data.
   * The user-defined function defines a transformation: iterator of `pandas.DataFrame` ->
   * iterator of `pandas.DataFrame`.
   * For each group, all elements in the group are passed as an iterator of `pandas.DataFrame`
   * along with corresponding state, and the results for all groups are combined into a new
   * [[DataFrame]].
   *
   * This function uses Apache Arrow as serialization format between Java executors and Python
   * workers.
   */
  private[sql] def transformWithStateInPandas(
      func: Column,
      outputStructType: StructType,
      outputModeStr: String,
      timeModeStr: String,
      initialState: RelationalGroupedDataset): DataFrame = {
    def exprToAttr(expr: Seq[Expression]): Seq[Attribute] = {
      expr.map {
        case ne: NamedExpression => ne
        case other => Alias(other, other.toString)()
      }.map(_.toAttribute)
    }

    val groupingAttrs = exprToAttr(groupingExprs)
    val outputAttrs = toAttributes(outputStructType)
    val outputMode = InternalOutputModes(outputModeStr)
    val timeMode = TimeModes(timeModeStr)

    val plan: LogicalPlan = if (initialState == null) {
      TransformWithStateInPandas(
        func.expr,
        groupingAttrs.length,
        outputAttrs,
        outputMode,
        timeMode,
        child = df.logicalPlan,
        hasInitialState = false,
        /* The followings are dummy variables because hasInitialState is false */
        initialState = LocalRelation(Seq.empty[Attribute]),
        initGroupingAttrsLen = 0,
        initialStateSchema = new StructType()
      )
    } else {
      val initGroupingAttrs = exprToAttr(initialState.groupingExprs)

      val leftChild = df.logicalPlan
      val rightChild = initialState.df.logicalPlan

      val left = df.sparkSession.sessionState.executePlan(
        Project(leftChild.output, leftChild)).analyzed
      val right = initialState.df.sparkSession.sessionState.executePlan(
        Project(rightChild.output, rightChild)).analyzed


      TransformWithStateInPandas(
        func.expr,
        groupingAttributesLen = groupingAttrs.length,
        outputAttrs,
        outputMode,
        timeMode,
        child = left,
        hasInitialState = true,
        initialState = right,
        initGroupingAttrsLen = initGroupingAttrs.length,
        initialStateSchema = initialState.df.schema
      )
    }
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

  private[sql] def handleGroupingExpression(
      logicalPlan: LogicalPlan,
      sparkSession: SparkSession,
      groupingExprs: Seq[Expression]): (QueryExecution, Seq[Attribute]) = {
    // Resolves grouping expressions.
    val dummyPlan = Project(groupingExprs.map(alias), LocalRelation(logicalPlan.output))
    val analyzedPlan = sparkSession.sessionState.analyzer.execute(dummyPlan)
      .asInstanceOf[Project]
    sparkSession.sessionState.analyzer.checkAnalysis(analyzedPlan)
    val aliasedGroupings = analyzedPlan.projectList

    // Adds the grouping expressions that are not in base DataFrame into outputs.
    val addedCols = aliasedGroupings.filter(g => !logicalPlan.outputSet.contains(g.toAttribute))
    val newPlan = Project(logicalPlan.output ++ addedCols, logicalPlan)
    val qe = sparkSession.sessionState.executePlan(newPlan)

    (qe, aliasedGroupings.map(_.toAttribute))
  }

  private def alias(expr: Expression): NamedExpression = expr match {
    case expr: NamedExpression => expr
    case a: AggregateExpression => UnresolvedAlias(a, Some(generateAlias))
    case _ if !expr.resolved => UnresolvedAlias(expr, None)
    case expr: Expression => Alias(expr, toPrettySQL(expr))()
  }

  private[sql] def collectPivotValues(df: DataFrame, pivotColumn: Column): Seq[Any] = {
    if (df.isStreaming) {
      throw new AnalysisException(
        errorClass = "_LEGACY_ERROR_TEMP_3063",
        messageParameters = Map.empty)
    }
    // This is to prevent unintended OOM errors when the number of distinct values is large
    val maxValues = df.sparkSession.sessionState.conf.dataFramePivotMaxValues
    // Get the distinct values of the column and sort them so its consistent
    val values = df.select(pivotColumn)
      .distinct()
      .limit(maxValues + 1)
      .sort(pivotColumn) // ensure that the output columns are in a consistent logical order
      .collect()
      .map(_.get(0))
      .toImmutableArraySeq

    if (values.length > maxValues) {
      throw QueryCompilationErrors.aggregationFunctionAppliedOnNonNumericColumnError(
        pivotColumn.toString, maxValues)
    }
    values
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
   * To indicate it's the GroupingSets
   */
  private[sql] case class GroupingSetsType(groupingSets: Seq[Seq[Expression]]) extends GroupType

  /**
   * To indicate it's the PIVOT
   */
  private[sql] case class PivotType(pivotCol: Expression, values: Seq[Expression]) extends GroupType
}
