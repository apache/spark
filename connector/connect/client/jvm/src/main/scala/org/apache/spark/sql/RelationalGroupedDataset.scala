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

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto

/**
 * A set of methods for aggregations on a `DataFrame`, created by [[Dataset#groupBy groupBy]],
 * [[Dataset#cube cube]] or [[Dataset#rollup rollup]] (and also `pivot`).
 *
 * The main method is the `agg` function, which has multiple variants. This class also contains
 * some first-order statistics such as `mean`, `sum` for convenience.
 *
 * @note
 *   This class was named `GroupedData` in Spark 1.x.
 *
 * @since 3.4.0
 */
class RelationalGroupedDataset private[sql] (
    protected val df: DataFrame,
    private[sql] val groupingExprs: Seq[Column],
    groupType: proto.Aggregate.GroupType,
    pivot: Option[proto.Aggregate.Pivot] = None,
    groupingSets: Option[Seq[proto.Aggregate.GroupingSets]] = None)
    extends api.RelationalGroupedDataset[Dataset] {
  type RGD = RelationalGroupedDataset
  import df.sparkSession.RichColumn

  protected def toDF(aggExprs: Seq[Column]): DataFrame = {
    df.sparkSession.newDataFrame { builder =>
      val aggBuilder = builder.getAggregateBuilder
        .setInput(df.plan.getRoot)
      groupingExprs.foreach(c => aggBuilder.addGroupingExpressions(c.expr))
      aggExprs.foreach(c => aggBuilder.addAggregateExpressions(c.typedExpr(df.encoder)))

      groupType match {
        case proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP =>
          aggBuilder.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP)
        case proto.Aggregate.GroupType.GROUP_TYPE_CUBE =>
          aggBuilder.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_CUBE)
        case proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY =>
          aggBuilder.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
        case proto.Aggregate.GroupType.GROUP_TYPE_PIVOT =>
          assert(pivot.isDefined)
          aggBuilder
            .setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_PIVOT)
            .setPivot(pivot.get)
        case proto.Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS =>
          assert(groupingSets.isDefined)
          aggBuilder.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS)
          groupingSets.get.foreach(aggBuilder.addGroupingSets)
        case g => throw new UnsupportedOperationException(g.toString)
      }
    }
  }

  protected def selectNumericColumns(colNames: Seq[String]): Seq[Column] = {
    // This behaves different than the classic implementation. The classic implementation validates
    // if a column is actually a number, and if it is not it throws an error immediately. In connect
    // it depends on the input type (casting) rules for the method invoked. If the input violates
    // the a different error will be thrown. However it is also possible to get a result for a
    // non-numeric column in connect, for example when you use min/max.
    colNames.map(df.col)
  }

  /** @inheritdoc */
  def as[K: Encoder, T: Encoder]: KeyValueGroupedDataset[K, T] = {
    KeyValueGroupedDatasetImpl[K, T](df, encoderFor[K], encoderFor[T], groupingExprs)
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
  override def pivot(
      pivotColumn: Column,
      values: java.util.List[Any]): RelationalGroupedDataset = {
    super.pivot(pivotColumn, values)
  }

  /** @inheritdoc */
  def pivot(pivotColumn: Column, values: Seq[Any]): RelationalGroupedDataset = {
    groupType match {
      case proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY =>
        val valueExprs = values.map {
          case c: Column if c.expr.hasLiteral => c.expr.getLiteral
          case c: Column if !c.expr.hasLiteral =>
            throw new IllegalArgumentException("values only accept literal Column")
          case v => functions.lit(v).expr.getLiteral
        }
        new RelationalGroupedDataset(
          df,
          groupingExprs,
          proto.Aggregate.GroupType.GROUP_TYPE_PIVOT,
          Some(
            proto.Aggregate.Pivot
              .newBuilder()
              .setCol(pivotColumn.expr)
              .addAllValues(valueExprs.asJava)
              .build()))
      case _ =>
        throw new UnsupportedOperationException()
    }
  }

  /** @inheritdoc */
  def pivot(pivotColumn: Column): RelationalGroupedDataset = {
    pivot(pivotColumn, Nil)
  }
}
