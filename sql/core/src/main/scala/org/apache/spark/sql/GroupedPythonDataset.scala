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

import org.apache.spark.api.python.PythonFunction
import org.apache.spark.sql.GroupedData.GroupType
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.PythonMapGroups
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.types.{DataType, StructType}

class GroupedPythonDataset private[sql](
    queryExecution: QueryExecution,
    groupingExprs: Seq[Expression],
    dataAttributes: Seq[Attribute],
    groupType: GroupType) {

  private def sqlContext = queryExecution.sqlContext

  protected[sql] def isPickled(): Boolean = EvaluatePython.isPickled(dataAttributes.toStructType)

  private def groupedData =
    new GroupedData(
      new DataFrame(sqlContext, queryExecution), groupingExprs, GroupedData.GroupByType)

  @scala.annotation.varargs
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    groupedData.agg(aggExpr, aggExprs: _*)
  }

  def agg(exprs: Map[String, String]): DataFrame = groupedData.agg(exprs)

  def agg(exprs: java.util.Map[String, String]): DataFrame = groupedData.agg(exprs)

  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = groupedData.agg(expr, exprs: _*)

  def count(): DataFrame = groupedData.count()

  @scala.annotation.varargs
  def mean(colNames: String*): DataFrame = groupedData.mean(colNames: _*)

  @scala.annotation.varargs
  def max(colNames: String*): DataFrame = groupedData.max(colNames: _*)

  @scala.annotation.varargs
  def avg(colNames: String*): DataFrame = groupedData.avg(colNames: _*)

  @scala.annotation.varargs
  def min(colNames: String*): DataFrame = groupedData.min(colNames: _*)

  @scala.annotation.varargs
  def sum(colNames: String*): DataFrame = groupedData.sum(colNames: _*)

  def pivot(pivotColumn: String): GroupedData = groupedData.pivot(pivotColumn)

  def pivot(pivotColumn: String, values: Seq[Any]): GroupedData =
    groupedData.pivot(pivotColumn, values)

  def pivot(pivotColumn: String, values: java.util.List[Any]): GroupedData =
    groupedData.pivot(pivotColumn, values)

  def flatMapGroups(f: PythonFunction, schemaJson: String): DataFrame = {
    val schema = DataType.fromJson(schemaJson).asInstanceOf[StructType]
    internalFlatMapGroups(f, schema)
  }

  def flatMapGroups(f: PythonFunction): DataFrame = {
    internalFlatMapGroups(f, EvaluatePython.schemaOfPickled)
  }

  private def internalFlatMapGroups(f: PythonFunction, schema: StructType): DataFrame = {
    new DataFrame(
      sqlContext,
      PythonMapGroups(
        f,
        groupingExprs,
        dataAttributes,
        schema.toAttributes,
        queryExecution.logical))
  }
}
