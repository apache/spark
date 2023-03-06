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

package org.apache.spark.sql.execution

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * A special operator for global [[SortExec]] if it is the root node with/without [[ProjectExec]].
 */
case class DriverSortExec(
    projectList: Seq[NamedExpression],
    sortOrder: Seq[SortOrder],
    child: SparkPlan) extends OrderPreservingUnaryExecNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
  override protected def outputExpressions: Seq[NamedExpression] = projectList
  override protected def orderingExpressions: Seq[SortOrder] = sortOrder
  override def outputPartitioning: Partitioning = SinglePartition

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "sort time"))

  private def collectAndSort(): Array[InternalRow] = {
    val sortTime = longMetric("sortTime")
    val rows = child.executeCollect()
    val ordering = new LazilyGeneratedOrdering(sortOrder, child.output)
    val startTime = System.nanoTime()
    val sortedRows = rows.sorted(ordering)
    sortTime += NANOSECONDS.toMillis(System.nanoTime() - startTime)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext, executionId, sortTime :: Nil)
    val project = UnsafeProjection.create(projectList, child.output)
    sortedRows.map(row => project(row).copy())
  }

  override def executeCollect(): Array[InternalRow] = {
    collectAndSort()
  }

  override def executeToIterator(): Iterator[InternalRow] = {
    collectAndSort().toIterator
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val sortedRows = collectAndSort()
    sparkContext.parallelize(sortedRows.toSeq, 1)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DriverSortExec =
    copy(child = newChild)
}
