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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * A special node that allows skipping query planning all the way to physical execution. This node
 * can be used when a query is being run repeatedly, and the plan doesn't need to be optimized
 * every time, or a plan was already optimized, and planned to the physical level, but we had to
 * go back to logical plan land for some reason (e.g. V1 DataSource write execution).
 */
case class AlreadyPlanned(
    physicalPlan: SparkPlan,
    output: Seq[Attribute]) extends LeafNode with MultiInstanceRelation {

  override def newInstance(): LogicalPlan = this.copy(output = output.map(_.newInstance()))

  override def computeStats(): Statistics = Statistics(sizeInBytes = conf.defaultSizeInBytes)
}

case class AlreadyPlannedExec(
    physicalPlan: SparkPlan,
    output: Seq[Attribute]) extends LeafExecNode {
  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
  override def supportsColumnar: Boolean = physicalPlan.supportsColumnar
  override def vectorTypes: Option[Seq[String]] = physicalPlan.vectorTypes
  override def outputPartitioning: Partitioning = physicalPlan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = physicalPlan.outputOrdering
}

object AlreadyPlanned {
  def dataFrame(sparkSession: SparkSession, query: SparkPlan): DataFrame = {
    val plan = AlreadyPlanned(query, query.output)
    Dataset.ofRows(sparkSession, plan)
  }
}

object ExtractAlreadyPlanned extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case AlreadyPlannedExec(alreadyPlanned: SparkPlan, output) =>
      val newAttr = output.map(o => o.name -> o).toMap
      val newPlan = alreadyPlanned.mapExpressions {
        case a: Attribute => a.withExprId(newAttr(a.name).exprId)
      }
      newPlan
  }
}
