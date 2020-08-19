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

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}

/**
 * A special node that allows skipping query planning all the way to physical execution. This node
 * can be used when a query was planned to the physical level, but we had to go back to logical plan
 * land for some reason (e.g. V1 DataSource write execution). This will allow the metrics, and the
 * query plan to properly appear as part of the query execution.
 */
case class AlreadyPlanned(physicalPlan: SparkPlan) extends LogicalPlan with MultiInstanceRelation {
  override def children: Seq[LogicalPlan] = Nil
  override lazy val resolved: Boolean = true
  override val output: Seq[Attribute] = physicalPlan.output
  override def newInstance(): LogicalPlan = {
    val newAttrs = output.map(a => a.exprId -> a.newInstance())
    val attrMap = newAttrs.toMap
    val projections = physicalPlan.output.map { o =>
      Alias(o, o.name)(attrMap(o.exprId).exprId, o.qualifier, Option(o.metadata))
    }
    AlreadyPlanned(ProjectExec(projections, physicalPlan))
  }
}

/** Query execution that skips re-analysis and planning. */
class AlreadyPlannedExecution(
    session: SparkSession,
    plan: AlreadyPlanned) extends QueryExecution(session, plan) {
  override lazy val analyzed: LogicalPlan = plan
  override lazy val optimizedPlan: LogicalPlan = plan
  override lazy val sparkPlan: SparkPlan = plan.physicalPlan
  override lazy val executedPlan: SparkPlan = plan.physicalPlan
}

object AlreadyPlanned {
  def dataFrame(sparkSession: SparkSession, query: SparkPlan): DataFrame = {
    val qe = new AlreadyPlannedExecution(sparkSession, AlreadyPlanned(query))
    new Dataset[Row](qe, RowEncoder(qe.analyzed.schema))
  }
}
