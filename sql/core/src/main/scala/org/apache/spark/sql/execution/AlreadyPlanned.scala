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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}

/**
 * A special node that allows skipping query planning all the way to physical execution. This node
 * can be used when a query is being run repeatedly, and the plan doesn't need to be optimized
 * every time, or a plan was already optimized, and planned to the physical level, but we had to
 * go back to logical plan land for some reason (e.g. V1 DataSource write execution).
 */
case class AlreadyPlanned(physicalPlan: SparkPlan) extends LeafNode {
  override def output: Seq[Attribute] = physicalPlan.output
}

class PlannedExecution(
    session: SparkSession,
    plan: AlreadyPlanned) extends QueryExecution(session, plan) {
  override lazy val analyzed: LogicalPlan = plan
  override lazy val optimizedPlan: LogicalPlan = plan
  override lazy val sparkPlan: SparkPlan = plan.physicalPlan
  override lazy val executedPlan: SparkPlan = plan.physicalPlan
}
