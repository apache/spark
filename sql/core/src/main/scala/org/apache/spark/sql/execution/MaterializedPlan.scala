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

import org.apache.spark.sql.catalyst.expressions.{Attribute, ExpressionSet}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

/**
 * A materialized plan is a LogicalPlan that already has been planned and is backed by a
 * [[SparkPlan]]. This is useful for spark plans that have side effects and which should only be
 * executed once. It is also useful in situations where we want to avoid (costly) re-planning of
 * a query fragment, for instance long ETL pipelines.
 */
case class MaterializedPlan(plan: SparkPlan) extends LeafNode {
  override def output: Seq[Attribute] = plan.output
  override lazy val constraints: ExpressionSet = plan.constraints
}
