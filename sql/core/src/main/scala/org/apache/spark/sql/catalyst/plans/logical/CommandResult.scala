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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.execution.SparkPlan

/**
 * Logical plan node for holding data from a command.
 *
 * `commandLogicalPlan` and `commandPhysicalPlan` are just used to display the plan tree
 * for EXPLAIN.
 * `rows` may not be serializable and ideally we should not send `rows` to the executors.
 * Thus marking them as transient.
 */
case class CommandResult(
    output: Seq[Attribute],
    @transient commandLogicalPlan: LogicalPlan,
    @transient commandPhysicalPlan: SparkPlan,
    @transient rows: Seq[InternalRow]) extends LeafNode {
  override def innerChildren: Seq[QueryPlan[_]] = Seq(commandLogicalPlan)

  override def computeStats(): Statistics =
    Statistics(sizeInBytes = EstimationUtils.getSizePerRow(output) * rows.length)
}
