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

package org.apache.spark.sql.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan

/**
 * Logical plan node for collecting data from a command.
 *
 * @param data The local collection holding the data. It doesn't need to be sent to executors
 *             and then doesn't need to be serializable.
 */
case class CommandResult(
    output: Seq[Attribute],
    commandLogicalPlan: LogicalPlan,
    commandPhysicalPlan: SparkPlan,
    data: Seq[InternalRow]) extends LeafNode {
  override def innerChildren: Seq[QueryPlan[_]] = Seq(commandLogicalPlan)
}
