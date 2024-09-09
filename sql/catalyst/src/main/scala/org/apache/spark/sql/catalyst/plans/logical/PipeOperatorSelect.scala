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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.trees.TreePattern.{PIPE_OPERATOR_SELECT, TreePattern}

/**
 * Represents a SELECT clause when used with the |> SQL pipe operator.
 * We use this operator to make sure that no aggregate functions exist in the SELECT expressions.
 */
case class PipeOperatorSelect(child: LogicalPlan) extends UnaryNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(PIPE_OPERATOR_SELECT)
  override def output: Seq[Attribute] = child.output
  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    PipeOperatorSelect(newChild)
}
