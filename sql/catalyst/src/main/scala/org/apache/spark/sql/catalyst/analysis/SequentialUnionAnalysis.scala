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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SequentialUnion}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Flattens nested SequentialUnion nodes into a single level.
 * This allows chaining: df1.followedBy(df2).followedBy(df3)
 */
object FlattenSequentialUnion extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(org.apache.spark.sql.catalyst.trees.TreePattern.UNION)) {
    case SequentialUnion(children, byName, allowMissingCol) =>
      val flattened = SequentialUnion.flatten(children)
      SequentialUnion(flattened, byName, allowMissingCol)
  }
}

/**
 * Validates SequentialUnion constraints:
 * - Minimum 2 children
 * - All children must be streaming relations
 * - No nested SequentialUnions (should be flattened first)
 */
object ValidateSequentialUnion extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.foreach {
      case su: SequentialUnion =>
        validateMinimumChildren(su)
        validateAllStreaming(su)
        validateNoNesting(su)
      case _ =>
    }
    plan
  }

  private def validateMinimumChildren(su: SequentialUnion): Unit = {
    if (su.children.length < 2) {
      throw QueryCompilationErrors.invalidNumberOfChildrenForUnionError(
        su.getClass.getSimpleName, su.children.length, 2)
    }
  }

  private def validateAllStreaming(su: SequentialUnion): Unit = {
    val nonStreamingChildren = su.children.filterNot(_.isStreaming)
    if (nonStreamingChildren.nonEmpty) {
      throw QueryCompilationErrors.notStreamingDatasetError("SequentialUnion")
    }
  }

  private def validateNoNesting(su: SequentialUnion): Unit = {
    su.children.foreach { child =>
      if (child.exists(_.isInstanceOf[SequentialUnion])) {
        throw QueryCompilationErrors.nestedSequentialUnionError()
      }
    }
  }
}
