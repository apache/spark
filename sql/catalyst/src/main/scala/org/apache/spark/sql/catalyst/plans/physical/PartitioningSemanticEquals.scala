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

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.trees.TreePattern

/**
 * A help trait that provide semantic equal for output partitioning.
 * For example, we treat the cast expression is semantic equal with its child if it can up cast.
 */
trait PartitioningSemanticEquals {
  private def canonicalize(expr: Expression): Expression = {
    expr.canonicalized.transformWithPruning(_.containsPattern(TreePattern.CAST)) {
      case Cast(child, dataType, _, _) if Cast.canUpCast(child.dataType, dataType) =>
        child
    }
  }

  def partitioningSemanticsEquals(left: Expression, right: Expression): Boolean = {
    left.deterministic && right.deterministic &&
      canonicalize(left) == canonicalize(right)
  }
}
