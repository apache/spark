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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object CombineCollectionFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case ArrayFilter(ArrayFilter(grandchild, filterFunc1), filterFunc2)
        if filterFunc1.deterministic && filterFunc2.deterministic =>
        ArrayFilter(grandchild, And(filterFunc1, filterFunc2))

      case MapFilter(MapFilter(grandchild, filterFunc1), filterFunc2)
        if filterFunc1.deterministic && filterFunc2.deterministic =>
        MapFilter(grandchild, And(filterFunc1, filterFunc2))

    }
  }
}

object CombineCollectionTransforms extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case ArrayTransform(ArrayTransform(grandchild, transformFunc1), transformFunc2) =>
        ArrayTransform(grandchild, transformFunc1.withNewChildren(transformFunc2 :: Nil))

      case TransformKeys(TransformKeys(grandchild, transformFunc1), transformFunc2) =>
        TransformKeys(grandchild, transformFunc1.withNewChildren(transformFunc2 :: Nil))

      case TransformValues(TransformValues(grandchild, transformFunc1), transformFunc2) =>
        TransformValues(grandchild, transformFunc1.withNewChildren(transformFunc2 :: Nil))

    }
  }
}

object PushDownArrayFilter extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown  {
      case ArrayFilter(ArraySort(grandchild, comparator), filterFunc)
        if filterFunc.deterministic && comparator.deterministic =>
        ArraySort(ArrayFilter(grandchild, filterFunc), comparator)
    }
  }
}

object EliminateArraySort extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressions  {
      case ArrayForAll(ArraySort(grandchild, comparator), filterFunc)
        if filterFunc.deterministic && comparator.deterministic =>
        ArrayForAll(grandchild, filterFunc)

      case ArrayExists(ArraySort(grandchild, comparator), filterFunc, followThreeValuedLogic)
        if filterFunc.deterministic && comparator.deterministic =>
        ArrayExists(grandchild, filterFunc, followThreeValuedLogic)
    }
  }
}
