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

/**
 *
 */
object OptimizeHigherOrderFunctions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case ArrayFilter(ArrayFilter(child, f1: LambdaFunction), f2: LambdaFunction) =>
      ArrayFilter(child, f1.copy(function = And(f1.function, f2.function)))

    case MapFilter(MapFilter(child, f1: LambdaFunction), f2: LambdaFunction) =>
      MapFilter(child, f1.copy(function = And(f1.function, f2.function)))

    case ArrayFilter(ArraySort(grandchild, comparator), filterFunc)
      if filterFunc.deterministic && comparator.deterministic =>
      ArraySort(ArrayFilter(grandchild, filterFunc), comparator)

    case ArrayForAll(ArraySort(child, comparator), filterFunc)
      if filterFunc.deterministic && comparator.deterministic =>
      ArrayForAll(child, filterFunc)

    case ArrayExists(ArraySort(child, comparator), filterFunc, followThreeValuedLogic)
      if filterFunc.deterministic && comparator.deterministic =>
      ArrayExists(child, filterFunc, followThreeValuedLogic)
  }
}
