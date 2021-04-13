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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.execution.{SparkPlan, UnionExec}

trait UnionAwareOptimizerRule {
  def optimizeWithUnion(plan: SparkPlan, rule: SparkPlan => SparkPlan): SparkPlan = {
    var shouldOptimizeWholePlan = true
    val optimized = plan transform {
      case u: UnionExec =>
        // Union is the special case that it's children are independent in query stage which means
        // they are not required all leaf node are query stages and had same partition number.
        // Then we can optimize Union's children one by one.
        // Note that, we only optimize the plan whose children does not exist Union in case of
        // repetitive optimization with nested Union.
        u.withNewChildren(u.children.map { child =>
          if (child.find(_.isInstanceOf[UnionExec]).isEmpty) {
            shouldOptimizeWholePlan = false
            rule(child)
          } else {
            child
          }
        })
      case other => other
    }
    if (shouldOptimizeWholePlan) {
      rule(plan)
    } else {
      optimized
    }
  }
}
