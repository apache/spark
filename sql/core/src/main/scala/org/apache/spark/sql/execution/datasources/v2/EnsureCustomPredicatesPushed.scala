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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.CustomPredicateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Post-optimizer rule that ensures all CustomPredicateExpressions have been
 * pushed to the data source. If any remain in the plan, the query fails with
 * a clear error before execution begins.
 *
 * Registered in SparkOptimizer.earlyScanPushDownRules, runs after
 * V2ScanRelationPushDown.
 */
object EnsureCustomPredicatesPushed extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.foreachUp {
      case f: Filter =>
        f.condition.foreach {
          case cpe: CustomPredicateExpression =>
            throw SparkException.internalError(
              s"Custom predicate '${cpe.descriptor.sqlName()}' " +
              s"(${cpe.descriptor.canonicalName()}) was not pushed to the data source. " +
              s"The data source must accept this predicate via pushPredicates().")
          case _ =>
        }
      case _ =>
    }
    plan
  }
}
