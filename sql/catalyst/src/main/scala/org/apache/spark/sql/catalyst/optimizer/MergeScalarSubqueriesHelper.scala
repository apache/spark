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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * The helper class used to merge scalar subqueries.
 */
trait MergeScalarSubqueriesHelper {

  // If 2 plans are identical return the attribute mapping from the left to the right.
  protected def checkIdenticalPlans(
      left: LogicalPlan, right: LogicalPlan): Option[AttributeMap[Attribute]] = {
    if (left.canonicalized == right.canonicalized) {
      Some(AttributeMap(left.output.zip(right.output)))
    } else {
      None
    }
  }

  protected def mapAttributes[T <: Expression](expr: T, outputMap: AttributeMap[Attribute]): T = {
    expr.transform {
      case a: Attribute => outputMap.getOrElse(a, a)
    }.asInstanceOf[T]
  }
}
