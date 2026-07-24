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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Describes a value projection from the rows of an existing standard hash broadcast.
 *
 * The source hash keys are the complete, ordered keys of the broadcast join. The value expression
 * is evaluated against the rows stored in the resulting hashed relation.
 */
case class BroadcastValueProjection(
    sourcePlan: LogicalPlan,
    sourceHashKeys: Seq[Expression],
    valueExpression: Expression) {

  lazy val canonicalized: BroadcastValueProjection = {
    val normalizedKeys = sourceHashKeys.map(
      QueryPlan.normalizeExpressions(_, sourcePlan.output))
    val normalizedValue = QueryPlan.normalizeExpressions(valueExpression, sourcePlan.output)
    copy(
      sourcePlan = sourcePlan.canonicalized,
      sourceHashKeys = normalizedKeys,
      valueExpression = normalizedValue)
  }
}
