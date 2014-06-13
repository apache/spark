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

package org.apache.spark.sql.catalyst.plans

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions.{ExprId, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._

/**
 * Provides helper methods for comparing plans.
 */
class PlanTest extends FunSuite {

  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  protected def normalizeExprIds(plan: LogicalPlan) = {
    val minId = plan.flatMap(_.expressions.flatMap(_.references).map(_.exprId.id)).min
    plan transformAllExpressions {
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(a.exprId.id - minId))
    }
  }

  /** Fails the test if the two plans do not match */
  protected def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan) {
    val normalized1 = normalizeExprIds(plan1)
    val normalized2 = normalizeExprIds(plan2)
    if (normalized1 != normalized2)
      fail(
        s"""
          |== FAIL: Plans do not match ===
          |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
        """.stripMargin)
  }
}
