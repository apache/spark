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

package org.apache.spark.sql.catalyst.trees

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Expression, IntegerLiteral, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}

class RuleExecutorSuite extends SparkFunSuite {
  object DecrementLiterals extends Rule[Expression] {
    def apply(e: Expression): Expression = e transform {
      case IntegerLiteral(i) if i > 0 => Literal(i - 1)
    }
  }

  test("only once") {
    object ApplyOnce extends RuleExecutor[Expression] {
      val batches = Batch("once", Once, DecrementLiterals) :: Nil
    }

    assert(ApplyOnce.execute(Literal(10)) === Literal(9))
  }

  test("to fixed point") {
    object ToFixedPoint extends RuleExecutor[Expression] {
      val batches = Batch("fixedPoint", FixedPoint(100), DecrementLiterals) :: Nil
    }

    assert(ToFixedPoint.execute(Literal(10)) === Literal(0))
  }

  test("to maxIterations") {
    object ToFixedPoint extends RuleExecutor[Expression] {
      val batches = Batch("fixedPoint", FixedPoint(10), DecrementLiterals) :: Nil
    }

    val message = intercept[TreeNodeException[LogicalPlan]] {
      ToFixedPoint.execute(Literal(100))
    }.getMessage
    assert(message.contains("Max iterations (10) reached for batch fixedPoint"))
  }
}
