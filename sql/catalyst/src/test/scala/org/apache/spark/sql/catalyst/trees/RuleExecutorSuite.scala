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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Expression, IntegerLiteral, Literal}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.internal.SQLConf

class RuleExecutorSuite extends SparkFunSuite with SQLConfHelper {
  object DecrementLiterals extends Rule[Expression] {
    def apply(e: Expression): Expression = e transform {
      case IntegerLiteral(i) if i > 0 => Literal(i - 1)
    }
  }

  object SetToZero extends Rule[Expression] {
    def apply(e: Expression): Expression = e transform {
      case IntegerLiteral(_) => Literal(0)
    }
  }

  test("idempotence") {
    object ApplyIdempotent extends RuleExecutor[Expression] {
      val batches = Batch("idempotent", Once, SetToZero) :: Nil
    }

    assert(ApplyIdempotent.execute(Literal(10)) === Literal(0))
    assert(ApplyIdempotent.execute(ApplyIdempotent.execute(Literal(10))) ===
      ApplyIdempotent.execute(Literal(10)))
  }

  test("only once") {
    object ApplyOnce extends RuleExecutor[Expression] {
      val batches = Batch("once", FixedPoint(1), DecrementLiterals) :: Nil
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

    val message = intercept[RuntimeException] {
      ToFixedPoint.execute(Literal(100))
    }.getMessage
    assert(message.contains("Max iterations (10) reached for batch fixedPoint"))
  }

  test("structural integrity validation - verify initial input") {
    object WithSIChecker extends RuleExecutor[Expression] {
      override protected def validatePlanChanges(
          previousPlan: Expression,
          currentPlan: Expression): Option[String] = currentPlan match {
        case IntegerLiteral(_) => None
        case _ => Some("not integer")
      }
      val batches = Batch("once", FixedPoint(1), DecrementLiterals) :: Nil
    }

    assert(WithSIChecker.execute(Literal(10)) === Literal(9))

    val e = intercept[SparkException] {
      // The input is already invalid as determined by WithSIChecker.isPlanIntegral
      WithSIChecker.execute(Literal(10.1))
    }
    assert(e.getMessage.contains("The input plan of"))
    assert(e.getMessage.contains("not integer"))
  }

  test("structural integrity checker - verify rule execution result") {
    object WithSICheckerForPositiveLiteral extends RuleExecutor[Expression] {
      override protected def validatePlanChanges(
          previousPlan: Expression,
          currentPlan: Expression): Option[String] = currentPlan match {
        case IntegerLiteral(i) if i > 0 => None
        case _ => Some("not positive integer")
      }
      val batches = Batch("once", FixedPoint(1), DecrementLiterals) :: Nil
    }

    assert(WithSICheckerForPositiveLiteral.execute(Literal(2)) === Literal(1))

    val e = intercept[SparkException] {
      WithSICheckerForPositiveLiteral.execute(Literal(1))
    }
    val ruleName = DecrementLiterals.ruleName
    assert(e.getMessage.contains(s"Rule $ruleName in batch once generated an invalid plan"))
    assert(e.getMessage.contains("not positive integer"))
  }

  private object OptimizerWithLightweightValidation extends RuleExecutor[Expression] {
    override protected def validatePlanChanges(
        previousPlan: Expression,
        currentPlan: Expression): Option[String] = {
      (previousPlan, currentPlan) match {
        case (IntegerLiteral(i), IntegerLiteral(j)) if i == j => None
        case _ => Some("value changed")
      }
    }
    override protected def validatePlanChangesLightweight(
        previousPlan: Expression,
        currentPlan: Expression): Option[String] = previousPlan match {
      case IntegerLiteral(i) if i < 0 => None
      case _ => Some("input is non-negative")
    }
    override val batches: Seq[Batch] = Batch("once", FixedPoint(1), DecrementLiterals) :: Nil
  }

  test("lightweight optimizer validation disabled") {
    withSQLConf(SQLConf.LIGHTWEIGHT_PLAN_CHANGE_VALIDATION.key -> "false") {
      // Test when full plan validation is both enabled and disabled.
      Seq("true", "false").foreach { fullValidation =>
        withSQLConf(SQLConf.PLAN_CHANGE_VALIDATION.key -> fullValidation) {
          // Input passes validation
          assert(OptimizerWithLightweightValidation.execute(Literal(0)) === Literal(0))

          // Input does not pass validation
          if (fullValidation == "false") {
            // no validation runs
            assert(OptimizerWithLightweightValidation.execute(Literal(1)) === Literal(0))
          } else {
            // full validation runs, taking the place of lightweight validation
            val e = intercept[SparkException] {
              OptimizerWithLightweightValidation.execute(Literal(1))
            }
            val ruleName = DecrementLiterals.ruleName
            assert(e.getMessage.contains(s"Rule $ruleName in batch once generated an invalid plan"))
            assert(e.getMessage.contains("value changed"))
          }
        }
      }
    }
  }

  test("lightweight optimizer validation enabled") {
    withSQLConf(SQLConf.LIGHTWEIGHT_PLAN_CHANGE_VALIDATION.key -> "true") {
      // Test when full plan validation is both enabled and disabled.
      Seq("true", "false").foreach { fullValidation =>
        withSQLConf(SQLConf.PLAN_CHANGE_VALIDATION.key -> fullValidation) {
          // Input passes validation
          assert(OptimizerWithLightweightValidation.execute(Literal(0)) === Literal(0))

          // Input does not pass validation
          val e = intercept[SparkException] {
            OptimizerWithLightweightValidation.execute(Literal(1))
          }
          val ruleName = DecrementLiterals.ruleName
          assert(e.getMessage.contains(s"Rule $ruleName in batch once generated an invalid plan"))
          if (fullValidation == "false") {
            // only lightweight validation runs
            assert(e.getMessage.contains("input is non-negative"))
          } else {
            // full validation runs, taking the place of lightweight validation
            assert(e.getMessage.contains("value changed"))
          }
        }
      }
    }
  }

  test("SPARK-27243: dumpTimeSpent when no rule has run") {
    RuleExecutor.resetMetrics()
    // This should not throw an exception
    RuleExecutor.dumpTimeSpent()
  }
}
