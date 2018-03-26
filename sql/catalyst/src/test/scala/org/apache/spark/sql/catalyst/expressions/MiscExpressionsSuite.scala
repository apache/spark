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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class MiscExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("assert_true") {
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Literal.create(false, BooleanType)), null)
    }
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Cast(Literal(0), BooleanType)), null)
    }
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Literal.create(null, NullType)), null)
    }
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Literal.create(null, BooleanType)), null)
    }
    checkEvaluation(AssertTrue(Literal.create(true, BooleanType)), null)
    checkEvaluation(AssertTrue(Cast(Literal(1), BooleanType)), null)
  }

  test("uuid") {
    def assertIncorrectEval(f: () => Unit): Unit = {
      intercept[Exception] {
        f()
      }.getMessage().contains("Incorrect evaluation")
    }

    checkEvaluation(Length(Uuid(Some(0))), 36)
    val r = new Random()
    val seed1 = Some(r.nextLong())
    val uuid1 = evaluate(Uuid(seed1)).asInstanceOf[UTF8String]
    checkEvaluation(Uuid(seed1), uuid1.toString)

    val seed2 = Some(r.nextLong())
    val uuid2 = evaluate(Uuid(seed2)).asInstanceOf[UTF8String]
    assertIncorrectEval(() => checkEvaluationWithoutCodegen(Uuid(seed1), uuid2))
    assertIncorrectEval(() => checkEvaluationWithGeneratedMutableProjection(Uuid(seed1), uuid2))
    assertIncorrectEval(() => checkEvalutionWithUnsafeProjection(Uuid(seed1), uuid2))
    assertIncorrectEval(() => checkEvaluationWithOptimization(Uuid(seed1), uuid2))
  }
}
