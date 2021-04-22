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

import org.apache.spark.SparkFunSuite

class TryEvalSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("try_add") {
    Seq(
      (1, 1, 2),
      (Int.MaxValue, 1, null),
      (Int.MinValue, -1, null)
    ).foreach { case (a, b, expected) =>
      checkEvaluation(TryEval(Add(Literal(a), Literal(b)).asAnsi), expected)
    }
  }


  test("try_subtract") {
    Seq(
      (1, 1, 0),
      (Int.MaxValue, -1, null),
      (Int.MinValue, 1, null)
    ).foreach { case (a, b, expected) =>
      checkEvaluation(TryEval(Subtract(Literal(a), Literal(b)).asAnsi), expected)
    }
  }

  test("try_multiply") {
    Seq(
      (1, 2, 2),
      (Int.MaxValue, 2, null),
      (Int.MinValue, 2, null)
    ).foreach { case (a, b, expected) =>
      checkEvaluation(TryEval(Multiply(Literal(a), Literal(b)).asAnsi), expected)
    }
  }

  test("try_divide") {
    Seq(
      (3.0, 2.0, 1.5),
      (1.0, 0.0, null),
      (-1.0, 0.0, null)
    ).foreach { case (a, b, expected) =>
      checkEvaluation(TryEval(Divide(Literal(a), Literal(b)).asAnsi), expected)
    }
  }

  test("try_div") {
    Seq(
      (2L, 2L, 1L),
      (1L, 0L, null),
      (-1L, 0L, null)
    ).foreach { case (a, b, expected) =>
      checkEvaluation(TryEval(IntegralDivide(Literal(a), Literal(b)).asAnsi), expected)
    }
  }
}
