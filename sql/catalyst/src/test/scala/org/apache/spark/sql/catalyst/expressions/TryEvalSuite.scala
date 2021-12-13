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
      val left = Literal(a)
      val right = Literal(b)
      val input = TryEval(Add(left, right, failOnError = true))
      checkEvaluation(input, expected)
    }
  }

  test("try_divide") {
    Seq(
      (3.0, 2.0, 1.5),
      (1.0, 0.0, null),
      (-1.0, 0.0, null)
    ).foreach { case (a, b, expected) =>
      val left = Literal(a)
      val right = Literal(b)
      val input = TryEval(Divide(left, right, failOnError = true))
      checkEvaluation(input, expected)
    }
  }

  test("try_element_at: array") {
    val left = Literal(Array(1, 2, 3))
    Seq(
      (0, null),
      (1, 1),
      (4, null)
    ).foreach { case (index, expected) =>
      val input = TryEval(ElementAt(left, Literal(index), failOnError = false))
      checkEvaluation(input, expected)
    }
  }

  test("try_element_at: map") {
    val left = Literal.create(Map(1 -> 1))
    Seq(
      (0, null),
      (1, 1),
      (4, null)
    ).foreach { case (index, expected) =>
      val input = TryEval(ElementAt(left, Literal(index), failOnError = false))
      checkEvaluation(input, expected)
    }
  }
}
