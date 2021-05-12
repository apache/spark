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
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FUNC_ALIAS

class TryEvalSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("try_add") {
    Seq(
      (1, 1, 2),
      (Int.MaxValue, 1, null),
      (Int.MinValue, -1, null)
    ).foreach { case (a, b, expected) =>
      val left = Literal(a)
      val right = Literal(b)
      val input = TryAdd(left, right)
      checkEvaluation(input, expected)
      input.setTagValue(FUNC_ALIAS, "try_add")
      assert(input.toString == s"try_add(${left.toString}, ${right.toString})")
      assert(input.sql == s"try_add(${left.sql}, ${right.sql})")
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
      val input = TryDivide(left, right)
      checkEvaluation(input, expected)
      input.setTagValue(FUNC_ALIAS, "try_divide")
      assert(input.toString == s"try_divide(${left.toString}, ${right.toString})")
      assert(input.sql == s"try_divide(${left.sql}, ${right.sql})")
    }
  }
}
