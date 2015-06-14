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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types.{BooleanType, StringType, ShortType}

class NullFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("null checking") {
    val row = create_row("^Ba*n", null, true, null)
    val c1 = 'a.string.at(0)
    val c2 = 'a.string.at(1)
    val c3 = 'a.boolean.at(2)
    val c4 = 'a.boolean.at(3)

    checkEvaluation(c1.isNull, false, row)
    checkEvaluation(c1.isNotNull, true, row)

    checkEvaluation(c2.isNull, true, row)
    checkEvaluation(c2.isNotNull, false, row)

    checkEvaluation(Literal.create(1, ShortType).isNull, false)
    checkEvaluation(Literal.create(1, ShortType).isNotNull, true)

    checkEvaluation(Literal.create(null, ShortType).isNull, true)
    checkEvaluation(Literal.create(null, ShortType).isNotNull, false)

    checkEvaluation(Coalesce(c1 :: c2 :: Nil), "^Ba*n", row)
    checkEvaluation(Coalesce(Literal.create(null, StringType) :: Nil), null, row)
    checkEvaluation(Coalesce(Literal.create(null, StringType) :: c1 :: c2 :: Nil), "^Ba*n", row)

    checkEvaluation(
      If(c3, Literal.create("a", StringType), Literal.create("b", StringType)), "a", row)
    checkEvaluation(If(c3, c1, c2), "^Ba*n", row)
    checkEvaluation(If(c4, c2, c1), "^Ba*n", row)
    checkEvaluation(If(Literal.create(null, BooleanType), c2, c1), "^Ba*n", row)
    checkEvaluation(If(Literal.create(true, BooleanType), c1, c2), "^Ba*n", row)
    checkEvaluation(If(Literal.create(false, BooleanType), c2, c1), "^Ba*n", row)
    checkEvaluation(If(Literal.create(false, BooleanType),
      Literal.create("a", StringType), Literal.create("b", StringType)), "b", row)

    checkEvaluation(c1 in (c1, c2), true, row)
    checkEvaluation(
      Literal.create("^Ba*n", StringType) in (Literal.create("^Ba*n", StringType)), true, row)
    checkEvaluation(
      Literal.create("^Ba*n", StringType) in (Literal.create("^Ba*n", StringType), c2), true, row)
  }
}
