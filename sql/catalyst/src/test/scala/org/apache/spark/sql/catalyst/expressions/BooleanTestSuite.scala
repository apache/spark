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
import org.apache.spark.sql.catalyst.expressions.BooleanTest._
import org.apache.spark.sql.types._

class BooleanTestSuite extends SparkFunSuite with ExpressionEvalHelper {

  val row0 = create_row(null)
  val row1 = create_row(false)
  val row2 = create_row(true)

  test("istrue and isnottrue") {
    checkEvaluation(BooleanTest(Literal.create(null, NullType), Some(true)), false, row0)
    checkEvaluation(Not(BooleanTest(Literal.create(null, NullType), Some(true))), true, row0)
    checkEvaluation(BooleanTest(Literal.create(false, BooleanType), Some(true)), false, row1)
    checkEvaluation(Not(BooleanTest(Literal.create(false, BooleanType), Some(true))), true, row1)
    checkEvaluation(BooleanTest(Literal.create(true, BooleanType), Some(true)), true, row2)
    checkEvaluation(Not(BooleanTest(Literal.create(true, BooleanType), Some(true))), false, row2)
  }

  test("isfalse and isnotfalse") {
    checkEvaluation(BooleanTest(Literal.create(null, NullType), Some(false)), false, row0)
    checkEvaluation(Not(BooleanTest(Literal.create(null, NullType), Some(false))), true, row0)
    checkEvaluation(BooleanTest(Literal.create(false, BooleanType), Some(false)), true, row1)
    checkEvaluation(Not(BooleanTest(Literal.create(false, BooleanType), Some(false))), false, row1)
    checkEvaluation(BooleanTest(Literal.create(true, BooleanType), Some(false)), false, row2)
    checkEvaluation(Not(BooleanTest(Literal.create(true, BooleanType), Some(false))), true, row2)
  }

  test("isunknown and isnotunknown") {
    checkEvaluation(BooleanTest(Literal.create(null, NullType), None), true, row0)
    checkEvaluation(Not(BooleanTest(Literal.create(null, NullType), None)), false, row0)
  }
}
