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
import org.apache.spark.sql.types._


class BitwiseFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("Bitwise operations") {
    val row = create_row(1, 2, 3, null)
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.int.at(2)
    val c4 = 'a.int.at(3)

    checkEvaluation(BitwiseAnd(c1, c4), null, row)
    checkEvaluation(BitwiseAnd(c1, c2), 0, row)
    checkEvaluation(BitwiseAnd(c1, Literal.create(null, IntegerType)), null, row)
    checkEvaluation(
      BitwiseAnd(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null, row)

    checkEvaluation(BitwiseOr(c1, c4), null, row)
    checkEvaluation(BitwiseOr(c1, c2), 3, row)
    checkEvaluation(BitwiseOr(c1, Literal.create(null, IntegerType)), null, row)
    checkEvaluation(
      BitwiseOr(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null, row)

    checkEvaluation(BitwiseXor(c1, c4), null, row)
    checkEvaluation(BitwiseXor(c1, c2), 3, row)
    checkEvaluation(BitwiseXor(c1, Literal.create(null, IntegerType)), null, row)
    checkEvaluation(
      BitwiseXor(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null, row)

    checkEvaluation(BitwiseNot(c4), null, row)
    checkEvaluation(BitwiseNot(c1), -2, row)
    checkEvaluation(BitwiseNot(Literal.create(null, IntegerType)), null, row)

    checkEvaluation(c1 & c2, 0, row)
    checkEvaluation(c1 | c2, 3, row)
    checkEvaluation(c1 ^ c2, 3, row)
    checkEvaluation(~c1, -2, row)
  }

  test("unary BitwiseNOT") {
    checkEvaluation(BitwiseNot(1), -2)
    assert(BitwiseNot(1).dataType === IntegerType)
    assert(BitwiseNot(1).eval(EmptyRow).isInstanceOf[Int])

    checkEvaluation(BitwiseNot(1.toLong), -2.toLong)
    assert(BitwiseNot(1.toLong).dataType === LongType)
    assert(BitwiseNot(1.toLong).eval(EmptyRow).isInstanceOf[Long])

    checkEvaluation(BitwiseNot(1.toShort), -2.toShort)
    assert(BitwiseNot(1.toShort).dataType === ShortType)
    assert(BitwiseNot(1.toShort).eval(EmptyRow).isInstanceOf[Short])

    checkEvaluation(BitwiseNot(1.toByte), -2.toByte)
    assert(BitwiseNot(1.toByte).dataType === ByteType)
    assert(BitwiseNot(1.toByte).eval(EmptyRow).isInstanceOf[Byte])
  }

}
