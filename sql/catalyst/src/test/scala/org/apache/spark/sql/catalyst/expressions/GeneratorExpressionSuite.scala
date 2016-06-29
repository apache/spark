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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

class GeneratorExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {
  private def checkTuple(actual: ExplodeBase, expected: Seq[InternalRow]): Unit = {
    assert(actual.eval(null).toSeq === expected)
  }

  private final val int_array = Seq(1, 2, 3)
  private final val str_array = Seq("a", "b", "c")

  test("explode") {
    val int_correct_answer = Seq(Seq(1), Seq(2), Seq(3))
    val str_correct_answer = Seq(
      Seq(UTF8String.fromString("a")),
      Seq(UTF8String.fromString("b")),
      Seq(UTF8String.fromString("c")))

    checkTuple(
      Explode(CreateArray(Seq.empty)),
      Seq.empty)

    checkTuple(
      Explode(CreateArray(int_array.map(Literal(_)))),
      int_correct_answer.map(InternalRow.fromSeq(_)))

    checkTuple(
      Explode(CreateArray(str_array.map(Literal(_)))),
      str_correct_answer.map(InternalRow.fromSeq(_)))
  }

  test("posexplode") {
    val int_correct_answer = Seq(Seq(0, 1), Seq(1, 2), Seq(2, 3))
    val str_correct_answer = Seq(
      Seq(0, UTF8String.fromString("a")),
      Seq(1, UTF8String.fromString("b")),
      Seq(2, UTF8String.fromString("c")))

    checkTuple(
      PosExplode(CreateArray(Seq.empty)),
      Seq.empty)

    checkTuple(
      PosExplode(CreateArray(int_array.map(Literal(_)))),
      int_correct_answer.map(InternalRow.fromSeq(_)))

    checkTuple(
      PosExplode(CreateArray(str_array.map(Literal(_)))),
      str_correct_answer.map(InternalRow.fromSeq(_)))
  }
}
