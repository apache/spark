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
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class CanonicalizeSuite extends SparkFunSuite {

  test("SPARK-24276: IN expression with different order are semantically equal") {
    val range = Range(1, 1, 1, 1)
    val idAttr = range.output.head

    val in1 = In(idAttr, Seq(Literal(1), Literal(2)))
    val in2 = In(idAttr, Seq(Literal(2), Literal(1)))
    val in3 = In(idAttr, Seq(Literal(1), Literal(2), Literal(3)))

    assert(in1.canonicalized.semanticHash() == in2.canonicalized.semanticHash())
    assert(in1.canonicalized.semanticHash() != in3.canonicalized.semanticHash())

    assert(range.where(in1).sameResult(range.where(in2)))
    assert(!range.where(in1).sameResult(range.where(in3)))

    val arrays1 = In(idAttr, Seq(CreateArray(Seq(Literal(1), Literal(2))),
      CreateArray(Seq(Literal(2), Literal(1)))))
    val arrays2 = In(idAttr, Seq(CreateArray(Seq(Literal(2), Literal(1))),
      CreateArray(Seq(Literal(1), Literal(2)))))
    val arrays3 = In(idAttr, Seq(CreateArray(Seq(Literal(1), Literal(2))),
      CreateArray(Seq(Literal(3), Literal(1)))))

    assert(arrays1.canonicalized.semanticHash() == arrays2.canonicalized.semanticHash())
    assert(arrays1.canonicalized.semanticHash() != arrays3.canonicalized.semanticHash())

    assert(range.where(arrays1).sameResult(range.where(arrays2)))
    assert(!range.where(arrays1).sameResult(range.where(arrays3)))
  }

  test("SPARK-26402: accessing nested fields with different cases in case insensitive mode") {
    val expId = NamedExpression.newExprId
    val qualifier = Seq.empty[String]
    val structType = StructType(
      StructField("a", StructType(StructField("b", IntegerType, false) :: Nil), false) :: Nil)

    // GetStructField with different names are semantically equal
    val fieldA1 = GetStructField(
      AttributeReference("data1", structType, false)(expId, qualifier),
      0, Some("a1"))
    val fieldA2 = GetStructField(
      AttributeReference("data2", structType, false)(expId, qualifier),
      0, Some("a2"))
    assert(fieldA1.semanticEquals(fieldA2))

    val fieldB1 = GetStructField(
      GetStructField(
        AttributeReference("data1", structType, false)(expId, qualifier),
        0, Some("a1")),
      0, Some("b1"))
    val fieldB2 = GetStructField(
      GetStructField(
        AttributeReference("data2", structType, false)(expId, qualifier),
        0, Some("a2")),
      0, Some("b2"))
    assert(fieldB1.semanticEquals(fieldB2))
  }
}
