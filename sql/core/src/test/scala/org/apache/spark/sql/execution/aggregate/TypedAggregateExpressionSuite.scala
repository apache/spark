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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._


class TypedAggregateExpressionSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private def testOptProductEncoder(encoder: ExpressionEncoder[_], expected: Boolean): Unit = {
    assert(TypedAggregateExpression.isOptProductEncoder(encoder) == expected)
  }

  test("check an encoder is for option of product") {
    testOptProductEncoder(encoderFor[Int], false)
    testOptProductEncoder(encoderFor[(Long, Long)], false)
    testOptProductEncoder(encoderFor[Option[Int]], false)
    testOptProductEncoder(encoderFor[Option[(Int, Long)]], true)
    testOptProductEncoder(encoderFor[Option[SimpleCaseClass]], true)
  }

  test("flatten encoders of option of product") {
    // Option[Product] is encoded as a struct column in a row.
    val optProductEncoder: ExpressionEncoder[Option[(Int, Long)]] = encoderFor[Option[(Int, Long)]]
    val optProductSchema = StructType(StructField("value", StructType(
      StructField("_1", IntegerType) :: StructField("_2", LongType) :: Nil)) :: Nil)

    assert(optProductEncoder.schema.length == 1)
    assert(DataType.equalsIgnoreCaseAndNullability(optProductEncoder.schema, optProductSchema))

    val flattenEncoder = TypedAggregateExpression.flattenOptProductEncoder(optProductEncoder)
      .resolveAndBind()
    assert(flattenEncoder.schema.length == 2)
    assert(DataType.equalsIgnoreCaseAndNullability(flattenEncoder.schema,
      optProductSchema.fields(0).dataType))

    val row = flattenEncoder.toRow(Some((1, 2L)))
    val expected = flattenEncoder.fromRow(row)
    assert(Some((1, 2L)) == expected)
  }
}

case class SimpleCaseClass(a: Int)
