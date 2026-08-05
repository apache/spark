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
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.types.{DataType, IntegerType}

class ApplyFunctionExpressionSuite extends SparkFunSuite {

  private val intIdentity = new ScalarFunction[Int] {
    override def inputTypes(): Array[DataType] = Array(IntegerType)
    override def resultType(): DataType = IntegerType
    override def name(): String = "int_identity"
    override def produceResult(input: InternalRow): Int = input.getInt(0)
  }

  test("SPARK-58578: ApplyFunctionExpression is stateful and produces fresh copies") {
    val expr = ApplyFunctionExpression(
      intIdentity, Seq(BoundReference(0, IntegerType, nullable = false)))
    assert(expr.stateful, "ApplyFunctionExpression.stateful should be true")
    val copy = expr.freshCopyIfContainsStatefulExpression()
    assert(copy ne expr,
      "freshCopyIfContainsStatefulExpression should return a new instance " +
        "for ApplyFunctionExpression")
    assert(copy.eval(InternalRow(7)) === 7)
  }
}
