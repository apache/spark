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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Length, Literal, StructFieldsOperation, UpdateFields, UpdateNestedField, Upper, WithField}
import org.apache.spark.sql.catalyst.trees.LeafLike
import org.apache.spark.sql.types.{DataType, IndeterminateStringType, IntegerType, StringType, StructType}

class CollationTypeCoercionSuite extends SparkFunSuite {

  private val struct = AttributeReference(
    "s",
    new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
      .add("n", new StructType().add("y", StringType)))()

  private val indeterminate = AttributeReference("x", IndeterminateStringType)()

  test("does not ask the field operations of UpdateFields for a data type") {
    val op = CountingFieldOperation("b")
    val nestedOp = CountingFieldOperation("y")
    val update = UpdateFields(struct,
      Seq(WithField("c", Literal(1)), op, UpdateNestedField("n", Seq(nestedOp))))
    // Visit every node, as CollationTypeCasts does.
    assert(update.transformUp { case e => CollationTypeCoercion(e) } eq update)
    assert(op.dataTypeRequests == 0)
    assert(nestedOp.dataTypeRequests == 0)
  }

  test("fails an expression that does not accept an indeterminate collation input") {
    checkError(
      exception = intercept[AnalysisException] {
        CollationTypeCoercion(Upper(indeterminate))
      },
      condition = "INDETERMINATE_COLLATION_IN_EXPRESSION",
      parameters = Map("expr" -> "\"upper(x)\""))
  }

  test("fails when only a later child has an indeterminate collation") {
    checkError(
      exception = intercept[AnalysisException] {
        CollationTypeCoercion(EqualTo(Literal("a"), indeterminate))
      },
      condition = "INDETERMINATE_COLLATION_IN_EXPRESSION",
      parameters = Map("expr" -> "\"(a = x)\""))
  }

  test("keeps an expression that accepts an indeterminate collation input") {
    val length = Length(indeterminate)
    assert(CollationTypeCoercion(length) eq length)
  }

  /**
   * A field operation that counts the requests for its data type, which, like every
   * [[StructFieldsOperation]], it does not have.
   */
  private case class CountingFieldOperation(name: String)
    extends StructFieldsOperation with LeafLike[Expression] {
    var dataTypeRequests = 0

    override def dataType: DataType = {
      dataTypeRequests += 1
      super.dataType
    }
  }
}
