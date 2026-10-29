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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DropField, Length, UpdateFields, Upper}
import org.apache.spark.sql.types.{DataType, IndeterminateStringType, IntegerType, StringType, StructType}

/** The indeterminate-collation check of [[CollationTypeCoercion]]. */
class IndeterminateCollationCheckSuite extends SparkFunSuite {

  private val struct = AttributeReference(
    "s", new StructType().add("a", IntegerType).add("b", StringType))()

  private val indeterminate = AttributeReference("x", IndeterminateStringType)()

  test("does not ask the field operations of UpdateFields for a data type") {
    val drop = new CountingDropField("b")
    val update = UpdateFields(struct, Seq(drop))
    assert(CollationTypeCoercion(update) eq update)
    assert(drop.dataTypeRequests == 0)
  }

  test("fails an expression that does not accept an indeterminate collation input") {
    checkError(
      exception = intercept[AnalysisException] {
        CollationTypeCoercion(Upper(indeterminate))
      },
      condition = "INDETERMINATE_COLLATION_IN_EXPRESSION",
      parameters = Map("expr" -> "\"upper(x)\""))
  }

  test("keeps an expression that accepts an indeterminate collation input") {
    val length = Length(indeterminate)
    assert(CollationTypeCoercion(length) eq length)
  }

  /** A [[DropField]] that counts the requests for its data type, which it does not have. */
  private class CountingDropField(name: String) extends DropField(name) {
    var dataTypeRequests = 0

    override def dataType: DataType = {
      dataTypeRequests += 1
      super.dataType
    }
  }
}
