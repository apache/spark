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

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}

class ExpressionSuite extends SparkFunSuite {
  test("ComplexTypeMergingExpression should throw exception if no children") {
    val coalesce = Coalesce(Seq.empty)

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        coalesce.dataType
      },
      errorClass = "COMPLEX_EXPRESSION_UNSUPPORTED_INPUT.NO_INPUTS",
      parameters = Map("expression" -> "COALESCE"))
  }

  test("ComplexTypeMergingExpression should throw " +
    "exception if children have different data types") {
    val coalesce = Coalesce(Seq(Literal(1), Literal("a"), Literal("a")))

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        coalesce.dataType
      },
      errorClass = "COMPLEX_EXPRESSION_UNSUPPORTED_INPUT.MISMATCHED_TYPES",
      parameters = Map(
        "expression" -> "COALESCE",
        "inputTypes" -> "[INTEGER, STRING, STRING]"))
  }

}
