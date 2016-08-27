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
import org.apache.spark.sql.types.{IntegerType, StringType}

class ScalaUDFSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("basic") {
    val intUdf = ScalaUDF((i: Int) => i + 1, IntegerType, Literal(1) :: Nil)
    checkEvaluation(intUdf, 2)

    val stringUdf = ScalaUDF((s: String) => s + "x", StringType, Literal("a") :: Nil)
    checkEvaluation(stringUdf, "ax")
  }

  test("better error message for NPE") {
    val udf = ScalaUDF(
      (s: String) => s.toLowerCase,
      StringType,
      Literal.create(null, StringType) :: Nil)

    val e1 = intercept[RuntimeException](udf.eval())
    assert(e1.getMessage.contains("Given UDF throws NPE during execution"))

    val e2 = intercept[RuntimeException] {
      checkEvalutionWithUnsafeProjection(udf, null)
    }
    assert(e2.getMessage.contains("Given UDF throws NPE during execution"))
  }

}
