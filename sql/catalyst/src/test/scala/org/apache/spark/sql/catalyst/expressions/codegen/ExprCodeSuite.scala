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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite

class ExprCodeSuite extends SparkFunSuite {

  test("ExprCode.isLiteral: literals") {
    val literals = Seq(
      ExprCode("", "", "true"),
      ExprCode("", "", "false"),
      ExprCode("", "", "1"),
      ExprCode("", "", "-1"),
      ExprCode("", "", "1L"),
      ExprCode("", "", "-1L"),
      ExprCode("", "", "1.0f"),
      ExprCode("", "", "-1.0f"),
      ExprCode("", "", "0.1f"),
      ExprCode("", "", "-0.1f"),
      ExprCode("", "", """"string""""),
      ExprCode("", "", "(byte)-1"),
      ExprCode("", "", "(short)-1"),
      ExprCode("", "", "null"))

    literals.foreach(l => assert(l.isLiteral() == true))
  }

  test("ExprCode.isLiteral: non literals") {
    val variables = Seq(
      ExprCode("", "", "var1"),
      ExprCode("", "", "_var2"),
      ExprCode("", "", "$var3"),
      ExprCode("", "", "v1a2r3"),
      ExprCode("", "", "_1v2a3r"),
      ExprCode("", "", "$1v2a3r"))

    variables.foreach(v => assert(v.isLiteral() == false))
  }
}
