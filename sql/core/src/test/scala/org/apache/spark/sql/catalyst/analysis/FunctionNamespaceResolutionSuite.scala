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

import org.apache.spark.sql.test.SharedSparkSession

class FunctionNamespaceResolutionSuite extends SharedSparkSession {

  test("scalar function in table context should give specific error") {
    // Check that abs exists as a scalar function
    assert(spark.sessionState.catalog.lookupBuiltinOrTempFunction("abs").isDefined,
      "abs should exist as a builtin function")

    // Now try to use it in table context - should get NOT_A_TABLE_FUNCTION error
    val ex = intercept[org.apache.spark.sql.AnalysisException] {
      sql("SELECT * FROM abs(-5)")
    }
    assert(ex.getCondition == "NOT_A_TABLE_FUNCTION",
      s"Expected NOT_A_TABLE_FUNCTION but got ${ex.getCondition}: ${ex.getMessage}")
  }

  test("table function in scalar context should give specific error") {
    // Check that range exists as a table function (but NOT as a scalar function)
    assert(spark.sessionState.catalog.lookupBuiltinOrTempTableFunction("range").isDefined,
      "range should exist as a builtin table function")
    assert(spark.sessionState.catalog.lookupBuiltinOrTempFunction("range").isEmpty,
      "range should NOT exist as a scalar function")

    // Now try to use it in scalar context - should get NOT_A_SCALAR_FUNCTION error
    val ex = intercept[org.apache.spark.sql.AnalysisException] {
      sql("SELECT range(1, 10)")
    }
    assert(ex.getCondition == "NOT_A_SCALAR_FUNCTION",
      s"Expected NOT_A_SCALAR_FUNCTION but got ${ex.getCondition}: ${ex.getMessage}")
  }
}
