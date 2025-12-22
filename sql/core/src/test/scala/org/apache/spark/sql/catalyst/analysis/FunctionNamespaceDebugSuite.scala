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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class FunctionNamespaceDebugSuite extends SharedSparkSession {

  test("debug: check FunctionRegistry.builtin contains abs") {
    val absInfo = FunctionRegistry.builtin.lookupFunction(FunctionIdentifier("abs"))
    assert(absInfo.isDefined, s"FunctionRegistry.builtin should contain abs. " +
      s"Available functions: ${FunctionRegistry.builtin.listFunction().take(20).mkString(", ")}")
  }

  test("debug: check SessionCatalog builtinFunctionRegistry") {
    val catalog = spark.sessionState.catalog
    // This tests our modified lookupBuiltinFunctionInfo method
    val absInfo = catalog.lookupBuiltinFunctionInfo("abs")
    assert(absInfo.isDefined, s"SessionCatalog.lookupBuiltinFunctionInfo should find abs")
  }

  test("debug: check SessionCatalog lookupBuiltinOrTempFunction") {
    val catalog = spark.sessionState.catalog
    val absInfo = catalog.lookupBuiltinOrTempFunction("abs")
    assert(absInfo.isDefined, s"SessionCatalog.lookupBuiltinOrTempFunction should find abs")
  }

  test("abs function should work unqualified") {
    val result = sql("SELECT abs(-5)").collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 5)
  }
}
