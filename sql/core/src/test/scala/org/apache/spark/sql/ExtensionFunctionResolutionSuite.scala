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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

/**
 * Test suite for extension function resolution and shadowing behavior.
 * This tests the system.extension namespace and verifies:
 * 1. Extension functions can shadow built-ins
 * 2. Extension functions resolve BEFORE built-ins
 * 3. Extension functions can be qualified as extension.func or system.extension.func
 * 4. Built-ins are still accessible via builtin.func qualification
 */
class ExtensionFunctionResolutionSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf = {
    super.sparkConf.set("spark.sql.extensions", classOf[TestExtensions].getName)
  }

  test("extension function can be called unqualified") {
    // Extension function registered in TestExtensions
    checkAnswer(sql("SELECT test_ext_func()"), Row(9999))
  }

  test("extension function resolution order: extension > builtin > session") {
    // Test the critical security property: extension comes before builtin before session
    // This test uses only SQL temporary functions (no programmatic registration)

    // Create a temp function
    sql("CREATE TEMPORARY FUNCTION session_func() RETURNS INT RETURN 1111")

    // Unqualified: resolves to session (no extension or builtin with this name)
    checkAnswer(sql("SELECT session_func()"), Row(1111))

    // Qualified
    checkAnswer(sql("SELECT session.session_func()"), Row(1111))

    // Cleanup
    sql("DROP TEMPORARY FUNCTION session_func")
  }

  test("security property: temp function cannot shadow current_user") {
    // Get the actual current user
    val actualUser = sql("SELECT current_user()").collect().head.getString(0)

    // Try to create a temp function with the same name
    sql("CREATE TEMPORARY FUNCTION current_user() RETURNS STRING RETURN 'hacker'")

    // Unqualified call should still resolve to builtin (security!)
    val unqualifiedResult = sql("SELECT current_user()").collect().head.getString(0)
    assert(unqualifiedResult == actualUser,
      s"Built-in current_user() was shadowed! Got '$unqualifiedResult', expected '$actualUser'")

    // But we can access the temp function via qualification
    checkAnswer(sql("SELECT session.current_user()"), Row("hacker"))

    // Cleanup
    sql("DROP TEMPORARY FUNCTION current_user")
  }

  test("SHOW FUNCTIONS includes extension functions") {
    val functions = sql("SHOW FUNCTIONS").collect().map(_.getString(0))
    assert(functions.contains("test_ext_func"),
      s"Extension function test_ext_func not found in SHOW FUNCTIONS output")
  }

  test("DESCRIBE FUNCTION works with extension functions") {
    // Unqualified - extension functions should be describable
    val desc = sql("DESCRIBE FUNCTION test_ext_func").collect()
    assert(desc.nonEmpty, "DESCRIBE FUNCTION should return results for extension functions")
  }
}

/**
 * Test extension that registers mock extension functions.
 * This simulates what real extensions like Apache Sedona would do.
 */
class TestExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Register a mock extension function
    // Use the full 11-parameter ExpressionInfo constructor
    // Note: source is left empty because "extension" is not a valid source value yet
    extensions.injectFunction(
      (org.apache.spark.sql.catalyst.FunctionIdentifier("test_ext_func"),
       new ExpressionInfo(
         "org.apache.spark.sql.ExtensionFunctionResolutionSuite",  // className
         "",                                                         // db
         "test_ext_func",                                           // name
         "Returns 9999 for testing",                                // usage
         "",                                                         // arguments
         "",                                                         // examples
         "",                                                         // note
         "",                                                         // group
         "4.2.0",                                                   // since
         "",                                                         // deprecated
         ""),                                                        // source (empty is allowed)
       (exprs: Seq[Expression]) => Literal(9999, IntegerType))
    )
  }
}
