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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for qualified temporary function DDL (CREATE/DROP with session.* qualification)
 */
class TempFunctionQualificationSuite extends QueryTest with SharedSparkSession {

  test("CREATE TEMPORARY FUNCTION with unqualified name") {
    sql("CREATE TEMPORARY FUNCTION my_func() RETURNS INT RETURN 42")
    val result = sql("SELECT my_func()").collect()
    assert(result(0).getInt(0) == 42)
    sql("DROP TEMPORARY FUNCTION my_func")
  }

  test("CREATE TEMPORARY FUNCTION with session qualification") {
    sql("CREATE TEMPORARY FUNCTION session.my_func2() RETURNS INT RETURN 99")
    val result = sql("SELECT my_func2()").collect()
    assert(result(0).getInt(0) == 99)
    sql("DROP TEMPORARY FUNCTION my_func2")
  }

  test("CREATE TEMPORARY FUNCTION with system.session qualification") {
    sql("CREATE TEMPORARY FUNCTION system.session.my_func3() RETURNS INT RETURN 77")
    val result = sql("SELECT my_func3()").collect()
    assert(result(0).getInt(0) == 77)
    sql("DROP TEMPORARY FUNCTION my_func3")
  }

  test("DROP TEMPORARY FUNCTION with session qualification") {
    sql("CREATE TEMPORARY FUNCTION my_func4() RETURNS INT RETURN 55")
    sql("DROP TEMPORARY FUNCTION session.my_func4")
    // Verify it's really gone
    intercept[Exception] {
      sql("SELECT my_func4()").collect()
    }
  }

  test("DROP TEMPORARY FUNCTION with system.session qualification") {
    sql("CREATE TEMPORARY FUNCTION my_func5() RETURNS INT RETURN 33")
    sql("DROP TEMPORARY FUNCTION system.session.my_func5")
    // Verify it's really gone
    intercept[Exception] {
      sql("SELECT my_func5()").collect()
    }
  }

  test("CREATE TEMPORARY FUNCTION with invalid database qualification fails") {
    val e = intercept[Exception] {
      sql("CREATE TEMPORARY FUNCTION mydb.my_func() RETURNS INT RETURN 1")
    }
    assert(e.getMessage.contains("mydb") || e.getMessage.contains("database"))
  }

  test("DROP TEMPORARY FUNCTION with invalid database qualification fails") {
    sql("CREATE TEMPORARY FUNCTION my_func6() RETURNS INT RETURN 11")
    val e = intercept[Exception] {
      sql("DROP TEMPORARY FUNCTION mydb.my_func6")
    }
    assert(e.getMessage.contains("mydb") || e.getMessage.contains("database"))
    // Clean up
    sql("DROP TEMPORARY FUNCTION my_func6")
  }

  test("CREATE TEMPORARY FUNCTION case insensitive qualification") {
    sql("CREATE TEMPORARY FUNCTION SESSION.my_func7() RETURNS INT RETURN 88")
    val result = sql("SELECT my_func7()").collect()
    assert(result(0).getInt(0) == 88)
    sql("DROP TEMPORARY FUNCTION SYSTEM.SESSION.my_func7")
  }

  test("Qualified and unqualified CREATE/DROP can be mixed") {
    sql("CREATE TEMPORARY FUNCTION session.my_func8() RETURNS INT RETURN 66")
    sql("DROP TEMPORARY FUNCTION my_func8")  // Drop without qualification

    sql("CREATE TEMPORARY FUNCTION my_func9() RETURNS INT RETURN 44")
    sql("DROP TEMPORARY FUNCTION system.session.my_func9")  // Drop with qualification
  }
}
