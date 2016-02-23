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

import org.apache.spark.sql.test.SharedSQLContext

class SubquerySuite extends QueryTest with SharedSQLContext {

  test("simple uncorrelated scalar subquery") {
    assertResult(Array(Row(1))) {
      sql("select (select 1 as b) as b").collect()
    }

    assertResult(Array(Row(1))) {
      sql("with t2 as (select 1 as b, 2 as c) " +
        "select a from (select 1 as a union all select 2 as a) t " +
        "where a = (select max(b) from t2) ").collect()
    }

    assertResult(Array(Row(3))) {
      sql("select (select (select 1) + 1) + 1").collect()
    }

    // more than one columns
    val error = intercept[AnalysisException] {
      sql("select (select 1, 2) as b").collect()
    }
    assert(error.message contains "Scalar subquery must return only one column, but got 2")

    // more than one rows
    val error2 = intercept[RuntimeException] {
      sql("select (select a from (select 1 as a union all select 2 as a) t) as b").collect()
    }
    assert(error2.getMessage contains
      "more than one row returned by a subquery used as an expression")

    // string type
    assertResult(Array(Row("s"))) {
      sql("select (select 's' as s) as b").collect()
    }

    // zero rows
    assertResult(Array(Row(null))) {
      sql("select (select 's' as s limit 0) as b").collect()
    }
  }

  test("uncorrelated scalar subquery on testData") {
    // initialize test Data
    testData

    assertResult(Array(Row(5))) {
      sql("select (select key from testData where key > 3 limit 1) + 1").collect()
    }

    assertResult(Array(Row(-100))) {
      sql("select -(select max(key) from testData)").collect()
    }

    assertResult(Array(Row(null))) {
      sql("select (select value from testData limit 0)").collect()
    }

    assertResult(Array(Row("99"))) {
      sql("select (select min(value) from testData" +
        " where key = (select max(key) from testData) - 1)").collect()
    }
  }
}
