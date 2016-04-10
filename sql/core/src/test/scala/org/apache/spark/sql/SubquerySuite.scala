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
  import testImplicits._

  test("simple uncorrelated scalar subquery") {
    assertResult(Array(Row(1))) {
      sql("select (select 1 as b) as b").collect()
    }

    assertResult(Array(Row(3))) {
      sql("select (select (select 1) + 1) + 1").collect()
    }

    // string type
    assertResult(Array(Row("s"))) {
      sql("select (select 's' as s) as b").collect()
    }
  }

  test("uncorrelated scalar subquery in CTE") {
    assertResult(Array(Row(1))) {
      sql("with t2 as (select 1 as b, 2 as c) " +
        "select a from (select 1 as a union all select 2 as a) t " +
        "where a = (select max(b) from t2) ").collect()
    }
  }

  test("uncorrelated scalar subquery should return null if there is 0 rows") {
    assertResult(Array(Row(null))) {
      sql("select (select 's' as s limit 0) as b").collect()
    }
  }

  test("runtime error when the number of rows is greater than 1") {
    val error2 = intercept[RuntimeException] {
      sql("select (select a from (select 1 as a union all select 2 as a) t) as b").collect()
    }
    assert(error2.getMessage.contains(
      "more than one row returned by a subquery used as an expression"))
  }

  test("uncorrelated scalar subquery on a DataFrame generated query") {
    val df = Seq((1, "one"), (2, "two"), (3, "three")).toDF("key", "value")
    df.registerTempTable("subqueryData")

    assertResult(Array(Row(4))) {
      sql("select (select key from subqueryData where key > 2 order by key limit 1) + 1").collect()
    }

    assertResult(Array(Row(-3))) {
      sql("select -(select max(key) from subqueryData)").collect()
    }

    assertResult(Array(Row(null))) {
      sql("select (select value from subqueryData limit 0)").collect()
    }

    assertResult(Array(Row("two"))) {
      sql("select (select min(value) from subqueryData" +
        " where key = (select max(key) from subqueryData) - 1)").collect()
    }
  }
}
