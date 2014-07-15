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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._

case class Data(a: Int, B: Int, n: Nested, nestedArray: Seq[Nested])
case class Nested(a: Int, B: Int)

/**
 * A set of test cases expressed in Hive QL that are not covered by the tests included in the hive distribution.
 */
class HiveResolutionSuite extends HiveComparisonTest {
  createQueryTest("table.attr",
    "SELECT src.key FROM src ORDER BY key LIMIT 1")

  createQueryTest("database.table",
    "SELECT key FROM default.src ORDER BY key LIMIT 1")

  createQueryTest("database.table table.attr",
    "SELECT src.key FROM default.src ORDER BY key LIMIT 1")

  createQueryTest("alias.attr",
    "SELECT a.key FROM src a ORDER BY key LIMIT 1")

  createQueryTest("subquery-alias.attr",
    "SELECT a.key FROM (SELECT * FROM src ORDER BY key LIMIT 1) a")

  createQueryTest("quoted alias.attr",
    "SELECT `a`.`key` FROM src a ORDER BY key LIMIT 1")

  createQueryTest("attr",
    "SELECT key FROM src a ORDER BY key LIMIT 1")

  createQueryTest("alias.star",
    "SELECT a.* FROM src a ORDER BY key LIMIT 1")

  test("case insensitivity with scala reflection") {
    // Test resolution with Scala Reflection
    TestHive.sparkContext.parallelize(Data(1, 2, Nested(1,2), Seq(Nested(1,2))) :: Nil)
      .registerAsTable("caseSensitivityTest")

    hql("SELECT a, b, A, B, n.a, n.b, n.A, n.B FROM caseSensitivityTest")
  }

  test("nested repeated resolution") {
    TestHive.sparkContext.parallelize(Data(1, 2, Nested(1,2), Seq(Nested(1,2))) :: Nil)
      .registerAsTable("nestedRepeatedTest")
    assert(hql("SELECT nestedArray[0].a FROM nestedRepeatedTest").collect().head(0) === 1)
  }

  /**
   * Negative examples.  Currently only left here for documentation purposes.
   * TODO(marmbrus): Test that catalyst fails on these queries.
   */

  /* SemanticException [Error 10009]: Line 1:7 Invalid table alias 'src'
  createQueryTest("table.*",
    "SELECT src.* FROM src a ORDER BY key LIMIT 1") */

  /* Invalid table alias or column reference 'src': (possible column names are: key, value)
  createQueryTest("tableName.attr from aliased subquery",
    "SELECT src.key FROM (SELECT * FROM src ORDER BY key LIMIT 1) a") */

}
