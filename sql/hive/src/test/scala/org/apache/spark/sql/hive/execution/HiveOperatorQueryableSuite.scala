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

import org.apache.spark.sql.{Row, QueryTest}
import org.apache.spark.sql.hive.test.TestHive._

/**
 * A set of tests that validates commands can also be queried by like a table
 */
class HiveOperatorQueryableSuite extends QueryTest {
  test("SPARK-5324 query result of describe command") {
    loadTestTable("src")

    // register a describe command to be a temp table
    sql("desc src").registerTempTable("mydesc")
    checkAnswer(
      sql("desc mydesc"),
      Seq(
        Row("col_name", "string", "name of the column"),
        Row("data_type", "string", "data type of the column"),
        Row("comment", "string", "comment of the column")))

    checkAnswer(
      sql("select * from mydesc"),
      Seq(
        Row("key", "int", null),
        Row("value", "string", null)))

    checkAnswer(
      sql("select col_name, data_type, comment from mydesc"),
      Seq(
        Row("key", "int", null),
        Row("value", "string", null)))
  }
}
