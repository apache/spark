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

package org.apache.spark.sql.jdbc

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row}

trait SharedJDBCIntegrationTests extends QueryTest {
  protected def jdbcUrl: String

  test("SPARK-52184: Wrap external engine syntax error") {
    val e = intercept[SparkException] {
      spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("query", "THIS IS NOT VALID SQL").load()
    }
    assert(e.getCondition.startsWith("JDBC_EXTERNAL_ENGINE_SYNTAX_ERROR"))
  }

  test("SPARK-53386: Parameter `query` should work when ending with semicolon") {
    // Create table using Databricks SQL
    sql(s"""
      CREATE OR REPLACE TEMPORARY VIEW tbl_semicolon
      USING org.apache.spark.sql.jdbc
      OPTIONS (
        url '$jdbcUrl',
        query 'SELECT 1 as id, ''test_data'' as name'
      )
    """)

    // Test that queries with semicolons work properly
    val dfWithSemicolon = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("query", "SELECT 1 as id, 'test_data' as name;")
      .load()

    val expectedResult = Seq(Row(1, "test_data"))
    checkAnswer(dfWithSemicolon, expectedResult)
  }
}
