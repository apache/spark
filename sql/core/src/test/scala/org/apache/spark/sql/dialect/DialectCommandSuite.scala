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

package org.apache.spark.sql.dialect

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.test.TestSQLContext.{udf => _, _}
import org.apache.spark.sql.{QueryTest, Row, SparkSQLParser, catalyst}

class TestDialect extends Dialect {
  val sqlParser = {
    val fallback = new catalyst.SqlParser
    new SparkSQLParser(fallback(_))
  }

  override def parse(sql: String): LogicalPlan = {
    sqlParser(sql)
  }

  override def description = "test dialect"
}

class DialectCommandSuite extends QueryTest {

  test("show dialect") {
    checkAnswer(
      sql("show dialect"),
      Row(SparkSqlDialect.name))

    checkAnswer(
      sql("show extended dialect"),
      Row(SparkSqlDialect.name,
        SparkSqlDialect.getClass.getCanonicalName.dropRight(1),
        SparkSqlDialect.description)
    )
  }

  test("create dialect") {
    val dialectName = "tql"

    // dialect is a keyword
    sql(s"""CREATE DIALECT $dialectName USING org.apache.spark.sql.`dialect`.TestDialect""")

    checkAnswer(
      sql("show dialects"),
      Row("sql") :: Row(dialectName) :: Nil
    )

    sql(s"use dialect $dialectName")

    checkAnswer(
      sql("show dialect"),
      Row(dialectName)
    )

    intercept[RuntimeException](sql(s"drop dialect $dialectName"))

    sql("use dialect sql")

    sql(s"drop dialect $dialectName")

    checkAnswer(
      sql("show dialects"),
      Row("sql")
    )
  }

  test("dialect parser") {
    // class name can't be empty
    intercept[RuntimeException](sql("create dialect test using "))
  }
}

