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

import org.apache.spark.sql.catalyst.plans.logical.{Project, NoRelation, LogicalPlan}
import org.scalatest.{FunSuite, Matchers}

object TestObjectDialect extends Dialect {

  override def parse(sql: String): LogicalPlan = {
    NoRelation
  }

  override def description = "test object dialect"
}

class TestClassDialect extends Dialect {

  override def parse(sql: String): LogicalPlan = {
    Project(Seq.empty, NoRelation)
  }

  override def description = TestClassDialect.description
}

object TestClassDialect {
  val description = "test class dialect"
}

object ErrorDialect

class ErrorClassDialect

class DefaultDialectManagerSuite extends FunSuite with Matchers {
  test("object dialect reflect") {
    val dialect = DefaultDialectManager
      .buildDialect("org.apache.spark.sql.dialect.TestObjectDialect")

    dialect.description should be (TestObjectDialect.description)
    dialect.parse("sql statement") should be (NoRelation)

    intercept[Exception](DefaultDialectManager
      .buildDialect("org.apache.spark.sql.dialect.ErrorDialect"))
  }

  test("class dialect reflect") {
    val dialect = DefaultDialectManager
      .buildDialect("org.apache.spark.sql.dialect.TestClassDialect")

    dialect.description should be (TestClassDialect.description)
    dialect.parse("sql statement") should be (Project(Seq.empty, NoRelation))

    intercept[Exception](DefaultDialectManager
      .buildDialect("org.apache.spark.sql.dialect.ErrorClassDialect"))
  }
}
