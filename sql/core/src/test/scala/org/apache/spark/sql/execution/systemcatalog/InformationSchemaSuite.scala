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

package org.apache.spark.sql.execution.systemcatalog

import java.util.UUID

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Information Schema Suite
 */
class InformationSchemaSuite extends QueryTest with SharedSQLContext {
  val TESTDB = s"db_${UUID.randomUUID().toString.replace('-', '_')}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    InformationSchema.registerInformationSchema(spark)
    sql(s"create database if not exists $TESTDB")
    sql(s"create table $TESTDB.t1(a int)")
    sql(s"create view $TESTDB.v1 as select 1")
    sql("use default")
  }

  override def afterAll(): Unit = {
    try {
      sql("use default")
      sql(s"drop database $TESTDB cascade")
    } finally {
      super.afterAll()
    }
  }

  test("use information_schema") {
    sql("use information_schema")
    sql("select * from databases")
    sql("select * from schemata")
    sql("select * from tables")
    sql("select * from views")
    sql("select * from columns")
    sql("select * from session_variables")
    sql("use default")
  }

  test("catalog functions") {
    sql("use default")
    assert(spark.catalog.listTables().collect().length == 0)
    spark.catalog.listColumns("information_schema.tables")
    intercept[NoSuchTableException] {
      spark.catalog.listColumns("tables")
    }

    sql("use information_schema")
    assert(spark.catalog.listTables().collect().length == 6)
    spark.catalog.listColumns("tables")

    sql("use default")
  }

  test("databases/schemata") {
    checkAnswer(
      sql("select * from information_schema.databases"),
      Seq(Row("default", "default"), Row("default", "information_schema"), Row("default", TESTDB)))

    checkAnswer(
      sql("select * from information_schema.schemata"),
      Seq(Row("default", "default"), Row("default", "information_schema"), Row("default", TESTDB)))

    checkAnswer(
      sql("select SCHEMA_NAME from information_schema.databases"),
      Row(TESTDB) :: Row("default") :: Row("information_schema") :: Nil)

    checkAnswer(
      sql("select SCHEMA_NAME from information_schema.schemata"),
      Row(TESTDB) :: Row("default") :: Row("information_schema") :: Nil)
  }

  test("tables") {
    checkAnswer(
      sql("select * from information_schema.tables"),
      Row("default", TESTDB, "t1", "TABLE") ::
      Row("default", TESTDB, "v1", "VIEW") :: Nil)

    checkAnswer(
      sql("select TABLE_NAME from information_schema.tables"),
      Row("t1") :: Row("v1") :: Nil)

    checkAnswer(
      sql(s"select * from information_schema.tables where TABLE_SCHEMA='$TESTDB'"),
      Row("default", TESTDB, "t1", "TABLE") ::
      Row("default", TESTDB, "v1", "VIEW") :: Nil)

    checkAnswer(
      sql("select * from information_schema.tables where TABLE_SCHEMA='default'"),
      Nil)
  }

  test("views") {
    checkAnswer(
      sql("select * from information_schema.views"),
      Row("default", TESTDB, "v1", "VIEW") :: Nil)

    checkAnswer(
      sql("select TABLE_NAME from information_schema.views"),
      Row("v1") :: Nil)

    checkAnswer(
      sql(s"select * from information_schema.views where TABLE_SCHEMA='$TESTDB'"),
      Row("default", TESTDB, "v1", "VIEW") :: Nil)

    checkAnswer(
      sql("select * from information_schema.views where TABLE_SCHEMA='default'"),
      Nil)
  }

  test("columns") {
    checkAnswer(
      sql("select * from information_schema.columns"),
      Row("default", TESTDB, "t1", "a", 0, true, "int") ::
      Row("default", TESTDB, "v1", "1", 0, false, "int") ::
      Nil)

    checkAnswer(
      sql("select COLUMN_NAME from information_schema.columns"),
      Row("a") :: Row("1") :: Nil)

    checkAnswer(
      sql("select COLUMN_NAME from information_schema.columns WHERE TABLE_NAME = 't1'"),
      Row("a") :: Nil)
  }

  test("session_variables") {
    val df = sql("select * from information_schema.session_variables").collect()
    assert(df.forall(_.length == 2))
    assert(df.exists(row => row(0) == "spark.app.id"))
    assert(df.exists(row => row(0) == "spark.app.name"))

    val df2 = sql("select VARIABLE_NAME from information_schema.session_variables").collect()
    assert(df2.forall(_.length == 1))
    assert(df2.exists(row => row(0) == "spark.app.id"))
    assert(df2.exists(row => row(0) == "spark.app.name"))
  }
}
