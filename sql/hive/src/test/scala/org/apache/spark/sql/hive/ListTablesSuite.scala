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

package org.apache.spark.sql.hive

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.SharedHiveContext

class ListTablesSuite extends QueryTest with SharedHiveContext {
  import testImplicits._

  private lazy val df =
    ctx.sparkContext.parallelize((1 to 10).map(i => (i, s"str$i"))).toDF("key", "value")

  override def beforeAll(): Unit = {
    super.beforeAll()
    // The catalog in HiveContext is a case insensitive one.
    ctx.catalog.registerTable(Seq("ListTablesSuiteTable"), df.logicalPlan)
    ctx.catalog.registerTable(Seq("ListTablesSuiteDB", "InDBListTablesSuiteTable"), df.logicalPlan)
    ctx.sql("CREATE TABLE HiveListTablesSuiteTable (key int, value string)")
    ctx.sql("CREATE DATABASE IF NOT EXISTS ListTablesSuiteDB")
    ctx.sql("CREATE TABLE ListTablesSuiteDB.HiveInDBListTablesSuiteTable (key int, value string)")
  }

  override def afterAll(): Unit = {
    try {
      ctx.catalog.unregisterTable(Seq("ListTablesSuiteTable"))
      ctx.catalog.unregisterTable(Seq("ListTablesSuiteDB", "InDBListTablesSuiteTable"))
      ctx.sql("DROP TABLE IF EXISTS HiveListTablesSuiteTable")
      ctx.sql("DROP TABLE IF EXISTS ListTablesSuiteDB.HiveInDBListTablesSuiteTable")
      ctx.sql("DROP DATABASE IF EXISTS ListTablesSuiteDB")
    } finally {
      super.afterAll()
    }
  }

  test("get all tables of current database") {
    Seq(ctx.tables(), ctx.sql("SHOW TABLes")).foreach {
      case allTables =>
        // We are using default DB.
        checkAnswer(
          allTables.filter("tableName = 'listtablessuitetable'"),
          Row("listtablessuitetable", true))
        assert(allTables.filter("tableName = 'indblisttablessuitetable'").count() === 0)
        checkAnswer(
          allTables.filter("tableName = 'hivelisttablessuitetable'"),
          Row("hivelisttablessuitetable", false))
        assert(allTables.filter("tableName = 'hiveindblisttablessuitetable'").count() === 0)
    }
  }

  test("getting all tables with a database name") {
    Seq(ctx.tables("listtablessuiteDb"), ctx.sql("SHOW TABLes in listTablesSuitedb")).foreach {
      case allTables =>
        checkAnswer(
          allTables.filter("tableName = 'listtablessuitetable'"),
          Row("listtablessuitetable", true))
        checkAnswer(
          allTables.filter("tableName = 'indblisttablessuitetable'"),
          Row("indblisttablessuitetable", true))
        assert(allTables.filter("tableName = 'hivelisttablessuitetable'").count() === 0)
        checkAnswer(
          allTables.filter("tableName = 'hiveindblisttablessuitetable'"),
          Row("hiveindblisttablessuitetable", false))
    }
  }
}
