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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.TableIdentifier

class ListTablesSuite extends QueryTest with BeforeAndAfter with SharedSQLContext {
  import testImplicits._

  private lazy val df = (1 to 10).map(i => (i, s"str$i")).toDF("key", "value")

  before {
    df.registerTempTable("ListTablesSuiteTable")
  }

  after {
    sqlContext.catalog.unregisterTable(TableIdentifier("ListTablesSuiteTable"))
  }

  test("get all tables") {
    checkAnswer(
      sqlContext.tables().filter("tableName = 'ListTablesSuiteTable'"),
      Row("ListTablesSuiteTable", true))

    checkAnswer(
      sql("SHOW tables").filter("tableName = 'ListTablesSuiteTable'"),
      Row("ListTablesSuiteTable", true))

    sqlContext.catalog.unregisterTable(TableIdentifier("ListTablesSuiteTable"))
    assert(sqlContext.tables().filter("tableName = 'ListTablesSuiteTable'").count() === 0)
  }

  test("getting all Tables with a database name has no impact on returned table names") {
    checkAnswer(
      sqlContext.tables("DB").filter("tableName = 'ListTablesSuiteTable'"),
      Row("ListTablesSuiteTable", true))

    checkAnswer(
      sql("show TABLES in DB").filter("tableName = 'ListTablesSuiteTable'"),
      Row("ListTablesSuiteTable", true))

    sqlContext.catalog.unregisterTable(TableIdentifier("ListTablesSuiteTable"))
    assert(sqlContext.tables().filter("tableName = 'ListTablesSuiteTable'").count() === 0)
  }

  test("query the returned DataFrame of tables") {
    val expectedSchema = StructType(
      StructField("tableName", StringType, false) ::
      StructField("isTemporary", BooleanType, false) :: Nil)

    Seq(sqlContext.tables(), sql("SHOW TABLes")).foreach {
      case tableDF =>
        assert(expectedSchema === tableDF.schema)

        tableDF.registerTempTable("tables")
        checkAnswer(
          sql(
            "SELECT isTemporary, tableName from tables WHERE tableName = 'ListTablesSuiteTable'"),
          Row(true, "ListTablesSuiteTable")
        )
        checkAnswer(
          sqlContext.tables().filter("tableName = 'tables'").select("tableName", "isTemporary"),
          Row("tables", true))
        sqlContext.dropTempTable("tables")
    }
  }
}
