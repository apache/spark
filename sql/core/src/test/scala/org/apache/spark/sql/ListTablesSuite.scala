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

import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

class ListTablesSuite extends QueryTest with BeforeAndAfter {

  import org.apache.spark.sql.test.TestSQLContext.implicits._

  val df =
    sparkContext.parallelize((1 to 10).map(i => (i,s"str$i"))).toDataFrame("key", "value")

  before {
    df.registerTempTable("ListTablesSuiteTable")
  }

  after {
    catalog.unregisterTable(Seq("ListTablesSuiteTable"))
  }

  test("get all tables") {
    checkAnswer(
      tables().filter("tableName = 'ListTablesSuiteTable'"),
      Row("ListTablesSuiteTable", true))

    catalog.unregisterTable(Seq("ListTablesSuiteTable"))
    assert(tables().filter("tableName = 'ListTablesSuiteTable'").count() === 0)
  }

  test("getting all Tables with a database name has no impact on returned table names") {
    checkAnswer(
      tables("DB").filter("tableName = 'ListTablesSuiteTable'"),
      Row("ListTablesSuiteTable", true))

    catalog.unregisterTable(Seq("ListTablesSuiteTable"))
    assert(tables().filter("tableName = 'ListTablesSuiteTable'").count() === 0)
  }

  test("query the returned DataFrame of tables") {
    val tableDF = tables()
    val schema = StructType(
      StructField("tableName", StringType, true) ::
      StructField("isTemporary", BooleanType, false) :: Nil)
    assert(schema === tableDF.schema)

    tableDF.registerTempTable("tables")
    checkAnswer(
      sql("SELECT isTemporary, tableName from tables WHERE tableName = 'ListTablesSuiteTable'"),
      Row(true, "ListTablesSuiteTable")
    )
    checkAnswer(
      tables().filter("tableName = 'tables'").select("tableName", "isTemporary"),
      Row("tables", true))
    dropTempTable("tables")
  }
}
