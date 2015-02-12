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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

class ListTablesSuite extends QueryTest with BeforeAndAfterAll {

  import org.apache.spark.sql.test.TestSQLContext.implicits._

  val df =
    sparkContext.parallelize((1 to 10).map(i => (i,s"str$i"))).toDataFrame("key", "value")

  override def beforeAll(): Unit = {
    (1 to 10).foreach(i => df.registerTempTable(s"table$i"))
  }

  override def afterAll(): Unit = {
    catalog.unregisterAllTables()
  }

  test("get All Tables") {
    checkAnswer(tables(), (1 to 10).map(i => Row(s"table$i", true)))
  }

  test("getting All Tables with a database name has not impact on returned table names") {
    checkAnswer(tables("DB"), (1 to 10).map(i => Row(s"table$i", true)))
  }

  test("query the returned DataFrame of tables") {
    val tableDF = tables()
    val schema = StructType(
      StructField("tableName", StringType, true) ::
      StructField("isTemporary", BooleanType, false) :: Nil)
    assert(schema === tableDF.schema)

    checkAnswer(
      tableDF.select("tableName"),
      (1 to 10).map(i => Row(s"table$i"))
    )

    tableDF.registerTempTable("tables")
    checkAnswer(
      sql("SELECT isTemporary, tableName from tables WHERE isTemporary"),
      (1 to 10).map(i => Row(true, s"table$i"))
    )
    checkAnswer(
      tables().filter("tableName = 'tables'").select("tableName", "isTemporary"),
      Row("tables", true))
    dropTempTable("tables")
  }
}