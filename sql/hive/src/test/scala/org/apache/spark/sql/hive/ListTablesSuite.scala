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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

class ListTablesSuite extends QueryTest with BeforeAndAfterAll {

  import org.apache.spark.sql.hive.test.TestHive.implicits._

  val sqlContext = TestHive
  val df =
    sparkContext.parallelize((1 to 10).map(i => (i,s"str$i"))).toDataFrame("key", "value")

  override def beforeAll(): Unit = {
    // The catalog in HiveContext is a case insensitive one.
    (1 to 10).foreach(i => catalog.registerTable(Seq(s"Table$i"), df.logicalPlan))
    (1 to 10).foreach(i => catalog.registerTable(Seq("db1", s"db1TempTable$i"), df.logicalPlan))
    (1 to 10).foreach {
      i => sql(s"CREATE TABLE hivetable$i (key int, value string)")
    }
    sql("CREATE DATABASE IF NOT EXISTS db1")
    (1 to 10).foreach {
      i => sql(s"CREATE TABLE db1.db1hivetable$i (key int, value string)")
    }
  }

  override def afterAll(): Unit = {
    catalog.unregisterAllTables()
    (1 to 10).foreach {
      i => sql(s"DROP TABLE IF EXISTS hivetable$i")
    }
    (1 to 10).foreach {
      i => sql(s"DROP TABLE IF EXISTS db1.db1hivetable$i")
    }
    sql("DROP DATABASE IF EXISTS db1")
  }

  test("get All Tables of current database") {
    // We are using default DB.
    val expectedTables =
      (1 to 10).map(i => Row(s"table$i", true)) ++
      (1 to 10).map(i => Row(s"hivetable$i", false))
    checkAnswer(tables(), expectedTables)
  }

  test("getting All Tables with a database name has not impact on returned table names") {
    val expectedTables =
      // We are expecting to see Table1 to Table10 since there is no database associated with them.
      (1 to 10).map(i => Row(s"table$i", true)) ++
      (1 to 10).map(i => Row(s"db1temptable$i", true)) ++
      (1 to 10).map(i => Row(s"db1hivetable$i", false))
    checkAnswer(tables("db1"), expectedTables)
  }

  test("query the returned DataFrame of tables") {
    val tableDF = tables()
    val schema = StructType(
      StructField("tableName", StringType, true) ::
      StructField("isTemporary", BooleanType, false) :: Nil)
    assert(schema === tableDF.schema)

    checkAnswer(
      tableDF.filter("NOT isTemporary").select("tableName"),
      (1 to 10).map(i => Row(s"hivetable$i"))
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
