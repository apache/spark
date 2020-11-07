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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}

trait ShowTablesSuite extends QueryTest with SharedSparkSession {
  protected def catalog: String
  protected def defaultUsing: String
  protected def namespaceColumn: String = "database"
  protected def namespace: String = "test"
  protected def tableColumn: String = "tableName"
  protected def table: String = "people"

  case class ShowRow(namespace: String, table: String, isTemporary: Boolean)

  protected def showSchema: StructType
  protected def getRows(showRows: Seq[ShowRow]): Seq[Row]

  protected def runShowTablesSql(sqlText: String, expected: Seq[ShowRow]): Unit = {
    val df = spark.sql(sqlText)
    assert(df.schema === showSchema)
    assert(df.collect() === getRows(expected))
  }

  protected def withSourceViews(f: => Unit): Unit = {
    withTable("source", "source2") {
      val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
      df.createOrReplaceTempView("source")
      val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
      df2.createOrReplaceTempView("source2")
      f
    }
  }

  test("show an existing table") {
    runShowTablesSql(s"SHOW TABLES IN $catalog.test", Seq(ShowRow(namespace, table, false)))
  }

  test("show tables with a pattern") {
    withDatabase(s"$catalog.db", s"$catalog.db2") {
      sql(s"CREATE DATABASE $catalog.db")
      sql(s"CREATE DATABASE $catalog.db2")
      withTable(
        s"$catalog.db.table",
        s"$catalog.db.table_name_1",
        s"$catalog.db.table_name_2",
        s"$catalog.db2.table_name_2") {
        sql(s"CREATE TABLE $catalog.db.table (id bigint, data string) $defaultUsing")
        sql(s"CREATE TABLE $catalog.db.table_name_1 (id bigint, data string) $defaultUsing")
        sql(s"CREATE TABLE $catalog.db.table_name_2 (id bigint, data string) $defaultUsing")
        sql(s"CREATE TABLE $catalog.db2.table_name_2 (id bigint, data string) $defaultUsing")

        runShowTablesSql(
          s"SHOW TABLES FROM $catalog.db",
          Seq(
            ShowRow("db", "table", false),
            ShowRow("db", "table_name_1", false),
            ShowRow("db", "table_name_2", false)))

        runShowTablesSql(
          s"SHOW TABLES FROM $catalog.db LIKE '*name*'",
          Seq(
            ShowRow("db", "table_name_1", false),
            ShowRow("db", "table_name_2", false)))

        runShowTablesSql(
          s"SHOW TABLES FROM $catalog.db LIKE '*2'",
          Seq(ShowRow("db", "table_name_2", false)))
      }
    }
  }

  test("SHOW TABLE EXTENDED for default") {
    withSourceViews {
      val expected = Seq(Row("", "source", true), Row("", "source2", true))
      val schema = new StructType()
        .add("database", StringType, nullable = false)
        .add("tableName", StringType, nullable = false)
        .add("isTemporary", BooleanType, nullable = false)
        .add("information", StringType, nullable = false)

      val df = sql("SHOW TABLE EXTENDED FROM default LIKE '*source*'")
      val result = df.collect()
      val resultWithoutInfo = result.map { case Row(db, table, temp, _) => Row(db, table, temp) }

      assert(df.schema === schema)
      assert(resultWithoutInfo === expected)
      result.foreach { case Row(_, _, _, info: String) => assert(info.nonEmpty) }
    }
  }
}
