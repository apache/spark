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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.execution.command.{ShowTablesSuite => CommonShowTablesSuite}
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}

class ShowTablesSuite extends CommonShowTablesSuite {
  override def catalog: String = "spark_catalog"
  override protected def defaultUsing: String = "USING parquet"
  override protected def showSchema: StructType = {
    new StructType()
      .add("database", StringType, nullable = false)
      .add("tableName", StringType, nullable = false)
      .add("isTemporary", BooleanType, nullable = false)
  }
  override protected def getRows(showRows: Seq[ShowRow]): Seq[Row] = {
    showRows.map {
      case ShowRow(namespace, table, isTemporary) => Row(namespace, table, isTemporary)
    }
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE DATABASE $namespace")
    sql(s"CREATE TABLE $namespace.$table (name STRING, id INT) USING PARQUET")
  }

  protected override def afterAll(): Unit = {
    sql(s"DROP TABLE $namespace.$table")
    sql(s"DROP DATABASE $namespace")
    super.afterAll()
  }

  test("show an existing table in V1 catalog") {
    val tables = sql(s"SHOW TABLES IN $catalog.test")
    assert(tables.schema.fieldNames.toSet === Set(namespaceColumn, tableColumn, "isTemporary"))
    checkAnswer(tables.select("isTemporary"), Row(false))
  }

  test("show table in a not existing namespace") {
    val msg = intercept[NoSuchDatabaseException] {
      runShowTablesSql(s"SHOW TABLES IN $catalog.unknown", Seq())
    }.getMessage
    assert(msg.contains("Database 'unknown' not found"))
  }

  private def withSourceViews(f: => Unit): Unit = {
    withTable("source", "source2") {
      val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
      df.createOrReplaceTempView("source")
      val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
      df2.createOrReplaceTempView("source2")
      f
    }
  }

  test("ShowTables: using v1 catalog") {
    withSourceViews {
      runShowTablesSql(
        "SHOW TABLES FROM default",
        Seq(ShowRow("", "source", true), ShowRow("", "source2", true)))
    }
  }

  test("ShowTables: using v1 catalog, db name with multipartIdentifier ('a.b') is not allowed.") {
    val exception = intercept[AnalysisException] {
      runShowTablesSql("SHOW TABLES FROM a.b", Seq())
    }
    assert(exception.getMessage.contains("The database name is not valid: a.b"))
  }

  test("ShowViews: using v1 catalog, db name with multipartIdentifier ('a.b') is not allowed.") {
    val exception = intercept[AnalysisException] {
      sql("SHOW TABLES FROM a.b")
    }
    assert(exception.getMessage.contains("The database name is not valid: a.b"))
  }

  test("ShowTables: namespace not specified and default v2 catalog not set - fallback to v1") {
    withSourceViews {
      runShowTablesSql(
        "SHOW TABLES",
        Seq(ShowRow("", "source", true),
            ShowRow("", "source2", true)))
      runShowTablesSql(
        "SHOW TABLES LIKE '*2'",
        Seq(ShowRow("", "source2", true)))
    }
  }

  test("SHOW TABLE EXTENDED valid v1") {
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
