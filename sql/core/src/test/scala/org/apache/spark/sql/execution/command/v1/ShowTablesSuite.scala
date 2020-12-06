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

import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}

trait ShowTablesSuiteBase extends command.ShowTablesSuiteBase {
  override def version: String = "V1"
  override def catalog: String = CatalogManager.SESSION_CATALOG_NAME
  override def defaultNamespace: Seq[String] = Seq("default")
  override def defaultUsing: String = "USING parquet"
  override def showSchema: StructType = {
    new StructType()
      .add("database", StringType, nullable = false)
      .add("tableName", StringType, nullable = false)
      .add("isTemporary", BooleanType, nullable = false)
  }
  override def getRows(showRows: Seq[ShowRow]): Seq[Row] = {
    showRows.map {
      case ShowRow(namespace, table, isTemporary) => Row(namespace, table, isTemporary)
    }
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

  // `SHOW TABLES` from v2 catalog returns empty result.
  test("v1 SHOW TABLES list the temp views") {
    withSourceViews {
      runShowTablesSql(
        "SHOW TABLES FROM default",
        Seq(ShowRow("", "source", true), ShowRow("", "source2", true)))
    }
  }

  test("v1 SHOW TABLES only support single-level namespace") {
    val exception = intercept[AnalysisException] {
      runShowTablesSql("SHOW TABLES FROM a.b", Seq())
    }
    assert(exception.getMessage.contains("The database name is not valid: a.b"))
  }

  test("SHOW TABLE EXTENDED from default") {
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

  test("case sensitivity of partition spec") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      val t = s"$catalog.ns.part_table"
      withTable(t) {
        sql(s"""
          |CREATE TABLE $t (price int, qty int, year int, month int)
          |$defaultUsing
          |partitioned by (year, month)""".stripMargin)
        sql(s"INSERT INTO $t PARTITION(year = 2015, month = 1) SELECT 1, 1")
        Seq(
          true -> "PARTITION(year = 2015, month = 1)",
          false -> "PARTITION(YEAR = 2015, Month = 1)"
        ).foreach { case (caseSensitive, partitionSpec) =>
          withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
            val df = sql(s"SHOW TABLE EXTENDED LIKE 'part_table' $partitionSpec")
            val information = df.select("information").first().getString(0)
            assert(information.contains("Partition Values: [year=2015, month=1]"))
          }
        }
      }
    }
  }
}

class ShowTablesSuite extends ShowTablesSuiteBase with SharedSparkSession {
  test("SPARK-33670: show partitions from a datasource table") {
    import testImplicits._
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      sql(s"USE $catalog.ns")
      val t = "part_datasrc"
      withTable(t) {
        val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")
        df.write.partitionBy("a").format("parquet").mode(SaveMode.Overwrite).saveAsTable(t)
        assert(sql(s"SHOW TABLE EXTENDED LIKE '$t' PARTITION(a = 1)").count() === 1)
      }
    }
  }
}
