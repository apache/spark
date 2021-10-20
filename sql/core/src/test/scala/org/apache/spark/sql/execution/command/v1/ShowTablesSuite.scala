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
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `SHOW TABLES` command that check V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowTablesSuite`
 *   - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.ShowTablesSuite`
 */
trait ShowTablesSuiteBase extends command.ShowTablesSuiteBase with command.TestsV1AndV2Commands {
  override def defaultNamespace: Seq[String] = Seq("default")

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
        Seq(Row("", "source", true), Row("", "source2", true)))
    }
  }

  test("only support single-level namespace") {
    val errMsg = intercept[AnalysisException] {
      runShowTablesSql("SHOW TABLES FROM a.b", Seq())
    }.getMessage
    assert(errMsg.contains("Nested databases are not supported by v1 session catalog: a.b"))
  }

  test("SHOW TABLE EXTENDED from default") {
    withSourceViews {
      val expected = Seq(Row("", "source", true), Row("", "source2", true))

      val df = sql("SHOW TABLE EXTENDED FROM default LIKE '*source*'")
      val result = df.collect()
      val resultWithoutInfo = result.map { case Row(db, table, temp, _) => Row(db, table, temp) }

      assert(resultWithoutInfo === expected)
      result.foreach { case Row(_, _, _, info: String) => assert(info.nonEmpty) }
    }
  }

  test("case sensitivity of partition spec") {
    withNamespaceAndTable("ns", "part_table") { t =>
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
          val df = sql(s"SHOW TABLE EXTENDED IN ns LIKE 'part_table' $partitionSpec")
          val information = df.select("information").first().getString(0)
          assert(information.contains("Partition Values: [year=2015, month=1]"))
        }
      }
    }
  }

  test("no database specified") {
    Seq(
      s"SHOW TABLES IN $catalog",
      s"SHOW TABLE EXTENDED IN $catalog LIKE '*tbl'").foreach { showTableCmd =>
      val errMsg = intercept[AnalysisException] {
        sql(showTableCmd)
      }.getMessage
      assert(errMsg.contains("Database from v1 session catalog is not specified"))
    }
  }

  test("SPARK-34157: Unify output of SHOW TABLES and pass output attributes properly") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      sql(s"USE $catalog.ns")
      withTable("tbl") {
        sql("CREATE TABLE tbl(col1 int, col2 string) USING parquet")
        checkAnswer(sql("show tables"), Row("ns", "tbl", false))
        assert(sql("show tables").schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary"))
        assert(sql("show table extended like 'tbl'").collect()(0).length == 4)
        assert(sql("show table extended like 'tbl'").schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary", "information"))

        // Keep the legacy output schema
        withSQLConf(SQLConf.LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA.key -> "true") {
          checkAnswer(sql("show tables"), Row("ns", "tbl", false))
          assert(sql("show tables").schema.fieldNames ===
            Seq("database", "tableName", "isTemporary"))
          assert(sql("show table extended like 'tbl'").collect()(0).length == 4)
          assert(sql("show table extended like 'tbl'").schema.fieldNames ===
            Seq("database", "tableName", "isTemporary", "information"))
        }
      }
    }
  }

  test("show table in a not existing namespace") {
    val msg = intercept[NoSuchDatabaseException] {
      runShowTablesSql(s"SHOW TABLES IN $catalog.unknown", Seq())
    }.getMessage
    assert(msg.matches("(Database|Namespace) 'unknown' not found"))
  }

}

/**
 * The class contains tests for the `SHOW TABLES` command to check V1 In-Memory table catalog.
 */
class ShowTablesSuite extends ShowTablesSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[ShowTablesSuiteBase].commandVersion

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
