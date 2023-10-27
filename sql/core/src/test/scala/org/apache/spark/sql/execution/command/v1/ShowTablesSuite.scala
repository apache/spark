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
    checkError(
      exception = intercept[AnalysisException] {
        runShowTablesSql("SHOW TABLES FROM a.b", Seq())
      },
      errorClass = "_LEGACY_ERROR_TEMP_1126",
      parameters = Map("catalog" -> "a.b")
    )
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(showTableCmd)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1125",
        parameters = Map.empty
      )
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

  test("show table extended in non-partitioned table") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing")
      val e = intercept[AnalysisException] {
        sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '$table' PARTITION(id = 1)")
      }
      checkError(
        exception = e,
        errorClass = "_LEGACY_ERROR_TEMP_1251",
        parameters = Map("action" -> "SHOW TABLE EXTENDED", "tableName" -> table)
      )
    }
  }

  test("show table extended in multi partition key - " +
    "the command's partition parameters are complete") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (id1 bigint, id2 bigint, data string) " +
        s"$defaultUsing PARTITIONED BY (id1, id2)")
      sql(s"ALTER TABLE $tbl ADD PARTITION (id1 = 1, id2 = 2)")

      val result = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace " +
        s"LIKE '$table' PARTITION(id1 = 1, id2 = 2)")
      assert(result.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result.collect()(0).length == 4)
      assert(result.collect()(0)(0) === namespace)
      assert(result.collect()(0)(1) === table)
      assert(result.collect()(0)(2) === false)
      val actualResult = exclude(result.collect()(0)(3).toString)
      val expectedResult = "Partition Values: [id1=1, id2=2]"
      assert(actualResult === expectedResult)
    }
  }

  test("show table extended in multi tables") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { _ =>
      sql(s"CREATE TABLE $catalog.$namespace.$table (id bigint, data string) " +
        s"$defaultUsing PARTITIONED BY (id)")
      val table1 = "tbl1"
      val table2 = "tbl2"
      withTable(table1, table2) {
        sql(s"CREATE TABLE $catalog.$namespace.$table1 (id1 bigint, data1 string) " +
          s"$defaultUsing PARTITIONED BY (id1)")
        sql(s"CREATE TABLE $catalog.$namespace.$table2 (id2 bigint, data2 string) " +
          s"$defaultUsing PARTITIONED BY (id2)")

        val result = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace LIKE '$table*'")
          .sort("tableName")
        assert(result.schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary", "information"))
        assert(result.collect().length == 3)

        assert(result.collect()(0).length == 4)
        assert(result.collect()(0)(1) === table)
        assert(result.collect()(0)(2) === false)
        val actualResult_0_3 = exclude(result.collect()(0)(3).toString)

        // exclude "Created Time", "Last Access", "Created By", "Location"
        val expectedResult_0_3 =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table
             |Type: MANAGED
             |Provider: parquet
             |Partition Provider: Catalog
             |Partition Columns: [`id`]
             |Schema: root
             | |-- data: string (nullable = true)
             | |-- id: long (nullable = true)""".stripMargin

        assert(actualResult_0_3 === expectedResult_0_3)

        assert(result.collect()(1).length == 4)
        assert(result.collect()(1)(1) === table1)
        assert(result.collect()(1)(2) === false)
        val actualResult_1_3 = exclude(result.collect()(1)(3).toString)

        // exclude "Created Time", "Last Access", "Created By", "Location"
        val expectedResult_1_3 =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table1
             |Type: MANAGED
             |Provider: parquet
             |Partition Provider: Catalog
             |Partition Columns: [`id1`]
             |Schema: root
             | |-- data1: string (nullable = true)
             | |-- id1: long (nullable = true)""".stripMargin
        assert(actualResult_1_3 === expectedResult_1_3)

        assert(result.collect()(2).length == 4)
        assert(result.collect()(2)(1) === table2)
        assert(result.collect()(2)(2) === false)
        val actualResult_2_3 = exclude(result.collect()(2)(3).toString)

        // exclude "Created Time", "Last Access", "Created By", "Location"
        val expectedResult_2_3 =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table2
             |Type: MANAGED
             |Provider: parquet
             |Partition Provider: Catalog
             |Partition Columns: [`id2`]
             |Schema: root
             | |-- data2: string (nullable = true)
             | |-- id2: long (nullable = true)""".stripMargin
        assert(actualResult_2_3 === expectedResult_2_3)
      }
    }
  }
}
