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

import java.time.{Duration, Period}
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.errors.QueryErrorsSuiteBase
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

case class Cases(lower: String, UPPER: String)

class HiveParquetSuite extends QueryTest
  with ParquetTest
  with TestHiveSingleton
  with QueryErrorsSuiteBase {

  test("Case insensitive attribute names") {
    withParquetTable((1 to 4).map(i => Cases(i.toString, i.toString)), "cases") {
      val expected = (1 to 4).map(i => Row(i.toString))
      checkAnswer(sql("SELECT upper FROM cases"), expected)
      checkAnswer(sql("SELECT LOWER FROM cases"), expected)
    }
  }

  test("SELECT on Parquet table") {
    val data = (1 to 4).map(i => (i, s"val_$i"))
    withParquetTable(data, "t") {
      checkAnswer(sql("SELECT * FROM t"), data.map(Row.fromTuple))
    }
  }

  test("Simple column projection + filter on Parquet table") {
    withParquetTable((1 to 4).map(i => (i % 2 == 0, i, s"val_$i")), "t") {
      checkAnswer(
        sql("SELECT `_1`, `_3` FROM t WHERE `_1` = true"),
        Seq(Row(true, "val_2"), Row(true, "val_4")))
    }
  }

  test("Converting Hive to Parquet Table via saveAsParquetFile") {
    withTempPath { dir =>
      sql("SELECT * FROM src").write.parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("p")
      withTempView("p") {
        checkAnswer(
          sql("SELECT * FROM src ORDER BY key"),
          sql("SELECT * from p ORDER BY key").collect().toSeq)
      }
    }
  }

  test("INSERT OVERWRITE TABLE Parquet table") {
    // Don't run with vectorized: currently relies on UnsafeRow.
    withParquetTable((1 to 10).map(i => (i, s"val_$i")), "t", false) {
      withTempPath { file =>
        sql("SELECT * FROM t LIMIT 1").write.parquet(file.getCanonicalPath)
        spark.read.parquet(file.getCanonicalPath).createOrReplaceTempView("p")
        withTempView("p") {
          // let's do three overwrites for good measure
          sql("INSERT OVERWRITE TABLE p SELECT * FROM t")
          sql("INSERT OVERWRITE TABLE p SELECT * FROM t")
          sql("INSERT OVERWRITE TABLE p SELECT * FROM t")
          checkAnswer(sql("SELECT * FROM p"), sql("SELECT * FROM t").collect().toSeq)
        }
      }
    }
  }

  test("SPARK-25206: wrong records are returned by filter pushdown " +
    "when Hive metastore schema and parquet schema are in different letter cases") {
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> true.toString) {
      withTempPath { path =>
        val data = spark.range(1, 10).toDF("id")
        data.write.parquet(path.getCanonicalPath)
        withTable("SPARK_25206") {
          sql("CREATE TABLE SPARK_25206 (ID LONG) USING parquet LOCATION " +
            s"'${path.getCanonicalPath}'")
          checkAnswer(sql("select id from SPARK_25206 where id > 0"), data)
        }
      }
    }
  }

  test("SPARK-25271: write empty map into hive parquet table") {
    import testImplicits._

    Seq(Map(1 -> "a"), Map.empty[Int, String]).toDF("m").createOrReplaceTempView("p")
    withTempView("p") {
      val targetTable = "targetTable"
      withTable(targetTable) {
        sql(s"CREATE TABLE $targetTable STORED AS PARQUET AS SELECT m FROM p")
        checkAnswer(sql(s"SELECT m FROM $targetTable"),
          Row(Map(1 -> "a")) :: Row(Map.empty[Int, String]) :: Nil)
      }
    }
  }

  test("SPARK-33323: Add query resolved check before convert hive relation") {
    withTable("t") {
      val query =
        s"""
           |CREATE TABLE t STORED AS PARQUET AS
           |SELECT * FROM (
           | SELECT c3 FROM (
           |  SELECT c1, c2 from values(1,2) t(c1, c2)
           |  )
           |)
           |""".stripMargin
      val ex = intercept[AnalysisException] {
        sql(query)
      }
      checkError(
        exception = ex,
        errorClass = "UNRESOLVED_COLUMN",
        errorSubClass = "WITH_SUGGESTION",
        sqlState = None,
        parameters = Map("objectName" -> "`c3`",
          "proposal" -> ("`__auto_generated_subquery_name`.`c1`, " +
            "`__auto_generated_subquery_name`.`c2`")),
        context = ExpectedContext(
          fragment = query.trim, start = 1, stop = 118)
       )
    }
  }

  test("SPARK-36948: Create a table with ANSI intervals using Hive external catalog") {
    val tbl = "tbl_with_ansi_intervals"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (ym INTERVAL YEAR TO MONTH, dt INTERVAL DAY TO SECOND) USING PARQUET")
      sql(
        s"""INSERT INTO $tbl VALUES (
           |  INTERVAL '1-1' YEAR TO MONTH,
           |  INTERVAL '1 02:03:04.123456' DAY TO SECOND)""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tbl"),
        Row(
          Period.ofYears(1).plusMonths(1),
          Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)
            .plus(123456, ChronoUnit.MICROS)))
    }
  }

  test("SPARK-37098: Alter table properties should invalidate cache") {
    // specify the compression in case we change it in future
    withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> "snappy") {
      withTempPath { dir =>
        withTable("t") {
          sql(s"CREATE TABLE t (c int) STORED AS PARQUET LOCATION '${dir.getCanonicalPath}'")
          // cache table metadata
          sql("SELECT * FROM t")
          sql("ALTER TABLE t SET TBLPROPERTIES('parquet.compression'='zstd')")
          sql("INSERT INTO TABLE t values(1)")
          val files1 = dir.listFiles().filter(_.getName.endsWith("zstd.parquet"))
          assert(files1.length == 1)

          // cache table metadata again
          sql("SELECT * FROM t")
          sql("ALTER TABLE t UNSET TBLPROPERTIES('parquet.compression')")
          sql("INSERT INTO TABLE t values(1)")
          val files2 = dir.listFiles().filter(_.getName.endsWith("snappy.parquet"))
          assert(files2.length == 1)
        }
      }
    }
  }
}
