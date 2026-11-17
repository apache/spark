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

package org.apache.spark.sql.execution.datasources.v2.adbc

import org.apache.spark.{SparkException, SparkIllegalArgumentException}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class AdbcWriteSuite extends QueryTest with SharedSparkSession with AdbcTestBase {
  import testImplicits._

  private def readTable(table: String): DataFrame = spark.read.format("adbc")
    .option("driver", jdbcBridge)
    .option("uri", url)
    .option("dbtable", table)
    .load()

  // Writes with a driver that quotes the table name of an ingestion, like native drivers.
  private def write(df: DataFrame, mode: String, options: (String, String)*): Unit =
    df.write.format("adbc")
      .option("driver", classOf[H2DriverFactory].getName)
      .option("uri", url)
      .options(options.toMap)
      .mode(mode)
      .save()

  private def withH2Table(name: String, columns: String)(f: String => Unit): Unit = {
    execJdbc(s"""CREATE TABLE "$name" ($columns)""")
    try f(name) finally execJdbc(s"""DROP TABLE "$name"""")
  }

  test("append rows in batches") {
    withH2Table("target", """"id" INTEGER, "name" VARCHAR(16)""") { table =>
      val rows = (1 to 25).map(i => (i, s"n$i"))
      write(rows.toDF("id", "name").repartition(3), "append",
        "dbtable" -> table, "batchsize" -> "4")
      write(Seq((26, null: String)).toDF("id", "name"), "append", "dbtable" -> table)
      checkAnswer(readTable(table), (rows :+ ((26, null))).map(Row.fromTuple))
    }
  }

  test("write each supported type") {
    // Not DATE: the JDBC bridge driver, which the test driver wraps, binds a date as midnight
    // UTC, which a JDBC driver in another time zone stores as the day before.
    withH2Table("types", """"id" INTEGER, "name" VARCHAR(32), "score" DOUBLE,
        |"active" BOOLEAN, "balance" DECIMAL(10, 2), "big" BIGINT""".stripMargin) { table =>
      val rows = Seq(
        Row(1, "alice", 1.5, true, BigDecimal("10.25"), 10000000000L),
        Row(2, null, null, null, null, null))
      val schema = readTable(table).schema
      write(spark.createDataFrame(spark.sparkContext.parallelize(rows), schema), "append",
        "dbtable" -> table)
      checkAnswer(readTable(table), rows)
    }
  }

  test("overwrite deletes the existing rows") {
    withH2Table("target", """"id" INTEGER""") { table =>
      write(Seq(1, 2).toDF("id"), "append", "dbtable" -> table)
      write(Seq(3).toDF("id"), "overwrite", "dbtable" -> table)
      checkAnswer(readTable(table), Row(3))
    }
  }

  test("a failed task rolls back the rows it wrote") {
    withH2Table("target", """"id" INTEGER PRIMARY KEY""") { table =>
      // The third row violates the primary key, after two batches were sent.
      val df = Seq(1, 2, 1).toDF("id").coalesce(1)
      val e = intercept[SparkException] {
        write(df, "append", "dbtable" -> table, "batchsize" -> "1")
      }
      assert(e.getMessage.contains("PRIMARY KEY"))
      checkAnswer(readTable(table), Seq.empty)
    }
  }

  test("write to a table in another schema, with a name that needs quoting") {
    execJdbc("""CREATE SCHEMA "other"""", """CREATE TABLE "other"."Mixed.Case" ("id" INTEGER)""")
    try {
      write(Seq(1, 2).toDF("id"), "append", "dbschema" -> "other", "dbtable" -> "Mixed.Case")
      write(Seq(3).toDF("id"), "overwrite", "dbschema" -> "other", "dbtable" -> "Mixed.Case")
      def readBack(): DataFrame = spark.read.format("adbc").option("driver", jdbcBridge)
        .option("uri", url).option("dbschema", "other").option("dbtable", "Mixed.Case").load()
      checkAnswer(readBack(), Row(3))
      // A qualified name is a single identifier, so it fails before any row is deleted.
      intercept[Exception] {
        write(Seq(4).toDF("id"), "overwrite", "dbtable" -> "other.Mixed.Case")
      }
      checkAnswer(readBack(), Row(3))
    } finally {
      execJdbc("""DROP SCHEMA "other" CASCADE""")
    }
  }

  test("the JDBC bridge driver writes to tables whose names need no quoting") {
    // It puts the table name into its SQL unchanged, so H2 folds it to upper case.
    execJdbc("""CREATE TABLE "UPPER" ("id" INTEGER)""")
    try {
      Seq(1, 2).toDF("id").write.format("adbc").option("driver", jdbcBridge)
        .option("uri", url).option("dbtable", "UPPER").mode("append").save()
      checkAnswer(readTable("UPPER"), Seq(Row(1), Row(2)))
    } finally {
      execJdbc("""DROP TABLE "UPPER"""")
    }
  }

  test("only append and overwrite are supported") {
    withH2Table("target", """"id" INTEGER""") { table =>
      Seq("errorifexists" -> "ErrorIfExists", "ignore" -> "Ignore").foreach {
        case (mode, expected) =>
          checkError(
            exception = intercept[AnalysisException] {
              write(Seq(1).toDF("id"), mode, "dbtable" -> table)
            },
            condition = "UNSUPPORTED_DATA_SOURCE_SAVE_MODE",
            parameters = Map("source" -> "adbc", "createMode" -> s""""$expected""""))
      }
    }
  }

  test("a query can't be written to") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        write(Seq(1).toDF("id"), "append", "query" -> "SELECT 1 AS \"id\"")
      },
      condition = "INVALID_ADBC_OPTIONS.WRITE_WITH_QUERY",
      parameters = Map("queryOption" -> "\"query\"", "tableOption" -> "\"dbtable\""))
  }

  test("batchsize is validated") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new AdbcOptions(Map("driver" -> jdbcBridge, "dbtable" -> "t", "batchsize" -> "0"))
      },
      condition = "INVALID_ADBC_OPTIONS.NOT_A_POSITIVE_INTEGER",
      parameters = Map("option" -> "\"batchsize\"", "value" -> "'0'"))
  }

  test("columns are written by name, and cast to the table's types") {
    withH2Table("target", """"id" BIGINT, "name" VARCHAR(16)""") { table =>
      write(Seq(("a", 1)).toDF("name", "id"), "append", "dbtable" -> table)
      checkAnswer(readTable(table), Row(1L, "a"))
      val e = intercept[AnalysisException] {
        write(Seq(1).toDF("missing"), "append", "dbtable" -> table)
      }
      assert(e.getCondition.startsWith("INCOMPATIBLE_DATA_FOR_TABLE"))
    }
  }
}
