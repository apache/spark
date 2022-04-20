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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Hex
import org.apache.spark.sql.connector.catalog.InMemoryPartitionTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.unsafe.types.UTF8String

/**
 * The base trait for SQL INSERT.
 */
trait SQLInsertTestSuite extends QueryTest with SQLTestUtils {

  import testImplicits._

  def format: String

  protected def createTable(
      table: String,
      cols: Seq[String],
      colTypes: Seq[String],
      partCols: Seq[String] = Nil): Unit = {
    val values = cols.zip(colTypes).map(tuple => tuple._1 + " " + tuple._2).mkString("(", ", ", ")")
    val partitionSpec = if (partCols.nonEmpty) {
      partCols.mkString("PARTITIONED BY (", ",", ")")
    } else ""
    sql(s"CREATE TABLE $table$values USING $format $partitionSpec")
  }

  protected def processInsert(
      tableName: String,
      input: DataFrame,
      cols: Seq[String] = Nil,
      partitionExprs: Seq[String] = Nil,
      overwrite: Boolean): Unit = {
    val tmpView = "tmp_view"
    val columnList = if (cols.nonEmpty) cols.mkString("(", ",", ")") else ""
    val partitionList = if (partitionExprs.nonEmpty) {
      partitionExprs.mkString("PARTITION (", ",", ")")
    } else ""
    withTempView(tmpView) {
      input.createOrReplaceTempView(tmpView)
      val overwriteStr = if (overwrite) "OVERWRITE" else "INTO"
      sql(
        s"INSERT $overwriteStr TABLE $tableName $partitionList $columnList SELECT * FROM $tmpView")
    }
  }

  protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  test("insert with column list - follow table output order") {
    withTable("t1") {
      val df = Seq((1, 2L, "3")).toDF()
      val cols = Seq("c1", "c2", "c3")
      createTable("t1", cols, Seq("int", "long", "string"))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols, overwrite = m)
        verifyTable("t1", df)
      }
    }
  }

  test("insert with column list - follow table output order + partitioned table") {
    val cols = Seq("c1", "c2", "c3", "c4")
    val df = Seq((1, 2, 3, 4)).toDF(cols: _*)
    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols, overwrite = m)
        verifyTable("t1", df)
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert(
          "t1", df.selectExpr("c1", "c2"), cols.take(2), Seq("c3=3", "c4=4"), overwrite = m)
        verifyTable("t1", df)
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1", df.selectExpr("c1", "c2", "c4"),
          cols.filterNot(_ == "c3"), Seq("c3=3", "c4"), overwrite = m)
        verifyTable("t1", df)
      }
    }
  }

  test("insert with column list - table output reorder") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      val df = Seq((1, 2, 3)).toDF(cols: _*)
      createTable("t1", cols, Seq("int", "int", "int"))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols.reverse, overwrite = m)
        verifyTable("t1", df.selectExpr(cols.reverse: _*))
      }
    }
  }

  test("insert with column list - table output reorder + partitioned table") {
    val cols = Seq("c1", "c2", "c3", "c4")
    val df = Seq((1, 2, 3, 4)).toDF(cols: _*)
    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols.reverse, overwrite = m)
        verifyTable("t1", df.selectExpr(cols.reverse: _*))
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert(
          "t1", df.selectExpr("c1", "c2"), cols.take(2).reverse, Seq("c3=3", "c4=4"), overwrite = m)
        verifyTable("t1", df.selectExpr("c2", "c1", "c3", "c4"))
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1",
          df.selectExpr("c1", "c2", "c4"), Seq("c4", "c2", "c1"), Seq("c3=3", "c4"), overwrite = m)
        verifyTable("t1", df.selectExpr("c4", "c2", "c3", "c1"))
      }
    }
  }

  test("insert with column list - duplicated columns") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      createTable("t1", cols, Seq("int", "long", "string"))
      val e1 = intercept[AnalysisException](sql(s"INSERT INTO t1 (c1, c2, c2) values(1, 2, 3)"))
      assert(e1.getMessage.contains("Found duplicate column(s) in the column list: `c2`"))
    }
  }

  test("insert with column list - invalid columns") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      createTable("t1", cols, Seq("int", "long", "string"))
      val e1 = intercept[AnalysisException](sql(s"INSERT INTO t1 (c1, c2, c4) values(1, 2, 3)"))
      assert(e1.getMessage.contains("Cannot resolve column name c4"))
    }
  }

  test("insert with column list - mismatched column list size") {
    val msgs = Seq("Cannot write to table due to mismatched user specified column size",
      "expected 3 columns but found")
    def test: Unit = {
      withTable("t1") {
        val cols = Seq("c1", "c2", "c3")
        createTable("t1", cols, Seq("int", "long", "string"))
        val e1 = intercept[AnalysisException](sql(s"INSERT INTO t1 (c1, c2) values(1, 2, 3)"))
        assert(e1.getMessage.contains(msgs(0)) || e1.getMessage.contains(msgs(1)))
        val e2 = intercept[AnalysisException](sql(s"INSERT INTO t1 (c1, c2, c3) values(1, 2)"))
        assert(e2.getMessage.contains(msgs(0)) || e2.getMessage.contains(msgs(1)))
      }
    }
    withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "false") {
      test
    }
    withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true") {
      test
    }
  }

  test("insert with column list - mismatched target table out size after rewritten query") {
    val v2Msg = "expected 2 columns but found"
    val cols = Seq("c1", "c2", "c3", "c4")

    withTable("t1") {
      createTable("t1", cols, Seq.fill(4)("int"))
      val e1 = intercept[AnalysisException](sql(s"INSERT INTO t1 (c1) values(1)"))
      assert(e1.getMessage.contains("target table has 4 column(s) but the inserted data has 1") ||
        e1.getMessage.contains("expected 4 columns but found 1") ||
        e1.getMessage.contains("not enough data columns") ||
        e1.getMessage.contains(v2Msg))
    }

    withTable("t1") {
      createTable("t1", cols, Seq.fill(4)("int"), cols.takeRight(2))
      val e1 = intercept[AnalysisException] {
        sql(s"INSERT INTO t1 partition(c3=3, c4=4) (c1) values(1)")
      }
      assert(e1.getMessage.contains("target table has 4 column(s) but the inserted data has 3") ||
        e1.getMessage.contains("not enough data columns") ||
        e1.getMessage.contains(v2Msg))
    }
  }

  test("SPARK-34223: static partition with null raise NPE") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c string) USING PARQUET PARTITIONED BY (c)")
      sql("INSERT OVERWRITE t PARTITION (c=null) VALUES ('1')")
      checkAnswer(spark.table("t"), Row("1", null))
    }
  }

  test("SPARK-33474: Support typed literals as partition spec values") {
    withTable("t1") {
      val binaryStr = "Spark SQL"
      val binaryHexStr = Hex.hex(UTF8String.fromString(binaryStr).getBytes).toString
      sql(
        """
          | CREATE TABLE t1(name STRING, part1 DATE, part2 TIMESTAMP, part3 BINARY,
          |  part4 STRING, part5 STRING, part6 STRING, part7 STRING)
          | USING PARQUET PARTITIONED BY (part1, part2, part3, part4, part5, part6, part7)
         """.stripMargin)

      sql(
        s"""
           | INSERT OVERWRITE t1 PARTITION(
           | part1 = date'2019-01-01',
           | part2 = timestamp'2019-01-01 11:11:11',
           | part3 = X'$binaryHexStr',
           | part4 = 'p1',
           | part5 = date'2019-01-01',
           | part6 = timestamp'2019-01-01 11:11:11',
           | part7 = X'$binaryHexStr'
           | ) VALUES('a')
        """.stripMargin)
      checkAnswer(sql(
        """
          | SELECT
          |   name,
          |   CAST(part1 AS STRING),
          |   CAST(part2 as STRING),
          |   CAST(part3 as STRING),
          |   part4,
          |   part5,
          |   part6,
          |   part7
          | FROM t1
        """.stripMargin),
        Row("a", "2019-01-01", "2019-01-01 11:11:11", "Spark SQL", "p1",
          "2019-01-01", "2019-01-01 11:11:11", "Spark SQL"))

      val e = intercept[AnalysisException] {
        sql("CREATE TABLE t2(name STRING, part INTERVAL) USING PARQUET PARTITIONED BY (part)")
      }.getMessage
      assert(e.contains("Cannot use interval"))
    }
  }

  test("SPARK-34556: " +
    "checking duplicate static partition columns should respect case sensitive conf") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c string) USING PARQUET PARTITIONED BY (c)")
      val e = intercept[AnalysisException] {
        sql("INSERT OVERWRITE t PARTITION (c='2', C='3') VALUES (1)")
      }
      assert(e.getMessage.contains("Found duplicate keys `c`"))
    }
    // The following code is skipped for Hive because columns stored in Hive Metastore is always
    // case insensitive and we cannot create such table in Hive Metastore.
    if (!format.startsWith("hive")) {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        withTable("t") {
          sql(s"CREATE TABLE t(i int, c string, C string) USING PARQUET PARTITIONED BY (c, C)")
          sql("INSERT OVERWRITE t PARTITION (c='2', C='3') VALUES (1)")
          checkAnswer(spark.table("t"), Row(1, "2", "3"))
        }
      }
    }
  }

  test("SPARK-30844: static partition should also follow StoreAssignmentPolicy") {
    val testingPolicies = if (format == "foo") {
      // DS v2 doesn't support the legacy policy
      Seq(SQLConf.StoreAssignmentPolicy.ANSI, SQLConf.StoreAssignmentPolicy.STRICT)
    } else {
      SQLConf.StoreAssignmentPolicy.values
    }

    def shouldThrowException(policy: SQLConf.StoreAssignmentPolicy.Value): Boolean = policy match {
      case SQLConf.StoreAssignmentPolicy.ANSI | SQLConf.StoreAssignmentPolicy.STRICT =>
        true
      case SQLConf.StoreAssignmentPolicy.LEGACY =>
        false
    }

    testingPolicies.foreach { policy =>
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> policy.toString) {
        withTable("t") {
          sql("create table t(a int, b string) using parquet partitioned by (a)")
          if (shouldThrowException(policy)) {
            val errorMsg = intercept[NumberFormatException] {
              sql("insert into t partition(a='ansi') values('ansi')")
            }.getMessage
            assert(errorMsg.contains("Invalid input syntax for type INT: 'ansi'"))
          } else {
            sql("insert into t partition(a='ansi') values('ansi')")
            checkAnswer(sql("select * from t"), Row("ansi", null) :: Nil)
          }
        }
      }
    }
  }

  test("SPARK-38228: legacy store assignment should not fail on error under ANSI mode") {
    // DS v2 doesn't support the legacy policy
    if (format != "foo") {
      Seq(true, false).foreach { ansiEnabled =>
        withSQLConf(
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.LEGACY.toString,
          SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
          withTable("t") {
            sql("create table t(a int) using parquet")
            sql("insert into t values('ansi')")
            checkAnswer(spark.table("t"), Row(null))
          }
        }
      }
    }
  }
}

class FileSourceSQLInsertTestSuite extends SQLInsertTestSuite with SharedSparkSession {
  override def format: String = "parquet"
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, format)
  }
}

class DSV2SQLInsertTestSuite extends SQLInsertTestSuite with SharedSparkSession {

  override def format: String = "foo"

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.testcat", classOf[InMemoryPartitionTableCatalog].getName)
      .set(SQLConf.DEFAULT_CATALOG.key, "testcat")
  }
}
