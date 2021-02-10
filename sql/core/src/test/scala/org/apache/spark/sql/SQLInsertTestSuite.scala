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
import org.apache.spark.sql.connector.InMemoryPartitionTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

/**
 * The base trait for DML - insert syntax
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
      assert(e1.getMessage === "Found duplicate column(s) in the column list: `c2`")
    }
  }

  test("insert with column list - invalid columns") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      createTable("t1", cols, Seq("int", "long", "string"))
      val e1 = intercept[AnalysisException](sql(s"INSERT INTO t1 (c1, c2, c4) values(1, 2, 3)"))
      assert(e1.getMessage === "Cannot resolve column name c4")
    }
  }

  test("insert with column list - mismatched column list size") {
    val msg = "Cannot write to table due to mismatched user specified column size"
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      createTable("t1", cols, Seq("int", "long", "string"))
      val e1 = intercept[AnalysisException](sql(s"INSERT INTO t1 (c1, c2) values(1, 2, 3)"))
      assert(e1.getMessage.contains(msg))
      val e2 = intercept[AnalysisException](sql(s"INSERT INTO t1 (c1, c2, c3) values(1, 2)"))
      assert(e2.getMessage.contains(msg))
    }
  }

  test("insert with column list - mismatched target table out size after rewritten query") {
    val v2Msg = "Cannot write to 'testcat.t1', not enough data columns:"
    val cols = Seq("c1", "c2", "c3", "c4")

    withTable("t1") {
      createTable("t1", cols, Seq.fill(4)("int"))
      val e1 = intercept[AnalysisException](sql(s"INSERT INTO t1 (c1) values(1)"))
      assert(e1.getMessage.contains("target table has 4 column(s) but the inserted data has 1") ||
        e1.getMessage.contains(v2Msg))
    }

    withTable("t1") {
      createTable("t1", cols, Seq.fill(4)("int"), cols.takeRight(2))
      val e1 = intercept[AnalysisException] {
        sql(s"INSERT INTO t1 partition(c3=3, c4=4) (c1) values(1)")
      }
      assert(e1.getMessage.contains("target table has 4 column(s) but the inserted data has 3") ||
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
