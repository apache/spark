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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

/**
 * A base suite contains a set of view related test cases for different kind of views
 * Currently, the test cases in this suite should have same behavior across all kind of views
 * TODO: Combine this with [[SQLViewSuite]]
 */
abstract class SQLViewTestSuite extends QueryTest with SQLTestUtils {
  import testImplicits._

  def testView(viewName: String, columnNames: Seq[String], sqlText: String)(f: => Unit): Unit

  def formatViewName(viewName: String): String = viewName

  test("change SQLConf should not change view behavior - caseSensitiveAnalysis") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      testView("v1", Seq("c1"), "SELECT C1 FROM t") {
        withSQLConf(CASE_SENSITIVE.key -> "true") {
          checkAnswer(
            sql(s"SELECT * FROM ${formatViewName("v1")}"),
            Seq(Row(2), Row(3), Row(1)))
        }
      }
    }
  }

  test("change SQLConf should not change view behavior - orderByOrdinal") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      testView("v1", Seq("c1"), "SELECT c1 FROM t ORDER BY 1 ASC, c1 DESC") {
        withSQLConf(ORDER_BY_ORDINAL.key -> "false") {
          checkAnswer(
            sql(s"SELECT * FROM ${formatViewName("v1")}"),
            Seq(Row(1), Row(2), Row(3)))
        }
      }
    }
  }

  test("change SQLConf should not change view behavior - groupByOrdinal") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      testView("v1", Seq("c1", "count"), "SELECT c1, count(c1) FROM t GROUP BY 1") {
        withSQLConf(GROUP_BY_ORDINAL.key -> "false") {
          checkAnswer(
            sql(s"SELECT * FROM ${formatViewName("v1")}"),
            Seq(Row(1, 1), Row(2, 1), Row(3, 1)))
        }
      }
    }
  }

  test("change SQLConf should not change view behavior - groupByAliases") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      testView("v1", Seq("a", "count"), "SELECT c1 as a, count(c1) FROM t GROUP BY a") {
        withSQLConf(GROUP_BY_ALIASES.key -> "false") {
          checkAnswer(
            sql(s"SELECT * FROM ${formatViewName("v1")}"),
            Seq(Row(1, 1), Row(2, 1), Row(3, 1)))
        }
      }
    }
  }

  test("change SQLConf should not change view behavior - ansiEnabled") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      testView("v1", Seq("c1"), "SELECT 1/0") {
        withSQLConf(ANSI_ENABLED.key -> "true") {
          checkAnswer(
            sql(s"SELECT * FROM ${formatViewName("v1")}"),
            Seq(Row(null)))
        }
      }
    }
  }

  test("change current database should not change view behavior") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      testView("v1", Seq.empty, "SELECT * from t") {
        withTempDatabase { db =>
          sql(s"USE $db")
          Seq(4, 5, 6).toDF("c1").write.format("parquet").saveAsTable("t")
          checkAnswer(
            sql(s"SELECT * FROM ${formatViewName("v1")}"),
            Seq(Row(2), Row(3), Row(1)))
        }
      }
    }

  }

  test("view should read the new data if table is updated") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      testView("v1", Seq("c1"), "SELECT c1 FROM t") {
        Seq(9, 7, 8).toDF("c1").write.mode("overwrite").format("parquet").saveAsTable("t")
        checkAnswer(sql("SELECT * FROM v1"), Seq(Row(9), Row(7), Row(8)))
      }
    }
  }

  test("add column for table should not affect view output") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      testView("v1", Seq.empty, "SELECT * FROM t") {
        sql("ALTER TABLE t ADD COLUMN (c2 INT)")
        checkAnswer(sql("SELECT * FROM v1"), Seq(Row(2), Row(3), Row(1)))
      }
    }
  }
}

class LocalTempViewTestSuite extends SQLViewTestSuite with SharedSparkSession {
  override def testView(
      viewName: String, columnNames: Seq[String], sqlText: String)(f: => Unit): Unit = {
    withTempView(viewName) {
      if (columnNames.isEmpty) {
        sql(s"CREATE TEMPORARY VIEW $viewName AS $sqlText")
      } else {
        sql(s"CREATE TEMPORARY VIEW $viewName ${columnNames.mkString("(", ",", ")")} AS $sqlText")
      }
      f
    }
  }
}

class GlobalTempViewTestSuite extends SQLViewTestSuite with SharedSparkSession {
  override def testView(
      viewName: String, columnNames: Seq[String], sqlText: String)(f: => Unit): Unit = {
    withGlobalTempView(viewName) {
      if (columnNames.isEmpty) {
        sql(s"CREATE GLOBAL TEMPORARY VIEW $viewName AS $sqlText")
      } else {
        sql(s"CREATE GLOBAL TEMPORARY VIEW $viewName " +
          s"${columnNames.mkString("(", ",", ")")} AS $sqlText")
      }
      f
    }
  }

  override def formatViewName(viewName: String): String = {
    val globalTempDB = spark.sharedState.globalTempViewManager.database
    s"$globalTempDB.$viewName"
  }
}

class PersistedViewTestSuite extends SQLViewTestSuite with SharedSparkSession {
  override def testView(
      viewName: String, columnNames: Seq[String], sqlText: String)(f: => Unit): Unit = {
    withView(viewName) {
      if (columnNames.isEmpty) {
        sql(s"CREATE VIEW $viewName AS $sqlText")
      } else {
        sql(s"CREATE VIEW $viewName ${columnNames.mkString("(", ",", ")")} AS $sqlText")
      }
      f
    }
  }
}
