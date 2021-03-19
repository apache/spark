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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.Repartition
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

/**
 * A base suite contains a set of view related test cases for different kind of views
 * Currently, the test cases in this suite should have same behavior across all kind of views
 * TODO: Combine this with [[SQLViewSuite]]
 */
abstract class SQLViewTestSuite extends QueryTest with SQLTestUtils {
  import testImplicits._

  protected def viewTypeString: String
  protected def formattedViewName(viewName: String): String

  def createView(
      viewName: String,
      sqlText: String,
      columnNames: Seq[String] = Seq.empty,
      replace: Boolean = false): String = {
    val replaceString = if (replace) "OR REPLACE" else ""
    val columnString = if (columnNames.nonEmpty) columnNames.mkString("(", ",", ")") else ""
    sql(s"CREATE $replaceString $viewTypeString $viewName $columnString AS $sqlText")
    formattedViewName(viewName)
  }

  def checkViewOutput(viewName: String, expectedAnswer: Seq[Row]): Unit = {
    checkAnswer(sql(s"SELECT * FROM $viewName"), expectedAnswer)
  }

  test("change SQLConf should not change view behavior - caseSensitiveAnalysis") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName = createView("v1", "SELECT c1 FROM t", Seq("C1"))
      withView(viewName) {
        Seq("true", "false").foreach { flag =>
          withSQLConf(CASE_SENSITIVE.key -> flag) {
            checkViewOutput(viewName, Seq(Row(2), Row(3), Row(1)))
          }
        }
      }
    }
  }

  test("change SQLConf should not change view behavior - orderByOrdinal") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName = createView("v1", "SELECT c1 FROM t ORDER BY 1 ASC, c1 DESC", Seq("c1"))
      withView(viewName) {
        Seq("true", "false").foreach { flag =>
          withSQLConf(ORDER_BY_ORDINAL.key -> flag) {
            checkViewOutput(viewName, Seq(Row(1), Row(2), Row(3)))
          }
        }
      }
    }
  }

  test("change SQLConf should not change view behavior - groupByOrdinal") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName = createView("v1", "SELECT c1, count(c1) FROM t GROUP BY 1", Seq("c1", "count"))
      withView(viewName) {
        Seq("true", "false").foreach { flag =>
          withSQLConf(GROUP_BY_ORDINAL.key -> flag) {
            checkViewOutput(viewName, Seq(Row(1, 1), Row(2, 1), Row(3, 1)))
          }
        }
      }
    }
  }

  test("change SQLConf should not change view behavior - groupByAliases") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName = createView(
        "v1", "SELECT c1 as a, count(c1) FROM t GROUP BY a", Seq("a", "count"))
      withView(viewName) {
        Seq("true", "false").foreach { flag =>
          withSQLConf(GROUP_BY_ALIASES.key -> flag) {
            checkViewOutput(viewName, Seq(Row(1, 1), Row(2, 1), Row(3, 1)))
          }
        }
      }
    }
  }

  test("change SQLConf should not change view behavior - ansiEnabled") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName = createView("v1", "SELECT 1/0", Seq("c1"))
      withView(viewName) {
        Seq("true", "false").foreach { flag =>
          withSQLConf(ANSI_ENABLED.key -> flag) {
            checkViewOutput(viewName, Seq(Row(null)))
          }
        }
      }
    }
  }

  test("change current database should not change view behavior") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName = createView("v1", "SELECT * FROM t")
      withView(viewName) {
        withTempDatabase { db =>
          sql(s"USE $db")
          Seq(4, 5, 6).toDF("c1").write.format("parquet").saveAsTable("t")
          checkViewOutput(viewName, Seq(Row(2), Row(3), Row(1)))
        }
      }
    }
  }

  test("view should read the new data if table is updated") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName = createView("v1", "SELECT c1 FROM t", Seq("c1"))
      withView(viewName) {
        Seq(9, 7, 8).toDF("c1").write.mode("overwrite").format("parquet").saveAsTable("t")
        checkViewOutput(viewName, Seq(Row(9), Row(7), Row(8)))
      }
    }
  }

  test("add column for table should not affect view output") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName = createView("v1", "SELECT * FROM t")
      withView(viewName) {
        sql("ALTER TABLE t ADD COLUMN (c2 INT)")
        checkViewOutput(viewName, Seq(Row(2), Row(3), Row(1)))
      }
    }
  }

  test("check cyclic view reference on CREATE OR REPLACE VIEW") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName1 = createView("v1", "SELECT * FROM t")
      val viewName2 = createView("v2", s"SELECT * FROM $viewName1")
      withView(viewName2, viewName1) {
        val e = intercept[AnalysisException] {
          createView("v1", s"SELECT * FROM $viewName2", replace = true)
        }.getMessage
        assert(e.contains("Recursive view"))
      }
    }
  }

  test("check cyclic view reference on ALTER VIEW") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName1 = createView("v1", "SELECT * FROM t")
      val viewName2 = createView("v2", s"SELECT * FROM $viewName1")
      withView(viewName2, viewName1) {
        val e = intercept[AnalysisException] {
          sql(s"ALTER VIEW $viewName1 AS SELECT * FROM $viewName2")
        }.getMessage
        assert(e.contains("Recursive view"))
      }
    }
  }

  test("restrict the nested level of a view") {
    val viewNames = scala.collection.mutable.ArrayBuffer.empty[String]
    val view0 = createView("view0", "SELECT 1")
    viewNames += view0
    for (i <- 1 to 10) {
      viewNames += createView(s"view$i", s"SELECT * FROM ${viewNames.last}")
    }
    withView(viewNames.reverse.toSeq: _*) {
      withSQLConf(MAX_NESTED_VIEW_DEPTH.key -> "10") {
        val e = intercept[AnalysisException] {
          sql(s"SELECT * FROM ${viewNames.last}")
        }.getMessage
        assert(e.contains("exceeds the maximum view resolution depth (10)"))
        assert(e.contains(s"Increase the value of ${MAX_NESTED_VIEW_DEPTH.key}"))
      }
    }
  }

  test("view should use captured catalog and namespace to resolve relation") {
    withTempDatabase { dbName =>
      withTable("default.t", s"$dbName.t") {
        withTempView("t") {
          // create a table in default database
          sql("USE DEFAULT")
          Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
          // create a view refer the created table in default database
          val viewName = createView("v1", "SELECT * FROM t")
          // using another database to create a table with same name
          sql(s"USE $dbName")
          Seq(4, 5, 6).toDF("c1").write.format("parquet").saveAsTable("t")
          // create a temporary view with the same name
          sql("CREATE TEMPORARY VIEW t AS SELECT 1")
          withView(viewName) {
            // view v1 should still refer the table defined in `default` database
            checkViewOutput(viewName, Seq(Row(2), Row(3), Row(1)))
          }
        }
      }
    }
  }

  test("SPARK-33692: view should use captured catalog and namespace to lookup function") {
    val avgFuncClass = "test.org.apache.spark.sql.MyDoubleAvg"
    val sumFuncClass = "test.org.apache.spark.sql.MyDoubleSum"
    val functionName = "test_udf"
    withTempDatabase { dbName =>
      withUserDefinedFunction(
        s"default.$functionName" -> false,
        s"$dbName.$functionName" -> false,
        functionName -> true) {
        // create a function in default database
        sql("USE DEFAULT")
        sql(s"CREATE FUNCTION $functionName AS '$avgFuncClass'")
        // create a view using a function in 'default' database
        val viewName = createView("v1", s"SELECT $functionName(col1) FROM VALUES (1), (2), (3)")
        // create function in another database with the same function name
        sql(s"USE $dbName")
        sql(s"CREATE FUNCTION $functionName AS '$sumFuncClass'")
        // create temporary function with the same function name
        sql(s"CREATE TEMPORARY FUNCTION $functionName AS '$sumFuncClass'")
        withView(viewName) {
          // view v1 should still using function defined in `default` database
          checkViewOutput(viewName, Seq(Row(102.0)))
        }
      }
    }
  }

  test("SPARK-34260: replace existing view using CREATE OR REPLACE") {
    val viewName = createView("testView", "SELECT * FROM (SELECT 1)")
    withView(viewName) {
      checkViewOutput(viewName, Seq(Row(1)))
      createView("testView", "SELECT * FROM (SELECT 2)", replace = true)
      checkViewOutput(viewName, Seq(Row(2)))
    }
  }

  test("SPARK-34490 - query should fail if the view refers a dropped table") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName = createView("testView", "SELECT * FROM t")
      withView(viewName) {
        // Always create a temp view in this case, not use `createView` on purpose
        sql("CREATE TEMP VIEW t AS SELECT 1 AS c1")
        withTempView("t") {
          checkViewOutput(viewName, Seq(Row(2), Row(3), Row(1)))
          // Manually drop table `t` to see if the query will fail
          sql("DROP TABLE IF EXISTS default.t")
          val e = intercept[AnalysisException] {
            sql(s"SELECT * FROM $viewName").collect()
          }.getMessage
          assert(e.contains("Table or view not found: t"))
        }
      }
    }
  }

  test("SPARK-34613: Fix view does not capture disable hint config") {
    withSQLConf(DISABLE_HINTS.key -> "true") {
      val viewName = createView("v1", "SELECT /*+ repartition(1) */ 1")
      withView(viewName) {
        assert(
          sql(s"SELECT * FROM $viewName").queryExecution.analyzed.collect {
            case e: Repartition => e
          }.isEmpty
        )
        checkViewOutput(viewName, Seq(Row(1)))
      }
    }
  }

  test("SPARK-34504: drop an invalid view") {
    withTable("t") {
      sql("CREATE TABLE t(s STRUCT<i: INT, j: INT>) USING json")
      val viewName = createView("v", "SELECT s.i FROM t")
      withView(viewName) {
        assert(spark.table(viewName).collect().isEmpty)

        // re-create the table without nested field `i` which is referred by the view.
        sql("DROP TABLE t")
        sql("CREATE TABLE t(s STRUCT<j: INT>) USING json")
        val e = intercept[AnalysisException](spark.table(viewName))
        assert(e.message.contains("No such struct field i in j"))

        // drop invalid view should be fine
        sql(s"DROP VIEW $viewName")
      }
    }
  }

  test("SPARK-34719: view query with duplicated output column names") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(CASE_SENSITIVE.key -> caseSensitive.toString) {
        withView("v1", "v2") {
          sql("CREATE VIEW v1 AS SELECT 1 a, 2 b")
          sql("CREATE VIEW v2 AS SELECT 1 col")

          val viewName = createView(
            viewName = "testView",
            sqlText = "SELECT *, 1 col, 2 col FROM v1",
            columnNames = Seq("c1", "c2", "c3", "c4"))
          withView(viewName) {
            checkViewOutput(viewName, Seq(Row(1, 2, 1, 2)))

            // One more duplicated column `COL` if caseSensitive=false.
            sql("CREATE OR REPLACE VIEW v1 AS SELECT 1 a, 2 b, 3 COL")
            if (caseSensitive) {
              checkViewOutput(viewName, Seq(Row(1, 2, 1, 2)))
            } else {
              val e = intercept[AnalysisException](spark.table(viewName).collect())
              assert(e.message.contains("incompatible schema change"))
            }
          }

          // v1 has 3 columns [a, b, COL], v2 has one column [col], so `testView2` has duplicated
          // output column names if caseSensitive=false.
          val viewName2 = createView(
            viewName = "testView2",
            sqlText = "SELECT * FROM v1, v2",
            columnNames = Seq("c1", "c2", "c3", "c4"))
          withView(viewName2) {
            checkViewOutput(viewName2, Seq(Row(1, 2, 3, 1)))

            // One less duplicated column if caseSensitive=false.
            sql("CREATE OR REPLACE VIEW v1 AS SELECT 1 a, 2 b")
            if (caseSensitive) {
              val e = intercept[AnalysisException](spark.table(viewName2).collect())
              assert(e.message.contains("'COL' is not found in '(a,b,col)'"))
            } else {
              val e = intercept[AnalysisException](spark.table(viewName2).collect())
              assert(e.message.contains("incompatible schema change"))
            }
          }
        }
      }
    }
  }
}

class LocalTempViewTestSuite extends SQLViewTestSuite with SharedSparkSession {
  override protected def viewTypeString: String = "TEMPORARY VIEW"
  override protected def formattedViewName(viewName: String): String = viewName
}

class GlobalTempViewTestSuite extends SQLViewTestSuite with SharedSparkSession {
  override protected def viewTypeString: String = "GLOBAL TEMPORARY VIEW"
  override protected def formattedViewName(viewName: String): String = {
    val globalTempDB = spark.sharedState.globalTempViewManager.database
    s"$globalTempDB.$viewName"
  }
}

class PersistedViewTestSuite extends SQLViewTestSuite with SharedSparkSession {
  override protected def viewTypeString: String = "VIEW"
  override protected def formattedViewName(viewName: String): String = s"default.$viewName"
}
