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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.Repartition
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.ArrayImplicits._

/**
 * A base suite contains a set of view related test cases for different kind of views
 * Currently, the test cases in this suite should have same behavior across all kind of views
 * TODO: Combine this with [[SQLViewSuite]]
 */
abstract class SQLViewTestSuite extends QueryTest with SQLTestUtils {
  import testImplicits._

  protected def viewTypeString: String
  protected def formattedViewName(viewName: String): String
  protected def tableIdentifier(viewName: String): TableIdentifier

  def createView(
      viewName: String,
      sqlText: String,
      columnNames: Seq[String] = Seq.empty,
      others: Seq[String] = Seq.empty,
      replace: Boolean = false): String = {
    val replaceString = if (replace) "OR REPLACE" else ""
    val columnString = if (columnNames.nonEmpty) columnNames.mkString("(", ",", ")") else ""
    val othersString = if (others.nonEmpty) others.mkString(" ") else ""
    sql(s"CREATE $replaceString $viewTypeString $viewName $columnString $othersString AS $sqlText")
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
      val viewName =
        createView("v1", "SELECT c1, count(c1) AS cnt FROM t GROUP BY 1", Seq("c1", "count"))
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
        "v1", "SELECT c1 as a, count(c1) AS cnt FROM t GROUP BY a", Seq("a", "count"))
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
      withSQLConf(ANSI_ENABLED.key -> "false") {
        val viewName = createView("v1", "SELECT 1/0 AS invalid", Seq("c1"))
        withView(viewName) {
          Seq("true", "false").foreach { flag =>
            withSQLConf(ANSI_ENABLED.key -> flag) {
              checkViewOutput(viewName, Seq(Row(null)))
            }
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
        checkError(
          exception = intercept[AnalysisException] {
            createView("v1", s"SELECT * FROM $viewName2", replace = true)
          },
          errorClass = "RECURSIVE_VIEW",
          parameters = Map(
            "viewIdent" -> tableIdentifier("v1").quotedString,
            "newPath" -> (s"${tableIdentifier("v1").quotedString} " +
              s"-> ${tableIdentifier("v2").quotedString} " +
              s"-> ${tableIdentifier("v1").quotedString}"))
        )
      }
    }
  }

  test("check cyclic view reference on ALTER VIEW") {
    withTable("t") {
      Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")
      val viewName1 = createView("v1", "SELECT * FROM t")
      val viewName2 = createView("v2", s"SELECT * FROM $viewName1")
      withView(viewName2, viewName1) {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"ALTER VIEW $viewName1 AS SELECT * FROM $viewName2")
          },
          errorClass = "RECURSIVE_VIEW",
          parameters = Map(
            "viewIdent" -> tableIdentifier("v1").quotedString,
            "newPath" -> (s"${tableIdentifier("v1").quotedString} " +
              s"-> ${tableIdentifier("v2").quotedString} " +
              s"-> ${tableIdentifier("v1").quotedString}"))
        )
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
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"SELECT * FROM ${viewNames.last}")
          },
          errorClass = "VIEW_EXCEED_MAX_NESTED_DEPTH",
          parameters = Map(
            "viewName" -> tableIdentifier("view0").quotedString,
            "maxNestedDepth" -> "10"),
          context = ExpectedContext(
            "VIEW",
            tableIdentifier("view1").unquotedString,
            14,
            13 + formattedViewName("view0").length,
            formattedViewName("view0")
          )
        )
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
        val viewName = createView(
          "v1", s"SELECT $functionName(col1) AS func FROM VALUES (1), (2), (3)")
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
      val viewName = createView("testview", "SELECT * FROM t")
      withView(viewName) {
        // Always create a temp view in this case, not use `createView` on purpose
        sql("CREATE TEMP VIEW t AS SELECT 1 AS c1")
        withTempView("t") {
          checkViewOutput(viewName, Seq(Row(2), Row(3), Row(1)))
          // Manually drop table `t` to see if the query will fail
          sql("DROP TABLE IF EXISTS default.t")
          val e = intercept[AnalysisException] {
            sql(s"SELECT * FROM $viewName").collect()
          }
          checkErrorTableNotFound(e, "`t`",
            ExpectedContext("VIEW", tableIdentifier("testview").unquotedString, 14, 14, "t"))
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

  test("SPARK-34152: view's identifier should be correctly stored") {
    Seq(true, false).foreach { storeAnalyzed =>
      withSQLConf(STORE_ANALYZED_PLAN_FOR_VIEW.key -> storeAnalyzed.toString) {
        val viewName = createView("v", "SELECT 1")
        withView(viewName) {
          val tblIdent = tableIdentifier("v")
          val metadata = spark.sessionState.catalog.getTempViewOrPermanentTableMetadata(tblIdent)
          assert(metadata.identifier == tblIdent)
        }
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
        checkError(
          exception = intercept[AnalysisException](spark.table(viewName)),
          errorClass = "FIELD_NOT_FOUND",
          parameters = Map("fieldName" -> "`i`", "fields" -> "`j`"),
          context = ExpectedContext(
            fragment = "s.i",
            objectName = tableIdentifier("v").unquotedString,
            objectType = "VIEW",
            startIndex = 7,
            stopIndex = 9))

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
              checkErrorMatchPVals(
                exception = intercept[AnalysisException](spark.table(viewName).collect()),
                errorClass = "INCOMPATIBLE_VIEW_SCHEMA_CHANGE",
                parameters = Map(
                  "viewName" -> ".*test[v|V]iew.*",
                  "actualCols" -> "\\[COL,col,col\\]",
                  "colName" -> "col",
                  "suggestion" -> "CREATE OR REPLACE.*",
                  "expectedNum" -> "2")
              )
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
            val e = intercept[AnalysisException](spark.table(viewName2).collect())
            assert(e.message.contains("incompatible schema change"))
          }
        }
      }
    }
  }

  test("SPARK-37219: time travel is unsupported") {
    val viewName = createView("testView", "SELECT 1 col")
    withView(viewName) {
      checkErrorMatchPVals(
        exception = intercept[AnalysisException](
          sql(s"SELECT * FROM $viewName VERSION AS OF 1").collect()
        ),
        errorClass = "UNSUPPORTED_FEATURE.TIME_TRAVEL",
        parameters = Map("relationId" -> ".*test[v|V]iew.*")
      )

      checkErrorMatchPVals(
        exception = intercept[AnalysisException](
          sql(s"SELECT * FROM $viewName TIMESTAMP AS OF '2000-10-10'").collect()
        ),
        errorClass = "UNSUPPORTED_FEATURE.TIME_TRAVEL",
        parameters = Map("relationId" -> ".*test[v|V]iew.*")
      )
    }
  }

  test("SPARK-37569: view should report correct nullability information for nested fields") {
    val sql = "SELECT id, named_struct('a', id) AS nested FROM RANGE(10)"
    val viewName = createView("testView", sql)
    withView(viewName) {
      val df = spark.sql(sql)
      val dfFromView = spark.table(viewName)
      assert(df.schema == dfFromView.schema)
    }
  }
}

abstract class TempViewTestSuite extends SQLViewTestSuite {

  def createOrReplaceDatasetView(df: DataFrame, viewName: String): Unit

  test("SPARK-37202: temp view should capture the function registered by catalog API") {
    val funcName = "tempFunc"
    withUserDefinedFunction(funcName -> true) {
      val catalogFunction = CatalogFunction(
        FunctionIdentifier(funcName, None), "org.apache.spark.myFunc", Seq.empty)
      val functionBuilder = (e: Seq[Expression]) => e.head
      spark.sessionState.catalog.registerFunction(
        catalogFunction, overrideIfExists = false, functionBuilder = Some(functionBuilder))
      val query = s"SELECT $funcName(max(a), min(a)) FROM VALUES (1), (2), (3) t(a)"
      val viewName = createView("tempView", query)
      withView(viewName) {
        checkViewOutput(viewName, sql(query).collect().toImmutableArraySeq)
      }
    }
  }

  test("show create table does not support temp view") {
    val viewName = "spark_28383"
    withView(viewName) {
      createView(viewName, "SELECT 1 AS a")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SHOW CREATE TABLE ${formattedViewName(viewName)}")
        },
        errorClass = "EXPECT_PERMANENT_VIEW_NOT_TEMP",
        parameters = Map(
          "viewName" -> toSQLId(tableIdentifier(viewName).nameParts),
          "operation" -> "SHOW CREATE TABLE"),
        context = ExpectedContext(
          fragment = formattedViewName(viewName),
          start = 18,
          stop = 17 + formattedViewName(viewName).length)
      )
    }
  }

  test("back compatibility: skip cyclic reference check if view is stored as logical plan") {
    val viewName = formattedViewName("v")
    withSQLConf(STORE_ANALYZED_PLAN_FOR_VIEW.key -> "false") {
      withView(viewName) {
        createOrReplaceDatasetView(sql("SELECT 1"), "v")
        createOrReplaceDatasetView(sql(s"SELECT * FROM $viewName"), "v")
        checkViewOutput(viewName, Seq(Row(1)))
      }
    }
    withSQLConf(STORE_ANALYZED_PLAN_FOR_VIEW.key -> "true") {
      withView(viewName) {
        createOrReplaceDatasetView(sql("SELECT 1"), "v")
        createOrReplaceDatasetView(sql(s"SELECT * FROM $viewName"), "v")
        checkViewOutput(viewName, Seq(Row(1)))

        createView("v", "SELECT 2", replace = true)
        createView("v", s"SELECT * FROM $viewName", replace = true)
        checkViewOutput(viewName, Seq(Row(2)))
      }
    }
  }
}

class LocalTempViewTestSuite extends TempViewTestSuite with SharedSparkSession {
  override protected def viewTypeString: String = "TEMPORARY VIEW"
  override protected def formattedViewName(viewName: String): String = viewName
  override protected def tableIdentifier(viewName: String): TableIdentifier = {
    TableIdentifier(viewName)
  }
  override def createOrReplaceDatasetView(df: DataFrame, viewName: String): Unit = {
    df.createOrReplaceTempView(viewName)
  }
}

class GlobalTempViewTestSuite extends TempViewTestSuite with SharedSparkSession {
  private def db: String = spark.sharedState.globalTempDB
  override protected def viewTypeString: String = "GLOBAL TEMPORARY VIEW"
  override protected def formattedViewName(viewName: String): String = {
    s"$db.$viewName"
  }
  override protected def tableIdentifier(viewName: String): TableIdentifier = {
    TableIdentifier(viewName, Some(db))
  }
  override def createOrReplaceDatasetView(df: DataFrame, viewName: String): Unit = {
    df.createOrReplaceGlobalTempView(viewName)
  }
}

class OneTableCatalog extends InMemoryCatalog {
  override def loadTable(ident: Identifier): Table = {
    if (ident.namespace.isEmpty && ident.name == "t") {
      new InMemoryTable(
        "t",
        StructType.fromDDL("c1 INT"),
        Array.empty,
        Map.empty[String, String].asJava)
    } else {
      super.loadTable(ident)
    }
  }
}

class PersistedViewTestSuite extends SQLViewTestSuite with SharedSparkSession {
  private def db: String = "default"
  override protected def viewTypeString: String = "VIEW"
  override protected def formattedViewName(viewName: String): String = s"$db.$viewName"
  override protected def tableIdentifier(viewName: String): TableIdentifier = {
    TableIdentifier(viewName, Some(db), Some(SESSION_CATALOG_NAME))
  }

  test("SPARK-35686: error out for creating view with auto gen alias") {
    withView("v") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE VIEW v AS SELECT count(*) FROM VALUES (1), (2), (3) t(a)")
        },
        errorClass = "CREATE_PERMANENT_VIEW_WITHOUT_ALIAS",
        parameters = Map("name" -> tableIdentifier("v").quotedString, "attr" -> "\"count(1)\"")
      )
      sql("CREATE VIEW v AS SELECT count(*) AS cnt FROM VALUES (1), (2), (3) t(a)")
      checkAnswer(sql("SELECT * FROM v"), Seq(Row(3)))
    }
  }

  test("SPARK-35686: error out for creating view with auto gen alias in subquery") {
    withView("v") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE VIEW v AS SELECT * FROM (SELECT a + b FROM VALUES (1, 2) t(a, b))")
        },
        errorClass = "CREATE_PERMANENT_VIEW_WITHOUT_ALIAS",
        parameters = Map("name" -> tableIdentifier("v").quotedString, "attr" -> "\"(a + b)\"")
      )
      sql("CREATE VIEW v AS SELECT * FROM (SELECT a + b AS col FROM VALUES (1, 2) t(a, b))")
      checkAnswer(sql("SELECT * FROM v"), Seq(Row(3)))
    }
  }

  test("SPARK-35686: error out for alter view with auto gen alias") {
    withView("v") {
      sql("CREATE VIEW v AS SELECT 1 AS a")
      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER VIEW v AS SELECT count(*) FROM VALUES (1), (2), (3) t(a)")
        },
        errorClass = "CREATE_PERMANENT_VIEW_WITHOUT_ALIAS",
        parameters = Map("name" -> tableIdentifier("v").quotedString, "attr" -> "\"count(1)\"")
      )
    }
  }

  test("SPARK-35686: legacy config to allow auto generated alias for view") {
    withSQLConf(ALLOW_AUTO_GENERATED_ALIAS_FOR_VEW.key -> "true") {
      withView("v") {
        sql("CREATE VIEW v AS SELECT count(*) FROM VALUES (1), (2), (3) t(a)")
        checkAnswer(sql("SELECT * FROM v"), Seq(Row(3)))
      }
    }
  }

  test("SPARK-35685: Prompt recreating view message for schema mismatch") {
    withTable("t") {
      sql("CREATE TABLE t USING json AS SELECT 1 AS col_i")
      val catalog = spark.sessionState.catalog
      withView("test_view") {
        sql("CREATE VIEW test_view AS SELECT * FROM t")
        val meta = catalog.getTableRawMetadata(TableIdentifier("test_view", Some("default")))
        // simulate a view meta with incompatible schema change
        val newProp = meta.properties
          .transform((_, v) => v.replace("col_i", "col_j"))
        val newSchema = StructType(Seq(StructField("col_j", IntegerType)))
        catalog.alterTable(meta.copy(properties = newProp, schema = newSchema))
        val e = intercept[AnalysisException] {
          sql(s"SELECT * FROM test_view")
        }
        val unquotedViewName = tableIdentifier("test_view").unquotedString
        checkError(
          exception = e,
          errorClass = "INCOMPATIBLE_VIEW_SCHEMA_CHANGE",
          parameters = Map(
            "viewName" -> tableIdentifier("test_view").quotedString,
            "suggestion" -> s"CREATE OR REPLACE VIEW $unquotedViewName AS SELECT * FROM t",
            "actualCols" -> "[]", "colName" -> "col_j",
            "expectedNum" -> "1")
        )
        val ddl = e.getMessageParameters.get("suggestion")
        sql(ddl)
        checkAnswer(sql("select * FROM test_view"), Row(1))
      }
    }
  }

  test("SPARK-36011: Disallow altering permanent views based on temporary views or UDFs") {
    import testImplicits._
    withTable("t") {
      (1 to 10).toDF("id").write.saveAsTable("t")
      withView("v1") {
        withTempView("v2") {
          sql("CREATE VIEW v1 AS SELECT * FROM t")
          sql("CREATE TEMPORARY VIEW v2 AS  SELECT * FROM t")
          checkError(
            exception = intercept[AnalysisException] {
              sql("ALTER VIEW v1 AS SELECT * FROM v2")
            },
            errorClass = "INVALID_TEMP_OBJ_REFERENCE",
            parameters = Map(
              "obj" -> "VIEW",
              "objName" -> tableIdentifier("v1").quotedString,
              "tempObj" -> "VIEW",
              "tempObjName" -> "`v2`"))
          val tempFunctionName = "temp_udf"
          val functionClass = "test.org.apache.spark.sql.MyDoubleAvg"
          withUserDefinedFunction(tempFunctionName -> true) {
            sql(s"CREATE TEMPORARY FUNCTION $tempFunctionName AS '$functionClass'")
            checkError(
              exception = intercept[AnalysisException] {
                sql(s"ALTER VIEW v1 AS SELECT $tempFunctionName(id) from t")
              },
              errorClass = "INVALID_TEMP_OBJ_REFERENCE",
              parameters = Map(
                "obj" -> "VIEW",
                "objName" -> tableIdentifier("v1").quotedString,
                "tempObj" -> "FUNCTION",
                "tempObjName" -> s"`$tempFunctionName`"))
          }
        }
      }
    }
  }

  test("SPARK-36466: Table in unloaded catalog referenced by view should load correctly") {
    val viewName = "v"
    val tableInOtherCatalog = "cat.t"
    try {
      spark.conf.set("spark.sql.catalog.cat", classOf[OneTableCatalog].getName)
      withTable(tableInOtherCatalog) {
        withView(viewName) {
          createView(viewName, s"SELECT count(*) AS cnt FROM $tableInOtherCatalog")
          checkViewOutput(viewName, Seq(Row(0)))
          spark.sessionState.catalogManager.reset()
          checkViewOutput(viewName, Seq(Row(0)))
        }
      }
    } finally {
      spark.sessionState.catalog.reset()
      spark.sessionState.catalogManager.reset()
      spark.sessionState.conf.clear()
    }
  }

  test("SPARK-37266: View text can only be SELECT queries") {
    withView("v") {
      sql("CREATE VIEW v AS SELECT 1")
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("v"))
      val dropView = "DROP VIEW v"
      // Simulate the behavior of hackers
      val tamperedTable = table.copy(viewText = Some(dropView))
      spark.sessionState.catalog.alterTable(tamperedTable)

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM v")
        },
        errorClass = "INVALID_VIEW_TEXT",
        parameters = Map(
          "viewText" -> "DROP VIEW v", "viewName" -> tableIdentifier("v").quotedString)
      )
    }
  }

  test("show create table for persisted simple view") {
    val viewName = "v1"
    Seq(true, false).foreach { serde =>
      withView(viewName) {
        createView(viewName, "SELECT 1 AS a")
        val expected = s"CREATE VIEW ${formattedViewName(viewName)} ( a) " +
          "WITH SCHEMA COMPENSATION AS SELECT 1 AS a"
        assert(getShowCreateDDL(formattedViewName(viewName), serde) == expected)
      }
    }
  }

  test("show create table for persisted view with output columns") {
    val viewName = "v1"
    Seq(true, false).foreach { serde =>
      withView(viewName) {
        createView(viewName, "SELECT 1 AS a, 2 AS b", Seq("a", "b COMMENT 'b column'"))
        val expected = s"CREATE VIEW ${formattedViewName(viewName)}" +
          s" ( a, b COMMENT 'b column') WITH SCHEMA COMPENSATION AS SELECT 1 AS a, 2 AS b"
        assert(getShowCreateDDL(formattedViewName(viewName), serde) == expected)
      }
    }
  }

  test("show create table for persisted simple view with table comment and properties") {
    val viewName = "v1"
    Seq(true, false).foreach { serde =>
      withView(viewName) {
        createView(viewName, "SELECT 1 AS c1, '2' AS c2", Seq("c1 COMMENT 'bla'", "c2"),
          Seq("COMMENT 'table comment'", "TBLPROPERTIES ( 'prop2' = 'value2', 'prop1' = 'value1')"))

        val expected = s"CREATE VIEW ${formattedViewName(viewName)} ( c1 COMMENT 'bla', c2)" +
          " COMMENT 'table comment'" +
          " TBLPROPERTIES ( 'prop1' = 'value1', 'prop2' = 'value2')" +
          " WITH SCHEMA COMPENSATION AS SELECT 1 AS c1, '2' AS c2"
        assert(getShowCreateDDL(formattedViewName(viewName), serde) == expected)
      }
    }
  }

  test("capture the session time zone config while creating a view") {
    val viewName = "v1_capture_test"
    withView(viewName) {
      assert(get.sessionLocalTimeZone === "America/Los_Angeles")
      createView(viewName,
        """select hour(ts) as H from (
          |  select cast('2022-01-01T00:00:00.000 America/Los_Angeles' as timestamp) as ts
          |)""".stripMargin, Seq("H"))
      withDefaultTimeZone(java.time.ZoneId.of("UTC-09:00")) {
        withSQLConf(SESSION_LOCAL_TIMEZONE.key -> "UTC-10:00") {
          checkAnswer(sql(s"select H from $viewName"), Row(0))
        }
      }
    }
  }

  def getShowCreateDDL(view: String, serde: Boolean = false): String = {
    val result = if (serde) {
      sql(s"SHOW CREATE TABLE $view AS SERDE")
    } else {
      sql(s"SHOW CREATE TABLE $view")
    }
    result.head().getString(0).split("\n").map(_.trim).mkString(" ")
  }
}
