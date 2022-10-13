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
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, HintInfo, Join, JoinHint}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class GlobalTempViewSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    globalTempDB = spark.sharedState.globalTempViewManager.database
  }

  private var globalTempDB: String = _

  test("basic semantic") {
    val expectedErrorMsg = "not found"
    withGlobalTempView("src") {
      sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1, 'a'")

      // If there is no database in table name, we should try local temp view first, if not found,
      // try table/view in current database, which is "default" in this case. So we expect
      // NoSuchTableException here.
      var e = intercept[AnalysisException](spark.table("src")).getMessage
      assert(e.contains(expectedErrorMsg))

      // Use qualified name to refer to the global temp view explicitly.
      checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a"))

      // Table name without database will never refer to a global temp view.
      e = intercept[AnalysisException](sql("DROP VIEW src")).getMessage
      assert(e.contains(expectedErrorMsg))

      sql(s"DROP VIEW $globalTempDB.src")
      // The global temp view should be dropped successfully.
      e = intercept[AnalysisException](spark.table(s"$globalTempDB.src")).getMessage
      assert(e.contains(expectedErrorMsg))

      // We can also use Dataset API to create global temp view
      Seq(1 -> "a").toDF("i", "j").createGlobalTempView("src")
      checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a"))

      // Use qualified name to rename a global temp view.
      sql(s"ALTER VIEW $globalTempDB.src RENAME TO src2")
      e = intercept[AnalysisException](spark.table(s"$globalTempDB.src")).getMessage
      assert(e.contains(expectedErrorMsg))
      checkAnswer(spark.table(s"$globalTempDB.src2"), Row(1, "a"))

      // Use qualified name to alter a global temp view.
      sql(s"ALTER VIEW $globalTempDB.src2 AS SELECT 2, 'b'")
      checkAnswer(spark.table(s"$globalTempDB.src2"), Row(2, "b"))

      // We can also use Catalog API to drop global temp view
      spark.catalog.dropGlobalTempView("src2")
      e = intercept[AnalysisException](spark.table(s"$globalTempDB.src2")).getMessage
      assert(e.contains(expectedErrorMsg))

      // We can also use Dataset API to replace global temp view
      Seq(2 -> "b").toDF("i", "j").createOrReplaceGlobalTempView("src")
      checkAnswer(spark.table(s"$globalTempDB.src"), Row(2, "b"))
    }
  }

  test("global temp view is shared among all sessions") {
    withGlobalTempView("src") {
      sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1, 2")
      checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, 2))
      val newSession = spark.newSession()
      checkAnswer(newSession.table(s"$globalTempDB.src"), Row(1, 2))
    }
  }

  test("global temp view database should be preserved") {
    val e = intercept[AnalysisException](sql(s"CREATE DATABASE $globalTempDB"))
    assert(e.message.contains("system preserved database"))

    val e2 = intercept[AnalysisException](sql(s"USE $globalTempDB"))
    assert(e2.message.contains("system preserved database"))
  }

  test("CREATE GLOBAL TEMP VIEW USING") {
    withTempPath { path =>
      withGlobalTempView("src") {
        Seq(1 -> "a").toDF("i", "j").write.parquet(path.getAbsolutePath)
        sql(s"CREATE GLOBAL TEMP VIEW src USING parquet OPTIONS (PATH '${path.toURI}')")
        checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a"))
        sql(s"INSERT INTO $globalTempDB.src SELECT 2, 'b'")
        checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a") :: Row(2, "b") :: Nil)
      }
    }
  }

  test("CREATE TABLE LIKE should work for global temp view") {
    withTable("cloned") {
      withGlobalTempView("src") {
        sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1 AS a, '2' AS b")
        sql(s"CREATE TABLE cloned LIKE $globalTempDB.src")
        val tableMeta = spark.sessionState.catalog.getTableMetadata(TableIdentifier("cloned"))
        assert(tableMeta.schema == new StructType()
          .add("a", "int", false).add("b", "string", false))
      }
    }
  }

  test("list global temp views") {
    try {
      sql("CREATE GLOBAL TEMP VIEW v1 AS SELECT 3, 4")
      sql("CREATE TEMP VIEW v2 AS SELECT 1, 2")

      checkAnswer(sql(s"SHOW TABLES IN $globalTempDB"),
        Row(globalTempDB, "v1", true) ::
        Row("", "v2", true) :: Nil)

      assert(spark.catalog.listTables(globalTempDB).collect().toSeq.map(_.name) == Seq("v1", "v2"))
    } finally {
      spark.catalog.dropGlobalTempView("v1")
      spark.catalog.dropTempView("v2")
    }
  }

  test("should lookup global temp view if and only if global temp db is specified") {
    withTempView("same_name") {
      withGlobalTempView("same_name") {
        sql("CREATE GLOBAL TEMP VIEW same_name AS SELECT 3, 4")
        sql("CREATE TEMP VIEW same_name AS SELECT 1, 2")

        checkAnswer(sql("SELECT * FROM same_name"), Row(1, 2))

        // we never lookup global temp views if database is not specified in table name
        spark.catalog.dropTempView("same_name")
        intercept[AnalysisException](sql("SELECT * FROM same_name"))

        // Use qualified name to lookup a global temp view.
        checkAnswer(sql(s"SELECT * FROM $globalTempDB.same_name"), Row(3, 4))
      }
    }
  }

  test("public Catalog should recognize global temp view") {
    withGlobalTempView("src")  {
      sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1, 2")

      assert(spark.catalog.tableExists(globalTempDB, "src"))
      assert(spark.catalog.getTable(globalTempDB, "src").toString == new Table(
        name = "src",
        catalog = null,
        namespace = Array(globalTempDB),
        description = null,
        tableType = "TEMPORARY",
        isTemporary = true).toString)
    }
  }

  test("broadcast hint on global temp view") {
    withGlobalTempView("v1") {
      spark.range(10).createGlobalTempView("v1")
      withTempView("v2") {
        spark.range(10).createTempView("v2")

        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          Seq(
            "SELECT /*+ MAPJOIN(v1) */ * FROM global_temp.v1, v2 WHERE v1.id = v2.id",
            "SELECT /*+ MAPJOIN(global_temp.v1) */ * FROM global_temp.v1, v2 WHERE v1.id = v2.id"
          ).foreach { statement =>
            sql(statement).queryExecution.optimizedPlan match {
              case Join(_, _, _, _, JoinHint(Some(HintInfo(Some(BROADCAST))), None)) =>
              case _ => fail("broadcast hint not found in a left-side table")
            }
          }
        }
      }
    }
  }
}
