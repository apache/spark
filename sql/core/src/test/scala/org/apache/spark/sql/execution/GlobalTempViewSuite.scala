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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class GlobalTempViewSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    globalTempDB = spark.sharedState.globalTempDB
  }

  private var globalTempDB: String = _

  test("basic semantic") {
    sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1, 'a'")

    // If there is no database in table name, we should try local temp view first, if not found,
    // try table/view in current database, which is "default" in this case. So we expect
    // NoSuchTableException here.
    intercept[NoSuchTableException](spark.table("src"))

    // Use qualified name to refer to the global temp view explicitly.
    checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a"))

    // Table name without database will never refer to a global temp view.
    intercept[NoSuchTableException](sql("DROP VIEW src"))

    sql(s"DROP VIEW $globalTempDB.src")
    // The global temp view should be dropped successfully.
    intercept[NoSuchTableException](spark.table(s"$globalTempDB.src"))

    // We can also use Dataset API to create global temp view
    Seq(1 -> "a").toDF("i", "j").createGlobalTempView("src")
    checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a"))

    // Use qualified name to rename a global temp view.
    sql(s"ALTER VIEW $globalTempDB.src RENAME TO src2")
    intercept[NoSuchTableException](spark.table(s"$globalTempDB.src"))
    checkAnswer(spark.table(s"$globalTempDB.src2"), Row(1, "a"))

    // Use qualified name to alter a global temp view.
    sql(s"ALTER VIEW $globalTempDB.src2 AS SELECT 2, 'b'")
    checkAnswer(spark.table(s"$globalTempDB.src2"), Row(2, "b"))

    // We can also use Catalog API to drop global temp view
    spark.catalog.dropGlobalTempView("src2")
    intercept[NoSuchTableException](spark.table(s"$globalTempDB.src2"))
  }

  test("global temp view database should be preserved") {
    val e = intercept[AnalysisException](sql(s"CREATE DATABASE $globalTempDB"))
    assert(e.message.contains("system preserved database"))

    val e2 = intercept[AnalysisException](sql(s"USE $globalTempDB"))
    assert(e2.message.contains("system preserved database"))
  }

  test("CREATE TABLE LIKE should work for global temp view") {
    try {
      sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1 AS a, '2' AS b")
      sql(s"CREATE TABLE cloned LIKE ${globalTempDB}.src")
      val tableMeta = spark.sessionState.catalog.getTableMetadata(TableIdentifier("cloned"))
      assert(tableMeta.schema == new StructType().add("a", "int", false).add("b", "string", false))
    } finally {
      spark.catalog.dropGlobalTempView("src")
      sql("DROP TABLE default.cloned")
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
      spark.catalog.dropTempView("v1")
      spark.catalog.dropGlobalTempView("v2")
    }
  }
}
