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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

class SimpleSQLViewSuite extends SQLViewSuite with SharedSQLContext

/**
 * A suite for testing view related functionality.
 */
abstract class SQLViewSuite extends QueryTest with SQLTestUtils {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    // Create a simple table with two columns: id and id1
    spark.range(1, 10).selectExpr("id", "id id1").write.format("json").saveAsTable("jt")
  }

  protected override def afterAll(): Unit = {
    try {
      spark.sql(s"DROP TABLE IF EXISTS jt")
    } finally {
      super.afterAll()
    }
  }

  test("create a permanent view on a permanent view") {
    withView("jtv1", "jtv2") {
      sql("CREATE VIEW jtv1 AS SELECT * FROM jt WHERE id > 3")
      sql("CREATE VIEW jtv2 AS SELECT * FROM jtv1 WHERE id < 6")
      checkAnswer(sql("select count(*) FROM jtv2"), Row(2))
    }
  }

  test("create a temp view on a permanent view") {
    withView("jtv1", "temp_jtv1") {
      sql("CREATE VIEW jtv1 AS SELECT * FROM jt WHERE id > 3")
      sql("CREATE TEMPORARY VIEW temp_jtv1 AS SELECT * FROM jtv1 WHERE id < 6")
      checkAnswer(sql("select count(*) FROM temp_jtv1"), Row(2))
    }
  }

  test("create a temp view on a temp view") {
    withView("temp_jtv1", "temp_jtv2") {
      sql("CREATE TEMPORARY VIEW temp_jtv1 AS SELECT * FROM jt WHERE id > 3")
      sql("CREATE TEMPORARY VIEW temp_jtv2 AS SELECT * FROM temp_jtv1 WHERE id < 6")
      checkAnswer(sql("select count(*) FROM temp_jtv2"), Row(2))
    }
  }

  test("create a permanent view on a temp view") {
    withView("jtv1", "temp_jtv1", "global_temp_jtv1") {
      sql("CREATE TEMPORARY VIEW temp_jtv1 AS SELECT * FROM jt WHERE id > 3")
      var e = intercept[AnalysisException] {
        sql("CREATE VIEW jtv1 AS SELECT * FROM temp_jtv1 WHERE id < 6")
      }.getMessage
      assert(e.contains("Not allowed to create a permanent view `jtv1` by " +
        "referencing a temporary view `temp_jtv1`"))

      val globalTempDB = spark.sharedState.globalTempViewManager.database
      sql("CREATE GLOBAL TEMP VIEW global_temp_jtv1 AS SELECT * FROM jt WHERE id > 0")
      e = intercept[AnalysisException] {
        sql(s"CREATE VIEW jtv1 AS SELECT * FROM $globalTempDB.global_temp_jtv1 WHERE id < 6")
      }.getMessage
      assert(e.contains(s"Not allowed to create a permanent view `jtv1` by referencing " +
        s"a temporary view `global_temp`.`global_temp_jtv1`"))
    }
  }

  test("error handling: existing a table with the duplicate name when creating/altering a view") {
    withTable("tab1") {
      sql("CREATE TABLE tab1 (id int) USING parquet")
      var e = intercept[AnalysisException] {
        sql("CREATE OR REPLACE VIEW tab1 AS SELECT * FROM jt")
      }.getMessage
      assert(e.contains("`tab1` is not a view"))
      e = intercept[AnalysisException] {
        sql("CREATE VIEW tab1 AS SELECT * FROM jt")
      }.getMessage
      assert(e.contains("`tab1` is not a view"))
      e = intercept[AnalysisException] {
        sql("ALTER VIEW tab1 AS SELECT * FROM jt")
      }.getMessage
      assert(e.contains("`tab1` is not a view"))
    }
  }

  test("existing a table with the duplicate name when CREATE VIEW IF NOT EXISTS") {
    withTable("tab1") {
      sql("CREATE TABLE tab1 (id int) USING parquet")
      sql("CREATE VIEW IF NOT EXISTS tab1 AS SELECT * FROM jt")
      checkAnswer(sql("select count(*) FROM tab1"), Row(0))
    }
  }

  test("Issue exceptions for ALTER VIEW on the temporary view") {
    val viewName = "testView"
    withTempView(viewName) {
      spark.range(10).createTempView(viewName)
      assertNoSuchTable(s"ALTER VIEW $viewName SET TBLPROPERTIES ('p' = 'an')")
      assertNoSuchTable(s"ALTER VIEW $viewName UNSET TBLPROPERTIES ('p')")
    }
  }

  test("Issue exceptions for ALTER TABLE on the temporary view") {
    val viewName = "testView"
    withTempView(viewName) {
      spark.range(10).createTempView(viewName)
      assertNoSuchTable(s"ALTER TABLE $viewName SET SERDE 'whatever'")
      assertNoSuchTable(s"ALTER TABLE $viewName PARTITION (a=1, b=2) SET SERDE 'whatever'")
      assertNoSuchTable(s"ALTER TABLE $viewName SET SERDEPROPERTIES ('p' = 'an')")
      assertNoSuchTable(s"ALTER TABLE $viewName SET LOCATION '/path/to/your/lovely/heart'")
      assertNoSuchTable(s"ALTER TABLE $viewName PARTITION (a='4') SET LOCATION '/path/to/home'")
      assertNoSuchTable(s"ALTER TABLE $viewName ADD IF NOT EXISTS PARTITION (a='4', b='8')")
      assertNoSuchTable(s"ALTER TABLE $viewName DROP PARTITION (a='4', b='8')")
      assertNoSuchTable(s"ALTER TABLE $viewName PARTITION (a='4') RENAME TO PARTITION (a='5')")
      assertNoSuchTable(s"ALTER TABLE $viewName RECOVER PARTITIONS")
    }
  }

  test("Issue exceptions for other table DDL on the temporary view") {
    val viewName = "testView"
    withTempView(viewName) {
      spark.range(10).createTempView(viewName)

      val e = intercept[AnalysisException] {
        sql(s"INSERT INTO TABLE $viewName SELECT 1")
      }.getMessage
      assert(e.contains("Inserting into an RDD-based table is not allowed"))

      val dataFilePath =
        Thread.currentThread().getContextClassLoader.getResource("data/files/employee.dat")
      assertNoSuchTable(s"""LOAD DATA LOCAL INPATH "$dataFilePath" INTO TABLE $viewName""")
      assertNoSuchTable(s"TRUNCATE TABLE $viewName")
      assertNoSuchTable(s"SHOW CREATE TABLE $viewName")
      assertNoSuchTable(s"SHOW PARTITIONS $viewName")
      assertNoSuchTable(s"ANALYZE TABLE $viewName COMPUTE STATISTICS")
      assertNoSuchTable(s"ANALYZE TABLE $viewName COMPUTE STATISTICS FOR COLUMNS id")
    }
  }

  private def assertNoSuchTable(query: String): Unit = {
    intercept[NoSuchTableException] {
      sql(query)
    }
  }

  test("error handling: insert/load/truncate table commands against a view") {
    val viewName = "testView"
    withView(viewName) {
      sql(s"CREATE VIEW $viewName AS SELECT id FROM jt")
      var e = intercept[AnalysisException] {
        sql(s"INSERT INTO TABLE $viewName SELECT 1")
      }.getMessage
      assert(e.contains("Inserting into a view is not allowed. View: `default`.`testview`"))

      val dataFilePath =
        Thread.currentThread().getContextClassLoader.getResource("data/files/employee.dat")
      e = intercept[AnalysisException] {
        sql(s"""LOAD DATA LOCAL INPATH "$dataFilePath" INTO TABLE $viewName""")
      }.getMessage
      assert(e.contains(s"Target table in LOAD DATA cannot be a view: `default`.`testview`"))

      e = intercept[AnalysisException] {
        sql(s"TRUNCATE TABLE $viewName")
      }.getMessage
      assert(e.contains(s"Operation not allowed: TRUNCATE TABLE on views: `default`.`testview`"))
    }
  }

  test("error handling: fail if the view sql itself is invalid") {
    // A database that does not exist
    assertInvalidReference("CREATE OR REPLACE VIEW myabcdview AS SELECT * FROM db_not_exist234.jt")

    // A table that does not exist
    assertInvalidReference("CREATE OR REPLACE VIEW myabcdview AS SELECT * FROM table_not_exist345")

    // A column that does not exist
    intercept[AnalysisException] {
      sql("CREATE OR REPLACE VIEW myabcdview AS SELECT random1234 FROM jt").collect()
    }
  }

  private def assertInvalidReference(query: String): Unit = {
    val e = intercept[AnalysisException] {
      sql(query)
    }.getMessage
    assert(e.contains("Table or view not found"))
  }


  test("error handling: fail if the temp view name contains the database prefix") {
    // Fully qualified table name like "database.table" is not allowed for temporary view
    val e = intercept[AnalysisException] {
      sql("CREATE OR REPLACE TEMPORARY VIEW default.myabcdview AS SELECT * FROM jt")
    }
    assert(e.message.contains("It is not allowed to add database prefix"))
  }

  test("error handling: disallow IF NOT EXISTS for CREATE TEMPORARY VIEW") {
    val e = intercept[AnalysisException] {
      sql("CREATE TEMPORARY VIEW IF NOT EXISTS myabcdview AS SELECT * FROM jt")
    }
    assert(e.message.contains("It is not allowed to define a TEMPORARY view with IF NOT EXISTS"))
  }

  test("error handling: fail if the temp view sql itself is invalid") {
    // A database that does not exist
    assertInvalidReference(
      "CREATE OR REPLACE TEMPORARY VIEW myabcdview AS SELECT * FROM db_not_exist234.jt")

    // A table that does not exist
    assertInvalidReference(
      "CREATE OR REPLACE TEMPORARY VIEW myabcdview AS SELECT * FROM table_not_exist1345")

    // A column that does not exist, for temporary view
    intercept[AnalysisException] {
      sql("CREATE OR REPLACE TEMPORARY VIEW myabcdview AS SELECT random1234 FROM jt")
    }
  }

  test("correctly parse CREATE VIEW statement") {
    withView("testView") {
      sql(
        """CREATE VIEW IF NOT EXISTS
          |default.testView (c1 COMMENT 'blabla', c2 COMMENT 'blabla')
          |TBLPROPERTIES ('a' = 'b')
          |AS SELECT * FROM jt
          |""".stripMargin)
      checkAnswer(sql("SELECT c1, c2 FROM testView ORDER BY c1"), (1 to 9).map(i => Row(i, i)))
    }
  }

  test("correctly parse a nested view") {
    withTempDatabase { db =>
      withView("view1", "view2", s"$db.view3") {
        sql("CREATE VIEW view1(x, y) AS SELECT * FROM jt")

        // Create a nested view in the same database.
        sql("CREATE VIEW view2(id, id1) AS SELECT * FROM view1")
        checkAnswer(sql("SELECT * FROM view2 ORDER BY id"), (1 to 9).map(i => Row(i, i)))

        // Create a nested view in a different database.
        activateDatabase(db) {
          sql(s"CREATE VIEW $db.view3(id, id1) AS SELECT * FROM default.view1")
          checkAnswer(sql("SELECT * FROM view3 ORDER BY id"), (1 to 9).map(i => Row(i, i)))
        }
      }
    }
  }

  test("correctly parse CREATE TEMPORARY VIEW statement") {
    withView("testView") {
      sql(
        """CREATE TEMPORARY VIEW
          |testView (c1 COMMENT 'blabla', c2 COMMENT 'blabla')
          |TBLPROPERTIES ('a' = 'b')
          |AS SELECT * FROM jt
          |""".stripMargin)
      checkAnswer(sql("SELECT c1, c2 FROM testView ORDER BY c1"), (1 to 9).map(i => Row(i, i)))
    }
  }

  test("should NOT allow CREATE TEMPORARY VIEW when TEMPORARY VIEW with same name exists") {
    withView("testView") {
      sql("CREATE TEMPORARY VIEW testView AS SELECT id FROM jt")

      val e = intercept[AnalysisException] {
        sql("CREATE TEMPORARY VIEW testView AS SELECT id FROM jt")
      }

      assert(e.message.contains("Temporary table") && e.message.contains("already exists"))
    }
  }

  test("should allow CREATE TEMPORARY VIEW when a permanent VIEW with same name exists") {
    withView("testView", "default.testView") {
      sql("CREATE VIEW testView AS SELECT id FROM jt")
      sql("CREATE TEMPORARY VIEW testView AS SELECT id FROM jt")
    }
  }

  test("should allow CREATE permanent VIEW when a TEMPORARY VIEW with same name exists") {
    withView("testView", "default.testView") {
      sql("CREATE TEMPORARY VIEW testView AS SELECT id FROM jt")
      sql("CREATE VIEW testView AS SELECT id FROM jt")
    }
  }

  test("correctly handle CREATE VIEW IF NOT EXISTS") {
    withTable("jt2") {
      withView("testView") {
        sql("CREATE VIEW testView AS SELECT id FROM jt")

        val df = (1 until 10).map(i => i -> i).toDF("i", "j")
        df.write.format("json").saveAsTable("jt2")
        sql("CREATE VIEW IF NOT EXISTS testView AS SELECT * FROM jt2")

        // make sure our view doesn't change.
        checkAnswer(sql("SELECT * FROM testView ORDER BY id"), (1 to 9).map(i => Row(i)))
      }
    }
  }

  test(s"correctly handle CREATE OR REPLACE TEMPORARY VIEW") {
    withTable("jt2") {
      withView("testView") {
        sql("CREATE OR REPLACE TEMPORARY VIEW testView AS SELECT id FROM jt")
        checkAnswer(sql("SELECT * FROM testView ORDER BY id"), (1 to 9).map(i => Row(i)))

        sql("CREATE OR REPLACE TEMPORARY VIEW testView AS SELECT id AS i, id AS j FROM jt")
        // make sure the view has been changed.
        checkAnswer(sql("SELECT * FROM testView ORDER BY i"), (1 to 9).map(i => Row(i, i)))
      }
    }
  }

  test("correctly handle CREATE OR REPLACE VIEW") {
    withTable("jt2") {
      sql("CREATE OR REPLACE VIEW testView AS SELECT id FROM jt")
      checkAnswer(sql("SELECT * FROM testView ORDER BY id"), (1 to 9).map(i => Row(i)))

      val df = (1 until 10).map(i => i -> i).toDF("i", "j")
      df.write.format("json").saveAsTable("jt2")
      sql("CREATE OR REPLACE VIEW testView AS SELECT * FROM jt2")
      // make sure the view has been changed.
      checkAnswer(sql("SELECT * FROM testView ORDER BY i"), (1 to 9).map(i => Row(i, i)))

      sql("DROP VIEW testView")

      val e = intercept[AnalysisException] {
        sql("CREATE OR REPLACE VIEW IF NOT EXISTS testView AS SELECT id FROM jt")
      }
      assert(e.message.contains(
        "CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed"))
    }
  }

  test("correctly handle ALTER VIEW") {
    withTable("jt2") {
      withView("testView") {
        sql("CREATE VIEW testView AS SELECT id FROM jt")

        val df = (1 until 10).map(i => i -> i).toDF("i", "j")
        df.write.format("json").saveAsTable("jt2")
        sql("ALTER VIEW testView AS SELECT * FROM jt2")
        // make sure the view has been changed.
        checkAnswer(sql("SELECT * FROM testView ORDER BY i"), (1 to 9).map(i => Row(i, i)))
      }
    }
  }

  test("correctly handle ALTER VIEW on a referenced view") {
    withView("view1", "view2") {
      sql("CREATE VIEW view1(x, y) AS SELECT * FROM jt")

      // Create a nested view.
      sql("CREATE VIEW view2(id, id1) AS SELECT * FROM view1")
      checkAnswer(sql("SELECT * FROM view2 ORDER BY id"), (1 to 9).map(i => Row(i, i)))

      // Alter the referenced view.
      sql("ALTER VIEW view1 AS SELECT id AS x, id1 + 1 As y FROM jt")
      checkAnswer(sql("SELECT * FROM view2 ORDER BY id"), (1 to 9).map(i => Row(i, i + 1)))
    }
  }

  test("should not allow ALTER VIEW AS when the view does not exist") {
    assertNoSuchTable("ALTER VIEW testView AS SELECT 1, 2")
    assertNoSuchTable("ALTER VIEW default.testView AS SELECT 1, 2")
  }

  test("ALTER VIEW AS should try to alter temp view first if view name has no database part") {
    withView("test_view") {
      withTempView("test_view") {
        sql("CREATE VIEW test_view AS SELECT 1 AS a, 2 AS b")
        sql("CREATE TEMP VIEW test_view AS SELECT 1 AS a, 2 AS b")

        sql("ALTER VIEW test_view AS SELECT 3 AS i, 4 AS j")

        // The temporary view should be updated.
        checkAnswer(spark.table("test_view"), Row(3, 4))

        // The permanent view should stay same.
        checkAnswer(spark.table("default.test_view"), Row(1, 2))
      }
    }
  }

  test("ALTER VIEW AS should alter permanent view if view name has database part") {
    withView("test_view") {
      withTempView("test_view") {
        sql("CREATE VIEW test_view AS SELECT 1 AS a, 2 AS b")
        sql("CREATE TEMP VIEW test_view AS SELECT 1 AS a, 2 AS b")

        sql("ALTER VIEW default.test_view AS SELECT 3 AS i, 4 AS j")

        // The temporary view should stay same.
        checkAnswer(spark.table("test_view"), Row(1, 2))

        // The permanent view should be updated.
        checkAnswer(spark.table("default.test_view"), Row(3, 4))
      }
    }
  }

  test("ALTER VIEW AS should keep the previous table properties, comment, create_time, etc.") {
    withView("test_view") {
      sql(
        """
          |CREATE VIEW test_view
          |COMMENT 'test'
          |TBLPROPERTIES ('key' = 'a')
          |AS SELECT 1 AS a, 2 AS b
        """.stripMargin)

      val catalog = spark.sessionState.catalog
      val viewMeta = catalog.getTableMetadata(TableIdentifier("test_view"))
      assert(viewMeta.comment == Some("test"))
      assert(viewMeta.properties("key") == "a")

      sql("ALTER VIEW test_view AS SELECT 3 AS i, 4 AS j")
      val updatedViewMeta = catalog.getTableMetadata(TableIdentifier("test_view"))
      assert(updatedViewMeta.comment == Some("test"))
      assert(updatedViewMeta.properties("key") == "a")
      assert(updatedViewMeta.createTime == viewMeta.createTime)
      // The view should be updated.
      checkAnswer(spark.table("test_view"), Row(3, 4))
    }
  }

  test("create view for json table") {
    // json table is not hive-compatible, make sure the new flag fix it.
    withView("testView") {
      sql("CREATE VIEW testView AS SELECT id FROM jt")
      checkAnswer(sql("SELECT * FROM testView ORDER BY id"), (1 to 9).map(i => Row(i)))
    }
  }

  test("create view for partitioned parquet table") {
    // partitioned parquet table is not hive-compatible, make sure the new flag fix it.
    withTable("parTable") {
      withView("testView") {
        val df = Seq(1 -> "a").toDF("i", "j")
        df.write.format("parquet").partitionBy("i").saveAsTable("parTable")
        sql("CREATE VIEW testView AS SELECT i, j FROM parTable")
        checkAnswer(sql("SELECT * FROM testView"), Row(1, "a"))
      }
    }
  }

  test("create view for joined tables") {
    // make sure the new flag can handle some complex cases like join and schema change.
    withTable("jt1", "jt2") {
      spark.range(1, 10).toDF("id1").write.format("json").saveAsTable("jt1")
      spark.range(1, 10).toDF("id2").write.format("json").saveAsTable("jt2")
      withView("testView") {
        sql("CREATE VIEW testView AS SELECT * FROM jt1 JOIN jt2 ON id1 == id2")
        checkAnswer(sql("SELECT * FROM testView ORDER BY id1"), (1 to 9).map(i => Row(i, i)))

        val df = (1 until 10).map(i => i -> i).toDF("id1", "newCol")
        df.write.format("json").mode(SaveMode.Overwrite).saveAsTable("jt1")
        checkAnswer(sql("SELECT * FROM testView ORDER BY id1"), (1 to 9).map(i => Row(i, i)))
      }
    }
  }

  test("CTE within view") {
    withView("cte_view") {
      sql("CREATE VIEW cte_view AS WITH w AS (SELECT 1 AS n) SELECT n FROM w")
      checkAnswer(sql("SELECT * FROM cte_view"), Row(1))
    }
  }

  test("Using view after switching current database") {
    withView("v") {
      sql("CREATE VIEW v AS SELECT * FROM jt")
      withTempDatabase { db =>
        activateDatabase(db) {
          // Should look up table `jt` in database `default`.
          checkAnswer(sql("SELECT * FROM default.v"), sql("SELECT * FROM default.jt"))

          // The new `jt` table shouldn't be scanned.
          sql("CREATE TABLE jt(key INT, value STRING) USING parquet")
          checkAnswer(sql("SELECT * FROM default.v"), sql("SELECT * FROM default.jt"))
        }
      }
    }
  }

  test("Using view after adding more columns") {
    withTable("add_col") {
      spark.range(10).write.saveAsTable("add_col")
      withView("v") {
        sql("CREATE VIEW v AS SELECT * FROM add_col")
        spark.range(10).select('id, 'id as 'a).write.mode("overwrite").saveAsTable("add_col")
        checkAnswer(sql("SELECT * FROM v"), spark.range(10).toDF())
      }
    }
  }

  test("error handling: fail if the referenced table or view is invalid") {
    withView("view1", "view2", "view3") {
      // Fail if the referenced table is defined in a invalid database.
      withTempDatabase { db =>
        withTable(s"$db.table1") {
          activateDatabase(db) {
            sql("CREATE TABLE table1(a int, b string) USING parquet")
            sql("CREATE VIEW default.view1 AS SELECT * FROM table1")
          }
        }
      }
      assertInvalidReference("SELECT * FROM view1")

      // Fail if the referenced table is invalid.
      withTable("table2") {
        sql("CREATE TABLE table2(a int, b string) USING parquet")
        sql("CREATE VIEW view2 AS SELECT * FROM table2")
      }
      assertInvalidReference("SELECT * FROM view2")

      // Fail if the referenced view is invalid.
      withView("testView") {
        sql("CREATE VIEW testView AS SELECT * FROM jt")
        sql("CREATE VIEW view3 AS SELECT * FROM testView")
      }
      assertInvalidReference("SELECT * FROM view3")
    }
  }

  test("correctly resolve a view in a self join") {
    withView("testView") {
      sql("CREATE VIEW testView AS SELECT * FROM jt")
      checkAnswer(
        sql("SELECT * FROM testView t1 JOIN testView t2 ON t1.id = t2.id ORDER BY t1.id"),
        (1 to 9).map(i => Row(i, i, i, i)))
    }
  }

  test("correctly handle a view with custom column names") {
    withTable("tab1") {
      spark.range(1, 10).selectExpr("id", "id + 1 id1").write.saveAsTable("tab1")
      withView("testView", "testView2") {
        sql("CREATE VIEW testView(x, y) AS SELECT * FROM tab1")

        // Correctly resolve a view with custom column names.
        checkAnswer(sql("SELECT * FROM testView ORDER BY x"), (1 to 9).map(i => Row(i, i + 1)))

        // Throw an AnalysisException if the number of columns don't match up.
        val e = intercept[AnalysisException] {
          sql("CREATE VIEW testView2(x, y, z) AS SELECT * FROM tab1")
        }.getMessage
        assert(e.contains("The number of columns produced by the SELECT clause (num: `2`) does " +
          "not match the number of column names specified by CREATE VIEW (num: `3`)."))

        // Correctly resolve a view when the referenced table schema changes.
        spark.range(1, 10).selectExpr("id", "id + id dummy", "id + 1 id1")
          .write.mode(SaveMode.Overwrite).saveAsTable("tab1")
        checkAnswer(sql("SELECT * FROM testView ORDER BY x"), (1 to 9).map(i => Row(i, i + 1)))

        // Throw an AnalysisException if the column name is not found.
        spark.range(1, 10).selectExpr("id", "id + 1 dummy")
          .write.mode(SaveMode.Overwrite).saveAsTable("tab1")
        intercept[AnalysisException](sql("SELECT * FROM testView"))
      }
    }
  }

  test("resolve a view when the dataTypes of referenced table columns changed") {
    withTable("tab1") {
      spark.range(1, 10).selectExpr("id", "id + 1 id1").write.saveAsTable("tab1")
      withView("testView") {
        sql("CREATE VIEW testView AS SELECT * FROM tab1")

        // Allow casting from IntegerType to LongType
        val df = (1 until 10).map(i => (i, i + 1)).toDF("id", "id1")
        df.write.format("json").mode(SaveMode.Overwrite).saveAsTable("tab1")
        checkAnswer(sql("SELECT * FROM testView ORDER BY id1"), (1 to 9).map(i => Row(i, i + 1)))

        // Casting from DoubleType to LongType might truncate, throw an AnalysisException.
        val df2 = (1 until 10).map(i => (i.toDouble, i.toDouble)).toDF("id", "id1")
        df2.write.format("json").mode(SaveMode.Overwrite).saveAsTable("tab1")
        intercept[AnalysisException](sql("SELECT * FROM testView"))

        // Can't cast from ArrayType to LongType, throw an AnalysisException.
        val df3 = (1 until 10).map(i => (i, Seq(i))).toDF("id", "id1")
        df3.write.format("json").mode(SaveMode.Overwrite).saveAsTable("tab1")
        intercept[AnalysisException](sql("SELECT * FROM testView"))
      }
    }
  }

  test("correctly handle a cyclic view reference") {
    withView("view1", "view2", "view3") {
      sql("CREATE VIEW view1 AS SELECT * FROM jt")
      sql("CREATE VIEW view2 AS SELECT * FROM view1")
      sql("CREATE VIEW view3 AS SELECT * FROM view2")

      // Detect cyclic view reference on ALTER VIEW.
      val e1 = intercept[AnalysisException] {
        sql("ALTER VIEW view1 AS SELECT * FROM view2")
      }.getMessage
      assert(e1.contains("Recursive view `default`.`view1` detected (cycle: `default`.`view1` " +
        "-> `default`.`view2` -> `default`.`view1`)"))

      // Detect the most left cycle when there exists multiple cyclic view references.
      val e2 = intercept[AnalysisException] {
        sql("ALTER VIEW view1 AS SELECT * FROM view3 JOIN view2")
      }.getMessage
      assert(e2.contains("Recursive view `default`.`view1` detected (cycle: `default`.`view1` " +
        "-> `default`.`view3` -> `default`.`view2` -> `default`.`view1`)"))

      // Detect cyclic view reference on CREATE OR REPLACE VIEW.
      val e3 = intercept[AnalysisException] {
        sql("CREATE OR REPLACE VIEW view1 AS SELECT * FROM view2")
      }.getMessage
      assert(e3.contains("Recursive view `default`.`view1` detected (cycle: `default`.`view1` " +
        "-> `default`.`view2` -> `default`.`view1`)"))

      // Detect cyclic view reference from subqueries.
      val e4 = intercept[AnalysisException] {
        sql("ALTER VIEW view1 AS SELECT * FROM jt WHERE EXISTS (SELECT 1 FROM view2)")
      }.getMessage
      assert(e4.contains("Recursive view `default`.`view1` detected (cycle: `default`.`view1` " +
        "-> `default`.`view2` -> `default`.`view1`)"))
    }
  }

  test("restrict the nested level of a view") {
    val viewNames = Array.range(0, 11).map(idx => s"view$idx")
    withView(viewNames: _*) {
      sql("CREATE VIEW view0 AS SELECT * FROM jt")
      Array.range(0, 10).foreach { idx =>
        sql(s"CREATE VIEW view${idx + 1} AS SELECT * FROM view$idx")
      }

      withSQLConf("spark.sql.view.maxNestedViewDepth" -> "10") {
        val e = intercept[AnalysisException] {
          sql("SELECT * FROM view10")
        }.getMessage
        assert(e.contains("The depth of view `default`.`view0` exceeds the maximum view " +
          "resolution depth (10). Analysis is aborted to avoid errors. Increase the value " +
          "of spark.sql.view.maxNestedViewDepth to work aroud this."))
      }

      val e = intercept[IllegalArgumentException] {
        withSQLConf("spark.sql.view.maxNestedViewDepth" -> "0") {}
      }.getMessage
      assert(e.contains("The maximum depth of a view reference in a nested view must be " +
        "positive."))
    }
  }

  test("permanent view should be case-preserving") {
    withView("v") {
      sql("CREATE VIEW v AS SELECT 1 as aBc")
      assert(spark.table("v").schema.head.name == "aBc")

      sql("CREATE OR REPLACE VIEW v AS SELECT 2 as cBa")
      assert(spark.table("v").schema.head.name == "cBa")
    }
  }
}
