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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

/**
 * A suite for testing view related functionality.
 */
class SQLViewSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import spark.implicits._

  override def beforeAll(): Unit = {
    // Create a simple table with two columns: id and id1
    spark.range(1, 10).selectExpr("id", "id id1").write.format("json").saveAsTable("jt")
  }

  override def afterAll(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS jt")
  }

  test("nested views (interleaved with temporary views)") {
    withView("jtv1", "jtv2", "jtv3", "temp_jtv1", "temp_jtv2", "temp_jtv3") {
      sql("CREATE VIEW jtv1 AS SELECT * FROM jt WHERE id > 3")
      sql("CREATE VIEW jtv2 AS SELECT * FROM jtv1 WHERE id < 6")
      checkAnswer(sql("select count(*) FROM jtv2"), Row(2))

      // Checks temporary views
      sql("CREATE TEMPORARY VIEW temp_jtv1 AS SELECT * FROM jt WHERE id > 3")
      sql("CREATE TEMPORARY VIEW temp_jtv2 AS SELECT * FROM temp_jtv1 WHERE id < 6")
      checkAnswer(sql("select count(*) FROM temp_jtv2"), Row(2))

      // Checks interleaved temporary view and normal view
      sql("CREATE TEMPORARY VIEW temp_jtv3 AS SELECT * FROM jt WHERE id > 3")
      sql("CREATE VIEW jtv3 AS SELECT * FROM temp_jtv3 WHERE id < 6")
      checkAnswer(sql("select count(*) FROM jtv3"), Row(2))
    }
  }

  test("error handling: fail if the view sql itself is invalid") {
    // A table that does not exist
    intercept[AnalysisException] {
      sql("CREATE OR REPLACE VIEW myabcdview AS SELECT * FROM table_not_exist1345").collect()
    }

    // A column that does not exist
    intercept[AnalysisException] {
      sql("CREATE OR REPLACE VIEW myabcdview AS SELECT random1234 FROM jt").collect()
    }
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
     // A table that does not exist for temporary view
    intercept[AnalysisException] {
      sql("CREATE OR REPLACE TEMPORARY VIEW myabcdview AS SELECT * FROM table_not_exist1345")
    }

    // A column that does not exist, for temporary view
    intercept[AnalysisException] {
      sql("CREATE OR REPLACE TEMPORARY VIEW myabcdview AS SELECT random1234 FROM jt")
    }
  }

  test("correctly parse CREATE VIEW statement") {
    withSQLConf(SQLConf.NATIVE_VIEW.key -> "true") {
      sql(
        """CREATE VIEW IF NOT EXISTS
          |default.testView (c1 COMMENT 'blabla', c2 COMMENT 'blabla')
          |TBLPROPERTIES ('a' = 'b')
          |AS SELECT * FROM jt""".stripMargin)
      checkAnswer(sql("SELECT c1, c2 FROM testView ORDER BY c1"), (1 to 9).map(i => Row(i, i)))
      sql("DROP VIEW testView")
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
    withSQLConf(SQLConf.NATIVE_VIEW.key -> "true") {
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

  test("should not allow ALTER VIEW AS when the view does not exist") {
    intercept[NoSuchTableException](
      sql("ALTER VIEW testView AS SELECT 1, 2")
    )

    intercept[NoSuchTableException](
      sql("ALTER VIEW default.testView AS SELECT 1, 2")
    )
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

  Seq(true, false).foreach { enabled =>
    val prefix = (if (enabled) "With" else "Without") + " canonical native view: "
    test(s"$prefix correctly handle CREATE OR REPLACE VIEW") {
      withSQLConf(
        SQLConf.NATIVE_VIEW.key -> "true", SQLConf.CANONICAL_NATIVE_VIEW.key -> enabled.toString) {
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
    }

    test(s"$prefix correctly handle ALTER VIEW") {
      withSQLConf(
        SQLConf.NATIVE_VIEW.key -> "true", SQLConf.CANONICAL_NATIVE_VIEW.key -> enabled.toString) {
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
    }

    test(s"$prefix create hive view for json table") {
      // json table is not hive-compatible, make sure the new flag fix it.
      withSQLConf(
        SQLConf.NATIVE_VIEW.key -> "true", SQLConf.CANONICAL_NATIVE_VIEW.key -> enabled.toString) {
        withView("testView") {
          sql("CREATE VIEW testView AS SELECT id FROM jt")
          checkAnswer(sql("SELECT * FROM testView ORDER BY id"), (1 to 9).map(i => Row(i)))
        }
      }
    }

    test(s"$prefix create hive view for partitioned parquet table") {
      // partitioned parquet table is not hive-compatible, make sure the new flag fix it.
      withSQLConf(
        SQLConf.NATIVE_VIEW.key -> "true", SQLConf.CANONICAL_NATIVE_VIEW.key -> enabled.toString) {
        withTable("parTable") {
          withView("testView") {
            val df = Seq(1 -> "a").toDF("i", "j")
            df.write.format("parquet").partitionBy("i").saveAsTable("parTable")
            sql("CREATE VIEW testView AS SELECT i, j FROM parTable")
            checkAnswer(sql("SELECT * FROM testView"), Row(1, "a"))
          }
        }
      }
    }
  }

  test("CTE within view") {
    withSQLConf(
      SQLConf.NATIVE_VIEW.key -> "true", SQLConf.CANONICAL_NATIVE_VIEW.key -> "true") {
      withView("cte_view") {
        sql("CREATE VIEW cte_view AS WITH w AS (SELECT 1 AS n) SELECT n FROM w")
        checkAnswer(sql("SELECT * FROM cte_view"), Row(1))
      }
    }
  }

  test("Using view after switching current database") {
    withSQLConf(
      SQLConf.NATIVE_VIEW.key -> "true", SQLConf.CANONICAL_NATIVE_VIEW.key -> "true") {
      withView("v") {
        sql("CREATE VIEW v AS SELECT * FROM src")
        withTempDatabase { db =>
          activateDatabase(db) {
            // Should look up table `src` in database `default`.
            checkAnswer(sql("SELECT * FROM default.v"), sql("SELECT * FROM default.src"))

            // The new `src` table shouldn't be scanned.
            sql("CREATE TABLE src(key INT, value STRING)")
            checkAnswer(sql("SELECT * FROM default.v"), sql("SELECT * FROM default.src"))
          }
        }
      }
    }
  }

  test("Using view after adding more columns") {
    withSQLConf(
      SQLConf.NATIVE_VIEW.key -> "true", SQLConf.CANONICAL_NATIVE_VIEW.key -> "true") {
      withTable("add_col") {
        spark.range(10).write.saveAsTable("add_col")
        withView("v") {
          sql("CREATE VIEW v AS SELECT * FROM add_col")
          spark.range(10).select('id, 'id as 'a).write.mode("overwrite").saveAsTable("add_col")
          checkAnswer(sql("SELECT * FROM v"), spark.range(10).toDF())
        }
      }
    }
  }

  test("create hive view for joined tables") {
    // make sure the new flag can handle some complex cases like join and schema change.
    withSQLConf(SQLConf.NATIVE_VIEW.key -> "true") {
      withTable("jt1", "jt2") {
        spark.range(1, 10).toDF("id1").write.format("json").saveAsTable("jt1")
        spark.range(1, 10).toDF("id2").write.format("json").saveAsTable("jt2")
        sql("CREATE VIEW testView AS SELECT * FROM jt1 JOIN jt2 ON id1 == id2")
        checkAnswer(sql("SELECT * FROM testView ORDER BY id1"), (1 to 9).map(i => Row(i, i)))

        val df = (1 until 10).map(i => i -> i).toDF("id1", "newCol")
        df.write.format("json").mode(SaveMode.Overwrite).saveAsTable("jt1")
        checkAnswer(sql("SELECT * FROM testView ORDER BY id1"), (1 to 9).map(i => Row(i, i)))

        sql("DROP VIEW testView")
      }
    }
  }

  test("SPARK-14933 - create view from hive parquet tabale") {
    withTable("t_part") {
      withView("v_part") {
        spark.sql("create table t_part stored as parquet as select 1 as a, 2 as b")
        spark.sql("create view v_part as select * from t_part")
        checkAnswer(
          sql("select * from t_part"),
          sql("select * from v_part"))
      }
    }
  }

  test("SPARK-14933 - create view from hive orc tabale") {
    withTable("t_orc") {
      withView("v_orc") {
        spark.sql("create table t_orc stored as orc as select 1 as a, 2 as b")
        spark.sql("create view v_orc as select * from t_orc")
        checkAnswer(
          sql("select * from t_orc"),
          sql("select * from v_orc"))
      }
    }
  }
}
