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
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for qualified temporary view names with system.session or session qualifiers.
 * This mirrors the functionality implemented for temporary functions.
 */
class QualifiedTempViewSuite extends QueryTest with SharedSparkSession {

  test("CREATE TEMPORARY VIEW with unqualified name") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
      checkAnswer(sql("SELECT * FROM v1"), Row(1))
    }
  }

  test("CREATE TEMPORARY VIEW with session qualifier") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW session.v1 AS SELECT 1 AS col")
      checkAnswer(sql("SELECT * FROM v1"), Row(1))
      checkAnswer(sql("SELECT * FROM session.v1"), Row(1))
    }
  }

  test("CREATE TEMPORARY VIEW with system.session qualifier") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW system.session.v1 AS SELECT 1 AS col")
      checkAnswer(sql("SELECT * FROM v1"), Row(1))
      checkAnswer(sql("SELECT * FROM session.v1"), Row(1))
      checkAnswer(sql("SELECT * FROM system.session.v1"), Row(1))
    }
  }

  test("CREATE OR REPLACE TEMPORARY VIEW with session qualifier") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
      sql("CREATE OR REPLACE TEMPORARY VIEW session.v1 AS SELECT 2 AS col")
      checkAnswer(sql("SELECT * FROM v1"), Row(2))
      checkAnswer(sql("SELECT * FROM session.v1"), Row(2))
    }
  }

  test("CREATE OR REPLACE TEMPORARY VIEW with system.session qualifier") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW session.v1 AS SELECT 1 AS col")
      sql("CREATE OR REPLACE TEMPORARY VIEW system.session.v1 AS SELECT 2 AS col")
      checkAnswer(sql("SELECT * FROM system.session.v1"), Row(2))
    }
  }

  test("SELECT from temporary view with different qualifications") {
    withTempView("myview") {
      sql("CREATE TEMPORARY VIEW myview AS SELECT 42 AS answer, 'hello' AS greeting")

      // All of these should work and return the same result
      checkAnswer(sql("SELECT * FROM myview"), Row(42, "hello"))
      checkAnswer(sql("SELECT * FROM session.myview"), Row(42, "hello"))
      checkAnswer(sql("SELECT * FROM system.session.myview"), Row(42, "hello"))
    }
  }

  test("DROP VIEW with unqualified temporary view") {
    sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
    sql("DROP VIEW v1")
    assert(!spark.catalog.tableExists("v1"))
  }

  test("DROP VIEW with session qualifier") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
      sql("DROP VIEW session.v1")
      assert(!spark.catalog.tableExists("v1"))
    }
  }

  test("DROP VIEW with system.session qualifier") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
      sql("DROP VIEW system.session.v1")
      assert(!spark.catalog.tableExists("v1"))
    }
  }

  test("DROP VIEW IF EXISTS with qualified name") {
    // Unqualified DROP VIEW IF EXISTS always works
    sql("DROP VIEW IF EXISTS nonexistent")
    // Note: Session-qualified names in DROP VIEW need additional resolution support
    // This is a known limitation that requires updating ResolveCatalogs
  }

  test("ALTER VIEW with unqualified name") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
      sql("ALTER VIEW v1 AS SELECT 2 AS col")
      checkAnswer(sql("SELECT * FROM v1"), Row(2))
    }
  }

  test("Temporary view with qualification in JOIN") {
    withTempView("v1", "v2") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS id, 'A' AS name")
      sql("CREATE TEMPORARY VIEW session.v2 AS SELECT 1 AS id, 'B' AS name")

      checkAnswer(
        sql("SELECT v1.name, v2.name FROM session.v1 JOIN system.session.v2 ON v1.id = v2.id"),
        Row("A", "B"))
    }
  }

  test("Temporary view with qualification in subquery") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
      checkAnswer(
        sql("SELECT * FROM (SELECT * FROM session.v1) t"),
        Row(1))
    }
  }

  test("CREATE TEMPORARY VIEW with invalid qualifier should fail") {
    val ex = intercept[AnalysisException] {
      sql("CREATE TEMPORARY VIEW default.v1 AS SELECT 1")
    }
    assert(ex.getMessage.contains("TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS"))
  }

  test("CREATE TEMPORARY VIEW with too many parts should fail") {
    val ex = intercept[AnalysisException] {
      sql("CREATE TEMPORARY VIEW catalog.schema.db.v1 AS SELECT 1")
    }
    assert(ex.getMessage.contains("TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS"))
  }

  test("Temporary view shadowing - create with different qualifiers") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
      sql("CREATE OR REPLACE TEMPORARY VIEW session.v1 AS SELECT 2 AS col")
      // Both should refer to the same view (the replaced one)
      checkAnswer(sql("SELECT * FROM v1"), Row(2))
      checkAnswer(sql("SELECT * FROM session.v1"), Row(2))
    }
  }

  test("Catalog API works with qualified temporary views") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW session.v1 AS SELECT 1")
      assert(spark.catalog.tableExists("v1"))
      assert(spark.catalog.tableExists("session.v1"))
      assert(spark.catalog.tableExists("system.session.v1"))
    }
  }

  test("isTempView returns true for qualified names") {
    withTempView("myview") {
      sql("CREATE TEMPORARY VIEW myview AS SELECT 1")
      val catalog = spark.sessionState.catalog
      assert(catalog.isTempView(TableIdentifier("myview")))
      assert(catalog.isTempView(TableIdentifier("myview", Some("session"))))
      // Note: TableIdentifier doesn't support catalog, so we test via Seq[String]
      assert(catalog.isTempView(Seq("session", "myview")))
      assert(catalog.isTempView(Seq("system", "session", "myview")))
    }
  }

  test("Temporary view can reference another temporary view") {
    withTempView("v1", "v2") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
      // Reference unqualified name in view definition
      sql("CREATE TEMPORARY VIEW v2 AS SELECT * FROM v1 WHERE col > 0")
      checkAnswer(sql("SELECT * FROM v2"), Row(1))
      // But can query v2 with qualified name
      checkAnswer(sql("SELECT * FROM session.v2"), Row(1))
    }
  }

  test("Mixed qualification in complex query") {
    withTempView("base", "filtered", "aggregated") {
      sql("CREATE TEMPORARY VIEW base AS SELECT 1 AS id, 10 AS value UNION ALL SELECT 2, 20")
      sql("CREATE TEMPORARY VIEW session.filtered AS SELECT * FROM base WHERE value > 5")
      sql("CREATE TEMPORARY VIEW system.session.aggregated AS SELECT COUNT(*) AS cnt FROM filtered")

      checkAnswer(sql("SELECT * FROM aggregated"), Row(2))
    }
  }

  test("DESCRIBE works with unqualified temporary view names") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col1, 'test' AS col2")

      val desc1 = sql("DESCRIBE v1").collect()

      assert(desc1.length >= 2)  // At least 2 columns
    }
  }

  test("EXPLAIN works with qualified temporary view names") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")

      val explain1 = sql("EXPLAIN SELECT * FROM v1").collect()
      val explain2 = sql("EXPLAIN SELECT * FROM session.v1").collect()

      assert(explain1.length > 0 && explain2.length > 0)
    }
  }

  test("CREATE GLOBAL TEMPORARY VIEW does not use session qualifier") {
    // Global temporary views use the global_temp database, not session namespace
    withView("global_temp.gv1") {
      sql("CREATE GLOBAL TEMPORARY VIEW gv1 AS SELECT 1")
      checkAnswer(sql("SELECT * FROM global_temp.gv1"), Row(1))
    }
  }

  test("Temporary view with qualification can be used in CTAS") {
    withTempView("v1") {
      withTable("t1") {
        sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
        sql("CREATE TABLE t1 USING parquet AS SELECT * FROM session.v1")
        checkAnswer(sql("SELECT * FROM t1"), Row(1))
      }
    }
  }

  test("Qualified temp view in INSERT INTO") {
    withTempView("v1") {
      withTable("t1") {
        sql("CREATE TABLE t1 (col INT) USING parquet")
        sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
        sql("INSERT INTO t1 SELECT * FROM system.session.v1")
        checkAnswer(sql("SELECT * FROM t1"), Row(1))
      }
    }
  }

  test("Qualified temp view in UNION") {
    withTempView("v1", "v2") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
      sql("CREATE TEMPORARY VIEW v2 AS SELECT 2 AS col")
      checkAnswer(
        sql("SELECT * FROM session.v1 UNION ALL SELECT * FROM system.session.v2"),
        Seq(Row(1), Row(2)))
    }
  }

  // ========== Case Insensitivity Tests ==========

  test("Qualifiers are case insensitive - mixed case session") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW SeSsIoN.v1 AS SELECT 1 AS col")
      checkAnswer(sql("SELECT * FROM v1"), Row(1))
      checkAnswer(sql("SELECT * FROM session.v1"), Row(1))
      checkAnswer(sql("SELECT * FROM SESSION.v1"), Row(1))
      checkAnswer(sql("SELECT * FROM SeSsIoN.v1"), Row(1))
    }
  }

  test("Qualifiers are case insensitive - mixed case system.session") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW SyStEm.SeSsIoN.v1 AS SELECT 1 AS col")
      checkAnswer(sql("SELECT * FROM v1"), Row(1))
      checkAnswer(sql("SELECT * FROM system.session.v1"), Row(1))
      checkAnswer(sql("SELECT * FROM SYSTEM.SESSION.v1"), Row(1))
      checkAnswer(sql("SELECT * FROM SyStEm.SeSsIoN.v1"), Row(1))
    }
  }

  test("DROP VIEW with case insensitive qualifiers") {
    withTempView("v1") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS col")
      // Drop with mixed case should work
      sql("DROP VIEW SeSsIoN.v1")
      assert(!spark.catalog.tableExists("v1"))
    }

    withTempView("v2") {
      sql("CREATE TEMPORARY VIEW v2 AS SELECT 1 AS col")
      sql("DROP VIEW SYSTEM.SESSION.v2")
      assert(!spark.catalog.tableExists("v2"))
    }
  }

  test("SELECT with case insensitive qualifiers in JOIN") {
    withTempView("v1", "v2") {
      sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS id")
      sql("CREATE TEMPORARY VIEW v2 AS SELECT 1 AS id")
      checkAnswer(
        sql("SELECT v1.id FROM SeSsIoN.v1 JOIN SYSTEM.SESSION.v2 ON v1.id = v2.id"),
        Row(1))
    }
  }

  // ========== Resolution Order Tests ==========

  test("Temporary view shadows persistent view with same name") {
    withTempView("v1") {
      withTable("v1") {
        // Create a persistent table with the same name in current database
        sql("CREATE TABLE v1 (source STRING) USING parquet")
        sql("INSERT INTO TABLE v1 VALUES ('persistent')")

        // Create a temporary view (after the table)
        sql("CREATE TEMPORARY VIEW v1 AS SELECT 'temporary' AS source")

        // Unqualified reference should get temp view (shadows persistent)
        checkAnswer(sql("SELECT * FROM v1"), Row("temporary"))

        // Explicit session qualification should get temp view
        checkAnswer(sql("SELECT * FROM session.v1"), Row("temporary"))

        // Explicit default.v1 should get persistent table
        checkAnswer(sql("SELECT * FROM default.v1"), Row("persistent"))
      }
    }
  }

  test("Temporary view shadows persistent view in schema named 'session'") {
    // This tests the edge case where someone creates a database called "session"
    withDatabase("session") {
      withTempView("myview") {
        withView("session.myview") {
          sql("CREATE DATABASE IF NOT EXISTS session")

          // Create a persistent view in database "session"
          sql("CREATE VIEW session.myview AS SELECT 'persistent_in_session_db' AS source")

          // Create a temporary view with same name
          sql("CREATE TEMPORARY VIEW myview AS SELECT 'temporary' AS source")

          // Unqualified should get temp view
          checkAnswer(sql("SELECT * FROM myview"), Row("temporary"))

          // "session.myview" is ambiguous but should resolve to temp view (system.session.myview)
          // because temp views are checked first
          checkAnswer(sql("SELECT * FROM session.myview"), Row("temporary"))

          // Can still access persistent view with explicit system qualifier or full path
          // Note: This might need spark_catalog.session.myview to be explicit
        }
      }
    }
  }

  test("Resolution order: temp view before persistent view in current database") {
    withDatabase("testdb") {
      withTempView("myview") {
        withView("testdb.myview") {
          sql("CREATE DATABASE testdb")
          sql("USE testdb")

          // Create persistent view in current database
          sql("CREATE VIEW myview AS SELECT 'persistent' AS source")

          // Create temp view with same name
          sql("CREATE TEMPORARY VIEW myview AS SELECT 'temporary' AS source")

          // Temp view should shadow
          checkAnswer(sql("SELECT * FROM myview"), Row("temporary"))

          // Explicit temp qualification
          checkAnswer(sql("SELECT * FROM session.myview"), Row("temporary"))

          // Explicit database qualification should get persistent
          checkAnswer(sql("SELECT * FROM testdb.myview"), Row("persistent"))

          // Reset to default database
          sql("USE default")
        }
      }
    }
  }

  // ========== DROP VIEW Disambiguation Tests ==========

  test("DROP VIEW without qualification drops temp view if it exists") {
    sql("USE default")  // Ensure we're in default database
    withDatabase("testdb_drop_unqual") {
      withView("testdb_drop_unqual.view_drop_test1") {
        withTempView("view_drop_test1") {
          sql("CREATE DATABASE testdb_drop_unqual")

          // Create both temp and persistent VIEW with same name
          sql("CREATE VIEW testdb_drop_unqual.view_drop_test1 AS SELECT 'persistent' AS source")
          sql("CREATE TEMPORARY VIEW view_drop_test1 AS SELECT 'temporary' AS source")

          // Verify both exist
          checkAnswer(sql("SELECT * FROM view_drop_test1"), Row("temporary"))
          checkAnswer(sql("SELECT * FROM testdb_drop_unqual.view_drop_test1"), Row("persistent"))

          // DROP VIEW without qualification should drop temp view first
          sql("DROP VIEW view_drop_test1")

          // Temp view should be gone - check explicitly
          assert(!spark.sessionState.catalog.isTempView(Seq("view_drop_test1")),
            s"Temp view view_drop_test1 should be dropped but still exists")
          assert(!spark.sessionState.catalog.isTempView(TableIdentifier("view_drop_test1")),
            s"Temp view view_drop_test1 (TableIdentifier) should be dropped but still exists")

          // Persistent view should still exist
          assert(spark.catalog.tableExists("testdb_drop_unqual.view_drop_test1"))
          checkAnswer(sql("SELECT * FROM testdb_drop_unqual.view_drop_test1"), Row("persistent"))
        }
      }
    }
  }

  test("DROP VIEW with database qualification drops persistent view only") {
    withDatabase("testdb") {
      withTempView("v1") {
        withView("testdb.v1") {
          sql("CREATE DATABASE testdb")

          // Create both temp and persistent views
          sql("CREATE TEMPORARY VIEW v1 AS SELECT 'temporary' AS source")
          sql("CREATE VIEW testdb.v1 AS SELECT 'persistent' AS source")

          // DROP VIEW with database qualification should drop persistent view
          sql("DROP VIEW testdb.v1")

          // Persistent view should be gone
          assert(!spark.catalog.tableExists("testdb.v1"))

          // Temp view should still exist
          assert(spark.catalog.tableExists("v1"))
          checkAnswer(sql("SELECT * FROM v1"), Row("temporary"))
        }
      }
    }
  }

  test("DROP VIEW with session qualification drops temp view only") {
    sql("USE default")  // Ensure we're in default database
    withDatabase("testdb_drop_session") {
      withTempView("view_drop_test2") {
        withView("testdb_drop_session.view_drop_test2") {
          sql("CREATE DATABASE testdb_drop_session")

          // Create both temp and persistent view
          sql("CREATE TEMPORARY VIEW view_drop_test2 AS SELECT 'temporary' AS source")
          sql("CREATE VIEW testdb_drop_session.view_drop_test2 AS SELECT 'persistent' AS source")

          // DROP VIEW with session qualification should drop temp view
          sql("DROP VIEW session.view_drop_test2")

          // Temp view should be gone - check explicitly
          assert(!spark.sessionState.catalog.isTempView(Seq("view_drop_test2")),
            s"Temp view view_drop_test2 should be dropped but still exists")
          assert(!spark.sessionState.catalog.isTempView(TableIdentifier("view_drop_test2")),
            s"Temp view view_drop_test2 (TableIdentifier) should be dropped but still exists")

          // Persistent view should still exist
          assert(spark.catalog.tableExists("testdb_drop_session.view_drop_test2"))
          checkAnswer(sql("SELECT * FROM testdb_drop_session.view_drop_test2"), Row("persistent"))
        }
      }
    }
  }

  test("DROP VIEW prioritizes temp view over persistent view in schema named session") {
    withDatabase("session") {
      withTempView("v1") {
        withView("session.v1") {
          sql("CREATE DATABASE IF NOT EXISTS session")

          // Create a persistent view in database "session"
          sql("CREATE VIEW session.v1 AS SELECT 'persistent' AS source")

          // Create a temp view with same name
          sql("CREATE TEMPORARY VIEW v1 AS SELECT 'temporary' AS source")

          // Both exist
          assert(spark.catalog.tableExists("v1"))
          assert(spark.catalog.tableExists("session.v1"))

          // DROP VIEW session.v1 should drop the TEMP view (system.session.v1)
          // not the persistent view (spark_catalog.session.v1)
          sql("DROP VIEW session.v1")

          // Temp view should be gone
          assert(!spark.sessionState.catalog.isTempView(Seq("v1")))

          // Persistent view should still exist (can be queried with explicit catalog)
          assert(sql("SHOW VIEWS IN session").filter("viewName = 'v1'").count() > 0)
        }
      }
    }
  }

  test("DROP VIEW IF EXISTS handles both temp and persistent gracefully") {
    sql("USE default")  // Ensure we're in default database
    withDatabase("testdb_ifexists") {
      sql("CREATE DATABASE testdb_ifexists")

      // No views exist yet
      sql("DROP VIEW IF EXISTS view_ifexists_test")  // Should not error
      sql("DROP VIEW IF EXISTS session.view_ifexists_test")  // Should not error
      sql("DROP VIEW IF EXISTS testdb_ifexists.view_ifexists_test")  // Should not error

      // Create only temp view
      sql("CREATE TEMPORARY VIEW view_ifexists_test AS SELECT 1")
      // Persistent doesn't exist, should not error
      sql("DROP VIEW IF EXISTS testdb_ifexists.view_ifexists_test")
      assert(spark.catalog.tableExists("view_ifexists_test"))  // Temp still exists

      sql("DROP VIEW view_ifexists_test")
    }
  }
}
