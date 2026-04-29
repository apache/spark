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

package org.apache.spark.sql.execution.command

import java.util.Locale

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}

/**
 * Unified tests for `CREATE VIEW` against V1 (session) and V2 view catalogs. Source data lives
 * at `spark_catalog.default.t` for both paths.
 */
trait CreateViewSuiteBase extends QueryTest with DDLCommandTestUtils {
  import testImplicits._
  override val command: String = "CREATE VIEW"

  protected def namespace: String = "default"

  protected def withSourceTable(values: Int*)(body: => Unit): Unit = {
    withTable("spark_catalog.default.src") {
      values.toSeq.toDF("x").write.saveAsTable("spark_catalog.default.src")
      body
    }
  }

  /**
   * Seed a non-view table at `qualified` (full `catalog.ns.name`) and run `body`. Same SQL
   * for v1 and v2 -- `InMemoryTableViewCatalog.createTable` accepts the parquet TableInfo
   * the same way the session catalog does, so both legs share this implementation.
   */
  protected final def withSeededTable(qualified: String)(body: => Unit): Unit = {
    withTable(qualified) {
      sql(s"CREATE TABLE $qualified (col STRING) USING parquet")
      body
    }
  }

  test("CREATE VIEW persists the body and the SELECT round-trips") {
    val view = s"$catalog.$namespace.v_create_basic"
    withSourceTable(1, 2, 3) {
      sql(s"CREATE VIEW $view AS SELECT x FROM spark_catalog.default.src WHERE x > 1")
      checkAnswer(spark.table(view), Seq(Row(2), Row(3)))
    }
  }

  test("CREATE VIEW IF NOT EXISTS is a no-op when the view already exists") {
    val view = s"$catalog.$namespace.v_create_ifne"
    withSourceTable(1, 2, 3) {
      sql(s"CREATE VIEW $view AS SELECT x FROM spark_catalog.default.src")
      // Re-running with IF NOT EXISTS must succeed without changing the body.
      sql(s"CREATE VIEW IF NOT EXISTS $view AS " +
        s"SELECT x + 100 AS x FROM spark_catalog.default.src")
      checkAnswer(spark.table(view), Seq(Row(1), Row(2), Row(3)))
    }
  }

  test("CREATE VIEW without IF NOT EXISTS fails when the view exists") {
    val view = s"$catalog.$namespace.v_create_dup"
    withSourceTable(1) {
      sql(s"CREATE VIEW $view AS SELECT x FROM spark_catalog.default.src")
      intercept[AnalysisException] {
        sql(s"CREATE VIEW $view AS SELECT x FROM spark_catalog.default.src")
      }
    }
  }

  test("CREATE OR REPLACE VIEW replaces the body of an existing view") {
    val view = s"$catalog.$namespace.v_create_replace"
    withSourceTable(1, 2, 3) {
      sql(s"CREATE VIEW $view AS " +
        s"SELECT x FROM spark_catalog.default.src WHERE x > 10")
      checkAnswer(spark.table(view), Seq.empty[Row])
      sql(s"CREATE OR REPLACE VIEW $view AS " +
        s"SELECT x FROM spark_catalog.default.src WHERE x > 1")
      checkAnswer(spark.table(view), Seq(Row(2), Row(3)))
    }
  }

  test("CREATE VIEW with a user-specified column list aliases the output") {
    val view = s"$catalog.$namespace.v_create_cols"
    withSourceTable(1, 2) {
      sql(s"CREATE VIEW $view (alpha, beta) AS " +
        s"SELECT x AS xa, (x + 1) AS xb FROM spark_catalog.default.src")
      val cols = spark.table(view).schema.fieldNames.toSeq
      assert(cols == Seq("alpha", "beta"))
    }
  }

  test("CREATE VIEW rejects too-few user-specified columns") {
    val view = s"$catalog.$namespace.v_create_few"
    withSourceTable(1) {
      intercept[AnalysisException] {
        sql(s"CREATE VIEW $view (a) AS " +
          s"SELECT x AS xa, x AS xb FROM spark_catalog.default.src")
      }
    }
  }

  test("CREATE VIEW rejects too-many user-specified columns") {
    val view = s"$catalog.$namespace.v_create_many"
    withSourceTable(1) {
      intercept[AnalysisException] {
        sql(s"CREATE VIEW $view (a, b, c) AS SELECT x FROM spark_catalog.default.src")
      }
    }
  }

  test("CREATE VIEW rejects reference to a temporary function") {
    val view = s"$catalog.$namespace.v_create_tempfn"
    withSourceTable(1, 2, 3) {
      spark.udf.register("temp_udf_create", (i: Int) => i + 1)
      val ex = intercept[AnalysisException] {
        sql(s"CREATE VIEW $view AS " +
          s"SELECT temp_udf_create(x) FROM spark_catalog.default.src")
      }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("temporary"))
    }
  }

  test("CREATE VIEW rejects reference to a temporary view") {
    val view = s"$catalog.$namespace.v_create_tempview"
    withTempView("tv_create") {
      spark.range(3).createOrReplaceTempView("tv_create")
      val ex = intercept[AnalysisException] {
        sql(s"CREATE VIEW $view AS SELECT id FROM tv_create")
      }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("temporary"))
    }
  }

  test("CREATE VIEW rejects reference to a temporary variable") {
    val view = s"$catalog.$namespace.v_create_tempvar"
    sql("DECLARE OR REPLACE VARIABLE temp_var_create INT DEFAULT 1")
    try {
      val ex = intercept[AnalysisException] {
        sql(s"CREATE VIEW $view AS SELECT temp_var_create AS x")
      }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("temporary"))
    } finally {
      sql("DROP TEMPORARY VARIABLE IF EXISTS temp_var_create")
    }
  }

  test("CREATE OR REPLACE VIEW detects a direct cyclic reference") {
    val a = s"$catalog.$namespace.v_create_cycle_a"
    val b = s"$catalog.$namespace.v_create_cycle_b"
    withSourceTable(1, 2, 3) {
      sql(s"CREATE VIEW $a AS SELECT x FROM spark_catalog.default.src")
      sql(s"CREATE VIEW $b AS SELECT x FROM $a")
      val ex = intercept[AnalysisException] {
        sql(s"CREATE OR REPLACE VIEW $a AS SELECT x FROM $b")
      }
      assert(ex.getCondition == "RECURSIVE_VIEW")
    }
  }

  test("CREATE VIEW over a non-view table entry surfaces the v1-parity errors") {
    // v1-parity error conditions when CREATE [OR REPLACE | IF NOT EXISTS] VIEW collides
    // with an existing non-view table at the same identifier. Running on both v1 and v2
    // pins parity from each side -- v1 hits the conditions through the long-established
    // session-catalog path, v2 hits them through the new `CreateV2ViewExec`.
    val view = s"$catalog.$namespace.v_create_table_collide"
    withSeededTable(view) {
      // CREATE OR REPLACE VIEW must not silently destroy a non-view table.
      val replaceEx = intercept[AnalysisException] {
        sql(s"CREATE OR REPLACE VIEW $view AS SELECT 1 AS col")
      }
      assert(replaceEx.getCondition == "EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE")

      // Plain CREATE VIEW over a table surfaces TABLE_OR_VIEW_ALREADY_EXISTS.
      val createEx = intercept[AnalysisException] {
        sql(s"CREATE VIEW $view AS SELECT 1 AS col")
      }
      assert(createEx.getCondition == "TABLE_OR_VIEW_ALREADY_EXISTS")

      // CREATE VIEW IF NOT EXISTS is a no-op -- the existing table is left alone.
      sql(s"CREATE VIEW IF NOT EXISTS $view AS SELECT 1 AS col")
    }
  }
}
