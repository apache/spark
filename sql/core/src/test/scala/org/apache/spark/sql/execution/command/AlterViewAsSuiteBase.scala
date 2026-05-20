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
 * Unified tests for `ALTER VIEW ... AS` against V1 (session) and V2 view catalogs.
 */
trait AlterViewAsSuiteBase extends QueryTest with DDLCommandTestUtils {
  import testImplicits._
  override val command: String = "ALTER VIEW ... AS"

  protected def namespace: String = "default"

  protected def withSourceTable(values: Int*)(body: => Unit): Unit = {
    withTable("spark_catalog.default.alter_src") {
      values.toSeq.toDF("x").write.saveAsTable("spark_catalog.default.alter_src")
      body
    }
  }

  test("ALTER VIEW updates the body of an existing view") {
    val view = s"$catalog.$namespace.v_alter_body"
    withSourceTable(1, 2, 3) {
      sql(s"CREATE VIEW $view AS " +
        s"SELECT x FROM spark_catalog.default.alter_src WHERE x > 10")
      checkAnswer(spark.table(view), Seq.empty[Row])
      sql(s"ALTER VIEW $view AS " +
        s"SELECT x FROM spark_catalog.default.alter_src WHERE x > 1")
      checkAnswer(spark.table(view), Seq(Row(2), Row(3)))
    }
  }

  test("ALTER VIEW on a missing view fails at analysis") {
    val view = s"$catalog.$namespace.v_alter_missing"
    intercept[AnalysisException] {
      sql(s"ALTER VIEW $view AS SELECT 1 AS x")
    }
  }

  test("ALTER VIEW rejects reference to a temporary function") {
    val view = s"$catalog.$namespace.v_alter_tempfn"
    withSourceTable(1, 2, 3) {
      sql(s"CREATE VIEW $view AS SELECT x FROM spark_catalog.default.alter_src")
      spark.udf.register("temp_udf_alter", (i: Int) => i + 1)
      val ex = intercept[AnalysisException] {
        sql(s"ALTER VIEW $view AS " +
          s"SELECT temp_udf_alter(x) FROM spark_catalog.default.alter_src")
      }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("temporary"))
    }
  }

  test("ALTER VIEW rejects reference to a temporary view") {
    val view = s"$catalog.$namespace.v_alter_tempview"
    withSourceTable(1) {
      sql(s"CREATE VIEW $view AS SELECT x FROM spark_catalog.default.alter_src")
      withTempView("tv_alter") {
        spark.range(3).createOrReplaceTempView("tv_alter")
        val ex = intercept[AnalysisException] {
          sql(s"ALTER VIEW $view AS SELECT id AS x FROM tv_alter")
        }
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("temporary"))
      }
    }
  }

  test("ALTER VIEW rejects reference to a temporary variable") {
    val view = s"$catalog.$namespace.v_alter_tempvar"
    withSourceTable(1) {
      sql(s"CREATE VIEW $view AS SELECT x FROM spark_catalog.default.alter_src")
      sql("DECLARE OR REPLACE VARIABLE temp_var_alter INT DEFAULT 1")
      try {
        val ex = intercept[AnalysisException] {
          sql(s"ALTER VIEW $view AS SELECT temp_var_alter AS x")
        }
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("temporary"))
      } finally {
        sql("DROP TEMPORARY VARIABLE IF EXISTS temp_var_alter")
      }
    }
  }

  test("ALTER VIEW preserves user-set TBLPROPERTIES") {
    val view = s"$catalog.$namespace.v_alter_keep_props"
    withSourceTable(1, 2, 3) {
      sql(s"CREATE VIEW $view " +
        s"TBLPROPERTIES ('key1' = 'val1') AS " +
        s"SELECT x FROM spark_catalog.default.alter_src")
      sql(s"ALTER VIEW $view SET TBLPROPERTIES ('key2' = 'val2')")
      sql(s"ALTER VIEW $view AS SELECT x + 1 AS x FROM spark_catalog.default.alter_src")
      val rows = sql(s"SHOW TBLPROPERTIES $view").collect()
      val pairs = rows.map(r => r.getString(0) -> r.getString(1)).toMap
      assert(pairs.get("key1").contains("val1"))
      assert(pairs.get("key2").contains("val2"))
    }
  }

  test("ALTER VIEW preserves SCHEMA EVOLUTION binding mode") {
    val view = s"$catalog.$namespace.v_alter_keep_schema_mode"
    withSourceTable(1, 2, 3) {
      sql(s"CREATE VIEW $view WITH SCHEMA EVOLUTION AS " +
        s"SELECT x FROM spark_catalog.default.alter_src")
      sql(s"ALTER VIEW $view AS SELECT x + 1 AS x FROM spark_catalog.default.alter_src")
      val ddl = sql(s"SHOW CREATE TABLE $view").collect().head.getString(0)
      assert(ddl.contains("WITH SCHEMA EVOLUTION"),
        s"schema-binding mode lost across ALTER VIEW AS:\n$ddl")
    }
  }

  test("CREATE OR REPLACE VIEW with a body referencing a missing table fails") {
    val view = s"$catalog.$namespace.v_alter_bad_body"
    withSourceTable(1) {
      sql(s"CREATE VIEW $view AS SELECT x FROM spark_catalog.default.alter_src")
      intercept[AnalysisException] {
        sql(s"CREATE OR REPLACE VIEW $view AS SELECT x FROM does_not_exist_at_all")
      }
    }
  }

  test("ALTER VIEW detects a direct cyclic reference") {
    val a = s"$catalog.$namespace.v_alter_cycle_a"
    val b = s"$catalog.$namespace.v_alter_cycle_b"
    withSourceTable(1, 2, 3) {
      sql(s"CREATE VIEW $a AS SELECT x FROM spark_catalog.default.alter_src")
      sql(s"CREATE VIEW $b AS SELECT x FROM $a")
      val ex = intercept[AnalysisException] {
        sql(s"ALTER VIEW $a AS SELECT x FROM $b")
      }
      assert(ex.getCondition == "RECURSIVE_VIEW")
    }
  }
}
