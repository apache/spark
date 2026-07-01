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
package org.apache.spark.sql.connect

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession, SQLHelper}
import org.apache.spark.sql.functions.current_path

/**
 * End-to-end coverage for the SQL Standard PATH feature over Spark Connect.
 *
 * SET PATH and the frozen-path semantics for persisted views / SQL functions are implemented
 * entirely server-side, but the analyzer state (`AnalysisContext`) that carries the pinned path
 * must survive plan reification across the gRPC boundary. These tests run the public surface over
 * a real Connect client so regressions there are caught:
 *   - `SET PATH = ...` is parsed and applied to the session,
 *   - `current_path()` (SQL and the DataFrame builtin) reflects it,
 *   - a persisted view created under one path resolves its body under the frozen path even when
 *     the invoker switches the session path.
 */
class SqlPathE2ETestSuite extends ConnectFunSuite with RemoteSparkSession with SQLHelper {

  test("SET PATH and current_path() round-trip over Connect") {
    withSQLConf("spark.sql.path.enabled" -> "true") {
      try {
        spark.sql("SET PATH = spark_catalog.default, system.builtin")
        val sqlPath = spark.sql("SELECT current_path()").head().getString(0)
        assert(
          sqlPath == "spark_catalog.default,system.builtin",
          s"current_path() over Connect should reflect SET PATH; got: $sqlPath")

        // DataFrame builtin should agree with the SQL form.
        val apiPath = spark.range(1).select(current_path()).head().getString(0)
        assert(
          apiPath == sqlPath,
          s"functions.current_path() should match SQL current_path(); got: $apiPath vs $sqlPath")
      } finally {
        spark.sql("SET PATH = DEFAULT_PATH")
      }
    }
  }

  test("Persisted view body uses frozen path over Connect") {
    withSQLConf("spark.sql.path.enabled" -> "true") {
      withDatabase("connect_path_a", "connect_path_b") {
        spark.sql("CREATE DATABASE connect_path_a")
        spark.sql("CREATE DATABASE connect_path_b")
        spark.sql("CREATE TABLE connect_path_a.frozen_t USING parquet AS SELECT 1 AS id")
        spark.sql("CREATE TABLE connect_path_b.frozen_t USING parquet AS SELECT 2 AS id")
        withView("default.v_path_connect") {
          try {
            // Create the view under PATH=a.
            spark.sql("SET PATH = spark_catalog.connect_path_a, system.builtin")
            spark.sql("CREATE VIEW default.v_path_connect AS SELECT id FROM frozen_t")

            // Switch the session path to b; bare `frozen_t` now resolves through b,
            // but the view's frozen path keeps it pinned to a.
            spark.sql("SET PATH = spark_catalog.connect_path_b, system.builtin")
            val bare = spark.sql("SELECT id FROM frozen_t").head().getInt(0)
            assert(bare == 2, s"Bare `frozen_t` should follow live PATH=b; got: $bare")
            val viaView = spark.sql("SELECT id FROM default.v_path_connect").head().getInt(0)
            assert(
              viaView == 1,
              s"View body should resolve via the frozen creation-time PATH; got: $viaView")
          } finally {
            spark.sql("SET PATH = DEFAULT_PATH")
          }
        }
      }
    }
  }

  test("SET PATH is rejected over Connect when feature is disabled") {
    withSQLConf("spark.sql.path.enabled" -> "false") {
      val ex = intercept[AnalysisException] {
        spark.sql("SET PATH = spark_catalog.default")
      }
      assert(
        ex.getCondition == "UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED",
        s"Expected SET_PATH_WHEN_DISABLED, got: ${ex.getCondition}")
    }
  }
}
