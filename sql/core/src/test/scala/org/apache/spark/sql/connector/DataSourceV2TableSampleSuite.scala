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

package org.apache.spark.sql.connector

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.{InMemoryTableWithJoinAndSampleCatalog, InMemoryTableWithLegacyTableSampleCatalog, InMemoryTableWithTableSampleCatalog}
import org.apache.spark.sql.internal.SQLConf

class DataSourceV2TableSampleSuite extends DatasourceV2SQLBase
  with DataSourcePushdownTestUtils {

  private val sampleCatalog = "testsample"

  private def withSampleTable(testFunc: String => Unit): Unit = {
    registerCatalog(sampleCatalog, classOf[InMemoryTableWithTableSampleCatalog])
    val tableName = s"$sampleCatalog.ns.sample_tbl"
    sql(s"CREATE TABLE $tableName (id bigint, data string) USING _")
    try {
      sql(s"INSERT INTO $tableName VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
      testFunc(tableName)
    } finally {
      sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  test("SPARK-55978: TABLESAMPLE SYSTEM pushdown removes Sample node") {
    withSampleTable { table =>
      val df = sql(s"SELECT * FROM $table TABLESAMPLE SYSTEM (50 PERCENT)")
      checkSamplePushed(df, pushed = true)
      checkPushedInfo(df, "SYSTEM SAMPLE (50.0) false SEED(")
    }
  }

  test("SPARK-55978: TABLESAMPLE BERNOULLI pushdown removes Sample node") {
    withSampleTable { table =>
      val df = sql(s"SELECT * FROM $table TABLESAMPLE BERNOULLI (50 PERCENT)")
      checkSamplePushed(df, pushed = true)
      checkPushedInfo(df, "BERNOULLI SAMPLE (50.0) false SEED(")
    }
  }

  test("SPARK-55978: TABLESAMPLE default (no qualifier) pushdown removes Sample node") {
    withSampleTable { table =>
      val df = sql(s"SELECT * FROM $table TABLESAMPLE (50 PERCENT)")
      checkSamplePushed(df, pushed = true)
    }
  }

  test("SPARK-55978: TABLESAMPLE SYSTEM 0 PERCENT returns no rows") {
    withSampleTable { table =>
      val df = sql(s"SELECT * FROM $table TABLESAMPLE SYSTEM (0 PERCENT)")
      checkSamplePushed(df, pushed = true)
      assert(df.collect().isEmpty)
    }
  }

  test("SPARK-55978: TABLESAMPLE SYSTEM 100 PERCENT returns all rows") {
    withSampleTable { table =>
      val df = sql(s"SELECT * FROM $table TABLESAMPLE SYSTEM (100 PERCENT)")
      checkSamplePushed(df, pushed = true)
      assert(df.collect().length == 5)
    }
  }

  test("SPARK-55978: TABLESAMPLE SYSTEM composes with projection") {
    withSampleTable { table =>
      val df = sql(s"SELECT id FROM $table TABLESAMPLE SYSTEM (100 PERCENT)")
      checkSamplePushed(df, pushed = true)
      assert(df.columns.sameElements(Array("id")))
      assert(df.collect().length == 5)
    }
  }

  test("SPARK-55978: TABLESAMPLE on non-pushdown catalog falls back to Sample node") {
    val table = "testcat.ns.no_sample_tbl"
    sql(s"CREATE TABLE $table (id bigint, data string) USING _")
    try {
      sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      val df = sql(s"SELECT * FROM $table TABLESAMPLE (50 PERCENT)")
      // testcat uses InMemoryCatalog which does NOT implement SupportsPushDownTableSample,
      // so the Sample node should remain in the plan.
      checkSamplePushed(df, pushed = false)
    } finally {
      sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  test("SPARK-55978: TABLESAMPLE SYSTEM on non-pushdown catalog errors") {
    val table = "testcat.ns.no_sample_tbl"
    sql(s"CREATE TABLE $table (id bigint, data string) USING _")
    try {
      sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      // testcat uses InMemoryCatalog whose ScanBuilder does not implement
      // SupportsPushDownTableSample, so SYSTEM sampling cannot be pushed down.
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM $table TABLESAMPLE SYSTEM (50 PERCENT)").collect()
        },
        condition = "UNSUPPORTED_FEATURE.TABLESAMPLE_SYSTEM")
    } finally {
      sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  test("SPARK-55978: TABLESAMPLE SYSTEM on subquery errors") {
    withSampleTable { table =>
      // SYSTEM sampling requires a direct table scan; applying it to a derived
      // query (here an aggregate) means there is no ScanBuilderHolder to push into.
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM (SELECT id, count(*) AS cnt FROM $table GROUP BY id) " +
            s"TABLESAMPLE SYSTEM (50 PERCENT)").collect()
        },
        condition = "UNSUPPORTED_FEATURE.TABLESAMPLE_SYSTEM_NO_SCAN")
    }
  }

  test("SPARK-55978: TABLESAMPLE SYSTEM with WHERE filter errors") {
    withSampleTable { table =>
      // A WHERE clause between the Sample and the scan produces a non-empty filter list
      // in PhysicalOperation, which falls through to the catch-all error branch.
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM (SELECT * FROM $table WHERE id > 1) " +
            s"TABLESAMPLE SYSTEM (50 PERCENT)").collect()
        },
        condition = "UNSUPPORTED_FEATURE.TABLESAMPLE_SYSTEM_NO_SCAN")
    }
  }

  test("SPARK-55978: TABLESAMPLE SYSTEM on DSv1 table errors") {
    withTable("dsv1_tbl") {
      sql("CREATE TABLE dsv1_tbl (id bigint, data string) USING parquet")
      sql("INSERT INTO dsv1_tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      // DSv1 tables have no ScanBuilderHolder, so SYSTEM sampling cannot be pushed down.
      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM dsv1_tbl TABLESAMPLE SYSTEM (50 PERCENT)").collect()
        },
        condition = "UNSUPPORTED_FEATURE.TABLESAMPLE_SYSTEM_NO_SCAN")
    }
  }

  test("SPARK-55978: join pushdown is skipped when a side has a pushed sample") {
    val joinSampleCatalog = "testjoinsample"
    registerCatalog(joinSampleCatalog, classOf[InMemoryTableWithJoinAndSampleCatalog])
    val t1 = s"$joinSampleCatalog.ns.t1"
    val t2 = s"$joinSampleCatalog.ns.t2"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING _")
    sql(s"CREATE TABLE $t2 (id bigint, data string) USING _")
    try {
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      sql(s"INSERT INTO $t2 VALUES (2, 'x'), (3, 'y'), (4, 'z')")
      withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
        // Without sample: join should be pushed down
        val dfNoSample = sql(s"SELECT * FROM $t1 JOIN $t2 ON $t1.id = $t2.id")
        checkJoinPushed(dfNoSample)

        // With SYSTEM sample on one side: join pushdown should be skipped
        val dfWithSample = sql(
          s"SELECT * FROM $t1 TABLESAMPLE SYSTEM (100 PERCENT) " +
          s"JOIN $t2 ON $t1.id = $t2.id")
        checkJoinNotPushed(dfWithSample)
        // The sample should still be pushed down though
        checkSamplePushed(dfWithSample, pushed = true)
      }
    } finally {
      sql(s"DROP TABLE IF EXISTS $t1")
      sql(s"DROP TABLE IF EXISTS $t2")
    }
  }

  test("SPARK-55978: legacy connector with only 4-arg pushTableSample - BERNOULLI pushes down") {
    val legacyCatalog = "testlegacysample"
    registerCatalog(legacyCatalog, classOf[InMemoryTableWithLegacyTableSampleCatalog])
    val tableName = s"$legacyCatalog.ns.legacy_tbl"
    sql(s"CREATE TABLE $tableName (id bigint, data string) USING _")
    try {
      sql(s"INSERT INTO $tableName VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      // BERNOULLI should push down via the default 5-arg method delegating to 4-arg
      val dfBernoulli = sql(s"SELECT * FROM $tableName TABLESAMPLE (50 PERCENT)")
      checkSamplePushed(dfBernoulli, pushed = true)

      // SYSTEM should fail because the default 5-arg method returns false for SYSTEM,
      // and SYSTEM requires successful pushdown.
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM $tableName TABLESAMPLE SYSTEM (50 PERCENT)").collect()
        },
        condition = "UNSUPPORTED_FEATURE.TABLESAMPLE_SYSTEM")
    } finally {
      sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }
}
