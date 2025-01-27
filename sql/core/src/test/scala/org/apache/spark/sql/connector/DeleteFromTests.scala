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

import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.sources.SimpleScanSource

/**
 * A collection of "DELETE" tests that can be run through the SQL APIs.
 */
trait DeleteFromTests extends DatasourceV2SQLBase {

  protected val catalogAndNamespace: String

  test("DeleteFrom with v2 filtering: basic - delete all") {
    val t = s"${catalogAndNamespace}tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t")
      checkAnswer(spark.table(t), Seq())
    }
  }

  test("DeleteFrom with v2 filtering: basic - delete with where clause") {
    val t = s"${catalogAndNamespace}tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t WHERE id = 2")
      checkAnswer(spark.table(t), Seq(
        Row(3, "c", 3)))
    }
  }

  test("DeleteFrom with v2 filtering: delete from aliased target table") {
    val t = s"${catalogAndNamespace}tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t AS tbl WHERE tbl.id = 2")
      checkAnswer(spark.table(t), Seq(
        Row(3, "c", 3)))
    }
  }

  test("DeleteFrom with v2 filtering: normalize attribute names") {
    val t = s"${catalogAndNamespace}tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t AS tbl WHERE tbl.ID = 2")
      checkAnswer(spark.table(t), Seq(
        Row(3, "c", 3)))
    }
  }

  test("DeleteFrom with v2 filtering: fail if has subquery") {
    val t = s"${catalogAndNamespace}tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      val exc = intercept[AnalysisException] {
        sql(s"DELETE FROM $t WHERE id IN (SELECT id FROM $t)")
      }

      assert(spark.table(t).count() === 3)
      assert(exc.getMessage.contains("Delete by condition with subquery is not supported"))
    }
  }

  test("DeleteFrom with v2 filtering: delete with unsupported predicates") {
    val t = s"${catalogAndNamespace}tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      val exc = intercept[AnalysisException] {
        sql(s"DELETE FROM $t WHERE id > 3 AND p > 3")
      }

      assert(spark.table(t).count() === 3)
      assert(exc.getMessage.contains(s"Cannot delete from table $t"))
    }
  }

  test("DeleteFrom: DELETE is only supported with v2 tables") {
    // use the default v2 session catalog.
    spark.conf.set(V2_SESSION_CATALOG_IMPLEMENTATION, "builtin")
    val v1Table = "tbl"
    withTable(v1Table) {
      sql(s"CREATE TABLE $v1Table" +
        s" USING ${classOf[SimpleScanSource].getName} OPTIONS (from=0,to=1)")
      val exc = intercept[AnalysisException] {
        sql(s"DELETE FROM $v1Table WHERE i = 2")
      }

      checkError(
        exception = exc,
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        sqlState = "0A000",
        parameters = Map("tableName" -> "`spark_catalog`.`default`.`tbl`",
          "operation" -> "DELETE"))
    }
  }

  test("SPARK-33652: DeleteFrom should refresh caches referencing the table") {
    val t = s"${catalogAndNamespace}tbl"
    val view = "view"
    withTable(t) {
      withTempView(view) {
        sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
        sql(s"CACHE TABLE view AS SELECT id FROM $t")
        assert(spark.table(view).count() == 3)

        sql(s"DELETE FROM $t WHERE id = 2")
        assert(spark.table(view).count() == 1)
      }
    }
  }
}
