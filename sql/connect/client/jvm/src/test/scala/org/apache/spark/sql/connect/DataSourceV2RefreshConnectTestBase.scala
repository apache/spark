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

import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession, SQLHelper}
import org.apache.spark.sql.connect.test.IntegrationTestUtils.isAssemblyJarsDirExists

/**
 * Shared infrastructure for DSv2 table refresh and pinning tests in Connect mode.
 *
 * In Connect, every action re-analyzes the plan on the server:
 *  - No stale QueryExecution (collect is NOT pinned)
 *  - Schema changes are picked up on every access
 *  - Type widening, column rename, column removal all succeed for DataFrames
 *  - Joins/unions always see consistent latest version
 */
trait DataSourceV2RefreshConnectTestBase
    extends QueryTest with RemoteSparkSession with SQLHelper {

  protected val T = "testcat.ns1.ns2.tbl"

  protected def setupTable(): Unit = {
    spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
    spark.sql(s"INSERT INTO $T VALUES (1, 100)")
  }

  protected def assumeCanRun(): Unit = {
    assume(spark != null && isAssemblyJarsDirExists, "Spark Connect server not available")
  }

  // In Connect, SQL temp views (SELECT *) capture column names.
  // Removing/renaming/retyping a captured column fails on re-analysis.
  // Adding columns or data is fine.
  // In Connect, DataFrames re-analyze on every action. ALL mods succeed for DFs.
  case class Mod(name: String, fn: String => Unit, sqlViewOk: Boolean, dfOk: Boolean)

  protected val mods: Seq[Mod] = Seq(
    Mod("data write",
      t => spark.sql(s"INSERT INTO $t VALUES (2, 200)"),
      sqlViewOk = true, dfOk = true),
    Mod("column addition",
      t => spark.sql(s"ALTER TABLE $t ADD COLUMN new_col INT"),
      sqlViewOk = true, dfOk = true),
    Mod("column removal",
      t => spark.sql(s"ALTER TABLE $t DROP COLUMN salary"),
      sqlViewOk = false, dfOk = true),
    Mod("column rename",
      t => spark.sql(s"ALTER TABLE $t RENAME COLUMN salary TO pay"),
      sqlViewOk = false, dfOk = true),
    Mod("type widening INT to BIGINT",
      t => spark.sql(s"ALTER TABLE $t ALTER COLUMN salary TYPE BIGINT"),
      sqlViewOk = false, dfOk = true),
    Mod("drop+add column same type",
      t => {
        spark.sql(s"ALTER TABLE $t DROP COLUMN salary")
        spark.sql(s"ALTER TABLE $t ADD COLUMN salary INT")
      },
      sqlViewOk = true, dfOk = true),
    Mod("drop+add column different type",
      t => {
        spark.sql(s"ALTER TABLE $t DROP COLUMN salary")
        spark.sql(s"ALTER TABLE $t ADD COLUMN salary STRING")
      },
      sqlViewOk = false, dfOk = true),
    Mod("drop/recreate table",
      t => {
        spark.sql(s"DROP TABLE $t")
        spark.sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      },
      sqlViewOk = true, dfOk = true))
}
