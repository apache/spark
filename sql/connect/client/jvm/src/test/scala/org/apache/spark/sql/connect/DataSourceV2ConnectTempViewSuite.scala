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

import org.apache.spark.sql.Row

/**
 * Design doc Section [1] in Connect: Temp views with stored plans.
 *
 * In Connect, SQL temp views (CREATE OR REPLACE TEMP VIEW ... AS SELECT *) re-analyze on each
 * access. Column names captured at creation time constrain which schema changes are tolerated:
 * data writes and column additions succeed, but column removal, rename, type change, and drop+add
 * different type fail.
 */
class DataSourceV2ConnectTempViewSuite extends DataSourceV2RefreshConnectTestBase {

  // Section 1: SQL Temp View x All Modifications
  mods.foreach { mod =>
    test(s"[S1] SQL temp view: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        spark.sql(s"CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM $T")
        checkAnswer(spark.sql("SELECT * FROM tmp"), Seq(Row(1, 100)))
        mod.fn(T)
        if (mod.sqlViewOk) {
          spark.sql("SELECT * FROM tmp").collect()
        } else {
          assertThrows[Exception] {
            spark.sql("SELECT * FROM tmp").collect()
          }
        }
      }
    }
  }

  test("[connect edge] createOrReplaceTempView + schema change") {
    assumeCanRun()
    withTable(T) {
      setupTable()
      spark.sql(s"CREATE OR REPLACE TEMP VIEW tv AS SELECT * FROM $T")
      checkAnswer(spark.sql("SELECT * FROM tv"), Seq(Row(1, 100)))
      spark.sql(s"ALTER TABLE $T ADD COLUMN bonus INT")
      spark.sql(s"INSERT INTO $T VALUES (2, 200, 50)")
      // SQL view re-analyzes: SELECT * picks up new column
      val r = spark.sql("SELECT * FROM tv").collect()
      assert(r.length == 2)
    }
  }
}
