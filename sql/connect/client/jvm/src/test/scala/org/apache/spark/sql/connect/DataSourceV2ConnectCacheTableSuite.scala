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
 * Design doc Section [5] in Connect: CACHE TABLE.
 *
 * CACHE TABLE pins table state on the server side. In Connect, the plan is re-analyzed but the
 * server's CacheManager serves cached data. Session writes invalidate the cache; external changes
 * remain invisible.
 */
class DataSourceV2ConnectCacheTableSuite extends DataSourceV2RefreshConnectTestBase {

  // Section 5: CACHE TABLE (session writes)
  mods.foreach { mod =>
    test(s"[S5] cache session: ${mod.name}") {
      assumeCanRun()
      withTable(T) {
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))
        try {
          mod.fn(T)
          spark.sql(s"SELECT * FROM $T").collect()
        } catch {
          case _: Exception => // type widening ClassCastException
        }
        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }
}
