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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.execution.command

trait DropTableSuiteBase extends command.DropTableSuiteBase {
  test("purge option") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")

      createTable(s"$catalog.ns.tbl")
      checkTables("ns", "tbl")

      sql(s"DROP TABLE $catalog.ns.tbl PURGE")
      checkTables("ns") // no tables
    }
  }
}

class DropTableSuite extends DropTableSuiteBase with CommandSuiteBase {
  // The test fails in Hive External catalog with:
  // org.apache.spark.sql.AnalysisException:
  //   spark_catalog.ns.tbl is not a valid TableIdentifier as it has more than 2 name parts.
  test("SPARK-33305: DROP TABLE should also invalidate cache") {
    val t = s"$catalog.ns.tbl"
    val view = "view"
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      withTempView(view, "source") {
        val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
        df.createOrReplaceTempView("source")
        sql(s"CREATE TABLE $t $defaultUsing AS SELECT id, data FROM source")
        sql(s"CACHE TABLE $view AS SELECT id FROM $t")
        checkAnswer(sql(s"SELECT * FROM $t"), spark.table("source").collect())
        checkAnswer(
          sql(s"SELECT * FROM $view"),
          spark.table("source").select("id").collect())

        assert(!spark.sharedState.cacheManager.lookupCachedData(spark.table(view)).isEmpty)
        sql(s"DROP TABLE $t")
        assert(spark.sharedState.cacheManager.lookupCachedData(spark.table(view)).isEmpty)
      }
    }
  }
}

