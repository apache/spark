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

package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class HiveStrategiesSuite extends SQLTestUtils with TestHiveSingleton {

  test("Test DetermineTableStats") {
    def assertStatsNonEmpty(catalog: SessionCatalog, cachedRelation: LogicalPlan): Unit = {
      assert(cachedRelation.isInstanceOf[LogicalRelation])
      assert(cachedRelation.asInstanceOf[LogicalRelation].catalogTable.nonEmpty)
      assert(cachedRelation.asInstanceOf[LogicalRelation].catalogTable.get.stats.nonEmpty)
    }

    withTable("t1") {
      sql("CREATE TABLE t1 (c1 bigint) STORED AS PARQUET")
      sql("INSERT INTO TABLE t1 VALUES (1)")
      sql("REFRESH TABLE t1")

      val catalog = spark.sessionState.catalog
      val defaultSizeInBytesValue = spark.sessionState.conf.defaultSizeInBytes
      val qualifiedTableName = QualifiedTableName(catalog.getCurrentDatabase, "t1")

      Seq(true, false, true).foreach { enableFallBack =>
        withSQLConf(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key -> enableFallBack.toString) {
          sql("SELECT * from t1").collect()
          val cachedRelation = catalog.getCachedTable(qualifiedTableName)
          assertStatsNonEmpty(catalog, cachedRelation)
          val stats = cachedRelation.asInstanceOf[LogicalRelation].catalogTable.get.stats.get
          if (enableFallBack) {
            assert(stats.sizeInBytes !== defaultSizeInBytesValue)
          } else {
            assert(stats.sizeInBytes === defaultSizeInBytesValue)
          }
        }
      }

      Seq(Long.MaxValue, 10000, 10).foreach { defaultSizeInBytes =>
        withSQLConf(SQLConf.DEFAULT_SIZE_IN_BYTES.key -> defaultSizeInBytes.toString) {
          sql("SELECT * from t1").collect()
          val cachedRelation = catalog.getCachedTable(qualifiedTableName)
          assertStatsNonEmpty(catalog, cachedRelation)
          val stats = cachedRelation.asInstanceOf[LogicalRelation].catalogTable.get.stats.get
          assert(stats.sizeInBytes === defaultSizeInBytes)
        }
      }
    }
  }
}
