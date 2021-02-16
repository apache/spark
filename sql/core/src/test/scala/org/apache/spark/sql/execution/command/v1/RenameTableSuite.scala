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
import org.apache.spark.storage.StorageLevel

/**
 * This base suite contains unified tests for the `RENAME TABLE` command that check V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.RenameTableSuite`
 *   - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.RenameTableSuite`
 */
trait RenameTableSuiteBase extends command.RenameTableSuiteBase {

}

/**
 * The class contains tests for the `RENAME TABLE` command to check V1 In-Memory table catalog.
 */
class RenameTableSuite extends RenameTableSuiteBase with CommandSuiteBase {
  test("SPARK-33786: Cache's storage level should be respected when a table name is altered") {
    import testImplicits._
    withNamespaceAndTable("ns", "src_tbl") { src =>
      withNamespaceAndTable("ns", "dst_tbl") { dst =>
        withTempPath { path =>
          def getStorageLevel(tableName: String): StorageLevel = {
            val table = spark.table(tableName)
            val cachedData = spark.sharedState.cacheManager.lookupCachedData(table).get
            cachedData.cachedRepresentation.cacheBuilder.storageLevel
          }

          Seq(1 -> "a").toDF("i", "j").write.parquet(path.getCanonicalPath)
          sql(s"CREATE TABLE $src $defaultUsing LOCATION '${path.toURI}'")
          sql(s"CACHE TABLE $src OPTIONS('storageLevel' 'MEMORY_ONLY')")
          val oldStorageLevel = getStorageLevel(src)

          sql(s"ALTER TABLE $src RENAME TO $dst")
          val newStorageLevel = getStorageLevel(dst)
          assert(oldStorageLevel === newStorageLevel)
        }
      }
    }
  }
}
