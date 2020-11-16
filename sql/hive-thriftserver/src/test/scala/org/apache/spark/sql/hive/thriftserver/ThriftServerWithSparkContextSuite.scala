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

package org.apache.spark.sql.hive.thriftserver

import org.apache.spark.tags.GitHubActionsUnstableTest

@GitHubActionsUnstableTest
class ThriftServerWithSparkContextSuite extends SharedThriftServer {

  test("SPARK-29911: Uncache cached tables when session closed") {
    val cacheManager = spark.sharedState.cacheManager
    val globalTempDB = spark.sharedState.globalTempViewManager.database
    withJdbcStatement { statement =>
      statement.execute("CACHE TABLE tempTbl AS SELECT 1")
    }
    // the cached data of local temporary view should be uncached
    assert(cacheManager.isEmpty)
    try {
      withJdbcStatement { statement =>
        statement.execute("CREATE GLOBAL TEMP VIEW globalTempTbl AS SELECT 1, 2")
        statement.execute(s"CACHE TABLE $globalTempDB.globalTempTbl")
      }
      // the cached data of global temporary view shouldn't be uncached
      assert(!cacheManager.isEmpty)
    } finally {
      withJdbcStatement { statement =>
        statement.execute(s"UNCACHE TABLE IF EXISTS $globalTempDB.globalTempTbl")
      }
      assert(cacheManager.isEmpty)
    }
  }
}
