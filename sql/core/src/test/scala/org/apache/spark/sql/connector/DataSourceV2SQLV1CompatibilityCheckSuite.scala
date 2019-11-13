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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.sources.SimpleScanSource
import org.apache.spark.sql.test.SharedSparkSession

class DataSourceV2SQLV1CompatibilityCheckSuite extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter {

  before {
    spark.conf.set(V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[V2SessionCatalog].getName)
  }

  after {
    spark.sessionState.catalog.reset()
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
  }

  test("DeleteFrom: DELETE is only supported with v2 tables") {
    val v1Table = "tbl"
    withTable(v1Table) {
      sql(s"CREATE TABLE $v1Table" +
          s" USING ${classOf[SimpleScanSource].getName} OPTIONS (from=0,to=1)")
      val exc = intercept[AnalysisException] {
        sql(s"DELETE FROM $v1Table WHERE i = 2")
      }

      assert(exc.getMessage.contains("DELETE is only supported with v2 tables"))
    }
  }
}
