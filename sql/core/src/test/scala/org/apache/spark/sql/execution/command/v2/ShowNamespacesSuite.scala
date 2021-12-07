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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.BasicInMemoryTableCatalog
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf

/**
 * The class contains tests for the `SHOW NAMESPACES` command to check V2 table catalogs.
 */
class ShowNamespacesSuite extends command.ShowNamespacesSuiteBase with CommandSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat_no_namespace", classOf[BasicInMemoryTableCatalog].getName)

  test("IN namespace doesn't exist") {
    withSQLConf(SQLConf.DEFAULT_CATALOG.key -> catalog) {
      runShowNamespacesSql("SHOW NAMESPACES in dummy", Seq.empty)
    }
    runShowNamespacesSql(s"SHOW NAMESPACES in $catalog.ns1", Seq.empty)
    runShowNamespacesSql(s"SHOW NAMESPACES in $catalog.ns1.ns3", Seq.empty)
  }

  test("default v2 catalog doesn't support namespace") {
    withSQLConf(SQLConf.DEFAULT_CATALOG.key -> "testcat_no_namespace") {
      val errMsg = intercept[AnalysisException] {
        sql("SHOW NAMESPACES")
      }.getMessage
      assert(errMsg.contains("does not support namespaces"))
    }
  }

  test("v2 catalog doesn't support namespace") {
    val errMsg = intercept[AnalysisException] {
      sql("SHOW NAMESPACES in testcat_no_namespace")
    }.getMessage
    assert(errMsg.contains("does not support namespaces"))
  }
}
