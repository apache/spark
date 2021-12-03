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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `DROP NAMESPACE` command to check V2 table catalogs.
 */
class DropNamespaceSuite extends command.DropNamespaceSuiteBase with CommandSuiteBase {
  test("DropNamespace: drop non-empty namespace with a non-cascading mode") {
    sql(s"CREATE NAMESPACE $catalog.ns")
    sql(s"CREATE TABLE $catalog.ns.table (id bigint) $defaultUsing")
    checkNamespace(s"SHOW NAMESPACES IN $catalog", Seq("ns") ++ builtinTopNamespaces)

    def assertDropFails(): Unit = {
      val e = intercept[SparkException] {
        sql(s"DROP NAMESPACE $catalog.ns")
      }
      assert(e.getMessage.contains("Cannot drop a non-empty namespace: ns"))
    }

    // $catalog.ns.table is present, thus $catalog.ns cannot be dropped.
    assertDropFails()
    sql(s"DROP TABLE $catalog.ns.table")

    // Now that $catalog.ns is empty, it can be dropped.
    sql(s"DROP NAMESPACE $catalog.ns")
    checkNamespace(s"SHOW NAMESPACES IN $catalog", builtinTopNamespaces)
  }

  test("DropNamespace: Namespace does not exist") {
    // Namespace $catalog.unknown does not exist.
    val message = intercept[AnalysisException] {
      sql(s"DROP DATABASE $catalog.unknown")
    }.getMessage
    assert(message.contains(s"Namespace 'unknown' not found"))
  }
}
