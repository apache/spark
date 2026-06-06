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
import org.apache.spark.sql.connector.catalog.ValidatingInMemoryTableCatalog
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `CREATE NAMESPACE` command to check V2 table catalogs.
 */
class CreateNamespaceSuite extends command.CreateNamespaceSuiteBase with CommandSuiteBase {
  override def namespace: String = "ns1.ns2"

  // A test catalog whose createNamespace validates before checking existence; used to
  // exercise CreateNamespaceExec's IF NOT EXISTS recovery path.
  private val validatingCatalog: String = "validating_test_catalog"

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$validatingCatalog",
      classOf[ValidatingInMemoryTableCatalog].getName)

  test("SPARK-55250: IF NOT EXISTS is a no-op on pre-existing namespace even when the " +
    "catalog raises a non-NamespaceAlreadyExistsException error") {
    val ns = s"$validatingCatalog.$namespace"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")
      // Without the IF NOT EXISTS recovery path, this would surface the catalog's
      // pre-existence validation error.
      sql(s"CREATE NAMESPACE IF NOT EXISTS $ns")
    }
  }
}
