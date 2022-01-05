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

import java.net.URI

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.execution.command

/**
 * This base suite contains unified tests for the `CREATE NAMESPACE` command that checks V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.CreateNamespaceSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.CreateNamespaceSuite`
 */
trait CreateNamespaceSuiteBase extends command.CreateNamespaceSuiteBase
    with command.TestsV1AndV2Commands {
  override def namespace: String = "db"
  override def notFoundMsgPrefix: String = "Database"

  test("Create namespace using default warehouse path") {
    val ns = s"$catalog.$namespace"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")
      assert(makeQualifiedPath(getNamespaceLocation(catalog, namespaceArray))
        === getDBPath(namespace))
    }
  }

  private def getDBPath(dbName: String): URI = {
    val warehousePath = makeQualifiedPath(spark.sessionState.conf.warehousePath)
    new Path(CatalogUtils.URIToString(warehousePath), s"$dbName.db").toUri
  }
}

/**
 * The class contains tests for the `CREATE NAMESPACE` command to check V1 In-Memory
 * table catalog.
 */
class CreateNamespaceSuite extends CreateNamespaceSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[CreateNamespaceSuiteBase].commandVersion
}
