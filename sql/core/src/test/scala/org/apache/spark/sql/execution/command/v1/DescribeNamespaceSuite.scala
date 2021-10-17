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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.execution.command

/**
 * This base suite contains unified tests for the `DESCRIBE NAMESPACE` command that checks V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.DescribeNamespaceSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.DescribeNamespaceSuite`
 */
trait DescribeNamespaceSuiteBase extends command.DescribeNamespaceSuiteBase {
  test("basic") {
    val namespaceNames = Seq("db1", "`database`")
    withNamespace(namespaceNames: _*) {
      namespaceNames.foreach { ns =>
        val dbNameWithoutBackTicks = cleanIdentifier(ns)
        val location = getDBPath(dbNameWithoutBackTicks)

        sql(s"CREATE NAMESPACE $ns")

        checkAnswer(
          sql(s"DESCRIBE NAMESPACE EXTENDED $ns")
            .toDF("key", "value")
            .where("key not like 'Owner%'"), // filter for consistency with in-memory catalog
          Row("Database Name", dbNameWithoutBackTicks) ::
            Row("Comment", "") ::
            Row("Location", CatalogUtils.URIToString(location)) ::
            Row("Properties", "") :: Nil)
      }
    }
  }
}

/**
 * The class contains tests for the `DESCRIBE NAMESPACE` command to check V1 In-Memory
 * table catalog.
 */
class DescribeNamespaceSuite extends DescribeNamespaceSuiteBase with CommandSuiteBase
