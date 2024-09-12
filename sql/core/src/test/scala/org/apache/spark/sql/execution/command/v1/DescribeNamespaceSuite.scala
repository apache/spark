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
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
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
trait DescribeNamespaceSuiteBase extends command.DescribeNamespaceSuiteBase
    with command.TestsV1AndV2Commands {
  override def notFoundMsgPrefix: String = "Database"

  test("basic") {
    val ns = "db1"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")

      val result = sql(s"DESCRIBE NAMESPACE EXTENDED $ns")
        .toDF("key", "value")
        .where("key not like 'Owner%'") // filter for consistency with in-memory catalog
        .collect()

      val namePrefix = if (conf.useV1Command) "Database" else "Namespace"
      assert(result.length == 5)
      assert(result(0) === Row(s"Catalog Name", SESSION_CATALOG_NAME))
      assert(result(1) === Row(s"$namePrefix Name", ns))
      assert(result(2) === Row("Comment", ""))
      // Check only the key for "Location" since its value depends on warehouse path, etc.
      assert(result(3).getString(0) === "Location")
      assert(result(4) === Row("Properties", ""))
    }
  }

  Seq(true).foreach { hasCollations =>
    test(s"DESCRIBE NAMESPACE EXTENDED with collation specified = $hasCollations") {
      withNamespace("ns") {
        val defaultCollation = if (hasCollations) "DEFAULT COLLATION uNiCoDe" else ""

        sql(s"CREATE NAMESPACE ns $defaultCollation")
        val descriptionDf = sql(s"DESCRIBE NAMESPACE EXTENDED ns")
          .where("info_name = 'Collation'")

        if (hasCollations) {
          checkAnswer(descriptionDf, Seq(Row("Collation", "UNICODE")))
        } else {
          assert(descriptionDf.isEmpty)
        }
      }
    }
  }
}

/**
 * The class contains tests for the `DESCRIBE NAMESPACE` command to check V1 In-Memory
 * table catalog.
 */
class DescribeNamespaceSuite extends DescribeNamespaceSuiteBase with CommandSuiteBase {
  override def notFoundMsgPrefix: String = if (conf.useV1Command) "Database" else "Namespace"
  override def commandVersion: String = super[DescribeNamespaceSuiteBase].commandVersion
}
