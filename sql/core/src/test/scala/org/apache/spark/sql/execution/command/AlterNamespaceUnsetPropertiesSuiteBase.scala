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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SupportsNamespaces}
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `ALTER NAMESPACE ... UNSET PROPERTIES` command
 * that check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are
 * located in more specific test suites:
 *
 *   - V2 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v2.AlterNamespaceUnsetPropertiesSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.AlterNamespaceUnsetPropertiesSuiteBase`
 *     - V1 In-Memory catalog:
 *       `org.apache.spark.sql.execution.command.v1.AlterNamespaceUnsetPropertiesSuite`
 *     - V1 Hive External catalog:
 *        `org.apache.spark.sql.hive.execution.command.AlterNamespaceUnsetPropertiesSuite`
 */
trait AlterNamespaceUnsetPropertiesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER NAMESPACE ... UNSET PROPERTIES"

  protected def namespace: String

  protected def getProperties(namespace: String): String = {
    val propsRow = sql(s"DESCRIBE NAMESPACE EXTENDED $namespace")
      .toDF("key", "value")
      .where("key like 'Properties%'")
      .collect()
    assert(propsRow.length == 1)
    propsRow(0).getString(1)
  }

  test("namespace does not exist") {
    val ns = "not_exist"
    val e = intercept[AnalysisException] {
      sql(s"ALTER NAMESPACE $catalog.$ns UNSET PROPERTIES ('d')")
    }
    checkError(e,
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> s"`$catalog`.`$ns`"))
  }

  test("basic test") {
    val ns = s"$catalog.$namespace"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")
      assert(getProperties(ns) === "")
      sql(s"ALTER NAMESPACE $ns SET PROPERTIES ('a'='a', 'b'='b', 'c'='c')")
      assert(getProperties(ns) === "((a,a), (b,b), (c,c))")
      sql(s"ALTER NAMESPACE $ns UNSET PROPERTIES ('b')")
      assert(getProperties(ns) === "((a,a), (c,c))")

      // unset non-existent properties
      // it will be successful, ignoring non-existent properties
      sql(s"ALTER NAMESPACE $ns UNSET PROPERTIES ('b')")
      assert(getProperties(ns) === "((a,a), (c,c))")
    }
  }

  test("test reserved properties") {
    import SupportsNamespaces._
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val ns = s"$catalog.$namespace"
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "false")) {
      CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        withNamespace(ns) {
          sql(s"CREATE NAMESPACE $ns")
          val sqlText = s"ALTER NAMESPACE $ns UNSET PROPERTIES ('$key')"
          checkErrorMatchPVals(
            exception = intercept[ParseException] {
              sql(sqlText)
            },
            errorClass = "UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY",
            parameters = Map("property" -> key, "msg" -> ".*"),
            sqlState = None,
            context = ExpectedContext(
              fragment = sqlText,
              start = 0,
              stop = 37 + ns.length + key.length)
          )
        }
      }
    }
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "true")) {
      CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        withNamespace(ns) {
          // Set the location explicitly because v2 catalog may not set the default location.
          // Without this, `meta.get(key)` below may return null.
          sql(s"CREATE NAMESPACE $ns LOCATION 'tmp/prop_test'")
          assert(getProperties(ns) === "")
          sql(s"ALTER NAMESPACE $ns UNSET PROPERTIES ('$key')")
          assert(getProperties(ns) === "", s"$key is a reserved namespace property and ignored")
          val meta = spark.sessionState.catalogManager.catalog(catalog)
            .asNamespaceCatalog.loadNamespaceMetadata(namespace.split('.'))
          assert(!meta.get(key).contains("foo"),
            "reserved properties should not have side effects")
        }
      }
    }
  }
}
