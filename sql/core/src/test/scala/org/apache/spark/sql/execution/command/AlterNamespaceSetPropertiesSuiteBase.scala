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
 * This base suite contains unified tests for the `ALTER NAMESPACE ... SET PROPERTIES` command that
 * check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are located
 * in more specific test suites:
 *
 *   - V2 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v2.AlterNamespaceSetPropertiesSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.AlterNamespaceSetPropertiesSuiteBase`
 *     - V1 In-Memory catalog:
 *       `org.apache.spark.sql.execution.command.v1.AlterNamespaceSetPropertiesSuite`
 *     - V1 Hive External catalog:
 *        `org.apache.spark.sql.hive.execution.command.AlterNamespaceSetPropertiesSuite`
 */
trait AlterNamespaceSetPropertiesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER NAMESPACE ... SET PROPERTIES"

  protected def namespace: String

  protected def notFoundMsgPrefix: String

  test("Namespace does not exist") {
    val ns = "not_exist"
    val message = intercept[AnalysisException] {
      sql(s"ALTER DATABASE $catalog.$ns SET PROPERTIES ('d'='d')")
    }.getMessage
    assert(message.contains(s"$notFoundMsgPrefix '$ns' not found"))
  }

  test("basic test") {
    val ns = s"$catalog.$namespace"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")
      assert(getProperties(ns) === "")
      sql(s"ALTER NAMESPACE $ns SET PROPERTIES ('a'='a', 'b'='b', 'c'='c')")
      assert(getProperties(ns) === "((a,a), (b,b), (c,c))")
      sql(s"ALTER DATABASE $ns SET PROPERTIES ('d'='d')")
      assert(getProperties(ns) === "((a,a), (b,b), (c,c), (d,d))")
    }
  }

  test("test with properties set while creating namespace") {
    val ns = s"$catalog.$namespace"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns WITH PROPERTIES ('a'='a','b'='b','c'='c')")
      assert(getProperties(ns) === "((a,a), (b,b), (c,c))")
      sql(s"ALTER NAMESPACE $ns SET PROPERTIES ('a'='b', 'b'='a')")
      assert(getProperties(ns) === "((a,b), (b,a), (c,c))")
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
          val exception = intercept[ParseException] {
            sql(s"ALTER NAMESPACE $ns SET PROPERTIES ('$key'='dummyVal')")
          }
          assert(exception.getMessage.contains(s"$key is a reserved namespace property"))
        }
      }
    }
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "true")) {
      CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        withNamespace(ns) {
          // Set the location explicitly because v2 catalog may not set the default location.
          // Without this, `meta.get(key)` below may return null.
          sql(s"CREATE NAMESPACE $ns LOCATION '/tmp'")
          assert(getProperties(ns) === "")
          sql(s"ALTER NAMESPACE $ns SET PROPERTIES ('$key'='foo')")
          assert(getProperties(ns) === "", s"$key is a reserved namespace property and ignored")
          val meta = spark.sessionState.catalogManager.catalog(catalog)
            .asNamespaceCatalog.loadNamespaceMetadata(namespace.split('.'))
          assert(!meta.get(key).contains("foo"),
            "reserved properties should not have side effects")
        }
      }
    }
  }

  protected def getProperties(namespace: String): String = {
    val propsRow = sql(s"DESCRIBE NAMESPACE EXTENDED $namespace")
      .toDF("key", "value")
      .where(s"key like 'Properties%'")
      .collect()
    assert(propsRow.length == 1)
    propsRow(0).getString(1)
  }
}
