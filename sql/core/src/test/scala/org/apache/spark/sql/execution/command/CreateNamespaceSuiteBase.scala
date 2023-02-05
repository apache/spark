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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, CatalogV2Util, SupportsNamespaces}
import org.apache.spark.sql.execution.command.DDLCommandTestUtils.V1_COMMAND_VERSION
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `CREATE NAMESPACE` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.CreateNamespaceSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.CreateNamespaceSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.CreateNamespaceSuite`
 *     - V1 Hive External catalog:
*        `org.apache.spark.sql.hive.execution.command.CreateNamespaceSuite`
 */
trait CreateNamespaceSuiteBase extends QueryTest with DDLCommandTestUtils {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override val command = "CREATE NAMESPACE"

  protected def namespace: String

  protected def namespaceArray: Array[String] = namespace.split('.')

  protected def notFoundMsgPrefix: String =
    if (commandVersion == V1_COMMAND_VERSION) "Database" else "Namespace"

  test("basic") {
    val ns = s"$catalog.$namespace"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")
      assert(getCatalog(catalog).asNamespaceCatalog.namespaceExists(namespaceArray))
    }
  }

  test("namespace with location") {
    val ns = s"$catalog.$namespace"
    withNamespace(ns) {
      withTempDir { tmpDir =>
        // The generated temp path is not qualified.
        val path = tmpDir.getCanonicalPath
        assert(!path.startsWith("file:/"))
        val sqlText = s"CREATE NAMESPACE $ns LOCATION ''"
        checkError(
          exception = intercept[SparkIllegalArgumentException] {
            sql(sqlText)
          },
          errorClass = "INVALID_EMPTY_LOCATION",
          parameters = Map("location" -> ""))
        val uri = new Path(path).toUri
        sql(s"CREATE NAMESPACE $ns LOCATION '$uri'")
        // Make sure the location is qualified.
        val expected = makeQualifiedPath(tmpDir.toString)
        assert("file" === expected.getScheme)
        assert(new Path(getNamespaceLocation(catalog, namespaceArray)).toUri === expected)
      }
    }
  }

  test("Namespace already exists") {
    val ns = s"$catalog.$namespace"

    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")

      val parsed = CatalystSqlParser.parseMultipartIdentifier(namespace)
        .map(part => quoteIdentifier(part)).mkString(".")

      val e = intercept[NamespaceAlreadyExistsException] {
        sql(s"CREATE NAMESPACE $ns")
      }
      checkError(e,
        errorClass = "SCHEMA_ALREADY_EXISTS",
        parameters = Map("schemaName" -> parsed))

      // The following will be no-op since the namespace already exists.
      sql(s"CREATE NAMESPACE IF NOT EXISTS $ns")
    }
  }

  test("CreateNameSpace: reserved properties") {
    import SupportsNamespaces._
    val ns = s"$catalog.$namespace"
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "false")) {
      CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        val exception = intercept[ParseException] {
          sql(s"CREATE NAMESPACE $ns WITH DBPROPERTIES('$key'='dummyVal')")
        }
        assert(exception.getMessage.contains(s"$key is a reserved namespace property"))
      }
    }
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "true")) {
      CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        withNamespace(ns) {
          sql(s"CREATE NAMESPACE $ns WITH DBPROPERTIES('$key'='foo')")
          assert(sql(s"DESC NAMESPACE EXTENDED $ns")
            .toDF("k", "v")
            .where("k='Properties'")
            .where("v=''")
            .count == 1, s"$key is a reserved namespace property and ignored")
          val meta =
            getCatalog(catalog).asNamespaceCatalog.loadNamespaceMetadata(namespaceArray)
          assert(meta.get(key) == null || !meta.get(key).contains("foo"),
            "reserved properties should not have side effects")
        }
      }
    }
  }

  protected def getNamespaceLocation(catalog: String, namespace: Array[String]): String = {
    val metadata = getCatalog(catalog).asNamespaceCatalog
      .loadNamespaceMetadata(namespace).asScala
    metadata(SupportsNamespaces.PROP_LOCATION)
  }

  private def getCatalog(name: String): CatalogPlugin = {
    spark.sessionState.catalogManager.catalog(name)
  }
}
