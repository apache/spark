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

package org.apache.spark.sql.jdbc.v2

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.logging.log4j.Level

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException
import org.apache.spark.sql.connector.catalog.{Identifier, NamespaceChange}
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.DockerIntegrationFunSuite
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.tags.DockerTest

@DockerTest
private[v2] trait V2JDBCNamespaceTest extends SharedSparkSession with DockerIntegrationFunSuite {
  val catalog = new JDBCTableCatalog()

  private val emptyProps: util.Map[String, String] = Collections.emptyMap[String, String]
  private val schema: StructType = new StructType()
    .add("id", IntegerType)
    .add("data", StringType)

  def builtinNamespaces: Array[Array[String]]

  def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    Array(namespace) ++ builtinNamespaces
  }

  def supportsSchemaComment: Boolean = true

  def supportsDropSchemaCascade: Boolean = true

  def supportsDropSchemaRestrict: Boolean = true

  def testListNamespaces(): Unit = {
    test("listNamespaces: basic behavior") {
      val commentMap = if (supportsSchemaComment) {
        Map("comment" -> "test comment")
      } else {
        Map.empty[String, String]
      }
      catalog.createNamespace(Array("foo"), commentMap.asJava)
      assert(catalog.listNamespaces() === listNamespaces(Array("foo")))
      assert(catalog.listNamespaces(Array("foo")) === Array())
      assert(catalog.namespaceExists(Array("foo")) === true)

      if (supportsSchemaComment) {
        val logAppender = new LogAppender("catalog comment")
        withLogAppender(logAppender) {
          catalog.alterNamespace(Array("foo"), NamespaceChange
            .setProperty("comment", "comment for foo"))
          catalog.alterNamespace(Array("foo"), NamespaceChange.removeProperty("comment"))
        }
        val createCommentWarning = logAppender.loggingEvents
          .filter(_.getLevel == Level.WARN)
          .map(_.getMessage.getFormattedMessage)
          .exists(_.contains("catalog comment"))
        assert(createCommentWarning === false)
      }

      if (supportsDropSchemaRestrict) {
        catalog.dropNamespace(Array("foo"), cascade = false)
      } else {
        catalog.dropNamespace(Array("foo"), cascade = true)
      }
      assert(catalog.namespaceExists(Array("foo")) === false)
      assert(catalog.listNamespaces() === builtinNamespaces)
      val e = intercept[AnalysisException] {
        catalog.listNamespaces(Array("foo"))
      }
      checkError(e,
        errorClass = "SCHEMA_NOT_FOUND",
        parameters = Map("schemaName" -> "`foo`"))
    }
  }

  def testDropNamespaces(): Unit = {
    test("Drop namespace") {
      val ident1 = Identifier.of(Array("foo"), "tab")
      // Drop empty namespace without cascade
      val commentMap = if (supportsSchemaComment) {
        Map("comment" -> "test comment")
      } else {
        Map.empty[String, String]
      }
      catalog.createNamespace(Array("foo"), commentMap.asJava)
      assert(catalog.namespaceExists(Array("foo")) === true)
      if (supportsDropSchemaRestrict) {
        catalog.dropNamespace(Array("foo"), cascade = false)
      } else {
        catalog.dropNamespace(Array("foo"), cascade = true)
      }
      assert(catalog.namespaceExists(Array("foo")) === false)

      // Drop non empty namespace without cascade
      catalog.createNamespace(Array("foo"), commentMap.asJava)
      assert(catalog.namespaceExists(Array("foo")) === true)
      catalog.createTable(ident1, schema, Array.empty, emptyProps)
      if (supportsDropSchemaRestrict) {
        intercept[NonEmptyNamespaceException] {
          catalog.dropNamespace(Array("foo"), cascade = false)
        }
      }

      // Drop non empty namespace with cascade
      if (supportsDropSchemaCascade) {
        assert(catalog.namespaceExists(Array("foo")) === true)
        catalog.dropNamespace(Array("foo"), cascade = true)
        assert(catalog.namespaceExists(Array("foo")) === false)
      }
    }
  }
}
