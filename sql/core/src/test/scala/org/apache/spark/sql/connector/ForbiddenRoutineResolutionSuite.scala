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

package org.apache.spark.sql.connector

import java.util.Collections

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog, SupportsNamespaces}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.catalog.procedures.UnboundProcedure
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Coverage of SPARK-57759: a catalog that denies access to a routine must surface its
 * `FORBIDDEN_OPERATION` error instead of having it converted into `UNRESOLVED_ROUTINE`,
 * which reports the routine as non-existent and hides the real cause.
 *
 * For a single-part name searched through the PATH, a denial from one entry must not abort
 * the search: a later entry that legitimately holds the name still wins, and the denial is
 * only reported when no candidate resolves at all.
 */
class ForbiddenRoutineResolutionSuite extends SharedSparkSession {

  private val emptyProps: java.util.Map[String, String] = Collections.emptyMap()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.catalog.denycat", classOf[DenyingRoutineCatalog].getName)
    spark.conf.set("spark.sql.catalog.okcat", classOf[InMemoryCatalog].getName)
  }

  override def afterAll(): Unit = {
    try {
      spark.sessionState.catalogManager.reset()
      spark.sessionState.conf.unsetConf("spark.sql.catalog.denycat")
      spark.sessionState.conf.unsetConf("spark.sql.catalog.okcat")
    } finally {
      super.afterAll()
    }
  }

  private def v2Catalog(name: String): InMemoryCatalog =
    spark.sessionState.catalogManager.catalog(name).asInstanceOf[InMemoryCatalog]

  private def createV2Namespace(catalog: String, ns: String): Unit = {
    v2Catalog(catalog).asInstanceOf[SupportsNamespaces]
      .createNamespace(Array(ns), emptyProps)
  }

  private def withPath(path: String)(f: => Unit): Unit = {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      try {
        sql(s"SET PATH = $path")
        f
      } finally {
        sql("SET PATH = DEFAULT_PATH")
      }
    }
  }

  test("qualified function name in a denying catalog reports FORBIDDEN_OPERATION") {
    val e = intercept[AnalysisException] {
      sql("SELECT denycat.fns.strlen('abc')").collect()
    }
    assert(e.getCondition == "FORBIDDEN_OPERATION",
      s"Expected FORBIDDEN_OPERATION; got: ${e.getCondition}: ${e.getMessage}")
  }

  test("unqualified function name resolves past a denying PATH entry") {
    createV2Namespace("okcat", "fns")
    v2Catalog("okcat").createFunction(Identifier.of(Array("fns"), "strlen"), StrLen(StrLenDefault))
    try {
      withPath("denycat.fns, okcat.fns, system.builtin") {
        checkAnswer(sql("SELECT strlen('abc')"), Row(3))
      }
    } finally {
      v2Catalog("okcat").clearFunctions()
    }
  }

  test("unqualified function name reports FORBIDDEN_OPERATION when no candidate resolves") {
    withPath("denycat.fns, system.builtin") {
      val e = intercept[AnalysisException] {
        sql("SELECT no_such_function('abc')").collect()
      }
      assert(e.getCondition == "FORBIDDEN_OPERATION",
        s"Expected FORBIDDEN_OPERATION; got: ${e.getCondition}: ${e.getMessage}")
    }
  }

  test("unqualified procedure name reports FORBIDDEN_OPERATION when no candidate resolves") {
    withPath("denycat.fns, system.builtin") {
      val e = intercept[AnalysisException] {
        sql("CALL no_such_procedure()").collect()
      }
      assert(e.getCondition == "FORBIDDEN_OPERATION",
        s"Expected FORBIDDEN_OPERATION; got: ${e.getCondition}: ${e.getMessage}")
    }
  }
}

/**
 * A catalog that denies every routine lookup the way an access-controlled catalog does:
 * `FORBIDDEN_OPERATION` rather than a "not found" signal.
 */
class DenyingRoutineCatalog extends InMemoryCatalog {

  private def denied(name: String): AnalysisException = new AnalysisException(
    errorClass = "FORBIDDEN_OPERATION",
    messageParameters = Map(
      "statement" -> "LOAD ROUTINE",
      "objectType" -> "ROUTINE",
      "objectName" -> name))

  override def loadFunction(ident: Identifier): UnboundFunction = throw denied(ident.name())

  override def functionExists(ident: Identifier): Boolean = throw denied(ident.name())

  override def loadProcedure(ident: Identifier): UnboundProcedure = throw denied(ident.name())
}
