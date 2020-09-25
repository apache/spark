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
package org.apache.spark.sql.connector.catalog

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.FakeV2SessionCatalog
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private case class DummyCatalogPlugin(override val name: String) extends CatalogPlugin {

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
}

class LookupCatalogSuite extends SparkFunSuite with LookupCatalog with Inside {
  import CatalystSqlParser._

  private val globalTempDB = SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
  private val catalogs =
    Seq("prod", "test", globalTempDB).map(x => x -> DummyCatalogPlugin(x)).toMap
  private val sessionCatalog = FakeV2SessionCatalog

  override val catalogManager: CatalogManager = {
    val manager = mock(classOf[CatalogManager])
    when(manager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      val name = invocation.getArgument[String](0)
      catalogs.getOrElse(name, throw new CatalogNotFoundException(s"$name not found"))
    })
    when(manager.currentCatalog).thenReturn(sessionCatalog)
    when(manager.v2SessionCatalog).thenReturn(sessionCatalog)
    when(manager.currentNamespace).thenReturn(Array.empty[String])
    manager
  }

  test("catalog and identifier") {
    Seq(
      ("tbl", sessionCatalog, Seq.empty, "tbl"),
      ("db.tbl", sessionCatalog, Seq("db"), "tbl"),
      (s"$globalTempDB.tbl", sessionCatalog, Seq(globalTempDB), "tbl"),
      (s"$globalTempDB.ns1.ns2.tbl", sessionCatalog, Seq(globalTempDB, "ns1", "ns2"), "tbl"),
      ("prod.func", catalogs("prod"), Seq.empty, "func"),
      ("ns1.ns2.tbl", sessionCatalog, Seq("ns1", "ns2"), "tbl"),
      ("prod.db.tbl", catalogs("prod"), Seq("db"), "tbl"),
      ("test.db.tbl", catalogs("test"), Seq("db"), "tbl"),
      ("test.ns1.ns2.ns3.tbl", catalogs("test"), Seq("ns1", "ns2", "ns3"), "tbl"),
      ("`db.tbl`", sessionCatalog, Seq.empty, "db.tbl"),
      ("parquet.`file:/tmp/db.tbl`", sessionCatalog, Seq("parquet"), "file:/tmp/db.tbl"),
      ("`org.apache.spark.sql.json`.`s3://buck/tmp/abc.json`", sessionCatalog,
        Seq("org.apache.spark.sql.json"), "s3://buck/tmp/abc.json")).foreach {
      case (sql, expectedCatalog, namespace, name) =>
        inside(parseMultipartIdentifier(sql)) {
          case CatalogAndIdentifier(catalog, ident) =>
            catalog shouldEqual expectedCatalog
            ident shouldEqual Identifier.of(namespace.toArray, name)
        }
    }
  }

  test("table identifier") {
    Seq(
      ("tbl", "tbl", None),
      ("db.tbl", "tbl", Some("db")),
      ("`db.tbl`", "db.tbl", None),
      ("parquet.`file:/tmp/db.tbl`", "file:/tmp/db.tbl", Some("parquet")),
      ("`org.apache.spark.sql.json`.`s3://buck/tmp/abc.json`", "s3://buck/tmp/abc.json",
        Some("org.apache.spark.sql.json"))).foreach {
      case (sql, table, db) =>
        inside (parseMultipartIdentifier(sql)) {
          case AsTableIdentifier(ident) =>
            ident shouldEqual TableIdentifier(table, db)
        }
    }
    Seq(
      "prod.func",
      "prod.db.tbl",
      "ns1.ns2.tbl").foreach { sql =>
      parseMultipartIdentifier(sql) match {
        case AsTableIdentifier(_) =>
          fail(s"$sql should not be resolved as TableIdentifier")
        case _ =>
      }
    }
  }
}

class LookupCatalogWithDefaultSuite extends SparkFunSuite with LookupCatalog with Inside {
  import CatalystSqlParser._

  private val catalogs = Seq("prod", "test").map(x => x -> DummyCatalogPlugin(x)).toMap

  override val catalogManager: CatalogManager = {
    val manager = mock(classOf[CatalogManager])
    when(manager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      val name = invocation.getArgument[String](0)
      catalogs.getOrElse(name, throw new CatalogNotFoundException(s"$name not found"))
    })
    when(manager.currentCatalog).thenReturn(catalogs("prod"))
    when(manager.currentNamespace).thenReturn(Array.empty[String])
    manager
  }

  test("catalog and identifier") {
    Seq(
      ("tbl", catalogs("prod"), Seq.empty, "tbl"),
      ("db.tbl", catalogs("prod"), Seq("db"), "tbl"),
      ("prod.func", catalogs("prod"), Seq.empty, "func"),
      ("ns1.ns2.tbl", catalogs("prod"), Seq("ns1", "ns2"), "tbl"),
      ("prod.db.tbl", catalogs("prod"), Seq("db"), "tbl"),
      ("test.db.tbl", catalogs("test"), Seq("db"), "tbl"),
      ("test.ns1.ns2.ns3.tbl", catalogs("test"), Seq("ns1", "ns2", "ns3"), "tbl"),
      ("`db.tbl`", catalogs("prod"), Seq.empty, "db.tbl"),
      ("parquet.`file:/tmp/db.tbl`", catalogs("prod"), Seq("parquet"), "file:/tmp/db.tbl"),
      ("`org.apache.spark.sql.json`.`s3://buck/tmp/abc.json`", catalogs("prod"),
          Seq("org.apache.spark.sql.json"), "s3://buck/tmp/abc.json")).foreach {
      case (sql, expectedCatalog, namespace, name) =>
        inside(parseMultipartIdentifier(sql)) {
          case CatalogAndIdentifier(catalog, ident) =>
            catalog shouldEqual expectedCatalog
            ident shouldEqual Identifier.of(namespace.toArray, name)
        }
    }
  }

  test("table identifier") {
    Seq(
      "tbl",
      "db.tbl",
      "`db.tbl`",
      "parquet.`file:/tmp/db.tbl`",
      "`org.apache.spark.sql.json`.`s3://buck/tmp/abc.json`",
      "prod.func",
      "prod.db.tbl",
      "ns1.ns2.tbl").foreach { sql =>
      parseMultipartIdentifier(sql) match {
        case AsTableIdentifier(_) =>
          fail(s"$sql should not be resolved as TableIdentifier")
        case _ =>
      }
    }
  }
}
