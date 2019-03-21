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
package org.apache.spark.sql.catalyst.catalog.v2

import org.scalatest.Matchers._

import org.apache.spark.sql.catalog.v2.{CatalogNotFoundException, CatalogPlugin}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private class TestCatalogPlugin(override val name: String) extends CatalogPlugin {

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = Unit
}

class ResolveMultipartIdentifierSuite extends AnalysisTest {
  import CatalystSqlParser._

  private val analyzer = makeAnalyzer(caseSensitive = false)

  private val catalogs = Seq("prod", "test").map(name => name -> new TestCatalogPlugin(name)).toMap

  private def lookupCatalog(catalog: String): CatalogPlugin =
    catalogs.getOrElse(catalog, throw new CatalogNotFoundException("Not found"))

  private def makeAnalyzer(caseSensitive: Boolean) = {
    val conf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> caseSensitive)
    new Analyzer(Some(lookupCatalog _), null, conf)
  }

  override protected def getAnalyzer(caseSensitive: Boolean) = analyzer

  private def checkResolution(sqlText: String, expectedCatalog: Option[CatalogPlugin],
      expectedNamespace: Array[String], expectedName: String): Unit = {

    import analyzer.CatalogObjectIdentifier
    val CatalogObjectIdentifier(catalog, ident) = parseMultipartIdentifier(sqlText)
    catalog shouldEqual expectedCatalog
    ident.namespace shouldEqual expectedNamespace
    ident.name shouldEqual expectedName
  }

  private def checkTableResolution(sqlText: String,
      expectedIdent: Option[TableIdentifier]): Unit = {

    import analyzer.AsTableIdentifier
    parseMultipartIdentifier(sqlText) match {
      case AsTableIdentifier(ident) =>
        assert(Some(ident) === expectedIdent)
      case _ =>
        assert(None === expectedIdent)
    }
  }

  test("resolve multipart identifier") {
    checkResolution("tbl", None, Array.empty, "tbl")
    checkResolution("db.tbl", None, Array("db"), "tbl")
    checkResolution("prod.func", catalogs.get("prod"), Array.empty, "func")
    checkResolution("ns1.ns2.tbl", None, Array("ns1", "ns2"), "tbl")
    checkResolution("prod.db.tbl", catalogs.get("prod"), Array("db"), "tbl")
    checkResolution("test.db.tbl", catalogs.get("test"), Array("db"), "tbl")
    checkResolution("test.ns1.ns2.ns3.tbl",
      catalogs.get("test"), Array("ns1", "ns2", "ns3"), "tbl")
    checkResolution("`db.tbl`", None, Array.empty, "db.tbl")
    checkResolution("parquet.`file:/tmp/db.tbl`", None, Array("parquet"), "file:/tmp/db.tbl")
    checkResolution("`org.apache.spark.sql.json`.`s3://buck/tmp/abc.json`", None,
      Array("org.apache.spark.sql.json"), "s3://buck/tmp/abc.json")
  }

  test("resolve table identifier") {
    checkTableResolution("tbl", Some(TableIdentifier("tbl")))
    checkTableResolution("db.tbl", Some(TableIdentifier("tbl", Some("db"))))
    checkTableResolution("prod.func", None)
    checkTableResolution("ns1.ns2.tbl", None)
    checkTableResolution("prod.db.tbl", None)
    checkTableResolution("`db.tbl`", Some(TableIdentifier("db.tbl")))
    checkTableResolution("parquet.`file:/tmp/db.tbl`",
      Some(TableIdentifier("file:/tmp/db.tbl", Some("parquet"))))
    checkTableResolution("`org.apache.spark.sql.json`.`s3://buck/tmp/abc.json`",
      Some(TableIdentifier("s3://buck/tmp/abc.json", Some("org.apache.spark.sql.json"))))
  }
}
