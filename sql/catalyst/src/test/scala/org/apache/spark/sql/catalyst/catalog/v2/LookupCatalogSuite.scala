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

import org.scalatest.Inside
import org.scalatest.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalog.v2.{CatalogNotFoundException, CatalogPlugin, Identifier, LookupCatalog}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private case class TestCatalogPlugin(override val name: String) extends CatalogPlugin {

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = Unit
}

class LookupCatalogSuite extends SparkFunSuite with Inside {
  import CatalystSqlParser._

  private val catalogs = Seq("prod", "test").map(x => x -> new TestCatalogPlugin(x)).toMap

  private def findCatalog(catalog: String): CatalogPlugin =
    catalogs.getOrElse(catalog, throw new CatalogNotFoundException("Not found"))

  private val lookupCatalog = new LookupCatalog {
    override def lookupCatalog: Option[String => CatalogPlugin] = Some(findCatalog)
  }

  test("catalog object identifier") {
    import lookupCatalog._
    Seq(
      ("tbl", None, Seq.empty, "tbl"),
      ("db.tbl", None, Seq("db"), "tbl"),
      ("prod.func", catalogs.get("prod"), Seq.empty, "func"),
      ("ns1.ns2.tbl", None, Seq("ns1", "ns2"), "tbl"),
      ("prod.db.tbl", catalogs.get("prod"), Seq("db"), "tbl"),
      ("test.db.tbl", catalogs.get("test"), Seq("db"), "tbl"),
      ("test.ns1.ns2.ns3.tbl", catalogs.get("test"), Seq("ns1", "ns2", "ns3"), "tbl"),
      ("`db.tbl`", None, Seq.empty, "db.tbl"),
      ("parquet.`file:/tmp/db.tbl`", None, Seq("parquet"), "file:/tmp/db.tbl"),
      ("`org.apache.spark.sql.json`.`s3://buck/tmp/abc.json`", None,
        Seq("org.apache.spark.sql.json"), "s3://buck/tmp/abc.json")).foreach {
      case (sql, expectedCatalog, namespace, name) =>
        inside(parseMultipartIdentifier(sql)) {
          case CatalogObjectIdentifier(catalog, ident) =>
            catalog shouldEqual expectedCatalog
            ident shouldEqual Identifier.of(namespace.toArray, name)
        }
    }
  }

  test("table identifier") {
    import lookupCatalog._
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

  test("lookup function not defined") {
    val noLookupFunction = new LookupCatalog {
      override def lookupCatalog: Option[String => CatalogPlugin] = None
    }
    import noLookupFunction._
    Seq(
      ("tbl", Seq.empty, "tbl"),
      ("db.tbl", Seq("db"), "tbl"),
      ("prod.func", Seq("prod"), "func"),
      ("ns1.ns2.tbl", Seq("ns1", "ns2"), "tbl"),
      ("prod.db.tbl", Seq("prod", "db"), "tbl"),
      ("test.db.tbl", Seq("test", "db"), "tbl"),
      ("test.ns1.ns2.ns3.tbl", Seq("test", "ns1", "ns2", "ns3"), "tbl"),
      ("`db.tbl`", Seq.empty, "db.tbl"),
      ("parquet.`file:/tmp/db.tbl`", Seq("parquet"), "file:/tmp/db.tbl"),
      ("`org.apache.spark.sql.json`.`s3://buck/tmp/abc.json`",
        Seq("org.apache.spark.sql.json"), "s3://buck/tmp/abc.json")).foreach {
      case (sql, namespace, name) =>
        inside (parseMultipartIdentifier(sql)) {
          case CatalogObjectIdentifier(None, ident) =>
            ident shouldEqual Identifier.of(namespace.toArray, name)
        }
    }

    Seq(
      ("tbl", "tbl", None),
      ("db.tbl", "tbl", Some("db")),
      ("prod.func", "func", Some("prod")),
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
