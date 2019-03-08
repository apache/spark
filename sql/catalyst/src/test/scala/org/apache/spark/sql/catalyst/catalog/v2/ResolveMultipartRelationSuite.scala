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

import org.apache.spark.sql.catalog.v2.{CatalogNotFoundException, CatalogPlugin}
import org.apache.spark.sql.catalyst.CatalogIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer, EliminateSubqueryAliases, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private case class TestUnresolvedMultipartRelation(parts: Seq[String])
    extends LeafNode with NamedRelation {
  override def name: String = "TestUnresolvedMultipartRelation"
  override def output: Seq[Attribute] = Nil
  override lazy val resolved = false
}

private case class TestMultipartRelation(catalog: Option[String], ident: CatalogIdentifier)
    extends LeafNode with NamedRelation {
  override def name: String = "TestMultipartRelation"
  override def output: Seq[Attribute] = Nil
  override lazy val resolved = true
}

private case class TestMultipartAnalysis(analyzer: Analyzer) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators  {
    case TestUnresolvedMultipartRelation(analyzer.CatalogRef(catalog, ident)) =>
      TestMultipartRelation(catalog, ident)
  }
}

private class TestCatalogPlugin(override val name: String) extends CatalogPlugin {

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = Unit
}

class ResolveMultipartRelationSuite extends AnalysisTest {
  import CatalystSqlParser._

  private val analyzer = makeAnalyzer(caseSensitive = false)

  private val catalogs = Map(
    "prod" -> new TestCatalogPlugin("prod"),
    "test" -> new TestCatalogPlugin("test"))

  private def lookupCatalog(catalog: String): CatalogPlugin =
    catalogs.getOrElse(catalog, throw new CatalogNotFoundException("Not found"))

  private def makeAnalyzer(caseSensitive: Boolean) = {
    val conf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> caseSensitive)
    new Analyzer(Some(lookupCatalog _), null, conf) {
      override val extendedResolutionRules =
        EliminateSubqueryAliases :: TestMultipartAnalysis(this) :: Nil
    }
  }

  override protected def getAnalyzer(caseSensitive: Boolean) = analyzer

  private def checkMultipartResolution(sqlText: String, catalog: Option[String],
      space: Seq[String], name: String): Unit =
    checkAnalysis(TestUnresolvedMultipartRelation(parseMultiPartIdentifier(sqlText)),
      TestMultipartRelation(catalog, CatalogIdentifier(space, name)))

  test("multipart identifier") {
    checkMultipartResolution("tbl", None, Nil, "tbl")
    checkMultipartResolution("db.tbl", None, Seq("db"), "tbl")
    checkMultipartResolution("prod.func", Some("prod"), Nil, "func")
    checkMultipartResolution("ns1.ns2.tbl", None, Seq("ns1", "ns2"), "tbl")
    checkMultipartResolution("prod.db.tbl", Some("prod"), Seq("db"), "tbl")
    checkMultipartResolution("test.db.tbl", Some("test"), Seq("db"), "tbl")
    checkMultipartResolution("test.ns1.ns2.ns3.tbl",
      Some("test"), Seq("ns1", "ns2", "ns3"), "tbl")
  }
}
