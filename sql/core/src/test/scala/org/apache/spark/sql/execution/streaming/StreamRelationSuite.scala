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

package org.apache.spark.sql.execution.streaming

import java.net.URI

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer, FunctionRegistry, TableFunctionRegistry, TestRelations, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.{Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class StreamRelationSuite extends SharedSparkSession with AnalysisTest {

  override protected def getAnalyzer: Analyzer = {
    val catalog = new SessionCatalog(
      new InMemoryCatalog, FunctionRegistry.builtin, TableFunctionRegistry.builtin)
    catalog.createDatabase(
      CatalogDatabase("default", "", new URI("loc"), Map.empty),
      ignoreIfExists = false)
    createTempView(catalog, "table", TestRelations.testRelation, overrideIfExists = true)
    createTempView(catalog, "table2", TestRelations.testRelation2, overrideIfExists = true)
    createTempView(catalog, "streamingTable", TestRelations.streamingRelation,
      overrideIfExists = true)
    createTempView(catalog, "streamingTable2", TestRelations.streamingRelation,
      overrideIfExists = true)
    new Analyzer(catalog) {
      override val extendedResolutionRules = extendedAnalysisRules
    }
  }

  test("Queries with valid STREAM table joins are successfully analyzed") {
    Seq(
      "SELECT * FROM STREAM streamingTable JOIN STREAM streamingTable2",
      "SELECT * FROM table JOIN STREAM streamingTable2",
      "SELECT * FROM STREAM streamingTable JOIN table",
      "SELECT * FROM STREAM streamingTable JOIN ( SELECT * FROM table )",
      "SELECT * FROM STREAM streamingTable JOIN LATERAL ( SELECT * FROM table )",
      """SELECT * FROM STREAM streamingTable, LATERAL
        |(SELECT a FROM table WHERE streamingTable.a < table.a)""".stripMargin
    ).foreach { query =>
      val plan = parsePlan(query)
      assert(plan.isStreaming)
      assertAnalysisSuccess(plan)
    }
  }

  test("Analysis Exception: non-streaming Relation within STREAM keyword") {
    // Entity within STREAM keyword must be a streaming relation
    // "table" here is a temp view with a batch relation, so it should throw.
    assertAnalysisError(
      inputPlan = parsePlan("SELECT * FROM STREAM table"),
      expectedErrors = Seq("is not a temp view of streaming logical plan")
    )
  }

  test("STREAM options are parsed correctly for streaming by identifier") {
    val plan = parsePlan("SELECT * FROM STREAM table1 AS t WITH ('key'='value')")
    comparePlans(
      plan,
      Project(
        projectList = Seq(UnresolvedStar(None)),
        child = SubqueryAlias(
          identifier = AliasIdentifier(
            name = "t",
            qualifier = Seq.empty
          ),
          child = UnresolvedRelation(
            multipartIdentifier = Seq("table1"),
            isStreaming = true,
            options = new CaseInsensitiveStringMap(Map("key" -> "value").asJava)
          )
        )
      )
    )
  }

  test("Analysis Exception: TABLE_OR_VIEW_NOT_FOUND") {
    assertAnalysisErrorCondition(
      inputPlan = parsePlan("SELECT * FROM STREAM(`stream`)"),
      expectedErrorCondition = "TABLE_OR_VIEW_NOT_FOUND",
      expectedMessageParameters = Map("relationName" -> "`stream`"),
      queryContext = Array(ExpectedContext("STREAM(`stream`)", 14, 29))
    )
    assertAnalysisErrorCondition(
      inputPlan = parsePlan("SELECT * FROM STREAM `stream`"),
      expectedErrorCondition = "TABLE_OR_VIEW_NOT_FOUND",
      expectedMessageParameters = Map("relationName" -> "`stream`"),
      queryContext = Array(ExpectedContext("STREAM `stream`", 14, 28))
    )
  }

  test("STREAM with options is correctly propagated to datasource in V1") {
    sql("CREATE TABLE t (id INT) USING PARQUET")
    sql("INSERT INTO t VALUES (1), (2)")

    val actualAnalyzedPlan = sql(
      """
        |SELECT id FROM STREAM t
        |WITH ('readOptionKey'='readOptionValue')""".stripMargin
    ).queryExecution.analyzed

    val catalogTable = spark.sessionState.catalog.getTableMetadata(
      TableIdentifier("t")
    )
    val idAttr = AttributeReference(name = "id", dataType = IntegerType)()

    val expectedAnalyzedPlan = Project(
      projectList = Seq(idAttr),
      child = SubqueryAlias(
        identifier = AliasIdentifier(name = "t", qualifier = Seq("spark_catalog", "default")),
        child = StreamingRelation(
          dataSource = DataSource(
            sparkSession = spark,
            className = catalogTable.provider.get,
            options = Map(
              "readOptionKey" -> "readOptionValue",
              "path" -> catalogTable.location.toString
            ),
            userSpecifiedSchema = Option(catalogTable.schema),
            catalogTable = Option(catalogTable)
          ),
          sourceName = s"FileSource[${catalogTable.location.toString}]",
          output = Seq(idAttr)
        )
      )
    )

    comparePlans(
      expectedAnalyzedPlan,
      actualAnalyzedPlan,
      checkAnalysis = true
    )
  }
}
