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

package org.apache.spark.sql.catalyst.analysis

import java.net.URI

import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{Project, SubqueryAlias}

class StreamRelationSuite extends AnalysisTest {
  import CatalystSqlParser._

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

  test("STREAM parses correctly on table identifier") {
    Seq("SELECT * FROM STREAM(t)", "SELECT * FROM STREAM t").foreach { query =>
      val plan = parsePlan(query)
      comparePlans(
        plan,
        Project(
          projectList = Seq(UnresolvedStar(None)),
          child = UnresolvedRelation(
            multipartIdentifier = Seq("t"),
            isStreaming = true
          )
        )
      )
    }
  }

  test("STREAM with alias is parsed correctly") {
    Seq(
      "SELECT * FROM STREAM(t) AS `a.b.c`",
      "SELECT * FROM STREAM t AS `a.b.c`"
    ).foreach { query =>
      val plan = parsePlan(query)
      comparePlans(
        plan,
        Project(
          projectList = Seq(UnresolvedStar(None)),
          child = SubqueryAlias(
            identifier = AliasIdentifier(
              name = "a.b.c",
              qualifier = Seq.empty
            ),
            child = UnresolvedRelation(
              multipartIdentifier = Seq("t"),
              isStreaming = true
            )
          )
        )
      )
    }
  }

  test("STREAM parses correctly on table valued functions") {
    Seq(
      "SELECT * FROM STREAM(range(1, 10))",
      "SELECT * FROM STREAM range(1, 10)"
    ).foreach { query =>
      val plan = parsePlan(query)
      comparePlans(
        plan,
        Project(
          projectList = Seq(UnresolvedStar(None)),
          child = UnresolvedTableValuedFunction(
            name = Seq("range"),
            functionArgs = Seq(Literal(1), Literal(10)),
            isStreaming = true
          )
        )
      )
    }
  }

  test("Parse queries with valid STREAM table joins") {
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

  test("Parse Exceptions: Unsupported STREAM relations") {
    // Sub-queries within STREAM keyword
    interceptParseException(parsePlan)("SELECT * FROM STREAM ( SELECT * FROM t3 )")(None)
    // Temporal Clause within STREAM keyword
    interceptParseException(parsePlan)(
      "SELECT * FROM STREAM ( t1 TIMESTAMP AS OF current_date() )"
    )(None)
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

  test("Analysis Exception: non-streaming Relation within STREAM keyword") {
    // Entity within STREAM keyword must be a streaming relation
    // "table" here is a temp view with a batch relation, so it should throw.
    assertAnalysisError(
      inputPlan = parsePlan("SELECT * FROM STREAM table"),
      expectedErrors = Seq("is not a temp view of streaming logical plan")
    )
  }

  test("Analysis Exception: UNSUPPORTED_STREAMING_TABLE_VALUED_FUNCTION") {
    assertAnalysisErrorCondition(
      inputPlan = parsePlan("SELECT * FROM STREAM RANGE(1, 100)"),
      expectedErrorCondition = "UNSUPPORTED_STREAMING_TABLE_VALUED_FUNCTION",
      expectedMessageParameters = Map("funcName" -> "`RANGE`"),
      queryContext = Array(ExpectedContext("RANGE(1, 100)", 21, 33))
    )
  }

  test("Analysis Exception: could not resolve `<funcName>` to a table-valued function") {
    assertAnalysisErrorCondition(
      inputPlan = parsePlan("SELECT * FROM STREAM some_func('arg')"),
      expectedErrorCondition = "UNRESOLVABLE_TABLE_VALUED_FUNCTION",
      expectedMessageParameters = Map("name" -> "`some_func`"),
      queryContext = Array(ExpectedContext("some_func('arg')", 21, 36))
    )
    assertAnalysisErrorCondition(
      inputPlan = parsePlan("SELECT * FROM STREAM(some_func('arg'))"),
      expectedErrorCondition = "UNRESOLVABLE_TABLE_VALUED_FUNCTION",
      expectedMessageParameters = Map("name" -> "`some_func`"),
      queryContext = Array(ExpectedContext("some_func('arg')", 21, 36))
    )
  }
}
