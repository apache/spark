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

package org.apache.spark.sql.catalyst.parser

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, NamedStreamingRelation, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.plans.logical.{Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.streaming.Unassigned
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class StreamRelationParserSuite extends AnalysisTest {
  import CatalystSqlParser._

  test("STREAM parses correctly on table identifier") {
    Seq("SELECT * FROM STREAM(t)", "SELECT * FROM STREAM t").foreach { query =>
      val plan = parsePlan(query)
      comparePlans(
        plan,
        Project(
          projectList = Seq(UnresolvedStar(None)),
          child = NamedStreamingRelation(
            child = UnresolvedRelation(
              multipartIdentifier = Seq("t"),
              isStreaming = true
            ),
            sourceIdentifyingName = Unassigned
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
            child = NamedStreamingRelation(
              child = UnresolvedRelation(
                multipartIdentifier = Seq("t"),
                isStreaming = true
              ),
              sourceIdentifyingName = Unassigned
            )
          )
        )
      )
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

  test("STREAM parses WITH options") {
    comparePlans(
      parsePlan("SELECT * FROM STREAM table WITH ('key'='value')"),
      Project(
        projectList = Seq(UnresolvedStar(None)),
        child = NamedStreamingRelation(
          child = UnresolvedRelation(
            multipartIdentifier = Seq("table"),
            options = new CaseInsensitiveStringMap(Map("key" -> "value").asJava),
            isStreaming = true
          ),
          sourceIdentifyingName = Unassigned
        )
      )
    )
  }

  test("STREAM with options and alias wraps NamedStreamingRelation below SubqueryAlias") {
    comparePlans(
      parsePlan("SELECT * FROM STREAM table WITH ('key'='value') AS src"),
      Project(
        projectList = Seq(UnresolvedStar(None)),
        child = SubqueryAlias(
          identifier = AliasIdentifier(
            name = "src",
            qualifier = Seq.empty
          ),
          child = NamedStreamingRelation(
            child = UnresolvedRelation(
              multipartIdentifier = Seq("table"),
              options = new CaseInsensitiveStringMap(Map("key" -> "value").asJava),
              isStreaming = true
            ),
            sourceIdentifyingName = Unassigned
          )
        )
      )
    )
  }

  test("STREAM with multiple key/value pairs in WITH options") {
    comparePlans(
      parsePlan("SELECT * FROM STREAM t WITH ('key1'='value1', 'key2'='value2', 'key3'='value3')"),
      Project(
        projectList = Seq(UnresolvedStar(None)),
        child = NamedStreamingRelation(
          child = UnresolvedRelation(
            multipartIdentifier = Seq("t"),
            options = new CaseInsensitiveStringMap(
              Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3").asJava),
            isStreaming = true
          ),
          sourceIdentifyingName = Unassigned
        )
      )
    )
  }

  test("inner join of two streaming tables") {
    val plan = parsePlan("SELECT * FROM STREAM t1 JOIN STREAM t2 ON t1.id = t2.id")

    // Verify both streaming sources are wrapped in NamedStreamingRelation
    val namedStreamingRelations = plan.collect {
      case n: NamedStreamingRelation => n
    }
    assert(namedStreamingRelations.size == 2,
      "Expected 2 NamedStreamingRelation nodes for join of two streaming tables")

    // Verify both have Unassigned names
    namedStreamingRelations.foreach { n =>
      assert(n.sourceIdentifyingName == Unassigned)
    }

    // Verify the underlying relations are streaming
    namedStreamingRelations.foreach { n =>
      n.child match {
        case u: UnresolvedRelation => assert(u.isStreaming)
        case _ => fail("Expected UnresolvedRelation as child")
      }
    }
  }

  test("streaming table in CTE (WITH clause)") {
    val plan = parsePlan(
      """
        |WITH streaming_cte AS (SELECT * FROM STREAM source_table)
        |SELECT * FROM streaming_cte
      """.stripMargin)

    // CTE definitions are stored in UnresolvedWith.cteRelations, not as children,
    // so we need to extract them manually
    val cteRelations = plan match {
      case w: org.apache.spark.sql.catalyst.plans.logical.UnresolvedWith => w.cteRelations
      case _ => fail("Expected UnresolvedWith")
    }
    assert(cteRelations.size == 1)

    // The CTE definition is wrapped in SubqueryAlias
    val cteDefinition = cteRelations.head._2
    val namedStreamingRelations = cteDefinition.collect {
      case n: NamedStreamingRelation => n
    }
    assert(namedStreamingRelations.size == 1,
      "Expected 1 NamedStreamingRelation node in CTE definition")
    assert(namedStreamingRelations.head.sourceIdentifyingName == Unassigned)
  }

  test("lateral join with streaming table") {
    val plan = parsePlan(
      "SELECT * FROM STREAM t1 JOIN LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) sub")

    // Verify the streaming source is wrapped in NamedStreamingRelation
    val namedStreamingRelations = plan.collect {
      case n: NamedStreamingRelation => n
    }
    assert(namedStreamingRelations.size == 1,
      "Expected 1 NamedStreamingRelation node for streaming table in lateral join")
    assert(namedStreamingRelations.head.sourceIdentifyingName == Unassigned)

    // Verify it's a LateralJoin
    val lateralJoins = plan.collect {
      case l: org.apache.spark.sql.catalyst.plans.logical.LateralJoin => l
    }
    assert(lateralJoins.size == 1, "Expected 1 LateralJoin node")
  }

  // ===================================
  // Negative test cases for WITH options syntax
  // ===================================

  test("Parse Exception: WITH option key without value") {
    interceptParseException(parsePlan)(
      "SELECT * FROM STREAM t WITH ('key')"
    )(None)
  }

  test("Parse Exception: WITH option using => instead of =") {
    interceptParseException(parsePlan)(
      "SELECT * FROM STREAM t WITH ('key'=>'value')"
    )(None)
  }

  test("Parse Exception: WITH option key with no value") {
    interceptParseException(parsePlan)(
      "SELECT * FROM STREAM t WITH ('key'=)"
    )(None)
  }

  test("Parse Exception: WITH option missing quotes around value") {
    interceptParseException(parsePlan)(
      "SELECT * FROM STREAM t WITH ('key'=value)"
    )(None)
  }

  test("Parse Exception: WITH option empty parentheses") {
    interceptParseException(parsePlan)(
      "SELECT * FROM STREAM t WITH ()"
    )(None)
  }
}
