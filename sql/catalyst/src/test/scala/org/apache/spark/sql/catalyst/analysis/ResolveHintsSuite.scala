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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._

class ResolveHintsSuite extends AnalysisTest {
  import org.apache.spark.sql.catalyst.analysis.TestRelations._

  test("invalid hints should be ignored") {
    checkAnalysis(
      UnresolvedHint("some_random_hint_that_does_not_exist", Seq("TaBlE"), table("TaBlE")),
      testRelation,
      caseSensitive = false)
  }

  test("case-sensitive or insensitive parameters") {
    checkAnalysis(
      UnresolvedHint("MAPJOIN", Seq("TaBlE"), table("TaBlE")),
      ResolvedHint(testRelation, HintInfo(isBroadcastable = Option(true))),
      caseSensitive = false)

    checkAnalysis(
      UnresolvedHint("MAPJOIN", Seq("table"), table("TaBlE")),
      ResolvedHint(testRelation, HintInfo(isBroadcastable = Option(true))),
      caseSensitive = false)

    checkAnalysis(
      UnresolvedHint("MAPJOIN", Seq("TaBlE"), table("TaBlE")),
      ResolvedHint(testRelation, HintInfo(isBroadcastable = Option(true))),
      caseSensitive = true)

    checkAnalysis(
      UnresolvedHint("MAPJOIN", Seq("table"), table("TaBlE")),
      testRelation,
      caseSensitive = true)
  }

  test("multiple broadcast hint aliases") {
    checkAnalysis(
      UnresolvedHint("MAPJOIN", Seq("table", "table2"), table("table").join(table("table2"))),
      Join(ResolvedHint(testRelation, HintInfo(isBroadcastable = Option(true))),
        ResolvedHint(testRelation2, HintInfo(isBroadcastable = Option(true))), Inner, None),
      caseSensitive = false)
  }

  test("do not traverse past existing broadcast hints") {
    checkAnalysis(
      UnresolvedHint("MAPJOIN", Seq("table"),
        ResolvedHint(table("table").where('a > 1), HintInfo(isBroadcastable = Option(true)))),
      ResolvedHint(testRelation.where('a > 1), HintInfo(isBroadcastable = Option(true))).analyze,
      caseSensitive = false)
  }

  test("should work for subqueries") {
    checkAnalysis(
      UnresolvedHint("MAPJOIN", Seq("tableAlias"), table("table").as("tableAlias")),
      ResolvedHint(testRelation, HintInfo(isBroadcastable = Option(true))),
      caseSensitive = false)

    checkAnalysis(
      UnresolvedHint("MAPJOIN", Seq("tableAlias"), table("table").subquery('tableAlias)),
      ResolvedHint(testRelation, HintInfo(isBroadcastable = Option(true))),
      caseSensitive = false)

    // Negative case: if the alias doesn't match, don't match the original table name.
    checkAnalysis(
      UnresolvedHint("MAPJOIN", Seq("table"), table("table").as("tableAlias")),
      testRelation,
      caseSensitive = false)
  }

  test("do not traverse past subquery alias") {
    checkAnalysis(
      UnresolvedHint("MAPJOIN", Seq("table"), table("table").where('a > 1).subquery('tableAlias)),
      testRelation.where('a > 1).analyze,
      caseSensitive = false)
  }

  test("should work for CTE") {
    checkAnalysis(
      CatalystSqlParser.parsePlan(
        """
          |WITH ctetable AS (SELECT * FROM table WHERE a > 1)
          |SELECT /*+ BROADCAST(ctetable) */ * FROM ctetable
        """.stripMargin
      ),
      ResolvedHint(testRelation.where('a > 1).select('a), HintInfo(isBroadcastable = Option(true)))
        .select('a).analyze,
      caseSensitive = false)
  }

  test("should not traverse down CTE") {
    checkAnalysis(
      CatalystSqlParser.parsePlan(
        """
          |WITH ctetable AS (SELECT * FROM table WHERE a > 1)
          |SELECT /*+ BROADCAST(table) */ * FROM ctetable
        """.stripMargin
      ),
      testRelation.where('a > 1).select('a).select('a).analyze,
      caseSensitive = false)
  }
}
