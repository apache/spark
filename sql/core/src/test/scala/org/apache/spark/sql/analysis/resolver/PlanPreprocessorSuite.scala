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

package org.apache.spark.sql.analysis.resolver

import org.apache.spark.sql.catalyst.analysis.{
  ExpressionWithUnresolvedIdentifier,
  PlanWithUnresolvedIdentifier,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedRelation,
  UnresolvedStarExceptOrReplace
}
import org.apache.spark.sql.catalyst.analysis.resolver.{
  PlanPreprocessor,
  Resolver,
  UnresolvedCteRelationRef
}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.test.SharedSparkSession

class PlanPreprocessorSuite extends SharedSparkSession {
  private def createPlanPreprocessor(): PlanPreprocessor = {
    new PlanPreprocessor(
      new Resolver(spark.sessionState.catalogManager),
      resolveExpressionIdentifiersInRootScope = false
    )
  }

  test("Plan is unchanged") {
    val substitutor = createPlanPreprocessor()

    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT) USING PARQUET")

      val unresolvedPlan = spark.sessionState.sqlParser.parsePlan("SELECT * FROM t1")

      val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
      assert(
        planAfterSubstitution
          .children(0)
          .isInstanceOf[UnresolvedRelation]
      )
    }
  }

  test("Plan with CTEs") {
    val substitutor = createPlanPreprocessor()

    withTable("t2") {
      spark.sql("CREATE TABLE t2 (col1 INT) USING PARQUET")

      val unresolvedPlan = spark.sessionState.sqlParser.parsePlan("""
      WITH t1 AS (
        WITH t2 AS (
          SELECT 1
        )
        SELECT * FROM t2
      )
      SELECT * FROM t2
      """)

      val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
      assert(
        planAfterSubstitution
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedRelation]
      )
      assert(
        planAfterSubstitution
          .asInstanceOf[UnresolvedWith]
          .cteRelations(0)
          .plan
          .children(0)
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedCteRelationRef]
      )
    }
  }

  test("Plan with CTEs not on top level") {
    val substitutor = createPlanPreprocessor()

    withTable("t2") {
      spark.sql("CREATE TABLE t2 (col1 INT) USING PARQUET")

      val unresolvedPlan = spark.sessionState.sqlParser.parsePlan("""
      SELECT * FROM (
        WITH t1 AS (
          WITH t2 AS (
            SELECT 1
          )
          SELECT * FROM t2
        )
        SELECT * FROM t2
      )
      """)

      val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
      assert(
        planAfterSubstitution
          .children(0)
          .children(0)
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedRelation]
      )
      assert(
        planAfterSubstitution
          .children(0)
          .children(0)
          .asInstanceOf[UnresolvedWith]
          .cteRelations(0)
          .plan
          .children(0)
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedCteRelationRef]
      )
    }
  }

  test("Plan with CTEs only in CTE definitions") {
    val substitutor = createPlanPreprocessor()

    withTable("t2") {
      spark.sql("CREATE TABLE t2 (col1 INT) USING PARQUET")

      val unresolvedPlan = spark.sessionState.sqlParser.parsePlan("""
      SELECT * FROM (
        WITH t1 AS (
          SELECT * FROM t2
        ),
        t2 AS (
          SELECT * FROM t1
        )
        SELECT 1
      )
      """)

      val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
      assert(
        planAfterSubstitution
          .children(0)
          .children(0)
          .asInstanceOf[UnresolvedWith]
          .cteRelations(0)
          .plan
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedRelation]
      )
      assert(
        planAfterSubstitution
          .children(0)
          .children(0)
          .asInstanceOf[UnresolvedWith]
          .cteRelations(1)
          .plan
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedCteRelationRef]
      )
    }
  }

  test("Plan with CTEs in scalar subquery") {
    val substitutor = createPlanPreprocessor()

    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT) USING PARQUET")

      val unresolvedPlan = spark.sessionState.sqlParser.parsePlan("""
      SELECT (
        WITH cte1 AS (
          SELECT 1
        )
        SELECT * FROM cte1
      ) + (
        WITH cte2 AS (
          SELECT 2
        )
        SELECT * FROM cte2
      )
      FROM
        t1
      """)

      val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
      assert(
        planAfterSubstitution
          .expressions(0)
          .children(0)
          .children(0)
          .asInstanceOf[SubqueryExpression]
          .plan
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedCteRelationRef]
      )
      assert(
        planAfterSubstitution
          .expressions(0)
          .children(0)
          .children(1)
          .asInstanceOf[SubqueryExpression]
          .plan
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedCteRelationRef]
      )
      assert(
        planAfterSubstitution
          .children(0)
          .isInstanceOf[UnresolvedRelation]
      )
    }
  }

  test("Plan with CTEs in EXISTS subquery") {
    val substitutor = createPlanPreprocessor()

    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT) USING PARQUET")

      val unresolvedPlan = spark.sessionState.sqlParser.parsePlan("""
      SELECT
        *
      FROM
        t1
      WHERE EXISTS (
        WITH cte2 AS (
          SELECT 1
        )
        SELECT * FROM cte2
      )
      """)

      val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
      assert(
        planAfterSubstitution
          .children(0)
          .expressions(0)
          .asInstanceOf[SubqueryExpression]
          .plan
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedCteRelationRef]
      )
      assert(
        planAfterSubstitution
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedRelation]
      )
    }
  }

  test("Plan with CTEs in IN subquery") {
    val substitutor = createPlanPreprocessor()

    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT) USING PARQUET")

      val unresolvedPlan = spark.sessionState.sqlParser.parsePlan("""
      SELECT
        *
      FROM
        t1
      WHERE col1 IN (
        WITH cte2 AS (
          SELECT 1
        )
        SELECT * FROM cte2
      )
      """)

      val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
      assert(
        planAfterSubstitution
          .children(0)
          .expressions(0)
          .asInstanceOf[InSubquery]
          .query
          .plan
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedCteRelationRef]
      )
      assert(
        planAfterSubstitution
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedRelation]
      )
    }
  }

  test("Plan with IDENTIFIER column reference") {
    val substitutor = createPlanPreprocessor()

    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(
      "SELECT IDENTIFIER('c1') FROM VALUES(1) AS T(c1)"
    )

    val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
    val projectExpressions = planAfterSubstitution.asInstanceOf[Project].projectList
    val unresolvedAlias = projectExpressions.head.asInstanceOf[UnresolvedAlias]
    val unresolvedAttribute = unresolvedAlias.child.asInstanceOf[UnresolvedAttribute]

    assert(unresolvedAttribute.nameParts == Seq("c1"))
  }

  test("Plan with IDENTIFIER qualified column reference") {
    val substitutor = createPlanPreprocessor()

    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(
      "SELECT IDENTIFIER('t.c1') FROM VALUES(1) AS T(c1)"
    )

    val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
    val projectExpressions = planAfterSubstitution.asInstanceOf[Project].projectList
    val unresolvedAlias = projectExpressions.head.asInstanceOf[UnresolvedAlias]
    val unresolvedAttribute = unresolvedAlias.child.asInstanceOf[UnresolvedAttribute]

    assert(unresolvedAttribute.nameParts == Seq("t", "c1"))
  }

  test("Plan with IDENTIFIER table reference") {
    val substitutor = createPlanPreprocessor()

    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(
      "SELECT * FROM IDENTIFIER('t1')"
    )

    val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)

    assert(
      planAfterSubstitution
        .children(0)
        .isInstanceOf[UnresolvedRelation]
    )
    assert(
      planAfterSubstitution
        .children(0)
        .asInstanceOf[UnresolvedRelation]
        .multipartIdentifier == Seq("t1")
    )
  }

  test("Plan with IDENTIFIER table reference computed by a builtin function") {
    val substitutor = createPlanPreprocessor()

    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(
      "SELECT * FROM IDENTIFIER(concat('t', '1'))"
    )

    val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
    val unresolvedRelation = planAfterSubstitution.children.head.asInstanceOf[UnresolvedRelation]

    assert(unresolvedRelation.multipartIdentifier == Seq("t1"))
  }

  test("Plan with IDENTIFIER concatenated string") {
    val substitutor = createPlanPreprocessor()

    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(
      "SELECT IDENTIFIER('c' || '1') FROM VALUES(1) AS T(c1)"
    )

    val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
    val projectExpressions = planAfterSubstitution.asInstanceOf[Project].projectList
    val unresolvedAlias = projectExpressions.head.asInstanceOf[UnresolvedAlias]
    val unresolvedAttribute = unresolvedAlias.child.asInstanceOf[UnresolvedAttribute]

    assert(unresolvedAttribute.nameParts == Seq("c1"))
  }

  test("Plan with IDENTIFIER function name from a SQL variable") {
    withSessionVariable("function_name") {
      spark.sql("DECLARE function_name STRING DEFAULT 'upper'")
      val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(
        "SELECT IDENTIFIER(function_name)(c1) FROM VALUES('a') AS T(c1)"
      )

      val defaultPlan = createPlanPreprocessor().substitutePlan(unresolvedPlan)
      val defaultAlias =
        defaultPlan.asInstanceOf[Project].projectList.head.asInstanceOf[UnresolvedAlias]

      assert(defaultAlias.child.isInstanceOf[ExpressionWithUnresolvedIdentifier])

      val rootScopePlan = new PlanPreprocessor(
        new Resolver(spark.sessionState.catalogManager),
        resolveExpressionIdentifiersInRootScope = true
      ).substitutePlan(unresolvedPlan)
      val rootScopeAlias =
        rootScopePlan.asInstanceOf[Project].projectList.head.asInstanceOf[UnresolvedAlias]
      val unresolvedFunction = rootScopeAlias.child.asInstanceOf[UnresolvedFunction]

      assert(unresolvedFunction.nameParts == Seq("upper"))
      assert(unresolvedFunction.arguments == Seq(UnresolvedAttribute("c1")))
    }
  }

  test("Plan with IDENTIFIER and CTEs") {
    val substitutor = createPlanPreprocessor()

    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(
      """
        |WITH cte1 AS (
        |  SELECT 1 AS c1
        |)
        |SELECT IDENTIFIER('c1') FROM cte1
        |""".stripMargin
    )

    val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
    val unresolvedWith = planAfterSubstitution.asInstanceOf[UnresolvedWith]
    val project = unresolvedWith.child.asInstanceOf[Project]
    val projectExpressions = project.projectList
    val unresolvedAlias = projectExpressions.head.asInstanceOf[UnresolvedAlias]
    val unresolvedAttribute = unresolvedAlias.child.asInstanceOf[UnresolvedAttribute]

    assert(unresolvedAttribute.nameParts == Seq("c1"))
    assert(project.children(0).isInstanceOf[UnresolvedCteRelationRef])
  }

  test("Plan with CTEs in LATERAL subquery") {
    val substitutor = createPlanPreprocessor()

    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT) USING PARQUET")

      val unresolvedPlan = spark.sessionState.sqlParser.parsePlan("""
      SELECT
        *
      FROM
        t1
      JOIN LATERAL (
        WITH cte2 AS (
          SELECT 1
        )
        SELECT * FROM cte2
      )
      """)

      val planAfterSubstitution = substitutor.substitutePlan(unresolvedPlan)
      assert(
        planAfterSubstitution
          .children(0)
          .expressions(0)
          .asInstanceOf[SubqueryExpression]
          .plan
          .children(0)
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedCteRelationRef]
      )
      assert(
        planAfterSubstitution
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedRelation]
      )
    }
  }

  test("IDENTIFIER inside a pipe SET replacement subquery is substituted") {
    val substitutor = createPlanPreprocessor()

    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(
      "TABLE src |> SET value = (SELECT value FROM IDENTIFIER('oth' || 'er') LIMIT 1)"
    )

    val subqueryPlan = starReplacementSubqueryPlan(substitutor.substitutePlan(unresolvedPlan))

    assert(subqueryPlan.collectFirst { case plan: PlanWithUnresolvedIdentifier => plan }.isEmpty)
    val relation = subqueryPlan.collectFirst { case relation: UnresolvedRelation => relation }.get
    assert(relation.multipartIdentifier == Seq("other"))
  }

  test("CTE name inside a pipe SET replacement subquery is substituted with a CTE reference") {
    val substitutor = createPlanPreprocessor()

    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(
      "WITH other AS (SELECT 'abc' AS value) " +
        "TABLE src |> SET value = (SELECT value FROM other LIMIT 1)"
    )

    val subqueryPlan = starReplacementSubqueryPlan(substitutor.substitutePlan(unresolvedPlan))

    assert(subqueryPlan.collectFirst { case relation: UnresolvedRelation => relation }.isEmpty)
    val cteReference =
      subqueryPlan.collectFirst { case reference: UnresolvedCteRelationRef => reference }.get
    assert(cteReference.name == "other")
  }

  /**
   * Returns the plan of the (single) subquery nested in the replacement expressions of the
   * [[UnresolvedStarExceptOrReplace]] carried by `plan` (e.g. the pipe `|> SET` target subquery).
   */
  private def starReplacementSubqueryPlan(plan: LogicalPlan): LogicalPlan = {
    val star = plan
      .collectFirst {
        case operator
            if operator.expressions.exists(
              _.exists(_.isInstanceOf[UnresolvedStarExceptOrReplace])) =>
          operator.expressions
            .flatMap(_.collect { case star: UnresolvedStarExceptOrReplace => star })
            .head
      }
      .get
    star.replacements.get.head
      .collectFirst { case subquery: SubqueryExpression => subquery }
      .get
      .plan
  }
}
