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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.resolver.{
  IdentifierAndCteSubstitutor,
  UnresolvedCteRelationRef
}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.test.SharedSparkSession

class IdentifierAndCteSubstitutorSuite extends QueryTest with SharedSparkSession {
  test("Plan is unchanged") {
    val substitutor = new IdentifierAndCteSubstitutor

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
    val substitutor = new IdentifierAndCteSubstitutor

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
          ._2
          .children(0)
          .children(0)
          .children(0)
          .isInstanceOf[UnresolvedCteRelationRef]
      )
    }
  }

  test("Plan with CTEs in scalar subquery") {
    val substitutor = new IdentifierAndCteSubstitutor

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
    val substitutor = new IdentifierAndCteSubstitutor

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
    val substitutor = new IdentifierAndCteSubstitutor

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

  test("Plan with CTEs in LATERAL subquery") {
    val substitutor = new IdentifierAndCteSubstitutor

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
}
