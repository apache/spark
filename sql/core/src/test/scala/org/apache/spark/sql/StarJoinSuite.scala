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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class StarJoinSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  // Creates tables in a star schema relationship i.e.
  //
  // d1 - f1 - d2
  //      |
  //      d3 - s3
  //
  // Table f1 is the fact table. Tables d1, d2, and d3 are the dimension tables.
  // Dimension d3 is further joined/normalized into table s3.
  //
  // Tables are created using Local Relations to easily control their size.
  // e.g. f1 > d3 > d1 > d2 > s3
  def createStarSchemaTables(f1: String, d1: String, d2: String, d3: String, s3: String): Unit = {
    Seq((1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4))
      .toDF("f1_fk1", "f1_fk2", "f1_fk3", "f1_c4").createOrReplaceTempView(f1)

    Seq((1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4))
      .toDF("d1_pk1", "d1_c2", "d1_c3", "d1_c4").createOrReplaceTempView(d1)

    Seq((1, 2, 3, 4), (1, 2, 3, 4))
      .toDF("d2_c2", "d2_pk1", "d2_c3", "d2_c4").createOrReplaceTempView(d2)

    Seq((1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4))
      .toDF("d3_fk1", "d3_c2", "d3_pk1", "d3_c4").createOrReplaceTempView(d3)

    Seq((1, 2, 3, 4))
      .toDF("s3_pk1", "s3_c2", "s3_c3", "s3_c4").createOrReplaceTempView(s3)
  }

  // Given two semantically equivalent queries, query and equivQuery, the function
  // executes the queries with the star join enabled and with the default, positional
  // join reordering, respectively, and compares their optimized plans and query results.
  def verifyStarJoinPlans(query: DataFrame, equivQuery: DataFrame, rows: List[Row]): Unit = {
    var equivQryPlan: Option[LogicalPlan] = None
    var qryPlan: Option[LogicalPlan] = None

    withSQLConf(SQLConf.STARJOIN_OPTIMIZATION.key -> "true") {
      qryPlan = Some(query.queryExecution.optimizedPlan)
      checkAnswer(query, rows)
    }

    withSQLConf(SQLConf.STARJOIN_OPTIMIZATION.key -> "false") {
      equivQryPlan = Some(equivQuery.queryExecution.optimizedPlan)
      checkAnswer(equivQuery, rows)
    }

    comparePlans(qryPlan.get, equivQryPlan.get)
  }

  test("SPARK-17791: Test qualifying star joins") {
    withTempView("f1", "d1", "d2", "d3", "s3") {
      createStarSchemaTables("f1", "d1", "d2", "d3", "s3")

      // Test 1: Selective star join with f1 as fact table.
      // Star join:
      //   (=)  (=)
      // d1 - f1 - d2
      //      | (=)
      //      d3 - s3
      //
      // Positional/default join reordering: d1, f1, d2, d3, s3
      // Star join reordering: f1, d2, d1, d3, s3
      val query1 = sql(
        """
          | select d3.*
          | from d1, d2, f1, d3, s3
          | where f1_fk2 = d2_pk1 and d2_c2 < 2
          | and f1_fk1 = d1_pk1
          | and f1_fk3 = d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      // Equivalent query for comparison with the positional join reordering.
      // Default join reordering: f1, d2, d1, d3, s3
      val equivQuery1 = sql(
        """
          | select d3.*
          | from f1, d2, d1, d3, s3
          | where f1_fk2 = d2_pk1 and d2_c2 < 2
          | and f1_fk1 = d1_pk1
          | and f1_fk3 = d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      // Run the queries and compare the results.
      verifyStarJoinPlans(query1, equivQuery1, Row(1, 2, 3, 4) :: Nil)

      // Test 2: Expanding star join with inequality join predicates.
      // Choose the next largest join, d3-s3.
      // Star join:
      //   (<)  (<)
      // d1 - f1 - d2
      //  |
      // s3 - d3
      //   (=)
      //
      // Default join reordering: d1, f1, d2, d3, s3
      // Star join reordering: d3, s3, d1, f1, d2
      val query2 = sql(
        """
          | select d3.*
          | from d1, d2, f1, d3, s3
          | where f1_fk2 <= d2_pk1
          | and f1_fk1 <= d1_pk1
          | and d1_c3 = s3_c3
          | and d3_fk1 = s3_pk1 and s3_c3 = 3
          | limit 1
        """.stripMargin)

      // Equivalent query
      // Default join reordering: d3, s3, d1, f1, d2
      val equivQuery2 = sql(
        """
          | select d3.*
          | from d3, s3, d1, f1, d2
          | where f1_fk2 <= d2_pk1
          | and f1_fk1 <= d1_pk1
          | and d1_c3 = s3_c3
          | and d3_fk1 = s3_pk1 and s3_c3 = 3
          | limit 1
        """.stripMargin)

      verifyStarJoinPlans(query2, equivQuery2, Row(1, 2, 3, 4) :: Nil)

      // Test 3: Expanding star join with fact table in Cartesian product.
      // Choose the next largest join, d3-s3.
      // Star join:
      // d1   f1
      //  |   | x
      // s3 - d3
      //   (=)
      //
      // Default join reordering: d1, s3, d3, f1
      // Star join reordering: d3, s3, d1, f1
      val query3 = sql(
        """
          | select f1.*
          | from d1, s3, d3 cross join f1
          | where d1_c2 = s3_c2
          | and d3_fk1 = s3_pk1 and s3_c3 = 3
          | limit 1
        """.stripMargin)

      // Equivalent query
      // Default join reordering: d3, s3, d1, f1
      val equivQuery3 = sql(
        """
          | select f1.*
          | from d3, s3, d1 cross join f1
          | where d1_c2 = s3_c2
          | and d3_fk1 = s3_pk1 and s3_c3 = 3
          | limit 1
        """.stripMargin)

      verifyStarJoinPlans(query3, equivQuery3, Row(1, 2, 3, 4) :: Nil)

      // Test 4: Selective star join on a subset of dimensions.
      // Star join:
      //  (<)   (=)
      // d1 - f1  -  d2
      //      | (<)
      //      d3 - s3
      //
      // Default join reordering: d3, f1, s3, d1, d2
      // Star reordering: f1, d2, d3, s3, d1
      val query4 = sql(
        """
          | select d3.*
          | from d3, f1, s3, d1, d2
          | where f1_fk2 = d2_pk1 and d2_c2 <= 2
          | and f1_fk1 <= d1_pk1
          | and f1_fk3 <= d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      // Equivalent query
      // Default join reordering: f1, d2, d3, s3, d1
      val equivQuery4 = sql(
        """
          | select d3.*
          | from f1, d2, d3, s3, d1
          | where f1_fk2 = d2_pk1 and d2_c2 <= 2
          | and f1_fk1 <= d1_pk1
          | and f1_fk3 <= d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      verifyStarJoinPlans(query4, equivQuery4, Row(1, 2, 3, 4) :: Nil)
    }
  }

  test("SPARK-17791: Test non qualifying star joins") {
    withTempView("f1", "d1", "d2", "d3", "s3") {
      createStarSchemaTables("f1", "d1", "d2", "d3", "s3")

      // Test 1: Fact and dimensions over non-base tables
      val query1 = sql(
        """
          | select cf1.*
          | from (select d2_pk1 as pk from d2 limit 2) cd2,
          |      (select d1_pk1 as pk from d1 limit 2) cd1,
          |      (select f1_fk1 as fk1, f1_fk2 as fk2 from f1 limit 2) cf1
          | where cf1.fk1 = cd1.pk and cf1.fk2 = cd2.pk
          | limit 1
        """.stripMargin)

      // Equivalent query: same as query1
      val equivQuery1 = sql(
        """
          | select cf1.*
          | from (select d2_pk1 as pk from d2 limit 2) cd2,
          |      (select d1_pk1 as pk from d1 limit 2) cd1,
          |      (select f1_fk1 as fk1, f1_fk2 as fk2 from f1 limit 2) cf1
          | where cf1.fk1 = cd1.pk and cf1.fk2 = cd2.pk
          | limit 1
        """.stripMargin)

      verifyStarJoinPlans(query1, equivQuery1, Row(1, 2) :: Nil)

      // Test 2: Fact over base table and dimensions over non-base tables
      val query2 = sql(
        """
          | select f1.*
          | from (select d2_pk1 as pk from d2 limit 2) cd2,
          |      (select d1_pk1 as pk from d1 limit 2) cd1,
          |      f1
          | where f1_fk1 = cd1.pk and f1_fk2 = cd2.pk
          | limit 1
        """.stripMargin)

      // Equivalent query: same as query2
      val equivQuery2 = sql(
        """
          | select f1.*
          | from (select d2_pk1 as pk from d2 limit 2) cd2,
          |      (select d1_pk1 as pk from d1 limit 2) cd1,
          |      f1
          | where f1_fk1 = cd1.pk and f1_fk2 = cd2.pk
          | limit 1
        """.stripMargin)

      verifyStarJoinPlans(query2, equivQuery2, Row(1, 2, 3, 4) :: Nil)

      // Test 3: Non-selective star join
      val query3 = sql(
        """
          | select d3.*
          | from d1, d2, f1, d3, s3
          | where f1_fk2 = d2_pk1
          | and f1_fk1 = d1_pk1
          | and f1_fk3 = d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      // Equivalent query: same as query3
      val equivQuery3 = sql(
        """
          | select d3.*
          | from d1, d2, f1, d3, s3
          | where f1_fk2 = d2_pk1
          | and f1_fk1 = d1_pk1
          | and f1_fk3 = d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      verifyStarJoinPlans(query3, equivQuery3, Row(1, 2, 3, 4) :: Nil)

      // Test 4: Non equi join predicates
      val query4 = sql(
        """
          | select d3.*
          | from d1, f1, d3, s3
          | where abs(f1_fk1) = abs(d1_pk1)
          | and f1_fk3 = d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      // Equivalent query: same as query4
      val equivQuery4 = sql(
        """
          | select d3.*
          | from d1, f1, d3, s3
          | where abs(f1_fk1) = abs(d1_pk1)
          | and f1_fk3 = d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      verifyStarJoinPlans(query4, equivQuery4, Row(1, 2, 3, 4) :: Nil)

      // Test 5: Multiple fact tables with the same size
      val query5 = sql(
        """
          | select f11.*
          | from d1, d2, f1 f11, f1 f12
          | where f11.f1_fk1 = d1_pk1
          | and f11.f1_fk2 = d2_pk1
          | and f11.f1_fk3 = f12.f1_fk3
          | limit 1
        """.stripMargin)

      // Equivalent query: same as query5
      val equivQuery5 = sql(
        """
          | select f11.*
          | from d1, d2, f1 f11, f1 f12
          | where f11.f1_fk1 = d1_pk1
          | and f11.f1_fk2 = d2_pk1
          | and f11.f1_fk3 = f12.f1_fk3
          | limit 1
        """.stripMargin)

      verifyStarJoinPlans(query5, equivQuery5, Row(1, 2, 3, 4) :: Nil)
    }
  }

  test("SPARK-17791: Miscellaneous tests with star join reordering") {
    withTempView("f1", "d1", "d2", "d3", "s3") {
      createStarSchemaTables("f1", "d1", "d2", "d3", "s3")

      // Star join reordering with uncorrelated subquery predicates
      // on the fact table.
      // Star join: f1, d2, d1, d3, s3
      // Default join reordering: d1, f1, d2, d3, s3
      val query1 = sql(
        """
          | select d3.*
          | from d1, d2, f1, d3, s3
          | where f1_fk2 = d2_pk1 and d2_c2 <= 2
          | and f1_fk1 = d1_pk1
          | and f1_c4 IN (select s3_c4 from s3 limit 2)
          | and f1_fk3 = d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      // Equivalent query
      // Default reordering: f1, d2, d1, d3, s3
      val equivQuery1 = sql(
        """
          | select d3.*
          | from f1, d2, d1, d3, s3
          | where f1_fk2 = d2_pk1 and d2_c2 <= 2
          | and f1_fk1 = d1_pk1
          | and f1_c4 IN (select s3_c4 from s3 limit 2)
          | and f1_fk3 = d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      verifyStarJoinPlans(query1, equivQuery1, Row(1, 2, 3, 4) :: Nil)

      // Star join reordering with correlated subquery predicates
      // on the fact table.
      // Star join: f1, d2, d1, d3, s3
      // Default join reordering: d1, f1, d2, d3, s3
      val query2 = sql(
        """
          | select d3.*
          | from d1, d2, f1, d3, s3
          | where f1_fk2 = d2_pk1 and d2_c2 <= 2
          | and f1_fk1 = d1_pk1
          | and f1_c4 IN (select s3_c4 from s3 where f1_fk3 = s3_c3)
          | and f1_fk3 = d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      // Equivalent query
      // Default reordering: f1, d2, d1, d3, s3
      val equivQuery2 = sql(
        """
          | select d3.*
          | from f1, d2, d1, d3, s3
          | where f1_fk2 = d2_pk1 and d2_c2 <= 2
          | and f1_fk1 = d1_pk1
          | and f1_c4 IN (select s3_c4 from s3 where f1_fk3 = s3_c3)
          | and f1_fk3 = d3_pk1
          | and d3_fk1 = s3_pk1
          | limit 1
        """.stripMargin)

      verifyStarJoinPlans(query2, equivQuery2, Row(1, 2, 3, 4) :: Nil)
    }
  }
}
