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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Pivot}

class PivotParserSuite extends AnalysisTest {

  import CatalystSqlParser._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    comparePlans(parsePlan(sqlCommand), plan, checkAnalysis = false)
  }

  test("pivot - alias") {
    Seq(
      "SELECT pv.* FROM t PIVOT (sum(a) FOR b IN (1, 2)) pv",
      "SELECT pv.* FROM t PIVOT (sum(a) FOR b IN (1, 2)) AS pv"
    ).foreach { sql =>
      withClue(sql) {
        assertEqual(
          sql,
          Pivot(
            None,
            UnresolvedAttribute("b"),
            Seq(Literal(1), Literal(2)),
            Seq(UnresolvedFunction("sum", Seq(UnresolvedAttribute("a")), isDistinct = false)),
            table("t"))
            .subquery("pv")
            .select(star("pv"))
        )
      }
    }
  }

  test("pivot - no alias") {
    assertEqual(
      "SELECT * FROM t PIVOT (sum(a) FOR b IN (1, 2))",
      Pivot(
        None,
        UnresolvedAttribute("b"),
        Seq(Literal(1), Literal(2)),
        Seq(UnresolvedFunction("sum", Seq(UnresolvedAttribute("a")), isDistinct = false)),
        table("t"))
        .select(star())
    )
  }

  test("pivot - alias with qualified column references") {
    Seq(
      "SELECT pv.x, pv.y FROM t PIVOT (sum(a) FOR b IN (1, 2)) pv",
      "SELECT pv.x, pv.y FROM t PIVOT (sum(a) FOR b IN (1, 2)) AS pv"
    ).foreach { sql =>
      withClue(sql) {
        assertEqual(
          sql,
          Pivot(
            None,
            UnresolvedAttribute("b"),
            Seq(Literal(1), Literal(2)),
            Seq(UnresolvedFunction("sum", Seq(UnresolvedAttribute("a")), isDistinct = false)),
            table("t"))
            .subquery("pv")
            .select($"pv.x", $"pv.y")
        )
      }
    }
  }

  test("pivot - alias with multiple aggregations") {
    Seq(
      "SELECT pv.* FROM t PIVOT (sum(a) s, avg(a) v FOR b IN (1, 2)) pv",
      "SELECT pv.* FROM t PIVOT (sum(a) s, avg(a) v FOR b IN (1, 2)) AS pv"
    ).foreach { sql =>
      withClue(sql) {
        assertEqual(
          sql,
          Pivot(
            None,
            UnresolvedAttribute("b"),
            Seq(Literal(1), Literal(2)),
            Seq(
              UnresolvedFunction("sum", Seq(UnresolvedAttribute("a")), isDistinct = false).as("s"),
              UnresolvedFunction("avg", Seq(UnresolvedAttribute("a")), isDistinct = false).as("v")),
            table("t"))
            .subquery("pv")
            .select(star("pv"))
        )
      }
    }
  }
}
