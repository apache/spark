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

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Unpivot}
import org.apache.spark.sql.internal.SQLConf

class UnpivotParserSuite extends AnalysisTest {

  import CatalystSqlParser._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    comparePlans(parsePlan(sqlCommand), plan, checkAnalysis = false)
  }

  private def intercept(sqlCommand: String, condition: Option[String], messages: String*): Unit =
    interceptParseException(parsePlan)(sqlCommand, messages: _*)(condition)

  test("unpivot - single value") {
    assertEqual(
      "SELECT * FROM t UNPIVOT (val FOR col in (a, b))",
      Unpivot(
        None,
        Some(Seq(Seq($"a"), Seq($"b"))),
        None,
        "col",
        Seq("val"),
        table("t"))
        .where(coalesce($"val").isNotNull)
        .select(star())
    )
  }

  test("unpivot - single value with alias") {
    Seq(
      "SELECT * FROM t UNPIVOT (val FOR col in (a A, b))",
      "SELECT * FROM t UNPIVOT (val FOR col in (a AS A, b))"
    ).foreach { sql =>
      withClue(sql) {
        assertEqual(
          sql,
          Unpivot(
            None,
            Some(Seq(Seq($"a"), Seq($"b"))),
            Some(Seq(Some("A"), None)),
            "col",
            Seq("val"),
            table("t"))
            .where(coalesce($"val").isNotNull)
            .select(star())
        )
      }
    }
  }

  test("unpivot - multiple values") {
    assertEqual(
      "SELECT * FROM t UNPIVOT ((val1, val2) FOR col in ((a, b), (c, d)))",
      Unpivot(
        None,
        Some(Seq(Seq($"a", $"b"), Seq($"c", $"d"))),
        None,
        "col",
        Seq("val1", "val2"),
        table("t"))
        .where(coalesce($"val1", $"val2").isNotNull)
        .select(star())
    )
  }

  test("unpivot - multiple values with alias") {
    Seq(
      "SELECT * FROM t UNPIVOT ((val1, val2) FOR col in ((a, b) first, (c, d)))",
      "SELECT * FROM t UNPIVOT ((val1, val2) FOR col in ((a, b) AS first, (c, d)))"
    ).foreach { sql =>
      withClue(sql) {
        assertEqual(
          sql,
          Unpivot(
            None,
            Some(Seq(Seq($"a", $"b"), Seq($"c", $"d"))),
            Some(Seq(Some("first"), None)),
            "col",
            Seq("val1", "val2"),
            table("t"))
            .where(coalesce($"val1", $"val2").isNotNull)
            .select(star())
        )
      }
    }
  }

  test("unpivot - multiple values with inner alias") {
    Seq(
      "SELECT * FROM t UNPIVOT ((val1, val2) FOR col in ((a A, b), (c, d)))",
      "SELECT * FROM t UNPIVOT ((val1, val2) FOR col in ((a AS A, b), (c, d)))"
    ).foreach { sql =>
      withClue(sql) {
        intercept(sql, Some("PARSE_SYNTAX_ERROR"), "Syntax error at or near ")
      }
    }
  }

  test("unpivot - alias") {
    Seq(
      "SELECT up.* FROM t UNPIVOT (val FOR col in (a, b)) up",
      "SELECT up.* FROM t UNPIVOT (val FOR col in (a, b)) AS up"
    ).foreach { sql =>
      withClue(sql) {
        assertEqual(
          sql,
          Unpivot(
            None,
            Some(Seq(Seq($"a"), Seq($"b"))),
            None,
            "col",
            Seq("val"),
            table("t"))
            .where(coalesce($"val").isNotNull)
            .subquery("up")
            .select(star("up"))
        )
      }
    }
  }

  test("unpivot - no unpivot value names") {
    intercept(
      "SELECT * FROM t UNPIVOT (() FOR col in ((a, b), (c, d)))",
      Some("PARSE_SYNTAX_ERROR"), "Syntax error at or near "
    )
  }

  test("unpivot - no unpivot columns") {
    Seq(
      "SELECT * FROM t UNPIVOT (val FOR col in ())",
      "SELECT * FROM t UNPIVOT ((val1, val2) FOR col in ())",
      "SELECT * FROM t UNPIVOT ((val1, val2) FOR col in (()))"
    ).foreach { sql =>
      withClue(sql) {
        intercept(sql, Some("PARSE_SYNTAX_ERROR"), "Syntax error at or near ")
      }
    }
  }

  test("unpivot - exclude nulls") {
    assertEqual(
      "SELECT * FROM t UNPIVOT EXCLUDE NULLS (val FOR col in (a, b))",
      Unpivot(
        None,
        Some(Seq(Seq($"a"), Seq($"b"))),
        None,
        "col",
        Seq("val"),
        table("t"))
        .where(coalesce($"val").isNotNull)
        .select(star())
    )
  }

  test("unpivot - include nulls") {
    assertEqual(
      "SELECT * FROM t UNPIVOT INCLUDE NULLS (val FOR col in (a, b))",
      Unpivot(
        None,
        Some(Seq(Seq($"a"), Seq($"b"))),
        None,
        "col",
        Seq("val"),
        table("t"))
        .select(star())
    )
  }

  test("unpivot - with joins") {
    // unpivot the left table
    assertEqual(
      "SELECT * FROM t1 UNPIVOT (val FOR col in (a, b)) JOIN t2",
      Unpivot(
        None,
        Some(Seq(Seq($"a"), Seq($"b"))),
        None,
        "col",
        Seq("val"),
        table("t1")
      ).where(coalesce($"val").isNotNull).join(table("t2")).select(star()))

    // unpivot the join result
    assertEqual(
      "SELECT * FROM t1 JOIN t2 UNPIVOT (val FOR col in (a, b))",
      Unpivot(
        None,
        Some(Seq(Seq($"a"), Seq($"b"))),
        None,
        "col",
        Seq("val"),
        table("t1").join(table("t2"))
      ).where(coalesce($"val").isNotNull).select(star()))

    // unpivot the right table
    assertEqual(
      "SELECT * FROM t1 JOIN (t2 UNPIVOT (val FOR col in (a, b)))",
      table("t1").join(
        Unpivot(
          None,
          Some(Seq(Seq($"a"), Seq($"b"))),
          None,
          "col",
          Seq("val"),
          table("t2")
        ).where(coalesce($"val").isNotNull)
      ).select(star()))
  }

  test("unpivot - with implicit joins") {
    // unpivot the left table
    assertEqual(
      "SELECT * FROM t1 UNPIVOT (val FOR col in (a, b)), t2",
      Unpivot(
        None,
        Some(Seq(Seq($"a"), Seq($"b"))),
        None,
        "col",
        Seq("val"),
        table("t1")
      ).where(coalesce($"val").isNotNull).join(table("t2")).select(star()))

    // unpivot the join result
    assertEqual(
      "SELECT * FROM t1, t2 UNPIVOT (val FOR col in (a, b))",
      Unpivot(
        None,
        Some(Seq(Seq($"a"), Seq($"b"))),
        None,
        "col",
        Seq("val"),
        table("t1").join(table("t2"))
      ).where(coalesce($"val").isNotNull).select(star()))

    // unpivot the right table - same SQL as above but with ANSI mode
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ANSI_RELATION_PRECEDENCE.key -> "true") {
      assertEqual(
        "SELECT * FROM t1, t2 UNPIVOT (val FOR col in (a, b))",
        table("t1").join(
          Unpivot(
            None,
            Some(Seq(Seq($"a"), Seq($"b"))),
            None,
            "col",
            Seq("val"),
            table("t2")
          ).where(coalesce($"val").isNotNull)
        ).select(star()))
    }

    // unpivot the right table
    assertEqual(
      "SELECT * FROM t1, (t2 UNPIVOT (val FOR col in (a, b)))",
      table("t1").join(
        Unpivot(
          None,
          Some(Seq(Seq($"a"), Seq($"b"))),
          None,
          "col",
          Seq("val"),
          table("t2")
        ).where(coalesce($"val").isNotNull)
      ).select(star()))

    // mixed with explicit joins
    assertEqual(
      // unpivot the join result of t1, t2 and t3
      "SELECT * FROM t1, t2 JOIN t3 UNPIVOT (val FOR col in (a, b))",
      Unpivot(
        None,
        Some(Seq(Seq($"a"), Seq($"b"))),
        None,
        "col",
        Seq("val"),
        table("t1").join(table("t2")).join(table("t3"))
      ).where(coalesce($"val").isNotNull).select(star()))
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ANSI_RELATION_PRECEDENCE.key -> "true") {
      assertEqual(
        // unpivot the join result of t2 and t3
        "SELECT * FROM t1, t2 JOIN t3 UNPIVOT (val FOR col in (a, b))",
        table("t1").join(
          Unpivot(
            None,
            Some(Seq(Seq($"a"), Seq($"b"))),
            None,
            "col",
            Seq("val"),
            table("t2").join(table("t3"))
          ).where(coalesce($"val").isNotNull)
        ).select(star()))
    }
  }

  test("unpivot - nested unpivot") {
    assertEqual(
      "SELECT * FROM t1 UNPIVOT (val FOR col in (a, b)) UNPIVOT (val FOR col in (a, b))",
      Unpivot(
        None,
        Some(Seq(Seq($"a"), Seq($"b"))),
        None,
        "col",
        Seq("val"),
        Unpivot(
          None,
          Some(Seq(Seq($"a"), Seq($"b"))),
          None,
          "col",
          Seq("val"),
          table("t1")
        ).where(coalesce($"val").isNotNull)
      ).where(coalesce($"val").isNotNull).select(star()))
  }
}
