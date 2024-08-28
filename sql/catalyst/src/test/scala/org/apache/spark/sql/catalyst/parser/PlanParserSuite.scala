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

import scala.annotation.nowarn

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, NamedParameter, PosParameter, RelationTimeTravel, UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedGenerator, UnresolvedInlineTable, UnresolvedRelation, UnresolvedStar, UnresolvedSubqueryColumnAliases, UnresolvedTableValuedFunction, UnresolvedTVFAliases}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Decimal, DecimalType, IntegerType, LongType, StringType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Parser test cases for rules defined in [[CatalystSqlParser]] / [[AstBuilder]].
 *
 * There is also SparkSqlParserSuite in sql/core module for parser rules defined in sql/core module.
 */
class PlanParserSuite extends AnalysisTest {
  import CatalystSqlParser._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    comparePlans(parsePlan(sqlCommand), plan, checkAnalysis = false)
  }

  private def parseException(sqlText: String): SparkThrowable = {
    super.parseException(parsePlan)(sqlText)
  }

  private def cte(
      plan: LogicalPlan,
      namedPlans: (String, (LogicalPlan, Seq[String]))*): UnresolvedWith = {
    val ctes = namedPlans.map {
      case (name, (cte, columnAliases)) =>
        val subquery = if (columnAliases.isEmpty) {
          cte
        } else {
          UnresolvedSubqueryColumnAliases(columnAliases, cte)
        }
        name -> SubqueryAlias(name, subquery)
    }
    UnresolvedWith(plan, ctes)
  }

  test("single comment case one") {
    val plan = table("a").select(star())
    assertEqual("-- single comment\nSELECT * FROM a", plan)
  }

  test("single comment case two") {
    val plan = table("a").select(star())
    assertEqual("-- single comment\\\nwith line continuity\nSELECT * FROM a", plan)
  }

  test("bracketed comment case one") {
    val plan = table("a").select(star())
    assertEqual(
      """
        |/* This is an example of SQL which should not execute:
        | * select 'multi-line';
        | */
        |SELECT * FROM a
      """.stripMargin, plan)
  }

  test("bracketed comment case two") {
    val plan = table("a").select(star())
    assertEqual(
      """
        |/*
        |SELECT 'trailing' as x1; -- inside block comment
        |*/
        |SELECT * FROM a
      """.stripMargin, plan)
  }

  test("nested bracketed comment case one") {
    val plan = table("a").select(star())
    assertEqual(
      """
        |/* This block comment surrounds a query which itself has a block comment...
        |SELECT /* embedded single line */ 'embedded' AS x2;
        |*/
        |SELECT * FROM a
      """.stripMargin, plan)
  }

  test("nested bracketed comment case two") {
    val plan = table("a").select(star())
    assertEqual(
      """
        |SELECT -- continued after the following block comments...
        |/* Deeply nested comment.
        |   This includes a single apostrophe to make sure we aren't decoding this part as a string.
        |SELECT 'deep nest' AS n1;
        |/* Second level of nesting...
        |SELECT 'deeper nest' as n2;
        |/* Third level of nesting...
        |SELECT 'deepest nest' as n3;
        |*/
        |Hoo boy. Still two deep...
        |*/
        |Now just one deep...
        |*/
        |* FROM a
      """.stripMargin, plan)
  }

  test("nested bracketed comment case three") {
    val plan = table("a").select(star())
    assertEqual(
      """
        |/* This block comment surrounds a query which itself has a block comment...
        |//* I am a nested bracketed comment.
        |*/
        |*/
        |SELECT * FROM a
      """.stripMargin, plan)
  }

  test("nested bracketed comment case four") {
    val plan = table("a").select(star())
    assertEqual(
      """
        |/*/**/*/
        |SELECT * FROM a
      """.stripMargin, plan)
  }

  test("nested bracketed comment case five") {
    val plan = table("a").select(star())
    assertEqual(
      """
        |/*/*abc*/*/
        |SELECT * FROM a
      """.stripMargin, plan)
  }

  test("nested bracketed comment case six") {
    val plan = table("a").select(star())
    assertEqual(
      """
        |/*/*foo*//*bar*/*/
        |SELECT * FROM a
      """.stripMargin, plan)
  }

  test("nested bracketed comment case seven") {
    val plan = OneRowRelation().select(Literal(1).as("a"))
    assertEqual(
      """
        |/*abc*/
        |select 1 as a
        |/*
        |
        |2 as b
        |/*abc */
        |, 3 as c
        |
        |/**/
        |*/
      """.stripMargin, plan)
  }

  test("unclosed bracketed comment one") {
    val query = """/*abc*/
                  |select 1 as a
                  |/*
                  |
                  |2 as b
                  |/*abc */
                  |, 3 as c
                  |
                  |/**/
                  |""".stripMargin
    checkError(
      exception = parseException(query),
      errorClass = "UNCLOSED_BRACKETED_COMMENT",
      parameters = Map.empty)
  }

  test("unclosed bracketed comment two") {
    val query = """/*abc*/
                  |select 1 as a
                  |/*
                  |
                  |2 as b
                  |/*abc */
                  |, 3 as c
                  |
                  |/**/
                  |select 4 as d
                  |""".stripMargin
    checkError(
      exception = parseException(query),
      errorClass = "UNCLOSED_BRACKETED_COMMENT",
      parameters = Map.empty)
  }

  test("case insensitive") {
    val plan = table("a").select(star())
    assertEqual("sELEct * FroM a", plan)
    assertEqual("select * fRoM a", plan)
    assertEqual("SELECT * FROM a", plan)
  }

  test("explain") {
    val sql1 = "EXPLAIN logical SELECT 1"
    checkError(
      exception = parseException(sql1),
      errorClass = "_LEGACY_ERROR_TEMP_0039",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = sql1,
        start = 0,
        stop = 23))

    val sql2 = "EXPLAIN formatted SELECT 1"
    checkError(
      exception = parseException(sql2),
      errorClass = "_LEGACY_ERROR_TEMP_0039",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = sql2,
        start = 0,
        stop = 25))
  }

  test("SPARK-42552: select and union without parentheses") {
    val plan = Distinct(OneRowRelation().select(Literal(1))
      .union(OneRowRelation().select(Literal(1))))
    assertEqual("select 1 union select 1", plan)
  }

  test("set operations") {
    val a = table("a").select(star())
    val b = table("b").select(star())

    assertEqual("select * from a union select * from b", Distinct(a.union(b)))
    assertEqual("select * from a union distinct select * from b", Distinct(a.union(b)))
    assertEqual("select * from a union all select * from b", a.union(b))
    assertEqual("select * from a except select * from b", a.except(b, isAll = false))
    assertEqual("select * from a except distinct select * from b", a.except(b, isAll = false))
    assertEqual("select * from a except all select * from b", a.except(b, isAll = true))
    assertEqual("select * from a minus select * from b", a.except(b, isAll = false))
    assertEqual("select * from a minus all select * from b", a.except(b, isAll = true))
    assertEqual("select * from a minus distinct select * from b", a.except(b, isAll = false))
    assertEqual("select * from a " +
      "intersect select * from b", a.intersect(b, isAll = false))
    assertEqual("select * from a intersect distinct select * from b", a.intersect(b, isAll = false))
    assertEqual("select * from a intersect all select * from b", a.intersect(b, isAll = true))
  }

  test("common table expressions") {
    assertEqual(
      "with cte1 as (select * from a) select * from cte1",
      cte(table("cte1").select(star()), "cte1" -> ((table("a").select(star()), Seq.empty))))
    assertEqual(
      "with cte1 (select 1) select * from cte1",
      cte(table("cte1").select(star()), "cte1" -> ((OneRowRelation().select(1), Seq.empty))))
    assertEqual(
      "with cte1 (select 1), cte2 as (select * from cte1) select * from cte2",
      cte(table("cte2").select(star()),
        "cte1" -> ((OneRowRelation().select(1), Seq.empty)),
        "cte2" -> ((table("cte1").select(star()), Seq.empty))))
    val sql = "with cte1 (select 1), cte1 as (select 1 from cte1) select * from cte1"
    checkError(
      exception = parseException(sql),
      errorClass = "_LEGACY_ERROR_TEMP_0038",
      parameters = Map("duplicateNames" -> "'cte1'"),
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 68))
  }

  test("simple select query") {
    assertEqual("select 1", OneRowRelation().select(1))
    assertEqual("select a, b", OneRowRelation().select($"a", $"b"))
    assertEqual("select a, b from db.c", table("db", "c").select($"a", $"b"))
    assertEqual("select a, b from db.c where x < 1",
      table("db", "c").where($"x" < 1).select($"a", $"b"))
    assertEqual(
      "select a, b from db.c having x < 1",
      table("db", "c").having()($"a", $"b")($"x" < 1))
    assertEqual("select distinct a, b from db.c", Distinct(table("db", "c").select($"a", $"b")))
    assertEqual("select all a, b from db.c", table("db", "c").select($"a", $"b"))
    assertEqual("select from tbl", OneRowRelation().select($"from".as("tbl")))
    assertEqual("select a from 1k.2m", table("1k", "2m").select($"a"))
  }

  test("hive-style single-FROM statement") {
    assertEqual("from a select b, c", table("a").select($"b", $"c"))
    assertEqual(
      "from db.a select b, c where d < 1", table("db", "a").where($"d" < 1).select($"b", $"c"))
    assertEqual("from a select distinct b, c", Distinct(table("a").select($"b", $"c")))

    // Weird "FROM table" queries, should be invalid anyway
    val sql1 = "from a"
    checkError(
      exception = parseException(sql1),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "end of input", "hint" -> ""))

    val sql2 = "from (from a union all from b) c select *"
    checkError(
      exception = parseException(sql2),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'union'", "hint" -> ""))
  }

  test("multi select query") {
    assertEqual(
      "from a select * select * where s < 10",
      table("a").select(star()).union(table("a").where($"s" < 10).select(star())))
    val sql1 = "from a select * select * from x where a.s < 10"
    checkError(
      exception = parseException(sql1),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'from'", "hint" -> ""))
    val sql2 = "from a select * from b"
    checkError(
      exception = parseException(sql2),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'from'", "hint" -> ""))
    assertEqual(
      "from a insert into tbl1 select * insert into tbl2 select * where s < 10",
      table("a").select(star()).insertInto("tbl1").union(
        table("a").where($"s" < 10).select(star()).insertInto("tbl2")))
    assertEqual(
      "select * from (from a select * select *)",
      table("a").select(star())
        .union(table("a").select(star()))
        .as("__auto_generated_subquery_name").select(star()))
  }

  test("query organization") {
    // Test all valid combinations of order by/sort by/distribute by/cluster by/limit/windows
    val baseSql = "select * from t"
    val basePlan = table("t").select(star())

    val ws = Map("w1" -> WindowSpecDefinition(Seq.empty, Seq.empty, UnspecifiedFrame))
    val limitWindowClauses = Seq(
      ("", (p: LogicalPlan) => p),
      (" limit 10", (p: LogicalPlan) => p.limit(10)),
      (" window w1 as ()", (p: LogicalPlan) => WithWindowDefinition(ws, p)),
      (" window w1 as () limit 10", (p: LogicalPlan) => WithWindowDefinition(ws, p).limit(10))
    )

    val orderSortDistrClusterClauses = Seq(
      ("", basePlan),
      (" order by a, b desc", basePlan.orderBy($"a".asc, $"b".desc)),
      (" sort by a, b desc", basePlan.sortBy($"a".asc, $"b".desc))
    )

    orderSortDistrClusterClauses.foreach {
      case (s1, p1) =>
        limitWindowClauses.foreach {
          case (s2, pf2) =>
            assertEqual(baseSql + s1 + s2, pf2(p1))
        }
    }

    val sql1 = s"$baseSql order by a sort by a"
    val parameters = Map("clauses" -> "ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY")
    checkError(
      exception = parseException(sql1),
      errorClass = "UNSUPPORTED_FEATURE.COMBINATION_QUERY_RESULT_CLAUSES",
      parameters = parameters,
      context = ExpectedContext(
        fragment = "order by a sort by a",
        start = 16,
        stop = 35))

    val sql2 = s"$baseSql cluster by a distribute by a"
    checkError(
      exception = parseException(sql2),
      errorClass = "UNSUPPORTED_FEATURE.COMBINATION_QUERY_RESULT_CLAUSES",
      parameters = parameters,
      context = ExpectedContext(
        fragment = "cluster by a distribute by a",
        start = 16,
        stop = 43))

    val sql3 = s"$baseSql order by a cluster by a"
    checkError(
      exception = parseException(sql3),
      errorClass = "UNSUPPORTED_FEATURE.COMBINATION_QUERY_RESULT_CLAUSES",
      parameters = parameters,
      context = ExpectedContext(
        fragment = "order by a cluster by a",
        start = 16,
        stop = 38))

    val sql4 = s"$baseSql order by a distribute by a"
    checkError(
      exception = parseException(sql4),
      errorClass = "UNSUPPORTED_FEATURE.COMBINATION_QUERY_RESULT_CLAUSES",
      parameters = parameters,
      context = ExpectedContext(
        fragment = "order by a distribute by a",
        start = 16,
        stop = 41))
  }

  test("insert into") {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    import org.apache.spark.sql.catalyst.dsl.plans._
    val sql = "select * from t"
    val plan = table("t").select(star())
    def insert(
        partition: Map[String, Option[String]],
        overwrite: Boolean = false,
        ifPartitionNotExists: Boolean = false): LogicalPlan =
      InsertIntoStatement(table("s"), partition, Nil, plan, overwrite, ifPartitionNotExists)

    // Single inserts
    assertEqual(s"insert overwrite table s $sql",
      insert(Map.empty, overwrite = true))
    assertEqual(s"insert overwrite table s partition (e = 1) if not exists $sql",
      insert(Map("e" -> Option("1")), overwrite = true, ifPartitionNotExists = true))
    assertEqual(s"insert into s $sql",
      insert(Map.empty))
    assertEqual(s"insert into table s partition (c = 'd', e = 1) $sql",
      insert(Map("c" -> Option("d"), "e" -> Option("1"))))

    // Multi insert
    val plan2 = table("t").where($"x" > 5).select(star())
    assertEqual("from t insert into s select * limit 1 insert into u select * where x > 5",
      plan.limit(1).insertInto("s").union(plan2.insertInto("u")))
  }

  test("aggregation") {
    val sql = "select a, b, sum(c) as c from d group by a, b"
    val sqlWithoutGroupBy = "select a, b, sum(c) as c from d"

    // Normal
    assertEqual(sql, table("d").groupBy($"a", $"b")($"a", $"b", $"sum".function($"c").as("c")))

    // Cube
    assertEqual(s"$sql with cube",
      table("d").groupBy(Cube(Seq(Seq($"a"), Seq($"b"))))($"a", $"b", $"sum".function($"c")
        .as("c")))
    assertEqual(s"$sqlWithoutGroupBy group by cube(a, b)",
      table("d").groupBy(Cube(Seq(Seq($"a"), Seq($"b"))))($"a", $"b", $"sum".function($"c")
        .as("c")))
    assertEqual(s"$sqlWithoutGroupBy group by cube (a, b)",
      table("d").groupBy(Cube(Seq(Seq($"a"), Seq($"b"))))($"a", $"b", $"sum".function($"c")
        .as("c")))

    // Rollup
    assertEqual(s"$sql with rollup",
      table("d").groupBy(Rollup(Seq(Seq($"a"), Seq($"b"))))($"a", $"b", $"sum".function($"c")
        .as("c")))
    assertEqual(s"$sqlWithoutGroupBy group by rollup(a, b)",
      table("d").groupBy(Rollup(Seq(Seq($"a"), Seq($"b"))))($"a", $"b", $"sum".function($"c")
        .as("c")))
    assertEqual(s"$sqlWithoutGroupBy group by rollup (a, b)",
      table("d").groupBy(Rollup(Seq(Seq($"a"), Seq($"b"))))($"a", $"b", $"sum".function($"c")
        .as("c")))

    // Grouping Sets
    assertEqual(s"$sql grouping sets((a, b), (a), ())",
      Aggregate(Seq(GroupingSets(Seq(Seq($"a", $"b"), Seq($"a"), Seq()), Seq($"a", $"b"))),
        Seq($"a", $"b", $"sum".function($"c").as("c")), table("d")))

    assertEqual(s"$sqlWithoutGroupBy group by grouping sets((a, b), (a), ())",
      Aggregate(Seq(GroupingSets(Seq(Seq($"a", $"b"), Seq($"a"), Seq()))),
        Seq($"a", $"b", $"sum".function($"c").as("c")), table("d")))

    val sql1 = "SELECT a, b, count(distinct a, distinct b) as c FROM d GROUP BY a, b"
    checkError(
      exception = parseException(sql1),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'b'", "hint" -> ": extra input 'b'"))
  }

  test("limit") {
    val sql = "select * from t"
    val plan = table("t").select(star())
    assertEqual(s"$sql limit 10", plan.limit(10))
    assertEqual(s"$sql limit cast(9 / 4 as int)", plan.limit(Cast(Literal(9) / 4, IntegerType)))
  }

  test("window spec") {
    // Note that WindowSpecs are testing in the ExpressionParserSuite
    val sql = "select * from t"
    val plan = table("t").select(star())
    val spec = WindowSpecDefinition(Seq($"a", $"b"), Seq($"c".asc),
      SpecifiedWindowFrame(RowFrame, -Literal(1), Literal(1)))

    // Test window resolution.
    val ws1 = Map("w1" -> spec, "w2" -> spec, "w3" -> spec)
    assertEqual(
      s"""$sql
         |window w1 as (partition by a, b order by c rows between 1 preceding and 1 following),
         |       w2 as w1,
         |       w3 as w1""".stripMargin,
      WithWindowDefinition(ws1, plan))
  }

  test("lateral view") {
    val explode = UnresolvedGenerator(FunctionIdentifier("explode"), Seq($"x"))
    val jsonTuple = UnresolvedGenerator(FunctionIdentifier("json_tuple"), Seq($"x", $"y"))

    // Single lateral view
    assertEqual(
      "select * from t lateral view explode(x) expl as x",
      table("t")
        .generate(explode, alias = Some("expl"), outputNames = Seq("x"))
        .select(star()))

    // Multiple lateral views
    assertEqual(
      """select *
        |from t
        |lateral view explode(x) expl
        |lateral view outer json_tuple(x, y) jtup q, z""".stripMargin,
      table("t")
        .generate(explode, alias = Some("expl"))
        .generate(jsonTuple, outer = true, alias = Some("jtup"), outputNames = Seq("q", "z"))
        .select(star()))

    // Multi-Insert lateral views.
    val from = table("t1").generate(explode, alias = Some("expl"), outputNames = Seq("x"))
    assertEqual(
      """from t1
        |lateral view explode(x) expl as x
        |insert into t2
        |select *
        |lateral view json_tuple(x, y) jtup q, z
        |insert into t3
        |select *
        |where s < 10
      """.stripMargin,
      Union(from
        .generate(jsonTuple, alias = Some("jtup"), outputNames = Seq("q", "z"))
        .select(star())
        .insertInto("t2"),
        from.where($"s" < 10).select(star()).insertInto("t3")))

    // Unresolved generator.
    val expected = table("t")
      .generate(
        UnresolvedGenerator(FunctionIdentifier("posexplode"), Seq($"x")),
        alias = Some("posexpl"),
        outputNames = Seq("x", "y"))
      .select(star())
    assertEqual(
      "select * from t lateral view posexplode(x) posexpl as x, y",
      expected)

    val sql1 =
      """select *
        |from t
        |lateral view explode(x) expl
        |pivot (
        |  sum(x)
        |  FOR y IN ('a', 'b')
        |)""".stripMargin
    val fragment1 =
      """from t
        |lateral view explode(x) expl
        |pivot (
        |  sum(x)
        |  FOR y IN ('a', 'b')
        |)""".stripMargin
    checkError(
      exception = parseException(sql1),
      errorClass = "NOT_ALLOWED_IN_FROM.LATERAL_WITH_PIVOT",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = fragment1,
        start = 9,
        stop = 84))

    val sql2 =
      """select *
        |from t
        |lateral view explode(x) expl
        |unpivot (
        |  val FOR y IN (x)
        |)""".stripMargin
    val fragment2 =
      """from t
        |lateral view explode(x) expl
        |unpivot (
        |  val FOR y IN (x)
        |)""".stripMargin
    checkError(
      exception = parseException(sql2),
      errorClass = "NOT_ALLOWED_IN_FROM.LATERAL_WITH_UNPIVOT",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = fragment2,
        start = 9,
        stop = 74))

    val sql3 =
      """select *
        |from t
        |lateral view explode(x) expl
        |pivot (
        |  sum(x)
        |  FOR y IN ('a', 'b')
        |)
        |unpivot (
        |  val FOR y IN (x)
        |)""".stripMargin
    val fragment3 =
      """from t
        |lateral view explode(x) expl
        |pivot (
        |  sum(x)
        |  FOR y IN ('a', 'b')
        |)
        |unpivot (
        |  val FOR y IN (x)
        |)""".stripMargin
    checkError(
      exception = parseException(sql3),
      errorClass = "NOT_ALLOWED_IN_FROM.UNPIVOT_WITH_PIVOT",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = fragment3,
        start = 9,
        stop = 115))
  }

  test("joins") {
    // Test single joins.
    val testUnconditionalJoin = (sql: String, jt: JoinType) => {
      assertEqual(
        s"select * from t as tt $sql u",
        table("t").as("tt").join(table("u"), jt, None).select(star()))
    }
    val testConditionalJoin = (sql: String, jt: JoinType) => {
      assertEqual(
        s"select * from t $sql u as uu on a = b",
        table("t").join(table("u").as("uu"), jt, Option($"a" === $"b")).select(star()))
    }
    val testNaturalJoin = (sql: String, jt: JoinType) => {
      assertEqual(
        s"select * from t tt natural $sql u as uu",
        table("t").as("tt").join(table("u").as("uu"), NaturalJoin(jt), None).select(star()))
    }
    val testUsingJoin = (sql: String, jt: JoinType) => {
      assertEqual(
        s"select * from t $sql u using(a, b)",
        table("t").join(table("u"), UsingJoin(jt, Seq("a", "b")), None).select(star()))
    }
    val testLateralJoin = (sql: String, jt: JoinType) => {
      assertEqual(
        s"select * from t $sql lateral (select * from u) uu",
        LateralJoin(
          table("t"),
          LateralSubquery(table("u").select(star()).as("uu")),
          jt, None).select(star()))
    }
    val testAllExceptLateral = Seq(testUnconditionalJoin, testConditionalJoin, testNaturalJoin,
      testUsingJoin)
    val testAll = testAllExceptLateral :+ testLateralJoin
    val testExistence = Seq(testUnconditionalJoin, testConditionalJoin, testUsingJoin)
    def test(sql: String, jt: JoinType, tests: Seq[(String, JoinType) => Unit]): Unit = {
      tests.foreach(_(sql, jt))
    }
    test("cross join", Cross, Seq(testUnconditionalJoin, testLateralJoin))
    test(",", Inner, Seq(testUnconditionalJoin, testLateralJoin))
    test("join", Inner, testAll)
    test("inner join", Inner, testAll)
    test("left join", LeftOuter, testAll)
    test("left outer join", LeftOuter, testAll)
    test("right join", RightOuter, testAllExceptLateral)
    test("right outer join", RightOuter, testAllExceptLateral)
    test("full join", FullOuter, testAllExceptLateral)
    test("full outer join", FullOuter, testAllExceptLateral)
    test("left semi join", LeftSemi, testExistence)
    test("semi join", LeftSemi, testExistence)
    test("left anti join", LeftAnti, testExistence)
    test("anti join", LeftAnti, testExistence)

    // Test natural cross join
    val sql1 = "select * from a natural cross join b"
    checkError(
      exception = parseException(sql1),
      errorClass = "INCOMPATIBLE_JOIN_TYPES",
      parameters = Map("joinType1" -> "NATURAL", "joinType2" -> "CROSS"),
      sqlState = "42613",
      context = ExpectedContext(
        fragment = "natural cross join b",
        start = 16,
        stop = 35))

    // Test natural join with a condition
    val sql2 = "select * from a natural join b on a.id = b.id"
    checkError(
      exception = parseException(sql2),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'on'", "hint" -> ""))

    // Test multiple consecutive joins
    assertEqual(
      "select * from a join b join c right join d",
      table("a").join(table("b")).join(table("c")).join(table("d"), RightOuter).select(star()))

    // SPARK-17296
    assertEqual(
      "select * from t1 cross join t2 join t3 on t3.id = t1.id join t4 on t4.id = t1.id",
      table("t1")
        .join(table("t2"), Cross)
        .join(table("t3"), Inner, Option($"t3.id" === $"t1.id"))
        .join(table("t4"), Inner, Option($"t4.id" === $"t1.id"))
        .select(star()))

    // Test multiple on clauses.
    val sql3 = "select * from t1 inner join t2 inner join t3 on col3 = col2 on col3 = col1"
    checkError(
      exception = parseException(sql3),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'on'", "hint" -> ""))

    // Parenthesis
    assertEqual(
      "select * from t1 inner join (t2 inner join t3 on col3 = col2) on col3 = col1",
      table("t1")
        .join(table("t2")
          .join(table("t3"), Inner, Option($"col3" === $"col2")), Inner,
            Option($"col3" === $"col1"))
        .select(star()))
    assertEqual(
      "select * from t1 inner join (t2 inner join t3) on col3 = col2",
      table("t1")
        .join(table("t2").join(table("t3"), Inner, None), Inner, Option($"col3" === $"col2"))
        .select(star()))
    assertEqual(
      "select * from t1 inner join (t2 inner join t3 on col3 = col2)",
      table("t1")
        .join(table("t2").join(table("t3"), Inner, Option($"col3" === $"col2")), Inner, None)
        .select(star()))

    // Implicit joins.
    assertEqual(
      "select * from t1, t3 join t2 on t1.col1 = t2.col2",
      table("t1")
        .join(table("t3"))
        .join(table("t2"), Inner, Option($"t1.col1" === $"t2.col2"))
        .select(star()))

    assertEqual(
      "select * from t1 JOIN t2, t3 join t2 on t1.col1 = t2.col2",
      table("t1")
        .join(table("t2"))
        .join(table("t3"))
        .join(table("t2"), Inner, Option($"t1.col1" === $"t2.col2"))
        .select(star()))

    // Implicit joins - ANSI mode
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ANSI_RELATION_PRECEDENCE.key -> "true") {

      assertEqual(
        "select * from t1, t3 join t2 on t1.col1 = t2.col2",
        table("t1").join(
          table("t3").join(table("t2"), Inner, Option($"t1.col1" === $"t2.col2")))
          .select(star()))

      assertEqual(
        "select * from t1 JOIN t2, t3 join t2 on t1.col1 = t2.col2",
        table("t1").join(table("t2")).join(
          table("t3").join(table("t2"), Inner, Option($"t1.col1" === $"t2.col2")))
          .select(star()))
    }

    // Test lateral join with join conditions
    assertEqual(
      s"select * from t join lateral (select * from u) uu on true",
      LateralJoin(
        table("t"),
        LateralSubquery(table("u").select(star()).as("uu")),
        Inner, Option(true)).select(star()))

    // Test multiple lateral joins
    assertEqual(
      "select * from a, lateral (select * from b) bb, lateral (select * from c) cc",
      LateralJoin(
        LateralJoin(
          table("a"),
          LateralSubquery(table("b").select(star()).as("bb")),
          Inner, None),
        LateralSubquery(table("c").select(star()).as("cc")),
        Inner, None).select(star())
    )
  }

  test("sampled relations") {
    val sql = "select * from t"
    assertEqual(s"$sql tablesample(100 rows)",
      table("t").limit(100).select(star()))
    assertEqual(s"$sql tablesample(43 percent) as x",
      Sample(0, .43d, withReplacement = false, 10L, table("t").as("x")).select(star()))
    assertEqual(s"$sql tablesample(bucket 4 out of 10) as x",
      Sample(0, .4d, withReplacement = false, 10L, table("t").as("x")).select(star()))

    val sql1 = s"$sql tablesample(bucket 4 out of 10 on x) as x"
    val fragment1 = "tablesample(bucket 4 out of 10 on x)"
    checkError(
      exception = parseException(sql1),
      errorClass = "_LEGACY_ERROR_TEMP_0015",
      parameters = Map("msg" -> "BUCKET x OUT OF y ON colname"),
      context = ExpectedContext(
        fragment = fragment1,
        start = 16,
        stop = 51))

    val sql2 = s"$sql tablesample(bucket 11 out of 10) as x"
    val fragment2 = "tablesample(bucket 11 out of 10)"
    checkError(
      exception = parseException(sql2),
      errorClass = "_LEGACY_ERROR_TEMP_0064",
      parameters = Map("msg" -> "Sampling fraction (1.1) must be on interval [0, 1]"),
      context = ExpectedContext(
        fragment = fragment2,
        start = 16,
        stop = 47))

    val sql3 = "SELECT * FROM parquet_t0 TABLESAMPLE(300M) s"
    val fragment3 = "TABLESAMPLE(300M)"
    checkError(
      exception = parseException(sql3),
      errorClass = "_LEGACY_ERROR_TEMP_0015",
      parameters = Map("msg" -> "byteLengthLiteral"),
      context = ExpectedContext(
        fragment = fragment3,
        start = 25,
        stop = 41))

    val sql4 = "SELECT * FROM parquet_t0 TABLESAMPLE(BUCKET 3 OUT OF 32 ON rand()) s"
    val fragment4 = "TABLESAMPLE(BUCKET 3 OUT OF 32 ON rand())"
    checkError(
      exception = parseException(sql4),
      errorClass = "_LEGACY_ERROR_TEMP_0015",
      parameters = Map("msg" -> "BUCKET x OUT OF y ON function"),
      context = ExpectedContext(
        fragment = fragment4,
        start = 25,
        stop = 65))
  }

  test("sub-query") {
    val plan = table("t0").select($"id")
    assertEqual("select id from (t0)", plan)
    assertEqual("select id from ((((((t0))))))", plan)
    assertEqual(
      "(select * from t1) union distinct (select * from t2)",
      Distinct(table("t1").select(star()).union(table("t2").select(star()))))
    assertEqual(
      "select * from ((select * from t1) union (select * from t2)) t",
      Distinct(
        table("t1").select(star()).union(table("t2").select(star()))).as("t").select(star()))
    assertEqual(
      """select  id
        |from (((select id from t0)
        |       union all
        |       (select  id from t0))
        |      union all
        |      (select id from t0)) as u_1
      """.stripMargin,
      plan.union(plan).union(plan).as("u_1").select($"id"))
  }

  test("scalar sub-query") {
    assertEqual(
      "select (select max(b) from s) ss from t",
      table("t").select(ScalarSubquery(table("s").select($"max".function($"b"))).as("ss")))
    assertEqual(
      "select * from t where a = (select b from s)",
      table("t").where($"a" === ScalarSubquery(table("s").select($"b"))).select(star()))
    assertEqual(
      "select g from t group by g having a > (select b from s)",
      table("t")
        .having($"g")($"g")($"a" > ScalarSubquery(table("s").select($"b"))))
  }

  test("table reference") {
    assertEqual("table t", table("t"))
    assertEqual("table d.t", table("d", "t"))
  }

  test("table valued function") {
    assertEqual(
      "select * from range(2)",
      UnresolvedTableValuedFunction("range", Literal(2) :: Nil).select(star()))

    // SPARK-34627
    val sql1 = "select * from default.range(2)"
    val fragment1 = "default.range(2)"
    checkError(
      exception = parseException(sql1),
      errorClass = "INVALID_SQL_SYNTAX.INVALID_TABLE_VALUED_FUNC_NAME",
      parameters = Map("funcName" -> "`default`.`range`"),
      context = ExpectedContext(
        fragment = fragment1,
        start = 14,
        stop = 29))

    // SPARK-38957
    val sql2 = "select * from spark_catalog.default.range(2)"
    val fragment2 = "spark_catalog.default.range(2)"
    checkError(
      exception = parseException(sql2),
      errorClass = "INVALID_SQL_SYNTAX.INVALID_TABLE_VALUED_FUNC_NAME",
      parameters = Map("funcName" -> "`spark_catalog`.`default`.`range`"),
      context = ExpectedContext(
        fragment = fragment2,
        start = 14,
        stop = 43))
  }

  test("SPARK-20311 range(N) as alias") {
    assertEqual(
      "SELECT * FROM range(10) AS t",
      SubqueryAlias("t", UnresolvedTableValuedFunction("range", Literal(10) :: Nil))
        .select(star()))
    assertEqual(
      "SELECT * FROM range(7) AS t(a)",
      SubqueryAlias("t",
        UnresolvedTVFAliases("range",
          UnresolvedTableValuedFunction("range", Literal(7) :: Nil), "a" :: Nil)
      ).select(star()))
  }

  test("SPARK-20841 Support table column aliases in FROM clause") {
    assertEqual(
      "SELECT * FROM testData AS t(col1, col2)",
      SubqueryAlias(
        "t",
        UnresolvedSubqueryColumnAliases(
          Seq("col1", "col2"),
          UnresolvedRelation(TableIdentifier("testData"))
        )
      ).select(star()))
  }

  test("SPARK-20962 Support subquery column aliases in FROM clause") {
    assertEqual(
      "SELECT * FROM (SELECT a AS x, b AS y FROM t) t(col1, col2)",
      SubqueryAlias(
        "t",
        UnresolvedSubqueryColumnAliases(
          Seq("col1", "col2"),
          UnresolvedRelation(TableIdentifier("t")).select($"a".as("x"), $"b".as("y"))
        )
      ).select(star()))
  }

  test("SPARK-20963 Support aliases for join relations in FROM clause") {
    val src1 = UnresolvedRelation(TableIdentifier("src1")).as("s1")
    val src2 = UnresolvedRelation(TableIdentifier("src2")).as("s2")
    assertEqual(
      "SELECT * FROM (src1 s1 INNER JOIN src2 s2 ON s1.id = s2.id) dst(a, b, c, d)",
      SubqueryAlias(
        "dst",
        UnresolvedSubqueryColumnAliases(
          Seq("a", "b", "c", "d"),
          src1.join(src2, Inner, Option($"s1.id" === $"s2.id"))
        )
      ).select(star()))
  }

  test("SPARK-34335 Support referencing subquery with column aliases by table alias") {
    assertEqual(
      "SELECT t.col1, t.col2 FROM (SELECT a AS x, b AS y FROM t) t(col1, col2)",
      SubqueryAlias(
        "t",
        UnresolvedSubqueryColumnAliases(
          Seq("col1", "col2"),
          UnresolvedRelation(TableIdentifier("t")).select($"a".as("x"), $"b".as("y")))
      ).select($"t.col1", $"t.col2")
    )
  }

  test("inline table") {
    assertEqual("values 1, 2, 3, 4",
      UnresolvedInlineTable(Seq("col1"), Seq(1, 2, 3, 4).map(x => Seq(Literal(x)))))

    assertEqual(
      "values (1, 'a'), (2, 'b') as tbl(a, b)",
      UnresolvedInlineTable(
        Seq("a", "b"),
        Seq(Literal(1), Literal("a")) :: Seq(Literal(2), Literal("b")) :: Nil).as("tbl"))
  }

  test("simple select query with !> and !<") {
    // !< is equivalent to >=
    assertEqual("select a, b from db.c where x !< 1",
      table("db", "c").where($"x" >= 1).select($"a", $"b"))
    // !> is equivalent to <=
    assertEqual("select a, b from db.c where x !> 1",
      table("db", "c").where($"x" <= 1).select($"a", $"b"))
  }

  test("select hint syntax") {
    // Hive compatibility: Missing parameter raises ParseException.
    val sql1 = "SELECT /*+ HINT() */ * FROM t"
    checkError(
      exception = parseException(sql1),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "')'", "hint" -> ""))

    // Disallow space as the delimiter.
    val sql2 = "SELECT /*+ INDEX(a b c) */ * from default.t"
    checkError(
      exception = parseException(sql2),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'b'", "hint" -> ""))

    comparePlans(
      parsePlan("SELECT /*+ HINT */ * FROM t"),
      UnresolvedHint("HINT", Seq.empty, table("t").select(star())))

    comparePlans(
      parsePlan("SELECT /*+ BROADCASTJOIN(u) */ * FROM t"),
      UnresolvedHint("BROADCASTJOIN", Seq($"u"), table("t").select(star())))

    comparePlans(
      parsePlan("SELECT /*+ MAPJOIN(u) */ * FROM t"),
      UnresolvedHint("MAPJOIN", Seq($"u"), table("t").select(star())))

    comparePlans(
      parsePlan("SELECT /*+ STREAMTABLE(a,b,c) */ * FROM t"),
      UnresolvedHint("STREAMTABLE", Seq($"a", $"b", $"c"), table("t").select(star())))

    comparePlans(
      parsePlan("SELECT /*+ INDEX(t, emp_job_ix) */ * FROM t"),
      UnresolvedHint("INDEX", Seq($"t", $"emp_job_ix"), table("t").select(star())))

    comparePlans(
      parsePlan("SELECT /*+ MAPJOIN(`default.t`) */ * from `default.t`"),
      UnresolvedHint("MAPJOIN", Seq(UnresolvedAttribute.quoted("default.t")),
        table("default.t").select(star())))

    comparePlans(
      parsePlan("SELECT /*+ MAPJOIN(t) */ a from t where true group by a order by a"),
      UnresolvedHint("MAPJOIN", Seq($"t"),
        table("t").where(Literal(true)).groupBy($"a")($"a")).orderBy($"a".asc))

    comparePlans(
      parsePlan("SELECT /*+ COALESCE(10) */ * FROM t"),
      UnresolvedHint("COALESCE", Seq(Literal(10)),
        table("t").select(star())))

    comparePlans(
      parsePlan("SELECT /*+ REPARTITION(100) */ * FROM t"),
      UnresolvedHint("REPARTITION", Seq(Literal(100)),
        table("t").select(star())))

    comparePlans(
      parsePlan(
        "INSERT INTO s SELECT /*+ REPARTITION(100), COALESCE(500), COALESCE(10) */ * FROM t"),
      InsertIntoStatement(table("s"), Map.empty, Nil,
        UnresolvedHint("REPARTITION", Seq(Literal(100)),
          UnresolvedHint("COALESCE", Seq(Literal(500)),
            UnresolvedHint("COALESCE", Seq(Literal(10)),
              table("t").select(star())))), overwrite = false, ifPartitionNotExists = false))

    comparePlans(
      parsePlan("SELECT /*+ BROADCASTJOIN(u), REPARTITION(100) */ * FROM t"),
      UnresolvedHint("BROADCASTJOIN", Seq($"u"),
        UnresolvedHint("REPARTITION", Seq(Literal(100)),
          table("t").select(star()))))

    val sql3 = "SELECT /*+ COALESCE(30 + 50) */ * FROM t"
    checkError(
      exception = parseException(sql3),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'+'", "hint" -> ""))

    comparePlans(
      parsePlan("SELECT /*+ REPARTITION(c) */ * FROM t"),
      UnresolvedHint("REPARTITION", Seq(UnresolvedAttribute("c")),
        table("t").select(star())))

    comparePlans(
      parsePlan("SELECT /*+ REPARTITION(100, c) */ * FROM t"),
      UnresolvedHint("REPARTITION", Seq(Literal(100), UnresolvedAttribute("c")),
        table("t").select(star())))

    comparePlans(
      parsePlan("SELECT /*+ REPARTITION(100, c), COALESCE(50) */ * FROM t"),
      UnresolvedHint("REPARTITION", Seq(Literal(100), UnresolvedAttribute("c")),
        UnresolvedHint("COALESCE", Seq(Literal(50)),
          table("t").select(star()))))

    comparePlans(
      parsePlan("SELECT /*+ REPARTITION(100, c), BROADCASTJOIN(u), COALESCE(50) */ * FROM t"),
      UnresolvedHint("REPARTITION", Seq(Literal(100), UnresolvedAttribute("c")),
        UnresolvedHint("BROADCASTJOIN", Seq($"u"),
          UnresolvedHint("COALESCE", Seq(Literal(50)),
            table("t").select(star())))))

    comparePlans(
      parsePlan(
        """
          |SELECT
          |/*+ REPARTITION(100, c), BROADCASTJOIN(u), COALESCE(50), REPARTITION(300, c) */
          |* FROM t
        """.stripMargin),
      UnresolvedHint("REPARTITION", Seq(Literal(100), UnresolvedAttribute("c")),
        UnresolvedHint("BROADCASTJOIN", Seq($"u"),
          UnresolvedHint("COALESCE", Seq(Literal(50)),
            UnresolvedHint("REPARTITION", Seq(Literal(300), UnresolvedAttribute("c")),
              table("t").select(star()))))))

    comparePlans(
      parsePlan("SELECT /*+ REPARTITION_BY_RANGE(c) */ * FROM t"),
      UnresolvedHint("REPARTITION_BY_RANGE", Seq(UnresolvedAttribute("c")),
        table("t").select(star())))

    comparePlans(
      parsePlan("SELECT /*+ REPARTITION_BY_RANGE(100, c) */ * FROM t"),
      UnresolvedHint("REPARTITION_BY_RANGE", Seq(Literal(100), UnresolvedAttribute("c")),
        table("t").select(star())))
  }

  test("SPARK-20854: select hint syntax with expressions") {
    comparePlans(
      parsePlan("SELECT /*+ HINT1(a, array(1, 2, 3)) */ * from t"),
      UnresolvedHint("HINT1", Seq($"a",
        UnresolvedFunction("array", Literal(1) :: Literal(2) :: Literal(3) :: Nil, false)),
        table("t").select(star())
      )
    )

    comparePlans(
      parsePlan("SELECT /*+ HINT1(a, 5, 'a', b) */ * from t"),
      UnresolvedHint("HINT1", Seq($"a", Literal(5), Literal("a"), $"b"),
        table("t").select(star())
      )
    )

    comparePlans(
      parsePlan("SELECT /*+ HINT1('a', (b, c), (1, 2)) */ * from t"),
      UnresolvedHint("HINT1",
        Seq(Literal("a"),
          CreateStruct($"b" :: $"c" :: Nil),
          CreateStruct(Literal(1) :: Literal(2) :: Nil)),
        table("t").select(star())
      )
    )
  }

  test("SPARK-20854: multiple hints") {
    comparePlans(
      parsePlan("SELECT /*+ HINT1(a, 1) hint2(b, 2) */ * from t"),
      UnresolvedHint("HINT1", Seq($"a", Literal(1)),
        UnresolvedHint("hint2", Seq($"b", Literal(2)),
          table("t").select(star())
        )
      )
    )

    comparePlans(
      parsePlan("SELECT /*+ HINT1(a, 1),hint2(b, 2) */ * from t"),
      UnresolvedHint("HINT1", Seq($"a", Literal(1)),
        UnresolvedHint("hint2", Seq($"b", Literal(2)),
          table("t").select(star())
        )
      )
    )

    comparePlans(
      parsePlan("SELECT /*+ HINT1(a, 1) */ /*+ hint2(b, 2) */ * from t"),
      UnresolvedHint("HINT1", Seq($"a", Literal(1)),
        UnresolvedHint("hint2", Seq($"b", Literal(2)),
          table("t").select(star())
        )
      )
    )

    comparePlans(
      parsePlan("SELECT /*+ HINT1(a, 1), hint2(b, 2) */ /*+ hint3(c, 3) */ * from t"),
      UnresolvedHint("HINT1", Seq($"a", Literal(1)),
        UnresolvedHint("hint2", Seq($"b", Literal(2)),
          UnresolvedHint("hint3", Seq($"c", Literal(3)),
            table("t").select(star())
          )
        )
      )
    )
  }

  test("TRIM function") {
    def assertTrimPlans(inputSQL: String, expectedExpression: Expression): Unit = {
      comparePlans(
        parsePlan(inputSQL),
        Project(Seq(UnresolvedAlias(expectedExpression)), OneRowRelation())
      )
    }

    val sql1 = "select ltrim(both 'S' from 'SS abc S'"
    checkError(
      exception = parseException(sql1),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'from'", "hint" -> "")) // expecting {')'

    val sql2 = "select rtrim(trailing 'S' from 'SS abc S'"
    checkError(
      exception = parseException(sql2),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'from'", "hint" -> "")) // expecting {')'

    assertTrimPlans(
      "SELECT TRIM(BOTH '@$%&( )abc' FROM '@ $ % & ()abc ' )",
      StringTrim(Literal("@ $ % & ()abc "), Some(Literal("@$%&( )abc")))
    )
    assertTrimPlans(
      "SELECT TRIM(LEADING 'c []' FROM '[ ccccbcc ')",
      StringTrimLeft(Literal("[ ccccbcc "), Some(Literal("c []")))
    )
    assertTrimPlans(
      "SELECT TRIM(TRAILING 'c&^,.' FROM 'bc...,,,&&&ccc')",
      StringTrimRight(Literal("bc...,,,&&&ccc"), Some(Literal("c&^,.")))
    )

    assertTrimPlans(
      "SELECT TRIM(BOTH FROM '  bunch o blanks  ')",
      StringTrim(Literal("  bunch o blanks  "), None)
    )
    assertTrimPlans(
      "SELECT TRIM(LEADING FROM '  bunch o blanks  ')",
      StringTrimLeft(Literal("  bunch o blanks  "), None)
    )
    assertTrimPlans(
      "SELECT TRIM(TRAILING FROM '  bunch o blanks  ')",
      StringTrimRight(Literal("  bunch o blanks  "), None)
    )

    assertTrimPlans(
      "SELECT TRIM('xyz' FROM 'yxTomxx')",
      StringTrim(Literal("yxTomxx"), Some(Literal("xyz")))
    )
  }

  test("OVERLAY function") {
    def assertOverlayPlans(inputSQL: String, expectedExpression: Expression): Unit = {
      comparePlans(
        parsePlan(inputSQL),
        Project(Seq(UnresolvedAlias(expectedExpression)), OneRowRelation())
      )
    }

    assertOverlayPlans(
      "SELECT OVERLAY('Spark SQL' PLACING '_' FROM 6)",
      new Overlay(Literal("Spark SQL"), Literal("_"), Literal(6))
    )

    assertOverlayPlans(
      "SELECT OVERLAY('Spark SQL' PLACING 'CORE' FROM 7)",
      new Overlay(Literal("Spark SQL"), Literal("CORE"), Literal(7))
    )

    assertOverlayPlans(
      "SELECT OVERLAY('Spark SQL' PLACING 'ANSI ' FROM 7 FOR 0)",
      Overlay(Literal("Spark SQL"), Literal("ANSI "), Literal(7), Literal(0))
    )

    assertOverlayPlans(
      "SELECT OVERLAY('Spark SQL' PLACING 'tructured' FROM 2 FOR 4)",
      Overlay(Literal("Spark SQL"), Literal("tructured"), Literal(2), Literal(4))
    )
  }

  test("precedence of set operations") {
    val a = table("a").select(star())
    val b = table("b").select(star())
    val c = table("c").select(star())
    val d = table("d").select(star())

    val query1 =
      """
        |SELECT * FROM a
        |UNION
        |SELECT * FROM b
        |EXCEPT
        |SELECT * FROM c
        |INTERSECT
        |SELECT * FROM d
      """.stripMargin

    val query2 =
      """
        |SELECT * FROM a
        |UNION
        |SELECT * FROM b
        |EXCEPT ALL
        |SELECT * FROM c
        |INTERSECT ALL
        |SELECT * FROM d
      """.stripMargin

    assertEqual(query1, Distinct(a.union(b)).except(c.intersect(d, isAll = false), isAll = false))
    assertEqual(query2, Distinct(a.union(b)).except(c.intersect(d, isAll = true), isAll = true))

    // Now disable precedence enforcement to verify the old behaviour.
    withSQLConf(SQLConf.LEGACY_SETOPS_PRECEDENCE_ENABLED.key -> "true") {
      assertEqual(query1,
        Distinct(a.union(b)).except(c, isAll = false).intersect(d, isAll = false))
      assertEqual(query2, Distinct(a.union(b)).except(c, isAll = true).intersect(d, isAll = true))
    }

    // Explicitly enable the precedence enforcement
    withSQLConf(SQLConf.LEGACY_SETOPS_PRECEDENCE_ENABLED.key -> "false") {
      assertEqual(query1,
        Distinct(a.union(b)).except(c.intersect(d, isAll = false), isAll = false))
      assertEqual(query2, Distinct(a.union(b)).except(c.intersect(d, isAll = true), isAll = true))
    }
  }

  test("create/alter view as insert into table") {
    val sql1 = "CREATE VIEW testView AS INSERT INTO jt VALUES(1, 1)"
    checkError(
      exception = parseException(sql1),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'INSERT'", "hint" -> ""))

    // Multi insert query
    val sql2 =
      """CREATE VIEW testView AS FROM jt
        |INSERT INTO tbl1 SELECT * WHERE jt.id < 5
        |INSERT INTO tbl2 SELECT * WHERE jt.id > 4""".stripMargin
    checkError(
      exception = parseException(sql2),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'INSERT'", "hint" -> ""))

    val sql3 = "ALTER VIEW testView AS INSERT INTO jt VALUES(1, 1)"
    checkError(
      exception = parseException(sql3),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'INSERT'", "hint" -> ""))

    // Multi insert query
    val sql4 =
      """ALTER VIEW testView AS FROM jt
        |INSERT INTO tbl1 SELECT * WHERE jt.id < 5
        |INSERT INTO tbl2 SELECT * WHERE jt.id > 4""".stripMargin
    checkError(
      exception = parseException(sql4),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'INSERT'", "hint" -> ""))
  }

  test("Invalid insert constructs in the query") {
    val sql1 = "SELECT * FROM (INSERT INTO BAR VALUES (2))"
    checkError(
      exception = parseException(sql1),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'BAR'", "hint" -> ": missing ')'"))

    val sql2 = "SELECT * FROM S WHERE C1 IN (INSERT INTO T VALUES (2))"
    checkError(
      exception = parseException(sql2),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'IN'", "hint" -> ""))
  }

  test("relation in v2 catalog") {
    assertEqual("TABLE testcat.db.tab", table("testcat", "db", "tab"))
    assertEqual("SELECT * FROM testcat.db.tab", table("testcat", "db", "tab").select(star()))

    assertEqual(
      """
        |WITH cte1 AS (SELECT * FROM testcat.db.tab)
        |SELECT * FROM cte1
      """.stripMargin,
      cte(table("cte1").select(star()),
        "cte1" -> ((table("testcat", "db", "tab").select(star()), Seq.empty))))

    assertEqual(
      "SELECT /*+ BROADCAST(tab) */ * FROM testcat.db.tab",
      table("testcat", "db", "tab").select(star()).hint("BROADCAST", $"tab"))
  }

  test("CTE with column alias") {
    assertEqual(
      "WITH t(x) AS (SELECT c FROM a) SELECT * FROM t",
      cte(table("t").select(star()), "t" -> ((table("a").select($"c"), Seq("x")))))
  }

  test("statement containing terminal semicolons") {
    assertEqual("select 1;", OneRowRelation().select(1))
    assertEqual("select a, b;", OneRowRelation().select($"a", $"b"))
    assertEqual("select a, b from db.c;;;", table("db", "c").select($"a", $"b"))
    assertEqual("select a, b from db.c; ;;  ;", table("db", "c").select($"a", $"b"))
  }

  test("table valued function with named arguments") {
    // All named arguments
    assertEqual(
      "select * from my_tvf(arg1 => 'value1', arg2 => true)",
      UnresolvedTableValuedFunction("my_tvf",
        NamedArgumentExpression("arg1", Literal("value1")) ::
          NamedArgumentExpression("arg2", Literal(true)) :: Nil).select(star()))

    // Unnamed and named arguments
    assertEqual(
      "select * from my_tvf(2, arg1 => 'value1', arg2 => true)",
      UnresolvedTableValuedFunction("my_tvf",
        Literal(2) ::
          NamedArgumentExpression("arg1", Literal("value1")) ::
          NamedArgumentExpression("arg2", Literal(true)) :: Nil).select(star()))

    // Mixed arguments
    assertEqual(
      "select * from my_tvf(arg1 => 'value1', 2, arg2 => true)",
      UnresolvedTableValuedFunction("my_tvf",
        NamedArgumentExpression("arg1", Literal("value1")) ::
          Literal(2) ::
          NamedArgumentExpression("arg2", Literal(true)) :: Nil).select(star()))
    assertEqual(
      "select * from my_tvf(group => 'abc')",
      UnresolvedTableValuedFunction("my_tvf",
        NamedArgumentExpression("group", Literal("abc")) :: Nil).select(star()))
  }

  test("table valued function with table arguments") {
    assertEqual(
      "select * from my_tvf(table (v1), table (select 1))",
      UnresolvedTableValuedFunction("my_tvf",
        FunctionTableSubqueryArgumentExpression(UnresolvedRelation(Seq("v1"))) ::
          FunctionTableSubqueryArgumentExpression(
            Project(Seq(UnresolvedAlias(Literal(1))), OneRowRelation())) :: Nil).select(star()))

    // All named arguments
    assertEqual(
      "select * from my_tvf(arg1 => table (v1), arg2 => table (select 1))",
      UnresolvedTableValuedFunction("my_tvf",
        NamedArgumentExpression("arg1",
          FunctionTableSubqueryArgumentExpression(UnresolvedRelation(Seq("v1")))) ::
          NamedArgumentExpression("arg2",
            FunctionTableSubqueryArgumentExpression(
              Project(Seq(UnresolvedAlias(Literal(1))), OneRowRelation()))) :: Nil).select(star()))

    // Unnamed and named arguments
    assertEqual(
      "select * from my_tvf(2, table (v1), arg1 => table (select 1))",
      UnresolvedTableValuedFunction("my_tvf",
        Literal(2) ::
          FunctionTableSubqueryArgumentExpression(UnresolvedRelation(Seq("v1"))) ::
          NamedArgumentExpression("arg1",
            FunctionTableSubqueryArgumentExpression(
              Project(Seq(UnresolvedAlias(Literal(1))), OneRowRelation()))) :: Nil).select(star()))

    // Mixed arguments
    assertEqual(
      "select * from my_tvf(arg1 => table (v1), 2, arg2 => true)",
      UnresolvedTableValuedFunction("my_tvf",
        NamedArgumentExpression("arg1",
          FunctionTableSubqueryArgumentExpression(UnresolvedRelation(Seq("v1")))) ::
          Literal(2) ::
          NamedArgumentExpression("arg2", Literal(true)) :: Nil).select(star()))

    // Negative tests:
    // Parentheses are missing from the table argument.
    val sql1 = "select * from my_tvf(arg1 => table v1)"
    checkError(
      exception = parseException(sql1),
      errorClass =
        "INVALID_SQL_SYNTAX.INVALID_TABLE_FUNCTION_IDENTIFIER_ARGUMENT_MISSING_PARENTHESES",
      parameters = Map("argumentName" -> "`v1`"),
      context = ExpectedContext(
        fragment = "table v1",
        start = 29,
        stop = sql1.length - 2))
  }

  test("SPARK-44503: Support PARTITION BY and ORDER BY clause for TVF TABLE arguments") {
    Seq("partition", "distribute").foreach { partition =>
      Seq("order", "sort").foreach { order =>
        // Positive tests.
        val sql1 = s"select * from my_tvf(arg1 => table(v1) $partition by col1)"
        assertEqual(
          sql1,
          Project(
            projectList = Seq(UnresolvedStar(target = None)),
            child = UnresolvedTableValuedFunction(
              name = Seq("my_tvf"),
              functionArgs = Seq(NamedArgumentExpression(
                key = "arg1",
                value = FunctionTableSubqueryArgumentExpression(
                  plan = UnresolvedRelation(multipartIdentifier = Seq("v1")),
                  partitionByExpressions = Seq(UnresolvedAttribute("col1"))
                ))))))
        val sql2 = s"select * from my_tvf(arg1 => table(v1) $partition by col1 $order by col2 asc)"
        assertEqual(
          sql2,
          Project(
            projectList = Seq(UnresolvedStar(target = None)),
            child = UnresolvedTableValuedFunction(
              name = Seq("my_tvf"),
              functionArgs = Seq(NamedArgumentExpression(
                key = "arg1",
                value = FunctionTableSubqueryArgumentExpression(
                  plan = UnresolvedRelation(multipartIdentifier = Seq("v1")),
                  partitionByExpressions = Seq(
                    UnresolvedAttribute("col1")),
                  orderByExpressions = Seq(SortOrder(
                    child = UnresolvedAttribute("col2"),
                    direction = Ascending,
                    nullOrdering = NullsFirst,
                    sameOrderExpressions = Seq.empty))
                ))))))
        val sql3 = s"select * from my_tvf(arg1 => table(v1) " +
          s"$partition by (col1, col2) $order by (col2 asc, col3 desc))"
        assertEqual(
          sql3,
          Project(
            projectList = Seq(UnresolvedStar(target = None)),
            child = UnresolvedTableValuedFunction(
              name = Seq("my_tvf"),
              functionArgs = Seq(NamedArgumentExpression(
                key = "arg1",
                value = FunctionTableSubqueryArgumentExpression(
                  plan = UnresolvedRelation(multipartIdentifier = Seq("v1")),
                  partitionByExpressions = Seq(
                    UnresolvedAttribute("col1"),
                    UnresolvedAttribute("col2")),
                  orderByExpressions = Seq(
                    SortOrder(
                      child = UnresolvedAttribute("col2"),
                      direction = Ascending,
                      nullOrdering = NullsFirst,
                      sameOrderExpressions = Seq.empty),
                    SortOrder(
                      child = UnresolvedAttribute("col3"),
                      direction = Descending,
                      nullOrdering = NullsLast,
                      sameOrderExpressions = Seq.empty))
                ))))))
        val sql4 = s"select * from my_tvf(arg1 => table(select col1, col2, col3 from v2) " +
          s"$partition by (col1, col2) order by (col2 asc, col3 desc))"
        assertEqual(
          sql4,
          Project(
            projectList = Seq(UnresolvedStar(target = None)),
            child = UnresolvedTableValuedFunction(
              name = Seq("my_tvf"),
              functionArgs = Seq(NamedArgumentExpression(
                key = "arg1",
                value = FunctionTableSubqueryArgumentExpression(
                  plan = Project(
                    projectList = Seq(
                      UnresolvedAttribute("col1"),
                      UnresolvedAttribute("col2"),
                      UnresolvedAttribute("col3")),
                    child = UnresolvedRelation(multipartIdentifier = Seq("v2"))),
                  partitionByExpressions = Seq(
                    UnresolvedAttribute("col1"),
                    UnresolvedAttribute("col2")),
                  orderByExpressions = Seq(
                    SortOrder(
                      child = UnresolvedAttribute("col2"),
                      direction = Ascending,
                      nullOrdering = NullsFirst,
                      sameOrderExpressions = Seq.empty),
                    SortOrder(
                      child = UnresolvedAttribute("col3"),
                      direction = Descending,
                      nullOrdering = NullsLast,
                      sameOrderExpressions = Seq.empty))
                ))))))
        val sql5 = s"select * from my_tvf(arg1 => table(v1) with single partition)"
        assertEqual(
          sql5,
          Project(
            projectList = Seq(UnresolvedStar(target = None)),
            child = UnresolvedTableValuedFunction(
              name = Seq("my_tvf"),
              functionArgs = Seq(NamedArgumentExpression(
                key = "arg1",
                value = FunctionTableSubqueryArgumentExpression(
                  plan = UnresolvedRelation(multipartIdentifier = Seq("v1")),
                  withSinglePartition = true
                ))))))
        // Negative tests.
        val sql6 = "select * from my_tvf(arg1 => table(1) partition by col1 with single partition)"
        checkError(
          exception = parseException(sql6),
          errorClass = "PARSE_SYNTAX_ERROR",
          parameters = Map(
            "error" -> "'partition'",
            "hint" -> ""))
        val sql7 = "select * from my_tvf(arg1 => table(1) order by col1)"
        checkError(
          exception = parseException(sql7),
          errorClass = "PARSE_SYNTAX_ERROR",
          parameters = Map(
            "error" -> "'order'",
            "hint" -> ""))
        val sql8tableArg = "table(select col1, col2, col3 from v2)"
        val sql8partition = s"$partition by col1, col2 order by col2 asc, col3 desc"
        val sql8 = s"select * from my_tvf(arg1 => $sql8tableArg $sql8partition)"
        checkError(
          exception = parseException(sql8),
          errorClass = "_LEGACY_ERROR_TEMP_0064",
          parameters = Map(
            "msg" ->
              ("The table function call includes a table argument with an invalid " +
              "partitioning/ordering specification: the PARTITION BY clause included multiple " +
              "expressions without parentheses surrounding them; please add parentheses around " +
              "these expressions and then retry the query again")),
          context = ExpectedContext(
            fragment = s"$sql8tableArg $sql8partition",
            start = 29,
            stop = 110 + partition.length)
        )
      }
    }
  }

  test("SPARK-32106: TRANSFORM plan") {
    // verify schema less
    assertEqual(
      """
        |SELECT TRANSFORM(a, b, c)
        |USING 'cat'
        |FROM testData
      """.stripMargin,
      ScriptTransformation(
        "cat",
        Seq(AttributeReference("key", StringType)(),
          AttributeReference("value", StringType)()),
        Project(Seq($"a", $"b", $"c"), UnresolvedRelation(TableIdentifier("testData"))),
        ScriptInputOutputSchema(List.empty, List.empty, None, None,
          List.empty, List.empty, None, None, true))
    )

    // verify without output schema
    assertEqual(
      """
        |SELECT TRANSFORM(a, b, c)
        |USING 'cat' AS (a, b, c)
        |FROM testData
      """.stripMargin,
      ScriptTransformation(
        "cat",
        Seq(AttributeReference("a", StringType)(),
          AttributeReference("b", StringType)(),
          AttributeReference("c", StringType)()),
        Project(Seq($"a", $"b", $"c"), UnresolvedRelation(TableIdentifier("testData"))),
        ScriptInputOutputSchema(List.empty, List.empty, None, None,
          List.empty, List.empty, None, None, false)))

    // verify with output schema
    assertEqual(
      """
        |SELECT TRANSFORM(a, b, c)
        |USING 'cat' AS (a int, b string, c long)
        |FROM testData
      """.stripMargin,
      ScriptTransformation(
        "cat",
        Seq(AttributeReference("a", IntegerType)(),
          AttributeReference("b", StringType)(),
          AttributeReference("c", LongType)()),
        Project(Seq($"a", $"b", $"c"), UnresolvedRelation(TableIdentifier("testData"))),
        ScriptInputOutputSchema(List.empty, List.empty, None, None,
          List.empty, List.empty, None, None, false)))

    // verify with ROW FORMAT DELIMETED
    @nowarn("cat=deprecation")
    val sqlWithRowFormatDelimiters: String =
      """
        |SELECT TRANSFORM(a, b, c)
        |  ROW FORMAT DELIMITED
        |  FIELDS TERMINATED BY '\t'
        |  COLLECTION ITEMS TERMINATED BY '\u0002'
        |  MAP KEYS TERMINATED BY '\u0003'
        |  LINES TERMINATED BY '\n'
        |  NULL DEFINED AS 'null'
        |  USING 'cat' AS (a, b, c)
        |  ROW FORMAT DELIMITED
        |  FIELDS TERMINATED BY '\t'
        |  COLLECTION ITEMS TERMINATED BY '\u0004'
        |  MAP KEYS TERMINATED BY '\u0005'
        |  LINES TERMINATED BY '\n'
        |  NULL DEFINED AS 'NULL'
        |FROM testData
      """.stripMargin
    assertEqual(
      sqlWithRowFormatDelimiters,
      ScriptTransformation(
        "cat",
        Seq(AttributeReference("a", StringType)(),
          AttributeReference("b", StringType)(),
          AttributeReference("c", StringType)()),
        Project(Seq($"a", $"b", $"c"), UnresolvedRelation(TableIdentifier("testData"))),
        ScriptInputOutputSchema(
          Seq(("TOK_TABLEROWFORMATFIELD", "\t"),
            ("TOK_TABLEROWFORMATCOLLITEMS", "\u0002"),
            ("TOK_TABLEROWFORMATMAPKEYS", "\u0003"),
            ("TOK_TABLEROWFORMATNULL", "null"),
            ("TOK_TABLEROWFORMATLINES", "\n")),
          Seq(("TOK_TABLEROWFORMATFIELD", "\t"),
            ("TOK_TABLEROWFORMATCOLLITEMS", "\u0004"),
            ("TOK_TABLEROWFORMATMAPKEYS", "\u0005"),
            ("TOK_TABLEROWFORMATNULL", "NULL"),
            ("TOK_TABLEROWFORMATLINES", "\n")), None, None,
          List.empty, List.empty, None, None, false)))

    // verify with ROW FORMAT SERDE
    val sql =
      """SELECT TRANSFORM(a, b, c)
        |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        |  WITH SERDEPROPERTIES(
        |    "separatorChar" = "\t",
        |    "quoteChar" = "'",
        |    "escapeChar" = "\\")
        |  USING 'cat' AS (a, b, c)
        |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        |  WITH SERDEPROPERTIES(
        |    "separatorChar" = "\t",
        |    "quoteChar" = "'",
        |    "escapeChar" = "\\")
        |FROM testData""".stripMargin
    checkError(
      exception = parseException(sql),
      errorClass = "UNSUPPORTED_FEATURE.TRANSFORM_NON_HIVE",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 393))
  }

  test("as of syntax") {
    def testVersion(version: String, plan: LogicalPlan): Unit = {
      Seq("VERSION", "SYSTEM_VERSION").foreach { keyword =>
        comparePlans(parsePlan(s"SELECT * FROM a.b.c $keyword AS OF $version"), plan)
        comparePlans(parsePlan(s"SELECT * FROM a.b.c FOR $keyword AS OF $version"), plan)
      }
    }

    testVersion("'Snapshot123456789'", Project(Seq(UnresolvedStar(None)),
      RelationTimeTravel(
        UnresolvedRelation(Seq("a", "b", "c")),
        None,
        Some("Snapshot123456789"))))

    testVersion("123456789", Project(Seq(UnresolvedStar(None)),
      RelationTimeTravel(
        UnresolvedRelation(Seq("a", "b", "c")),
        None,
        Some("123456789"))))

    def testTimestamp(timestamp: String, plan: LogicalPlan): Unit = {
      Seq("TIMESTAMP", "SYSTEM_TIME").foreach { keyword =>
        comparePlans(parsePlan(s"SELECT * FROM a.b.c $keyword AS OF $timestamp"), plan)
        comparePlans(parsePlan(s"SELECT * FROM a.b.c FOR $keyword AS OF $timestamp"), plan)
      }
    }

    testTimestamp("'2019-01-29 00:37:58'", Project(Seq(UnresolvedStar(None)),
      RelationTimeTravel(
        UnresolvedRelation(Seq("a", "b", "c")),
        Some(Literal("2019-01-29 00:37:58")),
        None)))

    testTimestamp("current_date()", Project(Seq(UnresolvedStar(None)),
      RelationTimeTravel(
        UnresolvedRelation(Seq("a", "b", "c")),
        Some(UnresolvedFunction(Seq("current_date"), Nil, isDistinct = false)),
        None)))

    testTimestamp("(SELECT current_date())", Project(Seq(UnresolvedStar(None)),
      RelationTimeTravel(
        UnresolvedRelation(Seq("a", "b", "c")),
        Some(ScalarSubquery(Project(UnresolvedAlias(UnresolvedFunction(
          Seq("current_date"), Nil, isDistinct = false)) :: Nil, OneRowRelation()))),
        None)))

    val sql = "SELECT * FROM a.b.c TIMESTAMP AS OF col"
    val fragment = "TIMESTAMP AS OF col"
    checkError(
      exception = parseException(sql),
      errorClass = "_LEGACY_ERROR_TEMP_0056",
      parameters = Map("reason" -> "timestamp expression cannot refer to any columns"),
      context = ExpectedContext(
        fragment = fragment,
        start = 20,
        stop = 38))
  }

  test("PERCENTILE_CONT & PERCENTILE_DISC") {
    def assertPercentilePlans(inputSQL: String, expectedExpression: Expression): Unit = {
      comparePlans(
        parsePlan(inputSQL),
        Project(Seq(UnresolvedAlias(expectedExpression)), OneRowRelation())
      )
    }

    assertPercentilePlans(
      "SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY col)",
      UnresolvedFunction(
        Seq("PERCENTILE_CONT"),
        Seq(Literal(Decimal(0.1), DecimalType(1, 1))),
        false,
        None,
        orderingWithinGroup = Seq(SortOrder(UnresolvedAttribute("col"), Ascending)))
    )

    assertPercentilePlans(
      "SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY col DESC)",
      UnresolvedFunction(
        Seq("PERCENTILE_CONT"),
        Seq(Literal(Decimal(0.1), DecimalType(1, 1))),
        false,
        None,
        orderingWithinGroup = Seq(SortOrder(UnresolvedAttribute("col"), Descending)))
    )

    assertPercentilePlans(
      "SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY col) FILTER (WHERE id > 10)",
      UnresolvedFunction(
        Seq("PERCENTILE_CONT"),
        Seq(Literal(Decimal(0.1), DecimalType(1, 1))),
        false,
        Some(GreaterThan(UnresolvedAttribute("id"), Literal(10))),
        orderingWithinGroup = Seq(SortOrder(UnresolvedAttribute("col"), Ascending)))
    )

    assertPercentilePlans(
      "SELECT PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY col)",
      UnresolvedFunction(
        Seq("PERCENTILE_DISC"),
        Seq(Literal(Decimal(0.1), DecimalType(1, 1))),
        false,
        None,
        orderingWithinGroup = Seq(SortOrder(UnresolvedAttribute("col"), Ascending)))
    )

    assertPercentilePlans(
      "SELECT PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY col DESC)",
      UnresolvedFunction(
        Seq("PERCENTILE_DISC"),
        Seq(Literal(Decimal(0.1), DecimalType(1, 1))),
        false,
        None,
        orderingWithinGroup = Seq(SortOrder(UnresolvedAttribute("col"), Descending)))
    )

    assertPercentilePlans(
      "SELECT PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY col) FILTER (WHERE id > 10)",
      UnresolvedFunction(
        Seq("PERCENTILE_DISC"),
        Seq(Literal(Decimal(0.1), DecimalType(1, 1))),
        false,
        Some(GreaterThan(UnresolvedAttribute("id"), Literal(10))),
        orderingWithinGroup = Seq(SortOrder(UnresolvedAttribute("col"), Ascending)))
    )
  }

  test("SPARK-41271: parsing of named parameters") {
    comparePlans(
      parsePlan("SELECT :param_1"),
      Project(UnresolvedAlias(NamedParameter("param_1"), None) :: Nil, OneRowRelation()))
    comparePlans(
      parsePlan("SELECT abs(:1Abc)"),
      Project(UnresolvedAlias(
        UnresolvedFunction(
          "abs" :: Nil,
          NamedParameter("1Abc") :: Nil,
          isDistinct = false), None) :: Nil,
        OneRowRelation()))
    comparePlans(
      parsePlan("SELECT * FROM a LIMIT :limitA"),
      table("a").select(star()).limit(NamedParameter("limitA")))
    // Invalid empty name and invalid symbol in a name
    checkError(
      exception = parseException(s"SELECT :-"),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'-'", "hint" -> ""))
    checkError(
      exception = parseException(s"SELECT :"),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "end of input", "hint" -> ""))
  }

  test("SPARK-42553: NonReserved keyword 'interval' can be column name") {
    comparePlans(
      parsePlan("SELECT interval FROM VALUES ('abc') AS tbl(interval);"),
      UnresolvedInlineTable(
        Seq("interval"),
        Seq(Literal("abc")) :: Nil).as("tbl").select($"interval")
    )
  }

  test("SPARK-44066: parsing of positional parameters") {
    comparePlans(
      parsePlan("SELECT ?"),
      Project(UnresolvedAlias(PosParameter(7), None) :: Nil, OneRowRelation()))
    comparePlans(
      parsePlan("SELECT abs(?)"),
      Project(UnresolvedAlias(
        UnresolvedFunction(
          "abs" :: Nil,
          PosParameter(11) :: Nil,
          isDistinct = false), None) :: Nil,
        OneRowRelation()))
    comparePlans(
      parsePlan("SELECT * FROM a LIMIT ?"),
      table("a").select(star()).limit(PosParameter(22)))
  }

  test("SPARK-45189: Creating UnresolvedRelation from TableIdentifier should include the" +
    " catalog field") {
    val tableId = TableIdentifier("t", Some("db"), Some("cat"))
    val unresolvedRelation = UnresolvedRelation(tableId)
    assert(unresolvedRelation.multipartIdentifier == Seq("cat", "db", "t"))
    val unresolvedRelation2 = UnresolvedRelation(tableId, CaseInsensitiveStringMap.empty, true)
    assert(unresolvedRelation2.multipartIdentifier == Seq("cat", "db", "t"))
    assert(unresolvedRelation2.options == CaseInsensitiveStringMap.empty)
    assert(unresolvedRelation2.isStreaming)
  }
}
