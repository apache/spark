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

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, RelationTimeTravel, UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedGenerator, UnresolvedInlineTable, UnresolvedRelation, UnresolvedStar, UnresolvedSubqueryColumnAliases, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

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

  private def assertQueryEqual(query: String, plan: LogicalPlan): Unit = {
    comparePlans(parseQuery(query), plan, checkAnalysis = false)
  }

  private def intercept(sqlCommand: String, messages: String*): Unit =
    interceptParseException(parsePlan)(sqlCommand, messages: _*)

  private def interceptQuery(query: String, messages: String*): Unit =
    interceptParseException(parseQuery)(query, messages: _*)

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
    val query = """
                  |/*abc*/
                  |select 1 as a
                  |/*
                  |
                  |2 as b
                  |/*abc */
                  |, 3 as c
                  |
                  |/**/
                  |""".stripMargin
    val e = intercept[ParseException](parsePlan(query))
    assert(e.getMessage.contains(s"Unclosed bracketed comment"))
  }

  test("unclosed bracketed comment two") {
    val query = """
                  |/*abc*/
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
    val e = intercept[ParseException](parsePlan(query))
    assert(e.getMessage.contains(s"Unclosed bracketed comment"))
  }

  test("case insensitive") {
    val plan = table("a").select(star())
    assertQueryEqual("sELEct * FroM a", plan)
    assertQueryEqual("select * fRoM a", plan)
    assertQueryEqual("SELECT * FROM a", plan)
  }

  test("explain") {
    intercept("EXPLAIN logical SELECT 1", "Unsupported SQL statement")
    intercept("EXPLAIN formatted SELECT 1", "Unsupported SQL statement")
  }

  test("set operations") {
    val a = table("a").select(star())
    val b = table("b").select(star())

    assertQueryEqual("select * from a union select * from b", Distinct(a.union(b)))
    assertQueryEqual("select * from a union distinct select * from b", Distinct(a.union(b)))
    assertQueryEqual("select * from a union all select * from b", a.union(b))
    assertQueryEqual("select * from a except select * from b", a.except(b, isAll = false))
    assertQueryEqual("select * from a except distinct select * from b", a.except(b, isAll = false))
    assertQueryEqual("select * from a except all select * from b", a.except(b, isAll = true))
    assertQueryEqual("select * from a minus select * from b", a.except(b, isAll = false))
    assertQueryEqual("select * from a minus all select * from b", a.except(b, isAll = true))
    assertQueryEqual("select * from a minus distinct select * from b", a.except(b, isAll = false))
    assertQueryEqual("select * from a " +
      "intersect select * from b", a.intersect(b, isAll = false))
    assertQueryEqual(
      "select * from a intersect distinct select * from b", a.intersect(b, isAll = false))
    assertQueryEqual("select * from a intersect all select * from b", a.intersect(b, isAll = true))
  }

  test("common table expressions") {
    assertQueryEqual(
      "with cte1 as (select * from a) select * from cte1",
      cte(table("cte1").select(star()), "cte1" -> ((table("a").select(star()), Seq.empty))))
    assertQueryEqual(
      "with cte1 (select 1) select * from cte1",
      cte(table("cte1").select(star()), "cte1" -> ((OneRowRelation().select(1), Seq.empty))))
    assertQueryEqual(
      "with cte1 (select 1), cte2 as (select * from cte1) select * from cte2",
      cte(table("cte2").select(star()),
        "cte1" -> ((OneRowRelation().select(1), Seq.empty)),
        "cte2" -> ((table("cte1").select(star()), Seq.empty))))
    interceptQuery(
      "with cte1 (select 1), cte1 as (select 1 from cte1) select * from cte1",
      "CTE definition can't have duplicate names: 'cte1'.")
  }

  test("simple select query") {
    assertQueryEqual("select 1", OneRowRelation().select(1))
    assertQueryEqual("select a, b", OneRowRelation().select('a, 'b))
    assertQueryEqual("select a, b from db.c", table("db", "c").select('a, 'b))
    assertQueryEqual(
      "select a, b from db.c where x < 1", table("db", "c").where('x < 1).select('a, 'b))
    assertQueryEqual(
      "select a, b from db.c having x < 1",
      table("db", "c").having()('a, 'b)('x < 1))
    assertQueryEqual("select distinct a, b from db.c", Distinct(table("db", "c").select('a, 'b)))
    assertQueryEqual("select all a, b from db.c", table("db", "c").select('a, 'b))
    assertQueryEqual("select from tbl", OneRowRelation().select('from.as("tbl")))
    assertQueryEqual("select a from 1k.2m", table("1k", "2m").select('a))
  }

  test("hive-style single-FROM statement") {
    assertQueryEqual("from a select b, c", table("a").select('b, 'c))
    assertQueryEqual(
      "from db.a select b, c where d < 1", table("db", "a").where('d < 1).select('b, 'c))
    assertQueryEqual("from a select distinct b, c", Distinct(table("a").select('b, 'c)))

    // Weird "FROM table" queries, should be invalid anyway
    intercept("from a", "no viable alternative at input 'from a'")
    intercept("from (from a union all from b) c select *", "no viable alternative at input 'from")
  }

  test("multi select query") {
    assertQueryEqual(
      "from a select * select * where s < 10",
      table("a").select(star()).union(table("a").where('s < 10).select(star())))
    intercept(
      "from a select * select * from x where a.s < 10",
      "mismatched input 'from' expecting")
    intercept(
      "from a select * from b",
      "mismatched input 'from' expecting")
    assertEqual(
      "from a insert into tbl1 select * insert into tbl2 select * where s < 10",
      table("a").select(star()).insertInto("tbl1").union(
        table("a").where('s < 10).select(star()).insertInto("tbl2")))
    assertQueryEqual(
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
      (" order by a, b desc", basePlan.orderBy('a.asc, 'b.desc)),
      (" sort by a, b desc", basePlan.sortBy('a.asc, 'b.desc))
    )

    orderSortDistrClusterClauses.foreach {
      case (s1, p1) =>
        limitWindowClauses.foreach {
          case (s2, pf2) =>
            assertQueryEqual(baseSql + s1 + s2, pf2(p1))
        }
    }

    val msg = "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported"
    interceptQuery(s"$baseSql order by a sort by a", msg)
    interceptQuery(s"$baseSql cluster by a distribute by a", msg)
    interceptQuery(s"$baseSql order by a cluster by a", msg)
    interceptQuery(s"$baseSql order by a distribute by a", msg)
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
    val plan2 = table("t").where('x > 5).select(star())
    assertEqual("from t insert into s select * limit 1 insert into u select * where x > 5",
      plan.limit(1).insertInto("s").union(plan2.insertInto("u")))
  }

  test("aggregation") {
    val sql = "select a, b, sum(c) as c from d group by a, b"
    val sqlWithoutGroupBy = "select a, b, sum(c) as c from d"

    // Normal
    assertQueryEqual(sql, table("d").groupBy('a, 'b)('a, 'b, 'sum.function('c).as("c")))

    // Cube
    assertQueryEqual(s"$sql with cube",
      table("d").groupBy(Cube(Seq(Seq('a), Seq('b))))('a, 'b, 'sum.function('c).as("c")))
    assertQueryEqual(s"$sqlWithoutGroupBy group by cube(a, b)",
      table("d").groupBy(Cube(Seq(Seq('a), Seq('b))))('a, 'b, 'sum.function('c).as("c")))
    assertQueryEqual(s"$sqlWithoutGroupBy group by cube (a, b)",
      table("d").groupBy(Cube(Seq(Seq('a), Seq('b))))('a, 'b, 'sum.function('c).as("c")))

    // Rollup
    assertQueryEqual(s"$sql with rollup",
      table("d").groupBy(Rollup(Seq(Seq('a), Seq('b))))('a, 'b, 'sum.function('c).as("c")))
    assertQueryEqual(s"$sqlWithoutGroupBy group by rollup(a, b)",
      table("d").groupBy(Rollup(Seq(Seq('a), Seq('b))))('a, 'b, 'sum.function('c).as("c")))
    assertQueryEqual(s"$sqlWithoutGroupBy group by rollup (a, b)",
      table("d").groupBy(Rollup(Seq(Seq('a), Seq('b))))('a, 'b, 'sum.function('c).as("c")))

    // Grouping Sets
    assertQueryEqual(s"$sql grouping sets((a, b), (a), ())",
      Aggregate(Seq(GroupingSets(Seq(Seq('a, 'b), Seq('a), Seq()), Seq('a, 'b))),
        Seq('a, 'b, 'sum.function('c).as("c")), table("d")))

    assertQueryEqual(s"$sqlWithoutGroupBy group by grouping sets((a, b), (a), ())",
      Aggregate(Seq(GroupingSets(Seq(Seq('a, 'b), Seq('a), Seq()))),
        Seq('a, 'b, 'sum.function('c).as("c")), table("d")))

    val m = intercept[ParseException] {
      parseQuery("SELECT a, b, count(distinct a, distinct b) as c FROM d GROUP BY a, b")
    }.getMessage
    assert(m.contains("extraneous input 'b'"))

  }

  test("limit") {
    val sql = "select * from t"
    val plan = table("t").select(star())
    assertQueryEqual(s"$sql limit 10", plan.limit(10))
    assertQueryEqual(
      s"$sql limit cast(9 / 4 as int)", plan.limit(Cast(Literal(9) / 4, IntegerType)))
  }

  test("window spec") {
    // Note that WindowSpecs are testing in the ExpressionParserSuite
    val sql = "select * from t"
    val plan = table("t").select(star())
    val spec = WindowSpecDefinition(Seq('a, 'b), Seq('c.asc),
      SpecifiedWindowFrame(RowFrame, -Literal(1), Literal(1)))

    // Test window resolution.
    val ws1 = Map("w1" -> spec, "w2" -> spec, "w3" -> spec)
    assertQueryEqual(
      s"""$sql
         |window w1 as (partition by a, b order by c rows between 1 preceding and 1 following),
         |       w2 as w1,
         |       w3 as w1""".stripMargin,
      WithWindowDefinition(ws1, plan))

    // Fail with no reference.
    interceptQuery(s"$sql window w2 as w1", "Cannot resolve window reference 'w1'")

    // Fail when resolved reference is not a window spec.
    interceptQuery(
      s"""$sql
         |window w1 as (partition by a, b order by c rows between 1 preceding and 1 following),
         |       w2 as w1,
         |       w3 as w2""".stripMargin,
      "Window reference 'w2' is not a window specification"
    )
  }

  test("lateral view") {
    val explode = UnresolvedGenerator(FunctionIdentifier("explode"), Seq('x))
    val jsonTuple = UnresolvedGenerator(FunctionIdentifier("json_tuple"), Seq('x, 'y))

    // Single lateral view
    assertQueryEqual(
      "select * from t lateral view explode(x) expl as x",
      table("t")
        .generate(explode, alias = Some("expl"), outputNames = Seq("x"))
        .select(star()))

    // Multiple lateral views
    assertQueryEqual(
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
        from.where('s < 10).select(star()).insertInto("t3")))

    // Unresolved generator.
    val expected = table("t")
      .generate(
        UnresolvedGenerator(FunctionIdentifier("posexplode"), Seq('x)),
        alias = Some("posexpl"),
        outputNames = Seq("x", "y"))
      .select(star())
    assertQueryEqual(
      "select * from t lateral view posexplode(x) posexpl as x, y",
      expected)

    interceptQuery(
      """select *
        |from t
        |lateral view explode(x) expl
        |pivot (
        |  sum(x)
        |  FOR y IN ('a', 'b')
        |)""".stripMargin,
      "LATERAL cannot be used together with PIVOT in FROM clause")
  }

  test("joins") {
    // Test single joins.
    val testUnconditionalJoin = (sql: String, jt: JoinType) => {
      assertQueryEqual(
        s"select * from t as tt $sql u",
        table("t").as("tt").join(table("u"), jt, None).select(star()))
    }
    val testConditionalJoin = (sql: String, jt: JoinType) => {
      assertQueryEqual(
        s"select * from t $sql u as uu on a = b",
        table("t").join(table("u").as("uu"), jt, Option('a === 'b)).select(star()))
    }
    val testNaturalJoin = (sql: String, jt: JoinType) => {
      assertQueryEqual(
        s"select * from t tt natural $sql u as uu",
        table("t").as("tt").join(table("u").as("uu"), NaturalJoin(jt), None).select(star()))
    }
    val testUsingJoin = (sql: String, jt: JoinType) => {
      assertQueryEqual(
        s"select * from t $sql u using(a, b)",
        table("t").join(table("u"), UsingJoin(jt, Seq("a", "b")), None).select(star()))
    }
    val testLateralJoin = (sql: String, jt: JoinType) => {
      assertQueryEqual(
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
    interceptQuery("select * from a natural cross join b")

    // Test natural join with a condition
    intercept("select * from a natural join b on a.id = b.id")

    // Test multiple consecutive joins
    assertQueryEqual(
      "select * from a join b join c right join d",
      table("a").join(table("b")).join(table("c")).join(table("d"), RightOuter).select(star()))

    // SPARK-17296
    assertQueryEqual(
      "select * from t1 cross join t2 join t3 on t3.id = t1.id join t4 on t4.id = t1.id",
      table("t1")
        .join(table("t2"), Cross)
        .join(table("t3"), Inner, Option(Symbol("t3.id") === Symbol("t1.id")))
        .join(table("t4"), Inner, Option(Symbol("t4.id") === Symbol("t1.id")))
        .select(star()))

    // Test multiple on clauses.
    intercept("select * from t1 inner join t2 inner join t3 on col3 = col2 on col3 = col1")

    // Parenthesis
    assertQueryEqual(
      "select * from t1 inner join (t2 inner join t3 on col3 = col2) on col3 = col1",
      table("t1")
        .join(table("t2")
          .join(table("t3"), Inner, Option('col3 === 'col2)), Inner, Option('col3 === 'col1))
        .select(star()))
    assertQueryEqual(
      "select * from t1 inner join (t2 inner join t3) on col3 = col2",
      table("t1")
        .join(table("t2").join(table("t3"), Inner, None), Inner, Option('col3 === 'col2))
        .select(star()))
    assertQueryEqual(
      "select * from t1 inner join (t2 inner join t3 on col3 = col2)",
      table("t1")
        .join(table("t2").join(table("t3"), Inner, Option('col3 === 'col2)), Inner, None)
        .select(star()))

    // Implicit joins.
    assertQueryEqual(
      "select * from t1, t3 join t2 on t1.col1 = t2.col2",
      table("t1")
        .join(table("t3"))
        .join(table("t2"), Inner, Option(Symbol("t1.col1") === Symbol("t2.col2")))
        .select(star()))

    // Test lateral join with join conditions
    assertQueryEqual(
      s"select * from t join lateral (select * from u) uu on true",
      LateralJoin(
        table("t"),
        LateralSubquery(table("u").select(star()).as("uu")),
        Inner, Option(true)).select(star()))

    // Test multiple lateral joins
    assertQueryEqual(
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
    assertQueryEqual(s"$sql tablesample(100 rows)",
      table("t").limit(100).select(star()))
    assertQueryEqual(s"$sql tablesample(43 percent) as x",
      Sample(0, .43d, withReplacement = false, 10L, table("t").as("x")).select(star()))
    assertQueryEqual(s"$sql tablesample(bucket 4 out of 10) as x",
      Sample(0, .4d, withReplacement = false, 10L, table("t").as("x")).select(star()))
    interceptQuery(s"$sql tablesample(bucket 4 out of 10 on x) as x",
      "TABLESAMPLE(BUCKET x OUT OF y ON colname) is not supported")
    interceptQuery(s"$sql tablesample(bucket 11 out of 10) as x",
      s"Sampling fraction (${11.0/10.0}) must be on interval [0, 1]")
    interceptQuery("SELECT * FROM parquet_t0 TABLESAMPLE(300M) s",
      "TABLESAMPLE(byteLengthLiteral) is not supported")
    interceptQuery("SELECT * FROM parquet_t0 TABLESAMPLE(BUCKET 3 OUT OF 32 ON rand()) s",
      "TABLESAMPLE(BUCKET x OUT OF y ON function) is not supported")
  }

  test("sub-query") {
    val plan = table("t0").select('id)
    assertQueryEqual("select id from (t0)", plan)
    assertQueryEqual("select id from ((((((t0))))))", plan)
    assertQueryEqual(
      "(select * from t1) union distinct (select * from t2)",
      Distinct(table("t1").select(star()).union(table("t2").select(star()))))
    assertQueryEqual(
      "select * from ((select * from t1) union (select * from t2)) t",
      Distinct(
        table("t1").select(star()).union(table("t2").select(star()))).as("t").select(star()))
    assertQueryEqual(
      """select  id
        |from (((select id from t0)
        |       union all
        |       (select  id from t0))
        |      union all
        |      (select id from t0)) as u_1
      """.stripMargin,
      plan.union(plan).union(plan).as("u_1").select('id))
  }

  test("scalar sub-query") {
    assertQueryEqual(
      "select (select max(b) from s) ss from t",
      table("t").select(ScalarSubquery(table("s").select('max.function('b))).as("ss")))
    assertQueryEqual(
      "select * from t where a = (select b from s)",
      table("t").where('a === ScalarSubquery(table("s").select('b))).select(star()))
    assertQueryEqual(
      "select g from t group by g having a > (select b from s)",
      table("t")
        .having('g)('g)('a > ScalarSubquery(table("s").select('b))))
  }

  test("table reference") {
    assertEqual("table t", table("t"))
    assertEqual("table d.t", table("d", "t"))
  }

  test("table valued function") {
    assertQueryEqual(
      "select * from range(2)",
      UnresolvedTableValuedFunction("range", Literal(2) :: Nil, Seq.empty).select(star()))
    // SPARK-34627
    interceptQuery("select * from default.range(2)",
      "table valued function cannot specify database name: default.range")
  }

  test("SPARK-20311 range(N) as alias") {
    assertQueryEqual(
      "SELECT * FROM range(10) AS t",
      SubqueryAlias("t", UnresolvedTableValuedFunction("range", Literal(10) :: Nil, Seq.empty))
        .select(star()))
    assertQueryEqual(
      "SELECT * FROM range(7) AS t(a)",
      SubqueryAlias("t", UnresolvedTableValuedFunction("range", Literal(7) :: Nil, "a" :: Nil))
        .select(star()))
  }

  test("SPARK-20841 Support table column aliases in FROM clause") {
    assertQueryEqual(
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
    assertQueryEqual(
      "SELECT * FROM (SELECT a AS x, b AS y FROM t) t(col1, col2)",
      SubqueryAlias(
        "t",
        UnresolvedSubqueryColumnAliases(
          Seq("col1", "col2"),
          UnresolvedRelation(TableIdentifier("t")).select('a.as("x"), 'b.as("y"))
        )
      ).select(star()))
  }

  test("SPARK-20963 Support aliases for join relations in FROM clause") {
    val src1 = UnresolvedRelation(TableIdentifier("src1")).as("s1")
    val src2 = UnresolvedRelation(TableIdentifier("src2")).as("s2")
    assertQueryEqual(
      "SELECT * FROM (src1 s1 INNER JOIN src2 s2 ON s1.id = s2.id) dst(a, b, c, d)",
      SubqueryAlias(
        "dst",
        UnresolvedSubqueryColumnAliases(
          Seq("a", "b", "c", "d"),
          src1.join(src2, Inner, Option(Symbol("s1.id") === Symbol("s2.id")))
        )
      ).select(star()))
  }

  test("SPARK-34335 Support referencing subquery with column aliases by table alias") {
    assertQueryEqual(
      "SELECT t.col1, t.col2 FROM (SELECT a AS x, b AS y FROM t) t(col1, col2)",
      SubqueryAlias(
        "t",
        UnresolvedSubqueryColumnAliases(
          Seq("col1", "col2"),
          UnresolvedRelation(TableIdentifier("t")).select('a.as("x"), 'b.as("y")))
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
    assertQueryEqual("select a, b from db.c where x !< 1",
      table("db", "c").where('x >= 1).select('a, 'b))
    // !> is equivalent to <=
    assertQueryEqual("select a, b from db.c where x !> 1",
      table("db", "c").where('x <= 1).select('a, 'b))
  }

  test("select hint syntax") {
    // Hive compatibility: Missing parameter raises ParseException.
    val m = intercept[ParseException] {
      parseQuery("SELECT /*+ HINT() */ * FROM t")
    }.getMessage
    assert(m.contains("mismatched input"))

    // Disallow space as the delimiter.
    val m3 = intercept[ParseException] {
      parseQuery("SELECT /*+ INDEX(a b c) */ * from default.t")
    }.getMessage
    assert(m3.contains("mismatched input 'b' expecting"))

    comparePlans(
      parseQuery("SELECT /*+ HINT */ * FROM t"),
      UnresolvedHint("HINT", Seq.empty, table("t").select(star())))

    comparePlans(
      parseQuery("SELECT /*+ BROADCASTJOIN(u) */ * FROM t"),
      UnresolvedHint("BROADCASTJOIN", Seq($"u"), table("t").select(star())))

    comparePlans(
      parseQuery("SELECT /*+ MAPJOIN(u) */ * FROM t"),
      UnresolvedHint("MAPJOIN", Seq($"u"), table("t").select(star())))

    comparePlans(
      parseQuery("SELECT /*+ STREAMTABLE(a,b,c) */ * FROM t"),
      UnresolvedHint("STREAMTABLE", Seq($"a", $"b", $"c"), table("t").select(star())))

    comparePlans(
      parseQuery("SELECT /*+ INDEX(t, emp_job_ix) */ * FROM t"),
      UnresolvedHint("INDEX", Seq($"t", $"emp_job_ix"), table("t").select(star())))

    comparePlans(
      parseQuery("SELECT /*+ MAPJOIN(`default.t`) */ * from `default.t`"),
      UnresolvedHint("MAPJOIN", Seq(UnresolvedAttribute.quoted("default.t")),
        table("default.t").select(star())))

    comparePlans(
      parseQuery("SELECT /*+ MAPJOIN(t) */ a from t where true group by a order by a"),
      UnresolvedHint("MAPJOIN", Seq($"t"),
        table("t").where(Literal(true)).groupBy('a)('a)).orderBy('a.asc))

    comparePlans(
      parseQuery("SELECT /*+ COALESCE(10) */ * FROM t"),
      UnresolvedHint("COALESCE", Seq(Literal(10)),
        table("t").select(star())))

    comparePlans(
      parseQuery("SELECT /*+ REPARTITION(100) */ * FROM t"),
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
      parseQuery("SELECT /*+ BROADCASTJOIN(u), REPARTITION(100) */ * FROM t"),
      UnresolvedHint("BROADCASTJOIN", Seq($"u"),
        UnresolvedHint("REPARTITION", Seq(Literal(100)),
          table("t").select(star()))))

    intercept("SELECT /*+ COALESCE(30 + 50) */ * FROM t", "mismatched input")

    comparePlans(
      parseQuery("SELECT /*+ REPARTITION(c) */ * FROM t"),
      UnresolvedHint("REPARTITION", Seq(UnresolvedAttribute("c")),
        table("t").select(star())))

    comparePlans(
      parseQuery("SELECT /*+ REPARTITION(100, c) */ * FROM t"),
      UnresolvedHint("REPARTITION", Seq(Literal(100), UnresolvedAttribute("c")),
        table("t").select(star())))

    comparePlans(
      parseQuery("SELECT /*+ REPARTITION(100, c), COALESCE(50) */ * FROM t"),
      UnresolvedHint("REPARTITION", Seq(Literal(100), UnresolvedAttribute("c")),
        UnresolvedHint("COALESCE", Seq(Literal(50)),
          table("t").select(star()))))

    comparePlans(
      parseQuery("SELECT /*+ REPARTITION(100, c), BROADCASTJOIN(u), COALESCE(50) */ * FROM t"),
      UnresolvedHint("REPARTITION", Seq(Literal(100), UnresolvedAttribute("c")),
        UnresolvedHint("BROADCASTJOIN", Seq($"u"),
          UnresolvedHint("COALESCE", Seq(Literal(50)),
            table("t").select(star())))))

    comparePlans(
      parseQuery(
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
      parseQuery("SELECT /*+ REPARTITION_BY_RANGE(c) */ * FROM t"),
      UnresolvedHint("REPARTITION_BY_RANGE", Seq(UnresolvedAttribute("c")),
        table("t").select(star())))

    comparePlans(
      parseQuery("SELECT /*+ REPARTITION_BY_RANGE(100, c) */ * FROM t"),
      UnresolvedHint("REPARTITION_BY_RANGE", Seq(Literal(100), UnresolvedAttribute("c")),
        table("t").select(star())))
  }

  test("SPARK-20854: select hint syntax with expressions") {
    comparePlans(
      parseQuery("SELECT /*+ HINT1(a, array(1, 2, 3)) */ * from t"),
      UnresolvedHint("HINT1", Seq($"a",
        UnresolvedFunction("array", Literal(1) :: Literal(2) :: Literal(3) :: Nil, false)),
        table("t").select(star())
      )
    )

    comparePlans(
      parseQuery("SELECT /*+ HINT1(a, 5, 'a', b) */ * from t"),
      UnresolvedHint("HINT1", Seq($"a", Literal(5), Literal("a"), $"b"),
        table("t").select(star())
      )
    )

    comparePlans(
      parseQuery("SELECT /*+ HINT1('a', (b, c), (1, 2)) */ * from t"),
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
      parseQuery("SELECT /*+ HINT1(a, 1) hint2(b, 2) */ * from t"),
      UnresolvedHint("HINT1", Seq($"a", Literal(1)),
        UnresolvedHint("hint2", Seq($"b", Literal(2)),
          table("t").select(star())
        )
      )
    )

    comparePlans(
      parseQuery("SELECT /*+ HINT1(a, 1),hint2(b, 2) */ * from t"),
      UnresolvedHint("HINT1", Seq($"a", Literal(1)),
        UnresolvedHint("hint2", Seq($"b", Literal(2)),
          table("t").select(star())
        )
      )
    )

    comparePlans(
      parseQuery("SELECT /*+ HINT1(a, 1) */ /*+ hint2(b, 2) */ * from t"),
      UnresolvedHint("HINT1", Seq($"a", Literal(1)),
        UnresolvedHint("hint2", Seq($"b", Literal(2)),
          table("t").select(star())
        )
      )
    )

    comparePlans(
      parseQuery("SELECT /*+ HINT1(a, 1), hint2(b, 2) */ /*+ hint3(c, 3) */ * from t"),
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
        parseQuery(inputSQL),
        Project(Seq(UnresolvedAlias(expectedExpression)), OneRowRelation())
      )
    }

    interceptQuery(
      "select ltrim(both 'S' from 'SS abc S'", "mismatched input 'from' expecting {')'")
    interceptQuery(
      "select rtrim(trailing 'S' from 'SS abc S'", "mismatched input 'from' expecting {')'")

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
        parseQuery(inputSQL),
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

    assertQueryEqual(
      query1, Distinct(a.union(b)).except(c.intersect(d, isAll = false), isAll = false))
    assertQueryEqual(
      query2, Distinct(a.union(b)).except(c.intersect(d, isAll = true), isAll = true))

    // Now disable precedence enforcement to verify the old behaviour.
    withSQLConf(SQLConf.LEGACY_SETOPS_PRECEDENCE_ENABLED.key -> "true") {
      assertQueryEqual(query1,
        Distinct(a.union(b)).except(c, isAll = false).intersect(d, isAll = false))
      assertEqual(query2, Distinct(a.union(b)).except(c, isAll = true).intersect(d, isAll = true))
    }

    // Explicitly enable the precedence enforcement
    withSQLConf(SQLConf.LEGACY_SETOPS_PRECEDENCE_ENABLED.key -> "false") {
      assertQueryEqual(query1,
        Distinct(a.union(b)).except(c.intersect(d, isAll = false), isAll = false))
      assertQueryEqual(
        query2, Distinct(a.union(b)).except(c.intersect(d, isAll = true), isAll = true))
    }
  }

  test("create/alter view as insert into table") {
    val m1 = intercept[ParseException] {
      parsePlan("CREATE VIEW testView AS INSERT INTO jt VALUES(1, 1)")
    }.getMessage
    assert(m1.contains("mismatched input 'INSERT' expecting"))
    // Multi insert query
    val m2 = intercept[ParseException] {
      parsePlan(
        """
          |CREATE VIEW testView AS FROM jt
          |INSERT INTO tbl1 SELECT * WHERE jt.id < 5
          |INSERT INTO tbl2 SELECT * WHERE jt.id > 4
        """.stripMargin)
    }.getMessage
    assert(m2.contains("mismatched input 'INSERT' expecting"))
    val m3 = intercept[ParseException] {
      parsePlan("ALTER VIEW testView AS INSERT INTO jt VALUES(1, 1)")
    }.getMessage
    assert(m3.contains("mismatched input 'INSERT' expecting"))
    // Multi insert query
    val m4 = intercept[ParseException] {
      parsePlan(
        """
          |ALTER VIEW testView AS FROM jt
          |INSERT INTO tbl1 SELECT * WHERE jt.id < 5
          |INSERT INTO tbl2 SELECT * WHERE jt.id > 4
        """.stripMargin
      )
    }.getMessage
    assert(m4.contains("mismatched input 'INSERT' expecting"))
  }

  test("Invalid insert constructs in the query") {
    val m1 = intercept[ParseException] {
      parsePlan("SELECT * FROM (INSERT INTO BAR VALUES (2))")
    }.getMessage
    assert(m1.contains("missing ')' at 'BAR'"))
    val m2 = intercept[ParseException] {
      parsePlan("SELECT * FROM S WHERE C1 IN (INSERT INTO T VALUES (2))")
    }.getMessage
    assert(m2.contains("mismatched input 'IN' expecting"))
  }

  test("relation in v2 catalog") {
    assertEqual("TABLE testcat.db.tab", table("testcat", "db", "tab"))
    assertQueryEqual("SELECT * FROM testcat.db.tab", table("testcat", "db", "tab").select(star()))

    assertQueryEqual(
      """
        |WITH cte1 AS (SELECT * FROM testcat.db.tab)
        |SELECT * FROM cte1
      """.stripMargin,
      cte(table("cte1").select(star()),
        "cte1" -> ((table("testcat", "db", "tab").select(star()), Seq.empty))))

    assertQueryEqual(
      "SELECT /*+ BROADCAST(tab) */ * FROM testcat.db.tab",
      table("testcat", "db", "tab").select(star()).hint("BROADCAST", $"tab"))
  }

  test("CTE with column alias") {
    assertQueryEqual(
      "WITH t(x) AS (SELECT c FROM a) SELECT * FROM t",
      cte(table("t").select(star()), "t" -> ((table("a").select('c), Seq("x")))))
  }

  test("statement containing terminal semicolons") {
    assertQueryEqual("select 1;", OneRowRelation().select(1))
    assertQueryEqual("select a, b;", OneRowRelation().select('a, 'b))
    assertQueryEqual("select a, b from db.c;;;", table("db", "c").select('a, 'b))
    assertQueryEqual("select a, b from db.c; ;;  ;", table("db", "c").select('a, 'b))
  }

  test("SPARK-32106: TRANSFORM plan") {
    // verify schema less
    assertQueryEqual(
      """
        |SELECT TRANSFORM(a, b, c)
        |USING 'cat'
        |FROM testData
      """.stripMargin,
      ScriptTransformation(
        "cat",
        Seq(AttributeReference("key", StringType)(),
          AttributeReference("value", StringType)()),
        Project(Seq('a, 'b, 'c), UnresolvedRelation(TableIdentifier("testData"))),
        ScriptInputOutputSchema(List.empty, List.empty, None, None,
          List.empty, List.empty, None, None, true))
    )

    // verify without output schema
    assertQueryEqual(
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
        Project(Seq('a, 'b, 'c), UnresolvedRelation(TableIdentifier("testData"))),
        ScriptInputOutputSchema(List.empty, List.empty, None, None,
          List.empty, List.empty, None, None, false)))

    // verify with output schema
    assertQueryEqual(
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
        Project(Seq('a, 'b, 'c), UnresolvedRelation(TableIdentifier("testData"))),
        ScriptInputOutputSchema(List.empty, List.empty, None, None,
          List.empty, List.empty, None, None, false)))

    // verify with ROW FORMAT DELIMETED
    assertQueryEqual(
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
      """.stripMargin,
      ScriptTransformation(
        "cat",
        Seq(AttributeReference("a", StringType)(),
          AttributeReference("b", StringType)(),
          AttributeReference("c", StringType)()),
        Project(Seq('a, 'b, 'c), UnresolvedRelation(TableIdentifier("testData"))),
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
    interceptQuery(
      """
        |SELECT TRANSFORM(a, b, c)
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
        |FROM testData
      """.stripMargin,
      "TRANSFORM with serde is only supported in hive mode")
  }


  test("as of syntax") {
    def testVersion(version: String, plan: LogicalPlan): Unit = {
      Seq("VERSION", "SYSTEM_VERSION").foreach { keyword =>
        comparePlans(parseQuery(s"SELECT * FROM a.b.c $keyword AS OF $version"), plan)
        comparePlans(parseQuery(s"SELECT * FROM a.b.c FOR $keyword AS OF $version"), plan)
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
        comparePlans(parseQuery(s"SELECT * FROM a.b.c $keyword AS OF $timestamp"), plan)
        comparePlans(parseQuery(s"SELECT * FROM a.b.c FOR $keyword AS OF $timestamp"), plan)
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

    interceptQuery("SELECT * FROM a.b.c TIMESTAMP AS OF col",
      "timestamp expression cannot refer to any columns")
    interceptQuery("SELECT * FROM a.b.c TIMESTAMP AS OF (select 1)",
      "timestamp expression cannot contain subqueries")
  }
}
