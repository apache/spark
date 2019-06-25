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
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedGenerator, UnresolvedInlineTable, UnresolvedRelation, UnresolvedSubqueryColumnAliases, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

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

  private def intercept(sqlCommand: String, messages: String*): Unit =
    interceptParseException(parsePlan)(sqlCommand, messages: _*)

  private def cte(plan: LogicalPlan, namedPlans: (String, (LogicalPlan, Seq[String]))*): With = {
    val ctes = namedPlans.map {
      case (name, (cte, columnAliases)) =>
        val subquery = if (columnAliases.isEmpty) {
          cte
        } else {
          UnresolvedSubqueryColumnAliases(columnAliases, cte)
        }
        name -> SubqueryAlias(name, subquery)
    }
    With(plan, ctes)
  }

  test("case insensitive") {
    val plan = table("a").select(star())
    assertEqual("sELEct * FroM a", plan)
    assertEqual("select * fRoM a", plan)
    assertEqual("SELECT * FROM a", plan)
  }

  test("explain") {
    intercept("EXPLAIN logical SELECT 1", "Unsupported SQL statement")
    intercept("EXPLAIN formatted SELECT 1", "Unsupported SQL statement")
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
    intercept(
      "with cte1 (select 1), cte1 as (select 1 from cte1) select * from cte1",
      "Found duplicate keys 'cte1'")
  }

  test("simple select query") {
    assertEqual("select 1", OneRowRelation().select(1))
    assertEqual("select a, b", OneRowRelation().select('a, 'b))
    assertEqual("select a, b from db.c", table("db", "c").select('a, 'b))
    assertEqual("select a, b from db.c where x < 1", table("db", "c").where('x < 1).select('a, 'b))
    assertEqual(
      "select a, b from db.c having x < 1",
      table("db", "c").groupBy()('a, 'b).where('x < 1))
    assertEqual("select distinct a, b from db.c", Distinct(table("db", "c").select('a, 'b)))
    assertEqual("select all a, b from db.c", table("db", "c").select('a, 'b))
    assertEqual("select from tbl", OneRowRelation().select('from.as("tbl")))
    assertEqual("select a from 1k.2m", table("1k", "2m").select('a))
  }

  test("hive-style single-FROM statement") {
    assertEqual("from a select b, c", table("a").select('b, 'c))
    assertEqual(
      "from db.a select b, c where d < 1", table("db", "a").where('d < 1).select('b, 'c))
    assertEqual("from a select distinct b, c", Distinct(table("a").select('b, 'c)))

    // Weird "FROM table" queries, should be invalid anyway
    intercept("from a", "no viable alternative at input 'from a'")
    intercept("from (from a union all from b) c select *", "no viable alternative at input 'from")
  }

  test("multi select query") {
    assertEqual(
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
      (" order by a, b desc", basePlan.orderBy('a.asc, 'b.desc)),
      (" sort by a, b desc", basePlan.sortBy('a.asc, 'b.desc))
    )

    orderSortDistrClusterClauses.foreach {
      case (s1, p1) =>
        limitWindowClauses.foreach {
          case (s2, pf2) =>
            assertEqual(baseSql + s1 + s2, pf2(p1))
        }
    }

    val msg = "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported"
    intercept(s"$baseSql order by a sort by a", msg)
    intercept(s"$baseSql cluster by a distribute by a", msg)
    intercept(s"$baseSql order by a cluster by a", msg)
    intercept(s"$baseSql order by a distribute by a", msg)
  }

  test("insert into") {
    val sql = "select * from t"
    val plan = table("t").select(star())
    def insert(
        partition: Map[String, Option[String]],
        overwrite: Boolean = false,
        ifPartitionNotExists: Boolean = false): LogicalPlan =
      InsertIntoTable(table("s"), partition, plan, overwrite, ifPartitionNotExists)

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
      InsertIntoTable(
        table("s"), Map.empty, plan.limit(1), false, ifPartitionNotExists = false).union(
        InsertIntoTable(
          table("u"), Map.empty, plan2, false, ifPartitionNotExists = false)))
  }

  test ("insert with if not exists") {
    val sql = "select * from t"
    intercept(s"insert overwrite table s partition (e = 1, x) if not exists $sql",
      "Dynamic partitions do not support IF NOT EXISTS. Specified partitions with value: [x]")
    intercept[ParseException](parsePlan(s"insert overwrite table s if not exists $sql"))
  }

  test("aggregation") {
    val sql = "select a, b, sum(c) as c from d group by a, b"

    // Normal
    assertEqual(sql, table("d").groupBy('a, 'b)('a, 'b, 'sum.function('c).as("c")))

    // Cube
    assertEqual(s"$sql with cube",
      table("d").groupBy(Cube(Seq('a, 'b)))('a, 'b, 'sum.function('c).as("c")))

    // Rollup
    assertEqual(s"$sql with rollup",
      table("d").groupBy(Rollup(Seq('a, 'b)))('a, 'b, 'sum.function('c).as("c")))

    // Grouping Sets
    assertEqual(s"$sql grouping sets((a, b), (a), ())",
      GroupingSets(Seq(Seq('a, 'b), Seq('a), Seq()), Seq('a, 'b), table("d"),
        Seq('a, 'b, 'sum.function('c).as("c"))))

    val m = intercept[ParseException] {
      parsePlan("SELECT a, b, count(distinct a, distinct b) as c FROM d GROUP BY a, b")
    }.getMessage
    assert(m.contains("extraneous input 'b'"))

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
    val spec = WindowSpecDefinition(Seq('a, 'b), Seq('c.asc),
      SpecifiedWindowFrame(RowFrame, -Literal(1), Literal(1)))

    // Test window resolution.
    val ws1 = Map("w1" -> spec, "w2" -> spec, "w3" -> spec)
    assertEqual(
      s"""$sql
         |window w1 as (partition by a, b order by c rows between 1 preceding and 1 following),
         |       w2 as w1,
         |       w3 as w1""".stripMargin,
      WithWindowDefinition(ws1, plan))

    // Fail with no reference.
    intercept(s"$sql window w2 as w1", "Cannot resolve window reference 'w1'")

    // Fail when resolved reference is not a window spec.
    intercept(
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
        from.where('s < 10).select(star()).insertInto("t3")))

    // Unresolved generator.
    val expected = table("t")
      .generate(
        UnresolvedGenerator(FunctionIdentifier("posexplode"), Seq('x)),
        alias = Some("posexpl"),
        outputNames = Seq("x", "y"))
      .select(star())
    assertEqual(
      "select * from t lateral view posexplode(x) posexpl as x, y",
      expected)

    intercept(
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
      assertEqual(
        s"select * from t as tt $sql u",
        table("t").as("tt").join(table("u"), jt, None).select(star()))
    }
    val testConditionalJoin = (sql: String, jt: JoinType) => {
      assertEqual(
        s"select * from t $sql u as uu on a = b",
        table("t").join(table("u").as("uu"), jt, Option('a === 'b)).select(star()))
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
    val testAll = Seq(testUnconditionalJoin, testConditionalJoin, testNaturalJoin, testUsingJoin)
    val testExistence = Seq(testUnconditionalJoin, testConditionalJoin, testUsingJoin)
    def test(sql: String, jt: JoinType, tests: Seq[(String, JoinType) => Unit]): Unit = {
      tests.foreach(_(sql, jt))
    }
    test("cross join", Cross, Seq(testUnconditionalJoin))
    test(",", Inner, Seq(testUnconditionalJoin))
    test("join", Inner, testAll)
    test("inner join", Inner, testAll)
    test("left join", LeftOuter, testAll)
    test("left outer join", LeftOuter, testAll)
    test("right join", RightOuter, testAll)
    test("right outer join", RightOuter, testAll)
    test("full join", FullOuter, testAll)
    test("full outer join", FullOuter, testAll)
    test("left semi join", LeftSemi, testExistence)
    test("semi join", LeftSemi, testExistence)
    test("left anti join", LeftAnti, testExistence)
    test("anti join", LeftAnti, testExistence)

    // Test natural cross join
    intercept("select * from a natural cross join b")

    // Test natural join with a condition
    intercept("select * from a natural join b on a.id = b.id")

    // Test multiple consecutive joins
    assertEqual(
      "select * from a join b join c right join d",
      table("a").join(table("b")).join(table("c")).join(table("d"), RightOuter).select(star()))

    // SPARK-17296
    assertEqual(
      "select * from t1 cross join t2 join t3 on t3.id = t1.id join t4 on t4.id = t1.id",
      table("t1")
        .join(table("t2"), Cross)
        .join(table("t3"), Inner, Option(Symbol("t3.id") === Symbol("t1.id")))
        .join(table("t4"), Inner, Option(Symbol("t4.id") === Symbol("t1.id")))
        .select(star()))

    // Test multiple on clauses.
    intercept("select * from t1 inner join t2 inner join t3 on col3 = col2 on col3 = col1")

    // Parenthesis
    assertEqual(
      "select * from t1 inner join (t2 inner join t3 on col3 = col2) on col3 = col1",
      table("t1")
        .join(table("t2")
          .join(table("t3"), Inner, Option('col3 === 'col2)), Inner, Option('col3 === 'col1))
        .select(star()))
    assertEqual(
      "select * from t1 inner join (t2 inner join t3) on col3 = col2",
      table("t1")
        .join(table("t2").join(table("t3"), Inner, None), Inner, Option('col3 === 'col2))
        .select(star()))
    assertEqual(
      "select * from t1 inner join (t2 inner join t3 on col3 = col2)",
      table("t1")
        .join(table("t2").join(table("t3"), Inner, Option('col3 === 'col2)), Inner, None)
        .select(star()))

    // Implicit joins.
    assertEqual(
      "select * from t1, t3 join t2 on t1.col1 = t2.col2",
      table("t1")
        .join(table("t3"))
        .join(table("t2"), Inner, Option(Symbol("t1.col1") === Symbol("t2.col2")))
        .select(star()))
  }

  test("sampled relations") {
    val sql = "select * from t"
    assertEqual(s"$sql tablesample(100 rows)",
      table("t").limit(100).select(star()))
    assertEqual(s"$sql tablesample(43 percent) as x",
      Sample(0, .43d, withReplacement = false, 10L, table("t").as("x")).select(star()))
    assertEqual(s"$sql tablesample(bucket 4 out of 10) as x",
      Sample(0, .4d, withReplacement = false, 10L, table("t").as("x")).select(star()))
    intercept(s"$sql tablesample(bucket 4 out of 10 on x) as x",
      "TABLESAMPLE(BUCKET x OUT OF y ON colname) is not supported")
    intercept(s"$sql tablesample(bucket 11 out of 10) as x",
      s"Sampling fraction (${11.0/10.0}) must be on interval [0, 1]")
    intercept("SELECT * FROM parquet_t0 TABLESAMPLE(300M) s",
      "TABLESAMPLE(byteLengthLiteral) is not supported")
    intercept("SELECT * FROM parquet_t0 TABLESAMPLE(BUCKET 3 OUT OF 32 ON rand()) s",
      "TABLESAMPLE(BUCKET x OUT OF y ON function) is not supported")
  }

  test("sub-query") {
    val plan = table("t0").select('id)
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
      plan.union(plan).union(plan).as("u_1").select('id))
  }

  test("scalar sub-query") {
    assertEqual(
      "select (select max(b) from s) ss from t",
      table("t").select(ScalarSubquery(table("s").select('max.function('b))).as("ss")))
    assertEqual(
      "select * from t where a = (select b from s)",
      table("t").where('a === ScalarSubquery(table("s").select('b))).select(star()))
    assertEqual(
      "select g from t group by g having a > (select b from s)",
      table("t")
        .groupBy('g)('g)
        .where('a > ScalarSubquery(table("s").select('b))))
  }

  test("table reference") {
    assertEqual("table t", table("t"))
    assertEqual("table d.t", table("d", "t"))
  }

  test("table valued function") {
    assertEqual(
      "select * from range(2)",
      UnresolvedTableValuedFunction("range", Literal(2) :: Nil, Seq.empty).select(star()))
  }

  test("SPARK-20311 range(N) as alias") {
    assertEqual(
      "SELECT * FROM range(10) AS t",
      SubqueryAlias("t", UnresolvedTableValuedFunction("range", Literal(10) :: Nil, Seq.empty))
        .select(star()))
    assertEqual(
      "SELECT * FROM range(7) AS t(a)",
      SubqueryAlias("t", UnresolvedTableValuedFunction("range", Literal(7) :: Nil, "a" :: Nil))
        .select(star()))
  }

  test("SPARK-20841 Support table column aliases in FROM clause") {
    assertEqual(
      "SELECT * FROM testData AS t(col1, col2)",
      UnresolvedSubqueryColumnAliases(
        Seq("col1", "col2"),
        SubqueryAlias("t", UnresolvedRelation(TableIdentifier("testData")))
      ).select(star()))
  }

  test("SPARK-20962 Support subquery column aliases in FROM clause") {
    assertEqual(
      "SELECT * FROM (SELECT a AS x, b AS y FROM t) t(col1, col2)",
      UnresolvedSubqueryColumnAliases(
        Seq("col1", "col2"),
        SubqueryAlias(
          "t",
          UnresolvedRelation(TableIdentifier("t")).select('a.as("x"), 'b.as("y")))
      ).select(star()))
  }

  test("SPARK-20963 Support aliases for join relations in FROM clause") {
    val src1 = UnresolvedRelation(TableIdentifier("src1")).as("s1")
    val src2 = UnresolvedRelation(TableIdentifier("src2")).as("s2")
    assertEqual(
      "SELECT * FROM (src1 s1 INNER JOIN src2 s2 ON s1.id = s2.id) dst(a, b, c, d)",
      UnresolvedSubqueryColumnAliases(
        Seq("a", "b", "c", "d"),
        SubqueryAlias(
          "dst",
          src1.join(src2, Inner, Option(Symbol("s1.id") === Symbol("s2.id"))))
      ).select(star()))
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
      table("db", "c").where('x >= 1).select('a, 'b))
    // !> is equivalent to <=
    assertEqual("select a, b from db.c where x !> 1",
      table("db", "c").where('x <= 1).select('a, 'b))
  }

  test("select hint syntax") {
    // Hive compatibility: Missing parameter raises ParseException.
    val m = intercept[ParseException] {
      parsePlan("SELECT /*+ HINT() */ * FROM t")
    }.getMessage
    assert(m.contains("mismatched input"))

    // Disallow space as the delimiter.
    val m3 = intercept[ParseException] {
      parsePlan("SELECT /*+ INDEX(a b c) */ * from default.t")
    }.getMessage
    assert(m3.contains("mismatched input 'b' expecting"))

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
        table("t").where(Literal(true)).groupBy('a)('a)).orderBy('a.asc))

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
      InsertIntoTable(table("s"), Map.empty,
        UnresolvedHint("REPARTITION", Seq(Literal(100)),
          UnresolvedHint("COALESCE", Seq(Literal(500)),
            UnresolvedHint("COALESCE", Seq(Literal(10)),
              table("t").select(star())))), overwrite = false, ifPartitionNotExists = false))

    comparePlans(
      parsePlan("SELECT /*+ BROADCASTJOIN(u), REPARTITION(100) */ * FROM t"),
      UnresolvedHint("BROADCASTJOIN", Seq($"u"),
        UnresolvedHint("REPARTITION", Seq(Literal(100)),
          table("t").select(star()))))

    intercept("SELECT /*+ COALESCE(30 + 50) */ * FROM t", "mismatched input")
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

    intercept("select ltrim(both 'S' from 'SS abc S'", "mismatched input 'from' expecting {')'")
    intercept("select rtrim(trailing 'S' from 'SS abc S'", "mismatched input 'from' expecting {')'")

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
      cte(table("t").select(star()), "t" -> ((table("a").select('c), Seq("x")))))
  }
}
