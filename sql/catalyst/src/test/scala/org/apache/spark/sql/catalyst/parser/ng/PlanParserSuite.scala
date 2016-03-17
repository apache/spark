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
package org.apache.spark.sql.catalyst.parser.ng

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{UnspecifiedFrame, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

class PlanParserSuite extends PlanTest {
  import CatalystSqlParser._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    comparePlans(parsePlan(sqlCommand), plan)
  }

  def intercept(sqlCommand: String, messages: String*): Unit = {
    val e = intercept[ParseException](parsePlan(sqlCommand))
    messages.foreach { message =>
      assert(e.message.contains(message))
    }
  }

  test("show functions") {
    assertEqual("show functions", ShowFunctions(None, None))
    assertEqual("show functions foo", ShowFunctions(None, Some("foo")))
    assertEqual("show functions foo.bar", ShowFunctions(Some("foo"), Some("bar")))
    assertEqual("show functions 'foo\\\\.*'", ShowFunctions(None, Some("foo\\.*")))
    intercept("show functions foo.bar.baz", "SHOW FUNCTIONS unsupported name")
  }

  test("describe function") {
    assertEqual("describe function bar", DescribeFunction("bar", isExtended = false))
    assertEqual("describe function extended bar", DescribeFunction("bar", isExtended = true))
    assertEqual("describe function foo.bar", DescribeFunction("foo.bar", isExtended = false))
    assertEqual("describe function extended f.bar", DescribeFunction("f.bar", isExtended = true))
  }

  test("set operations") {
    val a = table("a").select(all())
    val b = table("b").select(all())

    assertEqual("select * from a union select * from b", Distinct(a.unionAll(b)))
    assertEqual("select * from a union distinct select * from b", Distinct(a.unionAll(b)))
    assertEqual("select * from a union all select * from b", a.unionAll(b))
    assertEqual("select * from a except select * from b", a.except(b))
    intercept("select * from a except all select * from b", "EXCEPT ALL is not supported.")
    assertEqual("select * from a except distinct select * from b", a.except(b))
    assertEqual("select * from a intersect select * from b", a.intersect(b))
    intercept("select * from a intersect all select * from b", "INTERSECT ALL is not supported.")
    assertEqual("select * from a intersect distinct select * from b", a.intersect(b))
  }

  test("common table expressions") {
    def cte(plan: LogicalPlan, namedPlans: (String, LogicalPlan)*): With = {
      val ctes = namedPlans.map {
        case (name, cte) =>
          name -> SubqueryAlias(name, cte)
      }.toMap
      With(plan, ctes)
    }
    assertEqual(
      "with cte1 as (select * from a) select * from cte1",
      cte(table("cte1").select(all()), "cte1" -> table("a").select(all())))
    assertEqual(
      "with cte1 (select 1) select * from cte1",
      cte(table("cte1").select(all()), "cte1" -> OneRowRelation.select(1)))
    assertEqual(
      "with cte1 (select 1), cte2 as (select * from cte1) select * from cte2",
      cte(table("cte2").select(all()),
        "cte1" -> OneRowRelation.select(1),
        "cte2" -> table("cte1").select(all())))
    intercept(
      "with cte1 (select 1), cte1 as (select 1 from cte1) select * from cte1",
      "Name 'cte1' is used for multiple common table expressions")
  }

  test("simple select query") {
    assertEqual("select 1", OneRowRelation.select(1))
    assertEqual("select a, b", OneRowRelation.select('a, 'b))
    assertEqual("select a, b from db.c", table("db", "c").select('a, 'b))
    assertEqual("select a, b from db.c where x < 1", table("db", "c").where('x < 1).select('a, 'b))
    assertEqual("select a, b from db.c having x < 1", table("db", "c").select('a, 'b).where('x < 1))
    assertEqual("select distinct a, b from db.c", Distinct(table("db", "c").select('a, 'b)))
    assertEqual("select all a, b from db.c", table("db", "c").select('a, 'b))
  }

  test("transform query spec") {
    val p = ScriptTransformation(Seq('a, 'b), "func", Seq.empty, table("e"), null)
    assertEqual("select transform(a, b) using 'func' from e where f < 10",
      p.copy(child = p.child.where('f < 10)))
    assertEqual("map(a, b) using 'func' as c, d from e",
      p.copy(output = Seq('c.string, 'd.string)))
    assertEqual("reduce(a, b) using 'func' as (c: int, d decimal(10, 0)) from e",
      p.copy(output = Seq('c.int, 'd.decimal(10, 0))))
  }

  test("multi select query") {
    assertEqual(
      "from a select * select * where s < 10",
      table("a").select(all()).unionAll(table("a").where('s < 10).select(all())))
    intercept(
      "from a select * select * from x where a.s < 10",
      "Multi-Insert queries cannot have a FROM clause in their individual SELECT statements")
    assertEqual(
      "from a insert into tbl1 select * insert into tbl2 select * where s < 10",
      table("a").select(all()).insertInto("tbl1").unionAll(
        table("a").where('s < 10).select(all()).insertInto("tbl2")))
  }

  test("query organization") {
    // Test all valid combinations of order by/sort by/distribute by/cluster by/limit/windows
    val baseSql = "select * from t"
    val basePlan = table("t").select(all())

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
      (" sort by a, b desc", basePlan.sortBy('a.asc, 'b.desc)),
      (" distribute by a, b", basePlan.distribute('a, 'b)),
      (" distribute by a sort by b", basePlan.distribute('a).sortBy('b.asc)),
      (" cluster by a, b", basePlan.distribute('a, 'b).sortBy('a.asc, 'b.asc))
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
    val plan = table("t").select(all())
    def insert(
        partition: Map[String, Option[String]],
        overwrite: Boolean = false,
        ifNotExists: Boolean = false): LogicalPlan =
      InsertIntoTable(table("s"), partition, plan, overwrite, ifNotExists)

    // Single inserts
    assertEqual(s"insert overwrite table s $sql",
      insert(Map.empty, true))
    assertEqual(s"insert overwrite table s if not exists $sql",
      insert(Map.empty, true, true))
    assertEqual(s"insert into s $sql",
      insert(Map.empty))
    assertEqual(s"insert into table s partition (c = 'd', e = 1) $sql",
      insert(Map("c" -> Option("d"), "e" -> Option("1"))))
    assertEqual(s"insert overwrite table s partition (c = 'd', x) if not exists $sql",
      insert(Map("c" -> Option("d"), "x" -> None), true, true))

    // Multi insert
    val plan2 = table("t").where('x > 5).select(all())
    assertEqual("from t insert into s select * limit 1 insert into u select * where x > 5",
      InsertIntoTable(table("s"), Map.empty, plan.limit(1), false, false).unionAll(
        InsertIntoTable(table("u"), Map.empty, plan2, false, false)))
  }


  test("aggregation") {
    // Normal
    // Cube/Rollup/Grouping Sets
  }

  test("limit") {
    // Expressions
  }

  test("window spec") {
    // Spec.
    // Named
    // Referencing each other
  }

  test("lateral view") {
    // Single lateral view
    // Multiple lateral views
    // Multi-Insert lateral views.
  }

  test("joins") {
    // Alias
    // - Query
    // - Plan
  }

  test("sampled relations") {
    //
  }

  test("subquery") {
  }

  test("table reference") {
    assertEqual("table a", table("a"))
    assertEqual("table a.b", table("a", "b"))
  }

  test("inline table") {
    assertEqual("values 1, 2, 3, 4", LocalRelation.fromExternalRows(
      Seq('col1.int),
      Seq(1, 2, 3, 4).map(x => Row(x))))
    assertEqual(
      "values (1, 'a'), (2, 'b'), (3, 'c') as tbl(a, b)",
      LocalRelation.fromExternalRows(
        Seq('a.int, 'b.string),
        Seq((1, "a"), (2, "b"), (3, "c")).map(x => Row(x._1, x._2))).as("tbl"))
    intercept("values (a, 'a'), (b, 'b')",
      "All expressions in an inline table must be constants.")
    intercept("values (1, 'a'), (2, 'b') as tbl(a, b, c)",
      "Number of aliases must match the number of fields in an inline table.")
    intercept[ArrayIndexOutOfBoundsException](parsePlan("values (1, 'a'), (2, 'b', 5Y)"))
  }
}
