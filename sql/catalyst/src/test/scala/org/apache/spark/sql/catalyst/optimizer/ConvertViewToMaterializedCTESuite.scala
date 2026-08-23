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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, In, ListQuery, Literal, NamedExpression, OuterReference, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}

class ConvertViewToMaterializedCTESuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Convert View to Materialized CTE", FixedPoint(1), ConvertViewToMaterializedCTE) :: Nil
  }

  object OptimizeWithInlineCTE extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Convert View to Materialized CTE", FixedPoint(1), ConvertViewToMaterializedCTE) ::
      Batch("Inline CTE", FixedPoint(1), InlineCTE()) :: Nil
  }

  private def attr(name: String, id: Long): AttributeReference =
    AttributeReference(name, IntegerType, nullable = true)(exprId = ExprId(id))

  private def viewDesc(name: String, schema: StructType): CatalogTable =
    CatalogTable(
      identifier = TableIdentifier(name),
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = schema)

  private def tempView(name: String, child: LogicalPlan): View =
    View(viewDesc(name, child.schema), isTempView = true, child)

  /**
   * One view `name` referenced twice, as the analyzer produces it: two `View` occurrences
   * whose bodies are built from the same base attributes, except that the second
   * occurrence's attributes carry renewed expression ids. The renewed attributes are
   * returned as well for tests that reference them (e.g. in a join condition). Calling
   * this instead of writing `tempView("v", ...)` twice makes it unambiguous that the pair
   * stands for the same view used twice, not two views that happen to share a name.
   */
  private def sameViewTwice(
      name: String,
      base: AttributeReference,
      make: AttributeReference => LogicalPlan): (View, View, AttributeReference) = {
    val renewed = base.withExprId(NamedExpression.newExprId)
    (tempView(name, make(base)), tempView(name, make(renewed)), renewed)
  }

  private def sameViewTwice(
      name: String,
      base: Seq[AttributeReference],
      make: Seq[AttributeReference] => LogicalPlan): (View, View, Seq[AttributeReference]) = {
    val renewed = base.map(_.withExprId(NamedExpression.newExprId))
    (tempView(name, make(base)), tempView(name, make(renewed)), renewed)
  }

  // Same as above, for bodies that mint their own occurrence-specific ids per call
  // (e.g. through `NamedExpression.newExprId`), so there is no base attribute to renew.
  private def sameViewTwice(name: String, make: () => LogicalPlan): (View, View) = {
    (tempView(name, make()), tempView(name, make()))
  }

  // A simple deterministic view body: LocalRelation [a, b] filtered on a.
  private def simpleBody(a: AttributeReference, b: AttributeReference): LogicalPlan =
    LocalRelation(Seq(a, b)).where(a > 10)

  // A view body that contains a surviving (multi-ref) inner CTE. The references carry
  // distinct expression ids, mirroring analyzer output. The inner CTE survives inlining
  // either because it is non-deterministic or because it sets forceSkipInline.
  private def nestedCteBody(defId: Long, deterministic: Boolean): LogicalPlan = {
    val innerProject =
      if (deterministic) OneRowRelation().select(Literal(1).as("r"))
      else OneRowRelation().select(rand(0).as("r"))
    val innerDef = CTERelationDef(
      innerProject,
      id = defId,
      forceSkipInline = deterministic)
    def mkRef(): CTERelationRef = CTERelationRef(
      defId,
      _resolved = true,
      output = innerDef.output.map(_.withExprId(NamedExpression.newExprId)),
      isStreaming = false)
    WithCTE(Except(mkRef(), mkRef(), isAll = true), Seq(innerDef))
  }

  test("converts a self-joined view into one CTE definition and two references") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      val base = Seq(attr("a", 100), attr("b", 101))
      val (v1, v2, renewed) = sameViewTwice(
        "v", base, (as: Seq[AttributeReference]) => simpleBody(as(0), as(1)))
      val query = Join(v1, v2, Inner, Some(base(0) === renewed(0)), JoinHint(None, None))

      val optimized = Optimize.execute(query)

      // The root is wrapped in a WithCTE carrying exactly one definition.
      val WithCTE(mainPlan, cteDefs) = optimized
      assert(cteDefs.length == 1)
      val cteDef = cteDefs.head
      assert(cteDef.forceSkipInline,
        "converted CTE must set forceSkipInline so InlineCTE keeps it materialized")
      assert(cteDef.child.canonicalized == simpleBody(base(0), base(1)).canonicalized)

      // Exactly two references in the main plan.
      val refs = mainPlan.collect { case r: CTERelationRef => r }
      assert(refs.length == 2)

      // The first reference adopts the definition output directly.
      assert(refs.exists(_.output.map(_.exprId) == cteDef.output.map(_.exprId)))

      // The second reference is re-bound through an aliasing Project that re-mints the
      // original expression ids of that occurrence, so consumers need no rewriting.
      val projects = mainPlan.collect {
        case p @ Project(_, _: CTERelationRef) => p
      }
      assert(projects.length == 1)
      assert(projects.head.output.map(_.name) == Seq("a", "b"))
      assert(projects.head.output.map(_.exprId) == renewed.map(_.exprId))

      // The join condition still references the original attributes of both occurrences.
      val join = mainPlan.collect { case j: Join => j }.head
      assert(join.condition.get == (base(0) === renewed(0)))

      // The query output schema is unchanged.
      assert(optimized.output == query.output)
    }
  }

  test("leaves a single-reference view unchanged") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      val a1 = attr("a", 100)
      val b1 = attr("b", 101)
      val query = Filter(a1 > 5, tempView("v", simpleBody(a1, b1)))
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("does not convert non-deterministic views") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      def randBody(r: AttributeReference): LogicalPlan =
        Project(Seq(rand(0).as("r")), OneRowRelation())
      val (v1, v2, _) = sameViewTwice("v", attr("r", 100), r => randBody(r))
      val query = Join(v1, v2, Inner, None, JoinHint(None, None))
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("does not convert streaming views") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      val (v1, v2, _) = sameViewTwice(
        "v", attr("a", 100),
        (a: AttributeReference) => LocalRelation(Seq(a), Nil, isStreaming = true))
      val query = Join(v1, v2, Inner, None, JoinHint(None, None))
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("does not convert views with mixed effective SQL configs") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      val a1 = attr("a", 100)
      val b1 = attr("b", 101)
      val a2 = attr("a", 200)
      val b2 = attr("b", 201)
      val confKey = s"${CatalogTable.VIEW_SQL_CONFIG_PREFIX}spark.sql.foo"
      val descWithConf = viewDesc("v", simpleBody(a1, b1).schema).copy(
        properties = Map(confKey -> "bar"))
      val v1 = View(descWithConf, isTempView = true, simpleBody(a1, b1))
      val v2 = tempView("v", simpleBody(a2, b2))
      val query = Join(v1, v2, Inner, Some(a1 === a2), JoinHint(None, None))
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("bails out when occurrence schemas mismatch") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      // The two bodies are canonically equal, but the output column names differ
      // ("x" vs "y"), so positional re-binding is unsafe and the group must be skipped.
      val a1 = attr("a", 100)
      val a2 = attr("a", 200)
      val v1 = tempView("v", Project(Seq(a1.as("x")), LocalRelation(Seq(a1))))
      val v2 = tempView("v", Project(Seq(a2.as("y")), LocalRelation(Seq(a2))))
      val query = Join(v1, v2, Inner, None, JoinHint(None, None))
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("refuses conversion when occurrences of the same view diverge into multiple groups") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      // Degenerate scenario the analyzer cannot produce today: two pairs of occurrences
      // of the same view whose canonicalized bodies diverge. Each pair qualifies on its
      // own, but rewriting all four occurrences against whichever definition happens to
      // be visited first would re-bind the divergent pair positionally against the wrong
      // schema, so the identifier must be skipped entirely.
      val a1 = attr("a", 100)
      val a2 = attr("a", 200)
      val a3 = attr("a", 300)
      val a4 = attr("a", 400)
      def bodyWithOne(a: AttributeReference): LogicalPlan =
        Project(Seq(a.as("x")), LocalRelation(Seq(a)))
      def bodyWithTwo(a: AttributeReference): LogicalPlan =
        Project(Seq(a.as("x"), a.as("y")), LocalRelation(Seq(a)))
      val left = Join(
        tempView("v", bodyWithOne(a1)), tempView("v", bodyWithOne(a2)),
        Inner, None, JoinHint(None, None))
      val right = Join(
        tempView("v", bodyWithTwo(a3)), tempView("v", bodyWithTwo(a4)),
        Inner, None, JoinHint(None, None))
      val query = Join(left, right, Inner, None, JoinHint(None, None))
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("converts views referenced inside scalar subqueries") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      val (v1, v2, _) = sameViewTwice(
        "v", Seq(attr("a", 100), attr("b", 101)),
        (as: Seq[AttributeReference]) => simpleBody(as(0), as(1)))
      val sq1 = ScalarSubquery(v1)
      val sq2 = ScalarSubquery(v2)
      val query = OneRowRelation().select(sq1.as("s1"), sq2.as("s2"))

      val optimized = Optimize.execute(query)

      // The definitions are attached at the top-level scope while the references live
      // inside the scalar subqueries.
      val WithCTE(mainPlan, cteDefs) = optimized
      assert(cteDefs.length == 1)
      assert(cteDefs.head.forceSkipInline)
      val refs = optimized.collectWithSubqueries { case r: CTERelationRef => r }
      assert(refs.length == 2)
      assert(mainPlan.collect { case r: CTERelationRef => r }.isEmpty,
        "references must live inside the subqueries, not in the main plan")
      assert(optimized.output == query.output)
    }
  }

  test("converted definition survives the Inline CTE batch") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      val base = Seq(attr("a", 100), attr("b", 101))
      val (v1, v2, renewed) = sameViewTwice(
        "v", base, (as: Seq[AttributeReference]) => simpleBody(as(0), as(1)))
      val query = Join(v1, v2, Inner, Some(base(0) === renewed(0)), JoinHint(None, None))

      val optimized = OptimizeWithInlineCTE.execute(query)

      val withCTEs = optimized.collect { case w: WithCTE => w }
      assert(withCTEs.nonEmpty,
        "deterministic single-ref CTEs get inlined, but the converted def must survive")
      val defs = optimized.collect { case d: CTERelationDef => d }
      assert(defs.length == 1)
      assert(optimized.output == query.output)
    }
  }

  test("does not convert views whose body contains non-deterministic inner CTEs") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      // The view body itself evaluates a multi-ref non-deterministic inner CTE, so the view
      // yields different results per evaluation. Converting it to a compute-once CTE would
      // change query results, hence it must stay unconverted.
      val (v1, v2) = sameViewTwice(
        "v", () => nestedCteBody(987654321L, deterministic = false))
      val query = Join(v1, v2, Inner, None, JoinHint(None, None))
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("converts a view whose body contains a surviving deterministic inner CTE") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      // Both occurrences share the same inner CTE definition id, mirroring how the
      // analyzer duplicates a view body while keeping its inner CTE ids intact.
      val (v1, v2) = sameViewTwice(
        "v", () => nestedCteBody(987654321L, deterministic = true))
      val query = Join(v1, v2, Inner, None, JoinHint(None, None))

      val optimized = Optimize.execute(query)

      val WithCTE(mainPlan, cteDefs) = optimized
      assert(cteDefs.length == 1)
      assert(cteDefs.head.forceSkipInline)
      // The converted definition wraps the inner WithCTE of the view body.
      assert(cteDefs.head.child.isInstanceOf[WithCTE])
      val refs = mainPlan.collect { case r: CTERelationRef => r }
      assert(refs.length == 2)
      assert(optimized.output == query.output)
    }
  }

  test("converts a view whose body references another view") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      // v2's body is the v1 view referenced in the query twice, so both views are
      // referenced twice (v1 once inside each v2 body) and both groups qualify.
      val (v2a, v2b, _) = sameViewTwice(
        "v2", Seq(attr("a", 100), attr("b", 101)),
        (as: Seq[AttributeReference]) => tempView("v1", simpleBody(as(0), as(1))))
      val query = Join(v2a, v2b, Inner, None, JoinHint(None, None))

      val optimized = Optimize.execute(query)

      val WithCTE(mainPlan, cteDefs) = optimized
      assert(cteDefs.length == 2)
      assert(cteDefs.forall(_.forceSkipInline))
      // Bottom-up creation order puts the referenced (inner) definition before the
      // definition that references it, so the outer body resolves its inner reference
      // without needing the inner definition to be attached later.
      val innerDef = cteDefs.head
      val outerDef = cteDefs.last
      assert(innerDef.child.collect { case _: CTERelationRef => true }.isEmpty)
      val innerRefs = outerDef.child.collect { case r: CTERelationRef => r }
      assert(innerRefs.map(_.cteId) == Seq(innerDef.id))

      // Only the outer view is referenced from the main plan, once bare and once
      // re-bound through a rebinding Project.
      val refs = mainPlan.collect { case r: CTERelationRef => r }
      assert(refs.length == 2)
      assert(refs.forall(_.cteId == outerDef.id))
      assert(optimized.output == query.output)
    }
  }

  test("nested view definitions survive the Inline CTE batch") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      val (v2a, v2b, _) = sameViewTwice(
        "v2", Seq(attr("a", 100), attr("b", 101)),
        (as: Seq[AttributeReference]) => tempView("v1", simpleBody(as(0), as(1))))
      val query = Join(v2a, v2b, Inner, None, JoinHint(None, None))

      val optimized = OptimizeWithInlineCTE.execute(query)

      val defs = optimized.collect { case d: CTERelationDef => d }
      assert(defs.length == 2, "both nested definitions must survive inlining")
      // The order follows the bottom-up creation order: the referenced inner view's
      // definition (a bare body with no references) comes first, and the outer view's
      // definition wraps a reference to it.
      assert(defs.head.child.collect { case _: CTERelationRef => true }.isEmpty)
      val innerRefs = defs.last.child.collect { case r: CTERelationRef => r }
      assert(innerRefs.map(_.cteId) == Seq(defs.head.id))
      assert(optimized.output == query.output)
    }
  }

  test("is disabled by default") {
    val (v1, v2, _) = sameViewTwice(
      "v", Seq(attr("a", 100), attr("b", 101)),
      (as: Seq[AttributeReference]) => simpleBody(as(0), as(1)))
    val query = Join(v1, v2, Inner, None, JoinHint(None, None))
    comparePlans(Optimize.execute(query), query)
  }

  test("does not convert distinct views even with identical bodies") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      // The rule dedupes references of the SAME view, identified by its catalog identifier,
      // not views that happen to share a body. Two differently-named views with identical
      // bodies must not be merged into one CTE definition.
      val a1 = attr("a", 100)
      val b1 = attr("b", 101)
      val a2 = attr("a", 200)
      val b2 = attr("b", 201)
      val v1 = tempView("v1", simpleBody(a1, b1))
      val v2 = tempView("v2", simpleBody(a2, b2))
      val query = Join(v1, v2, Inner, Some(a1 === a2), JoinHint(None, None))
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("is idempotent") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      val base = Seq(attr("a", 100), attr("b", 101))
      val (v1, v2, renewed) = sameViewTwice(
        "v", base, (as: Seq[AttributeReference]) => simpleBody(as(0), as(1)))
      val query = Join(v1, v2, Inner, Some(base(0) === renewed(0)), JoinHint(None, None))
      val once = Optimize.execute(query)
      val twice = Optimize.execute(once)
      comparePlans(twice, once)
    }
  }

  test("converts a view whose body contains an internally correlated subquery") {
    withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
      // The body mirrors an analyzed `t WHERE x IN (SELECT y FROM s WHERE s.k = t.k)`:
      // the correlation references `t.k`, an attribute produced by a relation inside the
      // body itself, so it resolves within the body and does not escape it. The correlation
      // survives as an `OuterReference` inside the subquery plan; the body must not be
      // rejected for carrying it.
      def body(x: AttributeReference, tk: AttributeReference,
          s: AttributeReference, y: AttributeReference): LogicalPlan = {
        val correlated = LocalRelation(Seq(s, y)).where(OuterReference(tk) === s).select(y)
        LocalRelation(Seq(x, tk)).where(
          In(x, Seq(ListQuery(correlated, outerAttrs = Seq(tk), numCols = 1))))
      }
      val v1 = tempView("v", body(attr("x", 100), attr("t", 101), attr("s", 102), attr("y", 103)))
      val v2 = tempView("v", body(attr("x", 200), attr("t", 201), attr("s", 202), attr("y", 203)))
      val query = Join(v1, v2, Inner, None, JoinHint(None, None))

      val optimized = Optimize.execute(query)

      val WithCTE(mainPlan, cteDefs) = optimized
      assert(cteDefs.length == 1)
      assert(cteDefs.head.forceSkipInline)
      val refs = mainPlan.collect { case r: CTERelationRef => r }
      assert(refs.length == 2)
      assert(optimized.output == query.output)
    }
  }
}
