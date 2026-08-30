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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.expressions.{Ascending, CurrentRow, RowFrame, RowNumber}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, GreaterThan}
import org.apache.spark.sql.catalyst.expressions.{IsNotNull, Literal}
import org.apache.spark.sql.catalyst.expressions.{SortOrder, SpecifiedWindowFrame, UnboundedPreceding}
import org.apache.spark.sql.catalyst.expressions.{WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, CTERelationRef}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LocalRelation}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Union, Window, WithCTE}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.IntegerType

/**
 * Regression test for a staleness defect in
 * [[PushdownPredicatesAndPruneColumnsForCTEDef]]: on its second pass the rule rebuilt a
 * CTE definition from the plan snapshot stored in `originalPlanWithPredicates`, which was
 * taken during its first pass. Any change made to the CTE definition's child by other rules
 * in between (e.g. filters injected by `InferFiltersFromConstraints`, which runs in the
 * "Infer Filters" batch sandwiched between the two fixed-point batches that both contain
 * this rule) was silently discarded by the rebuild.
 *
 * The inter-pass mutations are produced by applying the real rules
 * ([[InferFiltersFromConstraints]] and [[PushPredicateThroughNonJoin]]) between two real
 * applications of the rule under test, so the suite keeps exercising the actual cross-rule
 * interaction as those rules evolve.
 */
class PushdownPredicatesForCTEDefStalenessSuite extends PlanTest {

  test("CTE def rebuild must not discard filters injected between rule passes") {
    val t1a = AttributeReference("a", IntegerType, nullable = true)()
    val t2b = AttributeReference("b", IntegerType, nullable = true)()
    val t1 = LocalRelation(t1a)
    val t2 = LocalRelation(t2b)
    val join = Join(t1, t2, Inner, Some(EqualTo(t1a, t2b)), JoinHint.NONE)

    val cteId = 0L
    val cteDef = CTERelationDef(join, cteId)
    val plan = withTwoRefs(cteDef)(
      out => EqualTo(out(0), Literal(5)),
      out => EqualTo(out(0), Literal(7)))

    // Pass 1 (batch "Operator Optimization before Inferring Filters"): the rule pushes the
    // combined reference predicates into the CTE definition and records them in
    // originalPlanWithPredicates.
    val afterPass1 = PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan)
    assert(theOnlyDef(afterPass1).originalPlanWithPredicates.isDefined)

    // Run the real "Infer Filters" batch rule between the two passes (it is sandwiched
    // between the two fixed-point batches that both contain this rule). It strengthens the
    // pushed filter itself (propagating a = 5 | a = 7 through the join condition onto b),
    // injects isnotnull filters below the join, and enriches the reference sites with
    // isnotnull, which re-arms the rule's guard on the next pass.
    val afterInfer = InferFiltersFromConstraints.apply(afterPass1)
    val injectedConds = theOnlyDef(afterInfer).child.collect { case f: Filter => f.condition }
    val pass1FilterCount = theOnlyDef(afterPass1).child.collect { case f: Filter => f }.length
    assert(injectedConds.length > pass1FilterCount,
      "test setup failed: InferFiltersFromConstraints did not inject filters into the " +
        "CTE definition")

    // Pass 2 (batch "Operator Optimization after Inferring Filters"): the rule sees the
    // enriched reference predicates and rebuilds the definition. Every filter present
    // after the Infer Filters batch must survive the rebuild; before the fix the rebuild
    // used the stale first-pass snapshot and discarded all of them.
    val defAfterPass2 = theOnlyDef(PushdownPredicatesAndPruneColumnsForCTEDef.apply(afterInfer))
    injectedConds.foreach { cond =>
      assert(hasFilterOn(defAfterPass2.child, cond),
        s"PushdownPredicatesAndPruneColumnsForCTEDef discarded a filter that " +
          s"InferFiltersFromConstraints injected between its two passes: $cond")
    }
  }

  test("rule is idempotent when no new predicates appear between passes") {
    val t1a = AttributeReference("a", IntegerType, nullable = true)()
    val t2b = AttributeReference("b", IntegerType, nullable = true)()
    val t1 = LocalRelation(t1a)
    val t2 = LocalRelation(t2b)
    val join = Join(t1, t2, Inner, Some(EqualTo(t1a, t2b)), JoinHint.NONE)

    val cteId = 0L
    val cteDef = CTERelationDef(join, cteId)
    val plan = withTwoRefs(cteDef)(
      out => EqualTo(out(0), Literal(5)),
      out => EqualTo(out(0), Literal(7)))

    // First application pushes the combined reference predicates and records them.
    val once = PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan)
    // A second application with no new reference predicates must be a no-op:
    // no re-push, no stacked filters.
    val twice = PushdownPredicatesAndPruneColumnsForCTEDef.apply(once)
    comparePlans(once, twice)

    // Even after a foreign mutation inside the CTE definition (e.g. a filter injected
    // by InferFiltersFromConstraints), as long as no NEW reference-site predicate
    // appears, the rule must leave the plan untouched. (The mutation is hand-written
    // because no real rule mutates only the definition without also enriching the
    // reference sites, which is exactly what would re-arm the guard.)
    val mutated = once.transform {
      case d @ CTERelationDef(Filter(cond, j: Join), `cteId`, Some(_), _, _, _) =>
        d.copy(child = Filter(cond, j.copy(right = Filter(GreaterThan(t2b, Literal(0)), j.right))))
    }
    val afterMutation = PushdownPredicatesAndPruneColumnsForCTEDef.apply(mutated)
    comparePlans(mutated, afterMutation)
  }

  test("rebuild removes the previous push-down even after it was pushed deeper") {
    val t1a = AttributeReference("a", IntegerType, nullable = true)()
    val t2b = AttributeReference("b", IntegerType, nullable = true)()
    val t1 = LocalRelation(t1a)
    val t2 = LocalRelation(t2b)
    val join = Join(t1, t2, Inner, Some(EqualTo(t1a, t2b)), JoinHint.NONE)

    // The CTE definition ends in a renaming projection, like a view selecting aliased columns.
    val pa = Alias(t1a, "pa")()
    val pb = Alias(t2b, "pb")()
    val project = Project(Seq(pa, pb), join)

    val cteId = 0L
    val cteDef = CTERelationDef(project, cteId)
    val plan = withTwoRefs(cteDef)(
      out => EqualTo(out(0), Literal(5)),
      out => EqualTo(out(0), Literal(7)))

    // Pass 1: pushes the combined predicate on top of the definition's projection.
    val afterPass1 = PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan)

    // Consume the pushed filter with the real push-down rule that shares the fixedPoint
    // batches with the rule under test: it moves below the renaming projection with the
    // attributes rewritten to the projection's input.
    val consumed = addIsNotNullToRef(PushPredicateThroughNonJoin.apply(afterPass1),
      cteId, Literal(7))
    assert(theOnlyDef(consumed).child match {
      case Project(_, Filter(_, _: Join)) => true
      case _ => false
    }, "test setup failed: push-down did not move the filter below the projection")

    // Pass 2: the rule must remove its previous push-down from wherever push-down left
    // it; otherwise the rebuilt definition carries the old and new combined predicates
    // as redundant stacked filters.
    assertSingleEnrichedTopFilter(consumed,
      "previous push-down was not removed before re-pushing")
  }

  test("rebuild removes the previous push-down pushed into union branches") {
    val l1 = AttributeReference("a", IntegerType, nullable = true)()
    val l2 = AttributeReference("b", IntegerType, nullable = true)()
    val r1 = AttributeReference("a", IntegerType, nullable = true)()
    val r2 = AttributeReference("b", IntegerType, nullable = true)()
    // The union output shares exprIds with the first branch; the def output is the union output.
    val union = Union(Seq(LocalRelation(l1, l2), LocalRelation(r1, r2)))

    val cteId = 0L
    val cteDef = CTERelationDef(union, cteId)
    val plan = withTwoRefs(cteDef)(
      out => And(EqualTo(out(0), Literal(5)), GreaterThan(out(1), Literal(0))),
      out => And(EqualTo(out(0), Literal(7)), GreaterThan(out(1), Literal(0))))

    val afterPass1 = PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan)

    // The real push-down rule copies the pushed filter into every union branch,
    // translating the union output attributes to each branch's output positionally.
    val consumed = addIsNotNullToRef(PushPredicateThroughNonJoin.apply(afterPass1),
      cteId, Literal(7))
    assert(theOnlyDef(consumed).child match {
      case u: Union => u.children.forall(_.isInstanceOf[Filter])
      case _ => false
    }, "test setup failed: push-down did not copy the filter into every union branch")

    assertSingleEnrichedTopFilter(consumed,
      "previous push-down was not removed from the union branches")
  }

  test("rebuild removes the previous push-down pushed below an aggregate") {
    val a = AttributeReference("a", IntegerType, nullable = true)()
    val b = AttributeReference("b", IntegerType, nullable = true)()
    val pa = Alias(a, "pa")()
    // Grouping-only aggregate (distinct), so the definition has a single output column.
    val aggregate = Aggregate(Seq(pa), Seq(pa), LocalRelation(a, b))

    val cteId = 0L
    val cteDef = CTERelationDef(aggregate, cteId)
    val plan = withTwoRefs(cteDef)(
      out => EqualTo(out(0), Literal(5)),
      out => EqualTo(out(0), Literal(7)))

    val afterPass1 = PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan)

    // The real push-down rule moves the filter below the aggregate, translating the
    // grouping alias back to its child attribute.
    val consumed = addIsNotNullToRef(PushPredicateThroughNonJoin.apply(afterPass1),
      cteId, Literal(7))
    assert(theOnlyDef(consumed).child match {
      case Aggregate(_, _, _: Filter, _) => true
      case _ => false
    }, "test setup failed: push-down did not move the filter below the aggregate")

    assertSingleEnrichedTopFilter(consumed,
      "previous push-down was not removed below the aggregate")
  }

  test("rebuild removes the previous push-down pushed below a window") {
    val a = AttributeReference("a", IntegerType, nullable = true)()
    val b = AttributeReference("b", IntegerType, nullable = true)()
    val rn = Alias(
      WindowExpression(RowNumber(), WindowSpecDefinition(Seq(a), Seq(SortOrder(a, Ascending)),
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))),
      "rn")()
    val window = Window(Seq(rn), Seq(a), Seq(SortOrder(a, Ascending)), LocalRelation(a, b))

    val cteId = 0L
    val cteDef = CTERelationDef(window, cteId)
    val plan = withTwoRefs(cteDef)(
      out => EqualTo(out(0), Literal(5)),
      out => EqualTo(out(0), Literal(7)))

    val afterPass1 = PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan)

    // The real push-down rule moves the filter below the window unchanged (the predicate
    // references only the partition column, which is an input attribute).
    val consumed = addIsNotNullToRef(PushPredicateThroughNonJoin.apply(afterPass1),
      cteId, Literal(7))
    assert(theOnlyDef(consumed).child match {
      case w: Window => w.child.isInstanceOf[Filter]
      case _ => false
    }, "test setup failed: push-down did not move the filter below the window")

    assertSingleEnrichedTopFilter(consumed,
      "previous push-down was not removed below the window")
  }

  test("rebuild preserves TreeNode tags on the nodes it rebuilds") {
    val t1a = AttributeReference("a", IntegerType, nullable = true)()
    val t2b = AttributeReference("b", IntegerType, nullable = true)()
    val t1 = LocalRelation(t1a)
    val t2 = LocalRelation(t2b)
    val join = Join(t1, t2, Inner, Some(EqualTo(t1a, t2b)), JoinHint.NONE)
    // Identity projection mimicking the analyzer-inserted projection above a natural or
    // USING join, which carries the hidden join-key columns in Project.hiddenOutputTag.
    val project = Project(Seq(t1a, t2b), join)

    val cteId = 0L
    val cteDef = CTERelationDef(project, cteId)
    val plan = withTwoRefs(cteDef)(
      out => EqualTo(out(0), Literal(5)),
      out => EqualTo(out(0), Literal(7)))

    val afterPass1 = PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan)

    // The real push-down rules move the pushed filter below the projection and then into
    // the join's left branch (the predicate references only the left side of the inner
    // join), so the next rebuild has to rebuild both the projection and the join.
    val consumed = addIsNotNullToRef(
      PushPredicateThroughJoin.apply(PushPredicateThroughNonJoin.apply(afterPass1)),
      cteId, Literal(7))
    assert(theOnlyDef(consumed).child match {
      case Project(_, Join(_: Filter, _, _, _, _)) => true
      case _ => false
    }, "test setup failed: push-down did not move the filter into the join branch")

    // Tag the nodes on the removal path in place, so the tags are present when the rule
    // under test rebuilds them. (The intermediate push-down rules rebuild nodes with
    // plain copies of their own; their tag handling is out of scope here.)
    val taggedProject = theOnlyDef(consumed).child.asInstanceOf[Project]
    val taggedJoin = taggedProject.child.asInstanceOf[Join]
    taggedProject.setTagValue(Project.hiddenOutputTag, Seq(t2b))
    taggedJoin.setTagValue(testTag, "preserved")

    // Pass 2: removing the previous push-down from the join branch rebuilds both the
    // join and the projection. The rebuild must carry their tags over.
    val rebuilt = theOnlyDef(PushdownPredicatesAndPruneColumnsForCTEDef.apply(consumed)).child
    val rebuiltProject = rebuilt.collect { case p: Project => p }.head
    val rebuiltJoin = rebuilt.collect { case j: Join => j }.head
    assert(rebuiltProject.getTagValue(Project.hiddenOutputTag).contains(Seq(t2b)),
      s"rebuild dropped Project.hiddenOutputTag: $rebuilt")
    assert(rebuiltJoin.getTagValue(testTag).contains("preserved"),
      s"rebuild dropped tags on the join: $rebuilt")
  }

  /** A tag with no consumer, used to verify that rebuilds preserve arbitrary tags. */
  private val testTag = TreeNodeTag[String]("cte_pushdown_test_tag")

  /**
   * Applies the rule under test to `plan` (pass 2) and asserts that the rebuilt CTE
   * definition carries exactly one filter: the freshly re-pushed combined predicate
   * (recognizable by the IsNotNull enrichment). Any leftover copy of the previous
   * push-down shows up as an additional filter and fails the test.
   */
  private def assertSingleEnrichedTopFilter(plan: LogicalPlan, message: String): Unit = {
    val cteDef = theOnlyDef(PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan))
    val filters = cteDef.child.collect { case f: Filter => f }
    assert(filters.length == 1, s"$message: ${cteDef.child}")
    assert(filters.head.condition.find(_.isInstanceOf[IsNotNull]).isDefined,
      s"expected the enriched combined predicate on top of the def: ${cteDef.child}")
  }

  /**
   * Builds one reference to the given CTE definition: `Project -> Filter -> CTERelationRef`
   * with fresh output attribute instances (`CTERelationRef` is a `MultiInstanceRelation`,
   * so every reference must own fresh exprIds). The project list covers all definition
   * output columns so that column pruning never kicks in. `pred` receives the fresh
   * output attributes and returns the reference-site predicate.
   */
  private def mkRef(cteId: Long, defOutput: Seq[Attribute])(
      pred: Seq[Attribute] => Expression): Project = {
    val out = defOutput.map(_.newInstance())
    Project(out, Filter(pred(out), CTERelationRef(cteId, true, out, false)))
  }

  /**
   * Builds `WithCTE(Union(refs), Seq(cteDef))` with two reference sites carrying the given
   * predicates, like a view referenced from multiple call sites.
   */
  private def withTwoRefs(cteDef: CTERelationDef)(
      pred1: Seq[Attribute] => Expression,
      pred2: Seq[Attribute] => Expression): LogicalPlan = {
    val refs = Seq(pred1, pred2).map(p => mkRef(cteDef.id, cteDef.output)(p))
    WithCTE(Union(refs), Seq(cteDef))
  }

  /** The single CTE definition in the given plan. */
  private def theOnlyDef(plan: LogicalPlan): CTERelationDef = {
    plan.collect { case d: CTERelationDef => d }.head
  }

  /**
   * Simulates the reference-site enrichment `InferFiltersFromConstraints` performs between
   * the rule's two passes: conjoins `IsNotNull` on the column the site predicate compares
   * to `marker` (e.g. the second reference's `Literal(7)`), which re-arms the rule's
   * rebuild on the next pass. The removal tests use this targeted mutation instead of the
   * real rule because the real rule would also rewrite the pushed filter inside the
   * definition, defeating the exact-match removal those tests isolate.
   */
  private def addIsNotNullToRef(
      plan: LogicalPlan, cteId: Long, marker: Literal): LogicalPlan = {
    plan.transform {
      case f @ Filter(cond, ref: CTERelationRef) if ref.cteId == cteId =>
        cond.collectFirst { case EqualTo(a: Attribute, m: Literal) if m == marker => a } match {
          case Some(a) => Filter(And(cond, IsNotNull(a)), ref)
          case _ => f
        }
    }
  }

  private def hasFilterOn(plan: LogicalPlan, condition: Expression): Boolean = {
    plan.collect { case f: Filter => f.condition }.exists(_.semanticEquals(condition))
  }
}
