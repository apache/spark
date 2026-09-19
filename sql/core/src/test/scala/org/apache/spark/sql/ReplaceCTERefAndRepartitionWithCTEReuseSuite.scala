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

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.optimizer.{ReplaceCTERefWithRepartition, ReplaceRepartitionWithCTEReuse}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.ExtendedMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class ReplaceCTERefAndRepartitionWithCTEReuseSuite
    extends QueryTest with SharedSparkSession {

  private val cteReuseConfKey = "spark.sql.optimizer.replaceCTERefWithCTEReuse.enabled"

  private def withCTEReuseEnabled(f: => Unit): Unit = {
    withSQLConf(
      cteReuseConfKey -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true"
    )(f)
  }

  // Runs the guaranteed-reuse pipeline (the three rules of the "Replace CTE with Repartition"
  // batch, in order) with the flag on, so tests can drive it on a hand-built plan.
  private def runReuseRules(plan: LogicalPlan): LogicalPlan = withSQLConf(
      cteReuseConfKey -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
    ReplaceRepartitionWithCTEReuse(ReplaceCTERefWithRepartition(plan))
  }

  private def collectCTEReuseRelations(plan: LogicalPlan): Seq[CTEReuseRelation] = {
    plan.collectWithSubqueries { case r: CTEReuseRelation => r }
  }

  // CTEReuseRelation is a leaf whose sharedSubplan is metadata (not a child), so a plain `collect`
  // stops at it. This variant descends into sharedSubplan so nested CTEReuseRelations are counted.
  private def collectCTEReuseRelationsDeep(plan: LogicalPlan): Seq[CTEReuseRelation] = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[CTEReuseRelation]
    def go(p: LogicalPlan): Unit = {
      p.foreachWithSubqueries {
        case r: CTEReuseRelation =>
          buf += r
          go(r.sharedSubplan)
        case _ =>
      }
    }
    go(plan)
    buf.toSeq
  }

  private def rel: LocalRelation = LocalRelation(AttributeReference("a", IntegerType)())

  private def planReuseRepartition(child: LogicalPlan, id: Long): RepartitionByExpression =
    RepartitionByExpression(Seq.empty, child, optNumPartitions = Some(1), id = id)

  // A resolved WithCTE with a single non-inlined CTE def referenced `refCount` times.
  private def withCteRefs(cteBody: LogicalPlan, refCount: Int): WithCTE = {
    val cteDef = CTERelationDef(cteBody)
    val refs = (0 until refCount).map { _ =>
      CTERelationRef(cteDef.id, _resolved = true, cteBody.output, _isStreaming = false)
    }
    // Chain the refs under Unions so they are all real children of the WithCTE body.
    val body = refs.reduce[LogicalPlan]((l, r) => Union(Seq(l, r)))
    WithCTE(body, Seq(cteDef))
  }

  private def assertCTEReuseWithSameIdHaveSameCanonical(
      reuses: Seq[CTEReuseRelation]): Unit = {
    reuses.groupBy(_.cteId).foreach { case (id, group) =>
      val canonicals = group.map(_.sharedSubplan.canonicalized).distinct
      assert(canonicals.size == 1,
        s"CTEReuseRelation nodes with id=$id have ${canonicals.size} " +
          s"distinct canonical forms, expected 1.\n" +
          canonicals.zipWithIndex.map { case (c, i) =>
            s"Canonical $i:\n${c.treeString}"
          }.mkString("\n"))
    }
  }

  // ---------------------------------------------------------------------------
  // CTE tests (CTE ref -> CTEReuseRelation)
  // ---------------------------------------------------------------------------

  test("CTE with non-deterministic function produces CTEReuseRelation") {
    withCTEReuseEnabled {
      val df = sql(
        """WITH cte AS (SELECT id, rand() as r FROM range(100))
          |SELECT * FROM cte c1 JOIN cte c2 ON c1.id = c2.id
          |""".stripMargin)
      val optimized = df.queryExecution.optimizedPlan
      val reuses = collectCTEReuseRelations(optimized)
      assert(reuses.nonEmpty,
        s"Expected CTEReuseRelation in plan:\n${optimized.treeString}")
      assertCTEReuseWithSameIdHaveSameCanonical(reuses)
    }
  }

  test("CTE with multiple references produces shared CTEReuseRelation") {
    withCTEReuseEnabled {
      val df = sql(
        """WITH cte AS (SELECT id, rand() as r FROM range(100))
          |SELECT c1.id, c2.id, c3.id
          |FROM cte c1, cte c2, cte c3
          |WHERE c1.id = c2.id AND c2.id = c3.id
          |""".stripMargin)
      val optimized = df.queryExecution.optimizedPlan
      val reuses = collectCTEReuseRelations(optimized)
      assert(reuses.size >= 3,
        s"Expected at least 3 CTEReuseRelation nodes, got ${reuses.size}." +
          s"\nPlan:\n${optimized.treeString}")
      assertCTEReuseWithSameIdHaveSameCanonical(reuses)
    }
  }

  test("multiple CTEs produce distinct CTEReuseRelation ids") {
    withCTEReuseEnabled {
      val df = sql(
        """WITH
          |  cte1 AS (SELECT id, rand() as r FROM range(100)),
          |  cte2 AS (SELECT id, rand() as r FROM range(200))
          |SELECT * FROM cte1 c1 JOIN cte1 c2 ON c1.id = c2.id
          |UNION ALL
          |SELECT * FROM cte2 c3 JOIN cte2 c4 ON c3.id = c4.id
          |""".stripMargin)
      val optimized = df.queryExecution.optimizedPlan
      val reuses = collectCTEReuseRelations(optimized)
      val distinctIds = reuses.map(_.cteId).distinct
      assert(distinctIds.size == 2,
        s"Expected 2 distinct CTEReuseRelation ids, got ${distinctIds.size}." +
          s"\nPlan:\n${optimized.treeString}")
      assertCTEReuseWithSameIdHaveSameCanonical(reuses)
    }
  }

  test("CTE with correlated subquery") {
    withCTEReuseEnabled {
      withTable("cte_t1") {
        sql("CREATE TABLE cte_t1 (a INT, b INT) USING parquet")
        sql("INSERT INTO cte_t1 VALUES (1, 2), (3, 4)")
        val df = sql(
          """WITH cte AS (SELECT a, rand() as r FROM cte_t1)
            |SELECT * FROM cte c1
            |WHERE c1.a IN (SELECT a FROM cte c2 WHERE c2.r > 0.5)
            |""".stripMargin)
        val optimized = df.queryExecution.optimizedPlan
        val reuses = collectCTEReuseRelations(optimized)
        assert(reuses.nonEmpty,
          s"Expected CTEReuseRelation in plan:\n${optimized.treeString}")
        assertCTEReuseWithSameIdHaveSameCanonical(reuses)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Flag off: fallback
  // ---------------------------------------------------------------------------

  test("flag off falls back to ReplaceCTERefWithRepartition") {
    withSQLConf(
      cteReuseConfKey -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true"
    ) {
      val df = sql(
        """WITH cte AS (SELECT id, rand() as r FROM range(100))
          |SELECT * FROM cte c1 JOIN cte c2 ON c1.id = c2.id
          |""".stripMargin)
      val optimized = df.queryExecution.optimizedPlan
      val reuses = collectCTEReuseRelations(optimized)
      assert(reuses.isEmpty,
        s"Expected no CTEReuseRelation when flag is off, but found:" +
          s"\n${optimized.treeString}")
    }
  }

  // ---------------------------------------------------------------------------
  // A CTE body already ending in a customer repartition gets no special treatment
  // ---------------------------------------------------------------------------

  test("CTE body ending in a customer repartition is not converted to CTEReuseRelation") {
    withCTEReuseEnabled {
      // The CTE body root is a user Repartition (REPARTITION hint). It is not a plan-reuse
      // repartition, so ReplaceCTERefWithRepartition leaves it in place (canSkipExtraRepartition)
      // rather than wrapping it in a plan-reuse shuffle -- the CTE is effectively inlined and its
      // repartition is not converted. So there is no CTEReuseRelation for this shape.
      val df = sql(
        """WITH cte AS (
          |  SELECT /*+ REPARTITION(3, id) */ id, rand() as r FROM range(100)
          |)
          |SELECT * FROM cte c1 JOIN cte c2 ON c1.id = c2.id
          |""".stripMargin)
      val optimized = df.queryExecution.optimizedPlan
      val reuses = collectCTEReuseRelations(optimized)
      assert(reuses.isEmpty,
        s"Expected no CTEReuseRelation for a CTE whose body root is a customer repartition:" +
          s"\n${optimized.treeString}")
    }
  }

  // ---------------------------------------------------------------------------
  // Singleton CTEReuseRelation cleanup
  // ---------------------------------------------------------------------------

  test("singleton CTEReuseRelation is unwrapped, multi-instance is kept") {
    // Build plans directly: replaceCTERef only produces CTEReuseRelations for CTEs that survive
    // InlineCTE (multi-ref + non-deterministic/force-skip), and a genuine singleton (one instance
    // per cteId) is otherwise hard to construct via SQL. Exercise the cleanup on such input:
    // plan-reuse repartitions carrying a repartitionId.
    val rel = LocalRelation(AttributeReference("a", IntegerType)())

    // Case A: two plan-reuse repartitions sharing one repartitionId -> two CTEReuseRelation
    // instances after conversion -> NOT a singleton -> kept.
    val sharedId = 12345L
    val r1 = RepartitionByExpression(Seq.empty, rel, Some(1), id = sharedId)
    val r2 = RepartitionByExpression(Seq.empty, rel, Some(1), id = sharedId)
    val multiPlan = Union(Seq(r1, r2))
    val multiResult = runReuseRules(multiPlan)
    val multiReuses = collectCTEReuseRelations(multiResult)
    assert(multiReuses.size == 2 && multiReuses.forall(_.cteId == sharedId),
      s"Expected two CTEReuseRelation(cteId=$sharedId), got:\n${multiResult.treeString}")

    // Case B: a single plan-reuse repartition -> one CTEReuseRelation instance -> singleton ->
    // unwrapped back to its sharedSubplan (the repartition), no CTEReuseRelation left.
    val loneId = 67890L
    val lone = RepartitionByExpression(Seq.empty, rel, Some(1), id = loneId)
    val singleResult = runReuseRules(lone)
    val singleReuses = collectCTEReuseRelations(singleResult)
    assert(singleReuses.isEmpty,
      s"Expected the singleton CTEReuseRelation to be unwrapped, got:\n${singleResult.treeString}")
    // The unwrapped result still contains the underlying repartition.
    assert(singleResult.collectFirst { case _: RepartitionOperation => true }.isDefined,
      s"Expected the repartition to remain after unwrapping:\n${singleResult.treeString}")
  }

  // ---------------------------------------------------------------------------
  // Nested nodes: a node converted to a leaf CTEReuseRelation must not hide
  // an inner CTERef / repartition from the other phase.
  // ---------------------------------------------------------------------------

  test("nested plan-reuse repartitions are all converted") {
    // Repartition(id=1) on top of Repartition(id=2), each referenced twice so neither is a
    // singleton. Both must become CTEReuseRelations; the inner one must survive inside the
    // outer's sharedSubplan.
    //
    // All instances of the same cteId must have canonically-equal sharedSubplans, otherwise the
    // rule's validation legitimately rejects the plan and falls back. Reuse the SAME node objects
    // for each id so the two id=1 (and the two id=2) instances are canonically identical.
    val inner = planReuseRepartition(rel, id = 2L)
    val outer = planReuseRepartition(Union(Seq(inner, inner)), id = 1L)
    val plan = Union(Seq(outer, outer))

    val result = runReuseRules(plan)

    val ids = collectCTEReuseRelationsDeep(result).map(_.cteId).toSet
    assert(ids == Set(1L, 2L),
      s"Expected CTEReuseRelations for both cteId 1 and 2, got $ids:\n${result.treeString}")
    // Every plan-reuse Repartition must now sit at the root of some CTEReuseRelation.sharedSubplan.
    // Equivalently: a plain `collect` over the tree (which does NOT descend into a
    // CTEReuseRelation's metadata sharedSubplan) must find no plan-reuse Repartition -- if it does,
    // that repartition was left unconverted.
    val unconverted = result.collect { case rp: PlanReusableRepartition if rp.isForPlanReuse => rp }
    assert(unconverted.isEmpty,
      s"Expected no unconverted plan-reuse Repartition outside a CTEReuseRelation, got:" +
        s"\n${result.treeString}")
  }

  test("nested CTEs are all converted (no unresolved CTERelationRef)") {
    // Outer CTE body references an inner CTE; both are multi-referenced non-inlined CTEs.
    // After the rule, no CTERelationRef may remain (all resolved) and both cteIds appear.
    val innerBody = rel
    val innerDef = CTERelationDef(innerBody)
    val innerRef1 =
      CTERelationRef(innerDef.id, _resolved = true, innerBody.output, _isStreaming = false)
    val innerRef2 =
      CTERelationRef(innerDef.id, _resolved = true, innerBody.output, _isStreaming = false)
    val outerBody = WithCTE(Union(Seq(innerRef1, innerRef2)), Seq(innerDef))

    val outerDef = CTERelationDef(outerBody)
    val outerRef1 =
      CTERelationRef(outerDef.id, _resolved = true, outerBody.output, _isStreaming = false)
    val outerRef2 =
      CTERelationRef(outerDef.id, _resolved = true, outerBody.output, _isStreaming = false)
    val plan = WithCTE(Union(Seq(outerRef1, outerRef2)), Seq(outerDef))

    val result = runReuseRules(plan)

    val remainingRefs = result.collectWithSubqueries { case r: CTERelationRef => r }
    assert(remainingRefs.isEmpty,
      s"Expected all CTERelationRef resolved, got ${remainingRefs.size}:\n${result.treeString}")
    assert(collectCTEReuseRelationsDeep(result).nonEmpty,
      s"Expected CTEReuseRelations for the nested CTEs:\n${result.treeString}")
  }

  // ---------------------------------------------------------------------------
  // Canonicalization: a CTEReuseRelation's metadata partitioning exprIds must be
  // normalized, otherwise per-reference exprIds poison sharedSubplan equality.
  // ---------------------------------------------------------------------------

  test("CTEReuseRelation canonicalization normalizes HashPartitioning exprIds") {
    // Two structurally-equal CTEReuseRelations built with independently minted exprIds (as
    // per-reference deduplication produces). Their HashPartitioning keys differ only by raw
    // exprId. `partitioning` is metadata, not a child, so canonicalization normalizes its exprIds
    // against the `allAttributes` override (sharedSubplan.output); without it the canonical forms
    // differ.
    def reuse(): CTEReuseRelation = {
      val leaf = LocalRelation(AttributeReference("a", IntegerType)())
      CTEReuseRelation(cteId = 1L, partitioning = HashPartitioning(leaf.output, 5),
        sharedSubplan = leaf)
    }
    val a = reuse()
    val b = reuse()
    assert(a.partitioning != b.partitioning,
      "expected the two copies to carry different raw exprIds in their partitioning")
    assert(a.canonicalized == b.canonicalized,
      s"expected canonical forms to match:\n${a.canonicalized}\n${b.canonicalized}")
  }

  test("nested plan-reuse HashPartitioning repartitions do not trigger validation fallback") {
    // Two structurally-equal copies of a nested plan-reuse tree, built with independently minted
    // exprIds. The inner repartition uses a HashPartitioning over the copy's own attribute; after
    // conversion it becomes a nested CTEReuseRelation whose partitioning is metadata.
    // Canonicalizing the outer cteId's sharedSubplan recurses into that inner one, so without
    // partitioning-exprId normalization the two copies' canonical forms differ and
    // validateCTEReuseRelations forces a fallback to ReplaceCTERefWithRepartition (no
    // CTEReuseRelation).
    def nestedCopy(): LogicalPlan = {
      val leaf = LocalRelation(AttributeReference("a", IntegerType)())
      val inner = RepartitionByExpression(leaf.output, leaf, Some(5), id = 2L)
      RepartitionByExpression(Nil, inner, Some(5), id = 1L)
    }
    val plan = Union(Seq(nestedCopy(), nestedCopy()))

    val result = runReuseRules(plan)

    val ids = collectCTEReuseRelationsDeep(result).map(_.cteId).toSet
    assert(ids == Set(1L, 2L),
      s"Expected CTEReuseRelations for cteId 1 and 2 (no fallback), got $ids:" +
        s"\n${result.treeString}")
    assert(LogicalPlanIntegrity.validateCTEReuseRelations(result).isEmpty,
      s"Expected CTEReuseRelation validation to pass:\n${result.treeString}")
  }

  test("plan-reuse repartition on top of a CTE reference is resolved") {
    // Repartition(isForPlanReuse) directly on top of a CTERelationRef. Converting the
    // Repartition into a leaf CTEReuseRelation first would hide the CTERelationRef and
    // leaving it unresolved. The rule must resolve the CTERelationRef regardless of ordering.
    val cteBody = rel
    val cteDef = CTERelationDef(cteBody)
    val ref1 = CTERelationRef(cteDef.id, _resolved = true, cteBody.output, _isStreaming = false)
    val ref2 = CTERelationRef(cteDef.id, _resolved = true, cteBody.output, _isStreaming = false)
    // A plan-reuse repartition sits directly above one of the refs.
    val body = Union(Seq(planReuseRepartition(ref1, id = 99L), ref2))
    val plan = WithCTE(body, Seq(cteDef))

    val result = runReuseRules(plan)

    val remainingRefs = result.collectWithSubqueries { case r: CTERelationRef => r } ++
      collectCTEReuseRelationsDeep(result).flatMap(_.sharedSubplan.collectWithSubqueries {
        case r: CTERelationRef => r
      })
    assert(remainingRefs.isEmpty,
      s"Expected the CTERelationRef under the repartition to be resolved, got " +
        s"${remainingRefs.size}:\n${result.treeString}")
  }

  // ---------------------------------------------------------------------------
  // EXPLAIN rendering
  // ---------------------------------------------------------------------------

  test("CTEReuseRelation renders its sharedSubplan in the tree string") {
    withCTEReuseEnabled {
      val df = sql(
        """WITH cte AS (SELECT id, rand() as r FROM range(100))
          |SELECT c1.id FROM cte c1 JOIN cte c2 ON c1.id = c2.id
          |""".stripMargin)
      val optimized = df.queryExecution.optimizedPlan
      val reuses = collectCTEReuseRelations(optimized)
      assert(reuses.nonEmpty,
        s"Expected CTEReuseRelation in plan:\n${optimized.treeString}")

      // The shared subplan is metadata, not a child. It must still be rendered (nested under the
      // CTEReuseRelation line via `innerChildren`) so EXPLAIN shows what is being reused instead
      // of an opaque leaf.
      val treeString = optimized.treeString
      assert(treeString.contains("CTEReuseRelation cteId="),
        s"Expected the CTEReuseRelation line in the tree string:\n$treeString")
      assert(treeString.contains("Repartition"),
        s"Expected the sharedSubplan's Repartition to be rendered:\n$treeString")
      assert(treeString.contains("Range"),
        s"Expected the CTE body's Range scan to be rendered:\n$treeString")

      // Same for the user-facing EXPLAIN EXTENDED output.
      val explained = df.queryExecution.explainString(ExtendedMode)
      assert(explained.contains("CTEReuseRelation cteId="),
        s"Expected the CTEReuseRelation line in EXPLAIN EXTENDED:\n$explained")
    }
  }

}
