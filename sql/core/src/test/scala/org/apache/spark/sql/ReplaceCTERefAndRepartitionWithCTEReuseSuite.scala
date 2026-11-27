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

import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Ascending, Attribute, AttributeReference, Literal, Rand, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.{PushdownPredicatesAndPruneColumnsForCTEDef, ReplaceCTERefWithRepartition, ReplaceRepartitionWithCTEReuse}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.execution.ExtendedMode
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class ReplaceCTERefAndRepartitionWithCTEReuseSuite
    extends QueryTest with SharedSparkSession {

  private val cteReuseConfKey = SQLConf.REPLACE_CTE_REF_WITH_CTE_REUSE.key

  private def withCTEReuseEnabled(f: => Unit): Unit = {
    withSQLConf(
      cteReuseConfKey -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true"
    )(f)
  }

  /**
   * Runs `text` with every CTE forced to materialize (`forceSkipInline = true`) so it opts into
   * guaranteed CTE shuffle reuse. `forceSkipInline` has no SQL syntax, so tag the analyzed plan's
   * CTE definitions and re-run the tagged plan.
   */
  private def sqlWithForcedCTEReuse(text: String): DataFrame = {
    val analyzed = spark.sql(text).queryExecution.analyzed
    val tagged = analyzed.transformWithSubqueries {
      case d: CTERelationDef => d.copy(forceSkipInline = true)
    }
    classic.Dataset.ofRows(spark, tagged)
  }

  /** Like [[sqlWithForcedCTEReuse]], but forces only the CTEs named in `names`. */
  private def sqlWithForcedCTEs(text: String, names: Set[String]): DataFrame = {
    val analyzed = spark.sql(text).queryExecution.analyzed
    val tagged = analyzed.transformWithSubqueries {
      case d: CTERelationDef if (d.child match {
        case a: SubqueryAlias => names.contains(a.alias)
        case _ => false
      }) => d.copy(forceSkipInline = true)
    }
    classic.Dataset.ofRows(spark, tagged)
  }

  // Runs the two guaranteed-reuse rules, ReplaceCTERefWithRepartition and then
  // ReplaceRepartitionWithCTEReuse (SparkOptimizer runs them in two consecutive Once batches), with
  // the reuse flag set to `enabled`, so tests can drive them on a hand-built plan.
  private def runReuseRules(plan: LogicalPlan, enabled: Boolean = true): LogicalPlan =
    withSQLConf(
      cteReuseConfKey -> enabled.toString,
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      ReplaceRepartitionWithCTEReuse(ReplaceCTERefWithRepartition(plan))
    }

  // Plan-reuse repartitions left anywhere in `plan`, including inside shared subplans.
  private def planReuseRepartitions(plan: LogicalPlan): Seq[PlanReusableRepartition] = {
    val found = scala.collection.mutable.ArrayBuffer.empty[PlanReusableRepartition]
    def go(p: LogicalPlan): Unit = p.foreachWithSubqueries {
      case r: CTEReuseRelation => go(r.sharedSubplan)
      case r: PlanReusableRepartition if r.isForPlanReuse => found += r
      case _ =>
    }
    go(plan)
    found.toSeq
  }

  // Repartition-like operators (repartitions and rebalances) in `plan`, outside shared subplans.
  private def numRepartitionLike(plan: LogicalPlan): Int = plan.collect {
    case r: RepartitionOperation => r
    case r: RebalancePartitions => r
  }.size

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
  private def withCteRefs(
      cteBody: LogicalPlan, refCount: Int, underSubquery: Boolean = false): WithCTE = {
    val cteDef = CTERelationDef(cteBody, underSubquery = underSubquery)
    val refs = (0 until refCount).map { _ =>
      CTERelationRef(cteDef.id, _resolved = true, cteBody.output, isStreaming = false)
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
      val df = sqlWithForcedCTEReuse(
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
      val df = sqlWithForcedCTEReuse(
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
      val df = sqlWithForcedCTEReuse(
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

  test("CTE referenced from an uncorrelated IN subquery") {
    withCTEReuseEnabled {
      withTable("cte_t1") {
        sql("CREATE TABLE cte_t1 (a INT, b INT) USING parquet")
        sql("INSERT INTO cte_t1 VALUES (1, 2), (3, 4)")
        val df = sqlWithForcedCTEReuse(
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

  test("flag off: a forced CTE is lowered like any other retained CTE") {
    withSQLConf(
      cteReuseConfKey -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true"
    ) {
      // The reuse flag only affects forceSkipInline definitions, so force this one.
      val df = sqlWithForcedCTEReuse(
        """WITH cte AS (SELECT id, rand() as r FROM range(100))
          |SELECT * FROM cte c1 JOIN cte c2 ON c1.id = c2.id
          |""".stripMargin)
      val optimized = df.queryExecution.optimizedPlan
      val reuses = collectCTEReuseRelations(optimized)
      assert(reuses.isEmpty,
        s"Expected no CTEReuseRelation when flag is off, but found:" +
          s"\n${optimized.treeString}")
      assert(planReuseRepartitions(optimized).isEmpty,
        s"Expected no plan-reuse repartition id when flag is off:\n${optimized.treeString}")
    }
  }

  test("flag off: forced CTEs of every root shape keep the plain lowering") {
    // Each shape is referenced twice, so with the flag on it would be sealed. With the flag off the
    // forced definition must lower exactly like the same definition without forceSkipInline: no
    // plan-reuse id, no CTEReuseRelation, and no repartition beyond the plain lowering.
    val shapes: Seq[(String, LogicalPlan, Boolean, Int)] = Seq(
      // (name, body, underSubquery, repartition-like nodes per reference)
      ("plain body", rel, false, 1),
      ("repartition root", RepartitionByExpression(Seq.empty, rel, Some(3)), false, 1),
      ("rebalance root", RebalancePartitions(Seq.empty, rel), false, 1),
      ("under-subquery body", rel, true, 0))
    shapes.foreach { case (name, body, underSubquery, perRef) =>
      val forced = runReuseRules(
        forcedCteRefs(body, refCount = 2, underSubquery = underSubquery), enabled = false)
      assert(collectCTEReuseRelationsDeep(forced).isEmpty,
        s"$name: expected no CTEReuseRelation with the flag off:\n${forced.treeString}")
      assert(planReuseRepartitions(forced).isEmpty,
        s"$name: expected no plan-reuse id with the flag off:\n${forced.treeString}")
      assert(numRepartitionLike(forced) == 2 * perRef,
        s"$name: expected the plain lowering (${2 * perRef} repartitions):\n${forced.treeString}")
      val notForced = runReuseRules(
        withCteRefs(body, refCount = 2, underSubquery = underSubquery), enabled = false)
      assert(forced == notForced,
        s"$name: expected the same plan as without forceSkipInline:\n${forced.treeString}\n" +
          notForced.treeString)
      // With the flag on, the same forced definition is sealed.
      val sealedPlan = runReuseRules(forcedCteRefs(body, refCount = 2, underSubquery))
      assert(collectCTEReuseRelations(sealedPlan).size == 2,
        s"$name: expected the definition to be sealed with the flag on:\n${sealedPlan.treeString}")
    }
  }

  // ---------------------------------------------------------------------------
  // A CTE that is not forceSkipInline keeps the plain lowering, whatever its root
  // ---------------------------------------------------------------------------

  test("CTE body ending in a customer repartition is not converted to CTEReuseRelation") {
    withCTEReuseEnabled {
      // The CTE is not forceSkipInline, so ReplaceCTERefWithRepartition keeps the plain lowering
      // whatever its root is: here it leaves the body's user REPARTITION in place
      // (canSkipExtraRepartition) and assigns no plan-reuse id, so there is no CTEReuseRelation.
      // The same REPARTITION-rooted body is sealed once it is forced (see "forced CTE rooted at an
      // existing repartition seals").
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
  // A plan-reuse id with a single copy is not sealed
  // ---------------------------------------------------------------------------

  test("a plan-reuse id with one copy is not sealed and is reset, two copies are sealed") {
    // Build plans directly: ReplaceCTERefWithRepartition assigns plan-reuse ids only to bodies
    // the plan holds two or more copies of, so SQL cannot produce an id
    // with a single copy, and resetting such an id is a defensive fallback. Exercise
    // ReplaceRepartitionWithCTEReuse on plan-reuse repartitions carrying an id.
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

    // Case B: a single plan-reuse repartition -> its id has one copy -> it is not sealed, and its
    // id is reset so the repartition plans as an ordinary shuffle.
    val loneId = 67890L
    val lone = RepartitionByExpression(Seq.empty, rel, Some(1), id = loneId)
    val singleResult = runReuseRules(lone)
    val singleReuses = collectCTEReuseRelations(singleResult)
    assert(singleReuses.isEmpty,
      s"Expected no CTEReuseRelation for a single copy, got:\n${singleResult.treeString}")
    // The repartition remains, without a plan-reuse id.
    assert(singleResult.collectFirst { case _: RepartitionOperation => true }.isDefined,
      s"Expected the repartition to remain:\n${singleResult.treeString}")
    assert(planReuseRepartitions(singleResult).isEmpty,
      s"Expected the unsealed repartition's id to be reset:\n${singleResult.treeString}")
  }

  test("validation failure seals nothing and resets every plan-reuse id") {
    // Two plan-reuse repartitions share an id over different children, so the sealed plan would
    // give one cteId two canonical forms: validation rejects it and the rule seals nothing.
    val other = LocalRelation(AttributeReference("b", IntegerType)(), AttributeReference(
      "c", IntegerType)())
    val plan = Union(Seq(
      planReuseRepartition(rel, id = 7L),
      Project(Seq(other.output.head), planReuseRepartition(other, id = 7L))))
    val result = runReuseRules(plan)
    assert(collectCTEReuseRelationsDeep(result).isEmpty,
      s"Expected no CTEReuseRelation after a validation failure:\n${result.treeString}")
    assert(planReuseRepartitions(result).isEmpty,
      s"Expected every plan-reuse id to be reset after a validation failure:\n" +
        result.treeString)
  }

  test("validation of nested same-id relations under different outer ids seals nothing") {
    // Each outer id is consistent on its own, but the nested id 9 has a different body under each
    // of them. Only the validation's descent into shared subplans sees that, because the two outer
    // ids' relations are compared only with relations of the same outer id.
    val other = LocalRelation(AttributeReference("b", IntegerType)(), AttributeReference(
      "c", IntegerType)())
    val innerA = planReuseRepartition(rel, id = 9L)
    val innerB = Project(Seq(other.output.head), planReuseRepartition(other, id = 9L))
    val outerA = planReuseRepartition(Union(Seq(innerA, innerA)), id = 10L)
    val outerB = planReuseRepartition(Union(Seq(innerB, innerB)), id = 11L)
    val plan = Union(Seq(outerA, outerA, outerB, outerB))
    val result = runReuseRules(plan)
    assert(collectCTEReuseRelationsDeep(result).isEmpty,
      s"Expected no CTEReuseRelation after a validation failure:\n${result.treeString}")
    assert(planReuseRepartitions(result).isEmpty,
      s"Expected every plan-reuse id to be reset after a validation failure:\n" +
        result.treeString)
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
    val innerDef = CTERelationDef(innerBody, forceSkipInline = true)
    val innerRef1 =
      CTERelationRef(innerDef.id, _resolved = true, innerBody.output, isStreaming = false)
    val innerRef2 =
      CTERelationRef(innerDef.id, _resolved = true, innerBody.output, isStreaming = false)
    val outerBody = WithCTE(Union(Seq(innerRef1, innerRef2)), Seq(innerDef))

    val outerDef = CTERelationDef(outerBody, forceSkipInline = true)
    val outerRef1 =
      CTERelationRef(outerDef.id, _resolved = true, outerBody.output, isStreaming = false)
    val outerRef2 =
      CTERelationRef(outerDef.id, _resolved = true, outerBody.output, isStreaming = false)
    val plan = WithCTE(Union(Seq(outerRef1, outerRef2)), Seq(outerDef))

    val result = runReuseRules(plan)

    val remainingRefs = result.collectWithSubqueries { case r: CTERelationRef => r }
    assert(remainingRefs.isEmpty,
      s"Expected all CTERelationRef resolved, got ${remainingRefs.size}:\n${result.treeString}")
    // The outer definition is sealed, and so is the inner one, with its own id, inside the
    // outer definition's shared body.
    val outerReuses = collectCTEReuseRelations(result)
    assertAllRefsShareOneNonzeroId(outerReuses, refCount = 2)
    val innerReuses = collectCTEReuseRelationsDeep(outerReuses.head.sharedSubplan)
    assertAllRefsShareOneNonzeroId(innerReuses, refCount = 2)
    assert(innerReuses.head.cteId != outerReuses.head.cteId,
      s"Expected the inner CTE to be sealed with its own id:\n${result.treeString}")
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
    // partitioning-exprId normalization the two copies' canonical forms differ,
    // validateCTEReuseRelations fails, and ReplaceRepartitionWithCTEReuse then seals nothing and
    // resets every plan-reuse id (no CTEReuseRelation).
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
    // leave it unresolved. The rule must resolve the CTERelationRef regardless of ordering.
    val cteBody = rel
    val cteDef = CTERelationDef(cteBody)
    val ref1 = CTERelationRef(cteDef.id, _resolved = true, cteBody.output, isStreaming = false)
    val ref2 = CTERelationRef(cteDef.id, _resolved = true, cteBody.output, isStreaming = false)
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
      val df = sqlWithForcedCTEReuse(
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

  // ---------------------------------------------------------------------------
  // forcePartitioning is honored only together with forceSkipInline
  // ---------------------------------------------------------------------------

  test("forcePartitioning is honored with forceSkipInline, preserving numPartitions") {
    // A CTE pinned with HashPartitioning and forceSkipInline = true materializes as a plan-reuse
    // repartition on the pinned keys (sealed for guaranteed reuse), and the exact requested
    // numPartitions is preserved rather than replaced by the session default.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val cteBody = LocalRelation(a, b)
    val cteDef = CTERelationDef(cteBody, forceSkipInline = true,
      forcePartitioning = Some(HashPartitioning(Seq(b), 7)))
    val ref1 = CTERelationRef(cteDef.id, _resolved = true, cteBody.output, isStreaming = false)
    val ref2 = CTERelationRef(cteDef.id, _resolved = true, cteBody.output, isStreaming = false)
    val plan = WithCTE(Union(Seq(ref1, ref2)), Seq(cteDef))

    val result = runReuseRules(plan)
    val reuses = collectCTEReuseRelations(result)
    assert(reuses.nonEmpty,
      s"Expected CTEReuseRelation for a forcePartitioning + forceSkipInline CTE:" +
        s"\n${result.treeString}")
    reuses.foreach { r =>
      r.partitioning match {
        case h: HashPartitioning =>
          assert(h.numPartitions == 7,
            s"Expected the pinned numPartitions=7 to be preserved, got ${h.numPartitions}")
          assert(h.expressions == Seq(b),
            s"Expected the pinned key b, got ${h.expressions}")
        case other =>
          fail(s"Expected a HashPartitioning on the reuse relation, got $other")
      }
    }
  }

  test("a forcePartitioning pin that is not HashPartitioning is rejected") {
    val a = AttributeReference("a", IntegerType)()
    val cteBody = LocalRelation(a)
    val cteDef = CTERelationDef(cteBody, forceSkipInline = true,
      forcePartitioning = Some(RangePartitioning(Seq(SortOrder(a, Ascending)), 3)))
    val ref1 = CTERelationRef(cteDef.id, _resolved = true, cteBody.output, isStreaming = false)
    val ref2 = CTERelationRef(cteDef.id, _resolved = true, cteBody.output, isStreaming = false)
    val plan = WithCTE(Union(Seq(ref1, ref2)), Seq(cteDef))
    val e = intercept[UnsupportedOperationException](runReuseRules(plan))
    assert(e.getMessage.contains("supports only HashPartitioning"), e.getMessage)
  }

  test("a pinned forced CTE referenced once is repartitioned on the pin but not sealed") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val cteBody = LocalRelation(a, b)
    val cteDef = CTERelationDef(cteBody, forceSkipInline = true,
      forcePartitioning = Some(HashPartitioning(Seq(b), 7)))
    val ref = CTERelationRef(cteDef.id, _resolved = true, cteBody.output, isStreaming = false)
    val result = runReuseRules(WithCTE(ref, Seq(cteDef)))
    assert(collectCTEReuseRelations(result).isEmpty && planReuseRepartitions(result).isEmpty,
      s"Expected a single reference not to be sealed:\n${result.treeString}")
    val repartitions = result.collect { case r: RepartitionByExpression => r }
    assert(repartitions.size == 1 && repartitions.head.partitionExpressions == Seq(b) &&
      repartitions.head.numPartitions == 7,
      s"Expected one repartition on the pinned key and partition count:\n${result.treeString}")
  }

  test("forcePartitioning is ignored without forceSkipInline") {
    // forcePartitioning is only honored together with forceSkipInline. A pin on a
    // non-forceSkipInline definition is not sealed for guaranteed reuse (such a definition may be
    // inlined by InlineCTE in a full optimizer run), so no CTEReuseRelation is produced.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val cteBody = LocalRelation(a, b)
    val cteDef = CTERelationDef(cteBody, forceSkipInline = false,
      forcePartitioning = Some(HashPartitioning(Seq(b), 7)))
    val ref1 = CTERelationRef(cteDef.id, _resolved = true, cteBody.output, isStreaming = false)
    val ref2 = CTERelationRef(cteDef.id, _resolved = true, cteBody.output, isStreaming = false)
    val plan = WithCTE(Union(Seq(ref1, ref2)), Seq(cteDef))

    val result = runReuseRules(plan)
    assert(collectCTEReuseRelations(result).isEmpty,
      s"Expected no CTEReuseRelation when forceSkipInline=false:\n${result.treeString}")
  }

  test("an ignored forcePartitioning pin leaves column pruning and the plan unchanged") {
    // The CTE outputs (a, b), every reference projects only `a`, and the pin is on `b`. A pin that
    // the lowering ignores, on a definition that is not forceSkipInline or with the reuse flag
    // off, must give the plan of the same definition without the pin, which prunes `b`.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val cteBody = LocalRelation(a, b)
    val pin = Some(HashPartitioning(Seq(b), 7))
    // Under the same two conditions, a pin of a kind the lowering does not support is ignored
    // too, rather than rejected as it is when honored.
    val ignoredPins = Seq(pin, Some(RangePartitioning(Seq(SortOrder(b, Ascending)), 3)))
    // The references share the definition's attributes, so the lowering keeps the body's exprIds
    // and the plans compare exactly.
    def plan(cteDef: CTERelationDef): LogicalPlan = WithCTE(Union(Seq.fill(2) {
      Project(Seq(a), CTERelationRef(cteDef.id, _resolved = true, cteBody.output,
        isStreaming = false))
    }), Seq(cteDef))
    def optimize(cteDef: CTERelationDef, enabled: Boolean): LogicalPlan =
      withSQLConf(cteReuseConfKey -> enabled.toString) {
        runReuseRules(PushdownPredicatesAndPruneColumnsForCTEDef(plan(cteDef)), enabled)
      }
    // Whether CTE column pruning projected the body down to `a`.
    def prunedToA(p: LogicalPlan): Boolean = p.collectWithSubqueries {
      case Project(list, `cteBody`) => list.map(_.toAttribute)
    }.exists(_ == Seq(a))

    for {
      (name, (unpinned, enabled)) <- Seq(
        "not forceSkipInline" -> (CTERelationDef(cteBody), true),
        "reuse flag off" -> (CTERelationDef(cteBody, forceSkipInline = true), false))
      ignoredPin <- ignoredPins
    } {
      val pinName = s"$name, ${ignoredPin.get.getClass.getSimpleName} pin"
      val withoutPin = optimize(unpinned, enabled)
      val withPin = optimize(unpinned.copy(forcePartitioning = ignoredPin), enabled)
      assert(prunedToA(withoutPin), s"$pinName: expected `b` to be pruned:\n$withoutPin")
      assert(withPin == withoutPin,
        s"$pinName: expected an ignored pin to leave the plan unchanged:\n$withPin")
    }

    // An honored pin keeps `b` through pruning and repartitions the shared body on it.
    val honored = optimize(CTERelationDef(cteBody, forceSkipInline = true, forcePartitioning = pin),
      enabled = true)
    val reuses = collectCTEReuseRelations(honored)
    assertAllRefsShareOneNonzeroId(reuses, refCount = 2)
    reuses.foreach { r =>
      val root = sharedRoot(r)
      assert(root.partitionExpressions == Seq(b) && root.numPartitions == 7 &&
        root.child.output.contains(b), s"Expected the pinned shuffle on `b`:\n${r.sharedSubplan}")
    }
  }

  test("forcePartitioning keys survive CTE column pruning") {
    // The CTE outputs (a, b) and is pinned on b, but no reference projects b. Column pruning must
    // not drop b, otherwise the later forced repartition on b would dangle. `b` is retained even
    // though it is not a user-visible consumer column.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val cteBody = LocalRelation(a, b)
    val cteDef = CTERelationDef(cteBody, forceSkipInline = true,
      forcePartitioning = Some(HashPartitioning(Seq(b), 5)))
    // A reference's output is a 1:1 copy of the def output; consumers project only `a`.
    def refProjectingA(): LogicalPlan = {
      val ref = CTERelationRef(cteDef.id, _resolved = true,
        Seq(AttributeReference("a", IntegerType)(), AttributeReference("b", IntegerType)()),
        isStreaming = false)
      Project(Seq(ref.output.head), ref)
    }
    val plan = WithCTE(Union(Seq(refProjectingA(), refProjectingA())), Seq(cteDef))

    val pruned = PushdownPredicatesAndPruneColumnsForCTEDef(plan)
    val prunedDef = pruned.collectWithSubqueries { case d: CTERelationDef => d }.head
    assert(prunedDef.output.exists(_.name == "b"),
      s"Expected the forced partition key `b` to survive column pruning:\n${pruned.treeString}")
  }

  // ---------------------------------------------------------------------------
  // forceSkipInline lowering is total: every body root shape seals for reuse
  // ---------------------------------------------------------------------------

  // A resolved WithCTE with one forceSkipInline CTE def referenced `refCount` times.
  private def forcedCteRefs(
      cteBody: LogicalPlan, refCount: Int, underSubquery: Boolean = false): WithCTE = {
    val cteDef = CTERelationDef(cteBody, underSubquery = underSubquery, forceSkipInline = true)
    val refs = (0 until refCount).map { _ =>
      CTERelationRef(cteDef.id, _resolved = true, cteBody.output, isStreaming = false)
    }
    val body = refs.reduce[LogicalPlan]((l, r) => Union(Seq(l, r)))
    WithCTE(body, Seq(cteDef))
  }

  private def assertAllRefsShareOneNonzeroId(
      reuses: Seq[CTEReuseRelation], refCount: Int): Unit = {
    assert(reuses.size == refCount,
      s"Expected $refCount CTEReuseRelation nodes (one per reference), got ${reuses.size}.")
    val ids = reuses.map(_.cteId).distinct
    assert(ids.size == 1 && ids.head != 0L,
      s"Expected all references to share one nonzero cteId, got $ids.")
    assertCTEReuseWithSameIdHaveSameCanonical(reuses)
  }

  test("forced CTE rooted at an existing repartition seals (stamped in place)") {
    val leaf = rel
    val body = RepartitionByExpression(Seq.empty, leaf, optNumPartitions = Some(3))
    val reuses = collectCTEReuseRelations(runReuseRules(forcedCteRefs(body, refCount = 2)))
    assertAllRefsShareOneNonzeroId(reuses, refCount = 2)
    // The repartition itself carries the plan-reuse id; no second shuffle sits above it.
    reuses.foreach { r =>
      val root = sharedRoot(r)
      assert(root.child == leaf && root.numPartitions == 3,
        s"Expected the repartition to be stamped in place:\n${r.sharedSubplan.treeString}")
    }
  }

  // The shared subplan's root: the single plan-reuse repartition of a sealed definition.
  private def sharedRoot(reuse: CTEReuseRelation): RepartitionByExpression =
    reuse.sharedSubplan match {
      case r: RepartitionByExpression => r
      case other => fail(s"Expected a RepartitionByExpression root, got:\n${other.treeString}")
    }

  test("forced CTE rooted at a rebalance seals (stamped in place)") {
    val leaf = rel
    val body = RebalancePartitions(Seq.empty, leaf)
    val reuses = collectCTEReuseRelations(runReuseRules(forcedCteRefs(body, refCount = 2)))
    assertAllRefsShareOneNonzeroId(reuses, refCount = 2)
    // The rebalance itself becomes the one plan-reuse shuffle; no wrapper sits above it.
    reuses.foreach { r =>
      val root = sharedRoot(r)
      assert(root.child == leaf && r.partitioning == body.partitioning,
        s"Expected the rebalance to be stamped in place:\n${r.sharedSubplan.treeString}")
    }
  }

  test("forced CTE rooted at a keyed rebalance seals on the rebalance keys") {
    val leaf = rel
    val body = RebalancePartitions(leaf.output, leaf)
    val reuses = collectCTEReuseRelations(runReuseRules(forcedCteRefs(body, refCount = 2)))
    assertAllRefsShareOneNonzeroId(reuses, refCount = 2)
    reuses.foreach { r =>
      assert(sharedRoot(r).child == leaf, r.sharedSubplan.treeString)
      r.partitioning match {
        case h: HashPartitioning => assert(h.expressions == leaf.output, h)
        case other => fail(s"Expected the rebalance's hash partitioning, got $other")
      }
    }
  }

  test("forced CTE rooted at REPARTITION(n) seals as one shuffle with n partitions") {
    val leaf = rel
    val body = Repartition(3, shuffle = true, leaf)
    val reuses = collectCTEReuseRelations(runReuseRules(forcedCteRefs(body, refCount = 2)))
    assertAllRefsShareOneNonzeroId(reuses, refCount = 2)
    reuses.foreach { r =>
      val root = sharedRoot(r)
      assert(root.child == leaf && root.numPartitions == 3 &&
        r.partitioning == RoundRobinPartitioning(3),
        s"Expected one round-robin shuffle with 3 partitions:\n${r.sharedSubplan.treeString}")
    }
  }

  test("forced CTE rooted at coalesce(n) seals and keeps n partitions") {
    val body = Repartition(2, shuffle = false, rel)
    val reuses = collectCTEReuseRelations(runReuseRules(forcedCteRefs(body, refCount = 2)))
    assertAllRefsShareOneNonzeroId(reuses, refCount = 2)
    reuses.foreach { r =>
      val root = sharedRoot(r)
      assert(root.child == body && root.numPartitions == 2,
        s"Expected the coalesce wrapped in a 2-partition shuffle:\n${r.sharedSubplan.treeString}")
    }
  }

  // ---------------------------------------------------------------------------
  // Only shared batch definitions are sealed
  // ---------------------------------------------------------------------------

  test("a forced CTE referenced once keeps the plain lowering") {
    Seq(
      "plain body" -> rel,
      "repartition root" -> Repartition(3, shuffle = true, rel),
      "rebalance root" -> RebalancePartitions(Seq.empty, rel)).foreach { case (name, body) =>
      val withFlag = runReuseRules(forcedCteRefs(body, refCount = 1))
      val withoutFlag = runReuseRules(forcedCteRefs(body, refCount = 1), enabled = false)
      assert(withFlag == withoutFlag,
        s"$name: expected the same plan as with the flag off:\n${withFlag.treeString}")
      assert(collectCTEReuseRelations(withFlag).isEmpty &&
        planReuseRepartitions(withFlag).isEmpty, s"$name:\n${withFlag.treeString}")
      assert(numRepartitionLike(withFlag) == 1,
        s"$name: expected a single shuffle for the reference:\n${withFlag.treeString}")
    }
  }

  test("a forced CTE in a streaming query is not sealed") {
    val streamingRel = LocalRelation(Seq(AttributeReference("a", IntegerType)()), Nil,
      isStreaming = true)
    val cteDef = CTERelationDef(streamingRel, forceSkipInline = true)
    val refs = (0 until 2).map { _ =>
      CTERelationRef(cteDef.id, _resolved = true, streamingRel.output, isStreaming = true)
    }
    val result = runReuseRules(WithCTE(Union(refs), Seq(cteDef)))
    assert(collectCTEReuseRelationsDeep(result).isEmpty && planReuseRepartitions(result).isEmpty,
      s"Expected a streaming forced CTE not to be sealed:\n${result.treeString}")
  }

  test("the CTE lowering rule does not ask a plan without a forced CTE whether it streams") {
    // `isStreaming` walks the plan, so a batch plan that holds no forced definition, with the
    // reuse flag on or off, must return before the rule asks for it.
    Seq(true, false).foreach { enabled =>
      Seq[(String, LogicalPlan => LogicalPlan)](
        "no CTE" -> (leaf => Project(leaf.output, leaf)),
        "an unforced CTE read twice" -> (leaf => withCteRefs(leaf, refCount = 2))
      ).foreach { case (name, build) =>
        val leaf = IsStreamingCountingRelation(Seq(AttributeReference("a", IntegerType)()))
        val plan = build(leaf)
        withSQLConf(cteReuseConfKey -> enabled.toString) {
          ReplaceCTERefWithRepartition(plan)
        }
        assert(leaf.isStreamingCalls == 0,
          s"$name, reuse flag $enabled: expected no isStreaming call, got " +
            s"${leaf.isStreamingCalls}")
      }
    }
  }

  test("forced CTEs with identical deterministic bodies share one plan-reuse id") {
    withCTEReuseEnabled {
      def cteIds(text: String): Set[Long] =
        collectCTEReuseRelations(sqlWithForcedCTEReuse(text).queryExecution.optimizedPlan)
          .map(_.cteId).toSet
      def query(bodyA: String, bodyB: String): String =
        s"""WITH a AS ($bodyA), b AS ($bodyB)
           |SELECT * FROM a UNION ALL SELECT * FROM a
           |UNION ALL SELECT * FROM b UNION ALL SELECT * FROM b
           |""".stripMargin
      val same = "SELECT id, id * 2 AS v FROM range(10)"
      assert(cteIds(query(same, same)).size == 1,
        "Expected identical deterministic bodies to share one cteId")
      val nondeterministic = "SELECT id, rand(42) AS v FROM range(10)"
      assert(cteIds(query(nondeterministic, nondeterministic)).size == 2,
        "Expected identical non-deterministic bodies to keep separate cteIds")
      assert(cteIds(query(same, "SELECT id, id * 3 AS v FROM range(10)")).size == 2,
        "Expected different bodies to keep separate cteIds")
    }
  }

  test("a CTE whose deterministic body equals a sealed forced body shares its plan-reuse id") {
    // `forced` is read twice and sealed. `twin`'s deterministic body is canonically equal, but
    // `twin` is not sealed on its own: it is a forced CTE read once or an ordinary MATERIALIZED
    // CTE. The plain lowering planned all three copies to equal shuffles that exchange reuse ran
    // once, so `twin` is sealed with `forced` under one plan-reuse id, whichever comes first.
    Seq(
      "plain body" -> (() => rel),
      "REPARTITION(n) root" -> (() => Repartition(3, shuffle = true, rel))).foreach {
      case (bodyName, body) =>
        Seq(
          "forced CTE read once" -> CTERelationDef(body(), forceSkipInline = true),
          "MATERIALIZED CTE" -> CTERelationDef(body(), materialized = Some(true))).foreach {
          case (twinName, twin) =>
            val forced = CTERelationDef(body(), forceSkipInline = true)
            Seq(Seq(forced, twin), Seq(twin, forced)).foreach { defs =>
              val order = if (defs.head eq forced) "defined second" else "defined first"
              withClue(s"$bodyName, $twinName $order: ") {
                val result = runReuseRules(
                  WithCTE(Union(Seq(refTo(forced), refTo(forced), refTo(twin))), defs))
                assertAllRefsShareOneNonzeroId(collectCTEReuseRelations(result), refCount = 3)
                assert(numRepartitionLike(result) == 0,
                  s"Expected no plain copy of the body:\n${result.treeString}")
              }
            }
        }
    }
  }

  test("only a CTE's own deterministic body shares a sealed body's plan-reuse id") {
    // A non-deterministic twin of a sealed body keeps its plain lowering, so it computes its own
    // values.
    def nondeterministic(): LogicalPlan = {
      val base = rel
      Project(base.output :+ Alias(Rand(Literal(42L)), "r")(), base)
    }
    val forced = CTERelationDef(nondeterministic(), forceSkipInline = true)
    val twin = CTERelationDef(nondeterministic(), forceSkipInline = true)
    val result = runReuseRules(
      WithCTE(Union(Seq(refTo(forced), refTo(forced), refTo(twin))), Seq(forced, twin)))
    assertAllRefsShareOneNonzeroId(collectCTEReuseRelations(result), refCount = 2)
    val plain = result.collect { case r: RepartitionByExpression => r }
    assert(plain.size == 1 && !plain.head.isForPlanReuse,
      s"Expected the non-deterministic twin's plain lowering:\n${result.treeString}")

    // A repartition outside any CTE that equals the sealed root keeps its plain plan.
    val deterministic = CTERelationDef(rel, forceSkipInline = true)
    val outside = RepartitionByExpression(Seq.empty, rel, None)
    val withOutside = runReuseRules(WithCTE(
      Union(Seq(refTo(deterministic), refTo(deterministic), outside)), Seq(deterministic)))
    assertAllRefsShareOneNonzeroId(collectCTEReuseRelations(withOutside), refCount = 2)
    assert(withOutside.collect { case r: RepartitionByExpression => r } == Seq(outside),
      s"Expected the repartition outside the CTE unchanged:\n${withOutside.treeString}")
  }

  test("a MATERIALIZED CTE whose body equals a sealed forced body is sealed with it") {
    withCTEReuseEnabled {
      val text =
        """WITH f AS (SELECT id, id * 2 AS v FROM range(10)),
          |  m AS MATERIALIZED (SELECT id, id * 2 AS v FROM range(10))
          |SELECT * FROM f UNION ALL SELECT * FROM f UNION ALL SELECT * FROM m
          |""".stripMargin
      val df = sqlWithForcedCTEs(text, names = Set("f"))
      assertAllRefsShareOneNonzeroId(
        collectCTEReuseRelations(df.queryExecution.optimizedPlan), refCount = 3)
      val expected = withSQLConf(cteReuseConfKey -> "false") {
        val unsealed = sqlWithForcedCTEs(text, names = Set("f"))
        assert(collectCTEReuseRelations(unsealed.queryExecution.optimizedPlan).isEmpty)
        unsealed.collect().toSeq
      }
      assert(expected.size == 30)
      checkAnswer(df, expected)
    }
  }

  test("a twin whose nested forced CTE is read only through it is sealed with its sealed twin") {
    // `g1` and `g2` are forced and have the same deterministic body. `t` is MATERIALIZED, read
    // twice and reads `g1`; `f` is forced, read twice and reads `g2`, which the query also reads
    // directly. `t`'s body equals `f`'s, so `t` is sealed with `f`. Sealing `t` leaves `g1`, read
    // only inside it, a single copy, but `g1` must stay sealed so that `t`'s root keeps equaling
    // `f`'s and every reference of both reads one shared body.
    val g1 = CTERelationDef(rel, forceSkipInline = true)
    val g2 = CTERelationDef(rel, forceSkipInline = true)
    def plusOne(g: CTERelationDef): LogicalPlan =
      Project(Seq(Alias(Add(g.output.head, Literal(1)), "x")()), refTo(g))
    val t = CTERelationDef(plusOne(g1), materialized = Some(true))
    val f = CTERelationDef(plusOne(g2), forceSkipInline = true)
    val plan = WithCTE(
      Union(Seq(refTo(t), refTo(t), refTo(f), refTo(f), refTo(g2))), Seq(g1, g2, t, f))
    val result = runReuseRules(plan)
    val tAndF = result match {
      case Union(children, _, _) => children.take(4)
      case other => fail(s"Expected a union:\n${other.treeString}")
    }
    val reuses = tAndF.collect { case r: CTEReuseRelation => r }
    withClue(s"Expected every reference of t and f to read one sealed body:\n$result\n") {
      assertAllRefsShareOneNonzeroId(reuses, refCount = 4)
    }
    assert(numRepartitionLike(result) == 0,
      s"Expected no plain copy of the t and f body:\n${result.treeString}")
    reuses.foreach { r =>
      val plain = r.sharedSubplan.collect {
        case p: RepartitionByExpression if !p.isForPlanReuse => p
      }
      assert(plain.isEmpty, s"Expected no plain repartition in the shared body:\n$r")
    }
  }

  test("a static forced CTE referenced twice in a streaming query is not sealed") {
    val base = rel
    val cteDef = CTERelationDef(base, forceSkipInline = true)
    val refs = (0 until 2).map { _ =>
      CTERelationRef(cteDef.id, _resolved = true, base.output, isStreaming = false)
    }
    val streamingRel = LocalRelation(base.output.map(_.newInstance()), Nil, isStreaming = true)
    val plan = WithCTE(Union(refs :+ streamingRel), Seq(cteDef))
    assert(plan.isStreaming)
    val result = runReuseRules(plan)
    assert(collectCTEReuseRelationsDeep(result).isEmpty && planReuseRepartitions(result).isEmpty,
      s"Expected no sealing in a streaming query:\n${result.treeString}")
    assert(result == runReuseRules(plan, enabled = false),
      s"Expected the same plan as with the flag off:\n${result.treeString}")
  }

  // A resolved reference that keeps the output of `cteDef`.
  private def refTo(cteDef: CTERelationDef): CTERelationRef =
    CTERelationRef(cteDef.id, _resolved = true, cteDef.output, isStreaming = false)

  test("a forced CTE read once inside a retained CTE read twice is sealed") {
    // `inner` is forced and referenced once, inside the body of `outer`, a MATERIALIZED CTE that
    // is not forced and is referenced twice. The lowering copies `outer`'s body, and the forced
    // body inside it, once per reference, so `inner` must be sealed to be materialized once.
    val base = rel
    val inner = CTERelationDef(
      Project(base.output :+ Alias(Rand(Literal(42L)), "r")(), base), forceSkipInline = true)
    val outer = CTERelationDef(
      Filter(Literal(true), refTo(inner)), materialized = Some(true))
    val plan = WithCTE(Union(Seq(refTo(outer), refTo(outer))), Seq(inner, outer))
    val result = runReuseRules(plan)
    assertAllRefsShareOneNonzeroId(collectCTEReuseRelations(result), refCount = 2)
    assert(collectCTEReuseRelations(result).forall(_.output == inner.output),
      s"Expected the forced body to be sealed:\n${result.treeString}")
  }

  test("a forced CTE with a single copy keeps the plain lowering wherever it is referenced") {
    // Referenced once in the main query and once inside a definition that nothing reads: the
    // lowered plan holds one copy, as with the flag off.
    val base = rel
    val forced = CTERelationDef(base, forceSkipInline = true)
    val unread = CTERelationDef(Filter(Literal(true), refTo(forced)), materialized = Some(true))
    val withUnread = WithCTE(Project(base.output, refTo(forced)), Seq(forced, unread))
    val unreadResult = runReuseRules(withUnread)
    assert(unreadResult == runReuseRules(withUnread, enabled = false),
      s"Expected the same plan as with the flag off:\n${unreadResult.treeString}")
    assert(collectCTEReuseRelationsDeep(unreadResult).isEmpty &&
      planReuseRepartitions(unreadResult).isEmpty, unreadResult.treeString)

    // Referenced once inside a sealed forced definition, which is materialized once: only the
    // outer definition is sealed, and the inner body keeps the plain lowering inside it.
    val inner = CTERelationDef(base, forceSkipInline = true)
    val outer = CTERelationDef(Filter(Literal(true), refTo(inner)), forceSkipInline = true)
    val nested = WithCTE(Union(Seq(refTo(outer), refTo(outer))), Seq(inner, outer))
    val nestedResult = runReuseRules(nested)
    val reuses = collectCTEReuseRelationsDeep(nestedResult)
    assertAllRefsShareOneNonzeroId(reuses, refCount = 2)
    assert(reuses.forall(_.output == outer.output), nestedResult.treeString)
    assert(planReuseRepartitions(nestedResult).map(_.repartitionId).toSet ==
      reuses.map(_.cteId).toSet, s"Expected only the outer id:\n${nestedResult.treeString}")
    reuses.foreach { r =>
      val innerLowering = r.sharedSubplan.collect {
        case f: Filter => f.child
      }
      assert(innerLowering == Seq(RepartitionByExpression(Seq.empty, base, None)),
        s"Expected the inner body's plain lowering:\n${r.sharedSubplan.treeString}")
    }
  }

  test("a forced CTE whose body only selects another forced CTE shares its plan-reuse id") {
    // `second`'s body is a reference that keeps `first`'s output, so it lowers to `first`'s
    // plan-reuse repartition, whose id `second` reuses. The body is non-deterministic, so only that
    // id-sharing path gives the four references one id.
    val base = rel
    val first = CTERelationDef(
      Project(base.output :+ Alias(Rand(Literal(42L)), "r")(), base), forceSkipInline = true)
    val second = CTERelationDef(refTo(first), forceSkipInline = true)
    val plan = WithCTE(
      Union(Seq(refTo(first), refTo(first), refTo(second), refTo(second))), Seq(first, second))
    val result = runReuseRules(plan)
    assertAllRefsShareOneNonzeroId(collectCTEReuseRelations(result), refCount = 4)
  }

  test("a forced CTE that only selects a retained CTE read elsewhere is sealed with it") {
    // `forced`'s body is a reference that keeps `retained`'s output, so it lowers to `retained`'s
    // own lowered root. When the plan also reads `retained` directly, `retained` is sealed
    // together with `forced`: every reference of both reads `retained`'s root, stamped in place,
    // so `retained`'s rows are shuffled once. A non-deterministic body shares the id too, because
    // both definitions read the very same body rather than equal copies of it.
    val base = rel
    Seq(
      "deterministic" -> Project(base.output :+ Alias(Literal(1), "r")(), base),
      "non-deterministic" -> Project(base.output :+ Alias(Rand(Literal(42L)), "r")(), base)
    ).foreach { case (kind, inner) =>
      Seq(
        "plain body" -> inner,
        "REPARTITION(n) root" -> Repartition(3, shuffle = true, inner),
        "rebalance root" -> RebalancePartitions(inner.output.take(1), inner)
      ).foreach { case (name, body) =>
        withClue(s"$kind $name: ") {
          val retained = CTERelationDef(body, materialized = Some(true))
          val forced = CTERelationDef(refTo(retained), forceSkipInline = true)
          def query(defs: Seq[CTERelationDef]): LogicalPlan =
            WithCTE(Union(Seq(refTo(forced), refTo(forced), refTo(retained))), defs)
          val result = runReuseRules(query(Seq(retained, forced)))
          val reuses = collectCTEReuseRelations(result)
          assertAllRefsShareOneNonzeroId(reuses, refCount = 3)
          reuses.foreach { r =>
            assert(sharedRoot(r).child == inner && r.partitioning == sealedRootPartitioning(body),
              s"expected the retained root stamped in place:\n${r.sharedSubplan.treeString}")
          }
          assert(numRepartitionLike(result) == 0,
            s"expected no plain copy of the retained body:\n${result.treeString}")

          // Read only through the forced CTE, the retained root has one copy and is stamped in
          // place.
          val onlyThrough = runReuseRules(
            WithCTE(Union(Seq(refTo(forced), refTo(forced))), Seq(retained, forced)))
          val onlyThroughReuses = collectCTEReuseRelations(onlyThrough)
          assertAllRefsShareOneNonzeroId(onlyThroughReuses, refCount = 2)
          onlyThroughReuses.foreach { r =>
            assert(sharedRoot(r).child == inner,
              s"expected the retained root stamped in place:\n${r.sharedSubplan.treeString}")
          }

          // With the flag off, `forced` is lowered as if it were not forced.
          assert(runReuseRules(query(Seq(retained, forced)), enabled = false) ==
            runReuseRules(
              query(Seq(retained, forced.copy(forceSkipInline = false))), enabled = false))
        }
      }
    }
  }

  // The partitioning a sealed forced definition rooted at `body` gets when stamped in place.
  private def sealedRootPartitioning(body: LogicalPlan): Partitioning = {
    val reuses = collectCTEReuseRelations(runReuseRules(forcedCteRefs(body, refCount = 2)))
    assert(reuses.nonEmpty)
    reuses.head.partitioning
  }

  test("a pinned forced CTE that only selects a retained CTE read elsewhere keeps its pin") {
    // `forced` is pinned and its body only selects `retained`, which the query also reads
    // directly. The pin takes precedence over sealing `retained` with `forced`: `forced` is sealed
    // as the pin's repartition over `retained`'s own lowering, and the direct reference of
    // `retained` keeps that lowering, a repartition with no plan-reuse id.
    val base = rel
    val retained = CTERelationDef(base, materialized = Some(true))
    val key = retained.output.head
    val forced = CTERelationDef(refTo(retained), forceSkipInline = true,
      forcePartitioning = Some(HashPartitioning(Seq(key), 3)))
    val result = runReuseRules(
      WithCTE(Union(Seq(refTo(forced), refTo(forced), refTo(retained))), Seq(retained, forced)))
    val children = result match {
      case Union(children, _, _) => children
      case other => fail(s"Expected a union:\n${other.treeString}")
    }
    val retainedLowering = RepartitionByExpression(Seq.empty, base, None)
    val reuses = children.take(2).collect { case r: CTEReuseRelation => r }
    assertAllRefsShareOneNonzeroId(reuses, refCount = 2)
    reuses.foreach { r =>
      assert(r.partitioning == HashPartitioning(Seq(key), 3),
        s"Expected the pinned partitioning, got ${r.partitioning}")
      val root = sharedRoot(r)
      assert(root.partitionExpressions == Seq(key) && root.numPartitions == 3 &&
        root.child == retainedLowering,
        s"Expected the pin's repartition over the retained lowering:\n${r.sharedSubplan}")
    }
    assert(children(2) == retainedLowering && !retainedLowering.isForPlanReuse,
      s"Expected the retained CTE's own lowering:\n${result.treeString}")
  }

  test("forced under-subquery CTE seals (wrapped)") {
    val reuses = collectCTEReuseRelations(
      runReuseRules(forcedCteRefs(rel, refCount = 2, underSubquery = true)))
    assertAllRefsShareOneNonzeroId(reuses, refCount = 2)
  }

  test("forced CTE under an existing repartition materializes once (one value set)") {
    withCTEReuseEnabled {
      // A nondeterministic body under a user REPARTITION. The UDF returns a fresh value on every
      // call, so if each reference materialized its own copy of the body, the two sides' values
      // would differ for every id; one shared materialization makes each id's value identical on
      // both sides of the self-join. rand() would not tell the two apart: its fixed seed and the
      // identical range partitions below the repartition reproduce the same values on every run.
      spark.udf.register("repartition_root_uuid",
        udf(() => java.util.UUID.randomUUID().toString).asNondeterministic())
      val df = sqlWithForcedCTEReuse(
        """WITH cte AS (SELECT /*+ REPARTITION(3, id) */ id, repartition_root_uuid() as r
          |  FROM range(20))
          |SELECT (c1.r = c2.r) AS same FROM cte c1 JOIN cte c2 ON c1.id = c2.id
          |""".stripMargin)
      assertAllRefsShareOneNonzeroId(
        collectCTEReuseRelations(df.queryExecution.optimizedPlan), refCount = 2)
      checkAnswer(df, (0 until 20).map(_ => Row(true)))
    }
  }

}

/** A batch leaf that counts the calls of its `isStreaming`. */
private case class IsStreamingCountingRelation(output: Seq[Attribute]) extends LeafNode {
  var isStreamingCalls = 0

  override def isStreaming: Boolean = {
    isStreamingCalls += 1
    false
  }
}
