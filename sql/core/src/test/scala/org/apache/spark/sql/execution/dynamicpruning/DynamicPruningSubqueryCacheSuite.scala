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

package org.apache.spark.sql.execution.dynamicpruning

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BroadcastValueProjection, DynamicPruningSubquery, Expression, GreaterThan, Literal, NamedExpression, Rand, ScalarSubquery, Unevaluable}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, Filter, HintInfo, Join, JoinHint, LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.{BaseSubqueryExec, FilterExec, InSubqueryExec, LocalTableScanExec, ProjectedBroadcastValueSubqueryExec, ReusedSubqueryExec, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.ReuseAdaptiveSubquery
import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, DataType, LongType}
import org.apache.spark.util.ThreadUtils

class DynamicPruningSubqueryCacheSuite extends QueryTest with SharedSparkSession {
  import DynamicPruningSubqueryCacheSuite._

  test("equivalent build queries reuse compilation and remap requested attributes") {
    withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
      val cache = new DynamicPruningSubqueryCache(spark)
      val first = LocalRelation(AttributeReference("k", LongType)())
      val second = LocalRelation(AttributeReference("alias", LongType)())
      val original = cache.compileAdaptiveQuery(first, first.output, Seq(0), true) {
        LocalTableScanExec(first.output, Nil, None)
      }
      val reused = cache.compileAdaptiveQuery(second, second.output, Seq(0), true) {
        fail("an equivalent DPP build query was compiled twice")
      }
      assert(reused eq original)
      assert(reused.bindKeys(second, second.output) == first.output)
    }
  }

  test("broadcast policy and ordered build keys distinguish initial AQE entries") {
    withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
      val cache = new DynamicPruningSubqueryCache(spark)
      val query = LocalRelation(
        AttributeReference("a", LongType)(), AttributeReference("b", LongType)())
      var builds = 0
      for ((keys, index, broadcastOnly) <- Seq(
          (query.output, 0, true),
          (query.output, 0, false),
          (query.output.reverse, 0, true),
          (query.output, 1, true))) {
        cache.compileAdaptiveQuery(query, keys, Seq(index), broadcastOnly) {
          builds += 1
          LocalTableScanExec(query.output, Nil, None)
        }
      }
      assert(builds == 4)
    }
  }

  test("disabled reuse and nondeterministic keys do not share execution plans") {
    val query = LocalRelation(AttributeReference("k", LongType)())
    for ((reuse, keys) <- Seq((false, query.output), (true, Seq(Rand(0))))) {
      withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> reuse.toString) {
        val cache = new DynamicPruningSubqueryCache(spark)
        var builds = 0
        for (_ <- 0 until 2) {
          cache.compileAdaptiveQuery(query, keys, Seq(0), true) {
            builds += 1
            LocalTableScanExec(query.output, Nil, None)
          }
        }
        assert(builds == 2)
      }
    }
  }

  test("independent AQE and non-AQE preparation have separate entries") {
    withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
      val cache = new DynamicPruningSubqueryCache(spark)
      val query = LocalRelation(AttributeReference("k", LongType)())
      var builds = 0
      for (adaptive <- Seq(false, true, false, true)) {
        cache.prepareIndependentQuery(query, adaptive) {
          builds += 1
          SubqueryExec("dpp", LocalTableScanExec(query.output, Nil, None))
        }
      }
      assert(builds == 2)
    }
  }

  test("a comparison entry transfers its executable input only once") {
    val query = LocalRelation(AttributeReference("k", LongType)())
    val plan = LocalTableScanExec(query.output, Nil, None)
    val entry = new DynamicPruningSubqueryCache.PlannedQuery(query, plan)
    assert(entry.takeInput().contains(plan))
    assert(entry.takeInput().isEmpty)
  }

  test("completed independent subqueries return traversable producers and stay query-local") {
    withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
      val query = LocalRelation(AttributeReference("k", LongType)())
      val cache = new DynamicPruningSubqueryCache(spark)
      var builds = 0
      def build(): SubqueryExec = {
        builds += 1
        SubqueryExec("dpp", LocalTableScanExec(query.output, Nil, None))
      }
      val original = cache.prepareIndependentQuery(query, adaptive = false) {
        build()
      }
      val reused = cache.prepareIndependentQuery(query, adaptive = false) {
        fail("a completed subquery was constructed twice")
      }
      assert(reused ne original)
      assert(reused.child eq original.child)
      assert(reused.collect { case _: LocalTableScanExec => 1 }.sum == 1)
      val separate = new DynamicPruningSubqueryCache(spark)
        .prepareIndependentQuery(query, adaptive = false) {
          build()
        }
      assert(builds == 2)
      assert(separate.child ne original.child)
    }
  }

  test("all logical planning caches preserve hints and reuse equivalent hinted requests") {
    withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
      val cache = new DynamicPruningSubqueryCache(spark)
      val builds = mutable.Map.empty[String, Int].withDefaultValue(0)
      val queries = Seq("MERGE", "BROADCAST", "MERGE", "BROADCAST").zipWithIndex.map {
        case (hint, i) =>
          spark.sql(s"""
            SELECT /*+ $hint(r$i) */ l.id AS k$i
            FROM range(10) l JOIN range(10) r$i ON l.id = r$i.id
            WHERE l.id < 5
          """).queryExecution.optimizedPlan
      }
      assert(queries.forall(_.sameResult(queries.head)))
      queries.foreach { query =>
        def input: LocalTableScanExec = LocalTableScanExec(query.output, Nil, None)
        cache.planQuery(query) {
          builds("plan") += 1
          input
        }
        cache.compileAdaptiveQuery(query, Seq(query.output.head), Seq(0), true) {
          builds("compile") += 1
          input
        }
        cache.prepareBroadcast(query, Seq(query.output.head), Seq(0), None) {
          builds("broadcast") += 1
          SubqueryExec("dpp", input)
        }
        for (adaptive <- Seq(false, true)) {
          cache.prepareIndependentQuery(query, adaptive) {
            builds(if (adaptive) "adaptive independent" else "independent") += 1
            SubqueryExec("dpp", input)
          }
        }
      }
      assert(builds.toMap == Map(
        "plan" -> 2,
        "compile" -> 2,
        "broadcast" -> 2,
        "independent" -> 2,
        "adaptive independent" -> 2))
    }
  }

  test("reuse rules handle distinct references and repeated references to the same producer") {
    withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
      for (adaptive <- Seq(false, true); sameReference <- Seq(false, true)) {
        val query = LocalRelation(AttributeReference("k", LongType)())
        val cache = new DynamicPruningSubqueryCache(spark)
        val original = cache.prepareIndependentQuery(query, adaptive) {
          SubqueryExec("dpp", LocalTableScanExec(query.output, Nil, None))
        }
        val reused = cache.prepareIndependentQuery(query, adaptive) {
          fail("the producer was constructed twice")
        }
        val second = if (sameReference) original else reused
        val plan = FilterExec(And(
          InSubqueryExec(Literal(1L), original, NamedExpression.newExprId),
          InSubqueryExec(Literal(2L), second, NamedExpression.newExprId)),
          LocalTableScanExec(query.output, Nil, None))
        val prepared = if (adaptive) {
          ReuseAdaptiveSubquery(TrieMap.empty).apply(plan)
        } else {
          ReuseExchangeAndSubquery.apply(plan)
        }
        val producers = prepared.collectWithSubqueries { case s: SubqueryExec => s }
        assert(producers.size == (if (sameReference) 2 else 1))
        assert(producers.forall(_ eq original))
        val wrappers = prepared.collectWithSubqueries { case r: ReusedSubqueryExec => r }
        assert(wrappers.size == (if (sameReference) 0 else 1))
        assert(wrappers.forall(_.child eq original))
      }
    }
  }

  test("hint signatures preserve join sides, tree positions and nested DPP hints") {
    val signatures = new DynamicPruningSubqueryCache.HintSignatureCache
    val relation = LocalRelation(AttributeReference("k", LongType)())
    val empty = Join(relation, relation, Inner, None, JoinHint.NONE)
    val hint = Some(HintInfo(Some(BROADCAST)))
    val left = empty.copy(hint = JoinHint(hint, None))
    val right = empty.copy(hint = JoinHint(None, hint))
    assert(signatures.signature(left) != signatures.signature(right))
    assert(signatures.signature(left) == signatures.signature(left.copy()))
    assert(signatures.signature(empty.copy(left = left)) !=
      signatures.signature(empty.copy(right = left)))
    def nested(query: LogicalPlan): LogicalPlan = Filter(
      DynamicPruningSubquery(relation.output.head, query, Seq(query.output.head), Seq(0), true)(),
      relation)
    assert(signatures.signature(nested(left)) != signatures.signature(nested(right)))
    assert(signatures.signature(nested(left)) == signatures.signature(nested(left.copy())))
  }

  test("nested DPP signatures preserve projected broadcast values") {
    val signatures = new DynamicPruningSubqueryCache.HintSignatureCache
    val probe = LocalRelation(AttributeReference("k", LongType)())
    val build = LocalRelation(AttributeReference("k", LongType)())
    val source = LocalRelation(
      AttributeReference("a", LongType)(), AttributeReference("b", LongType)())
    val equivalentSource = LocalRelation(
      AttributeReference("x", LongType)(), AttributeReference("y", LongType)())
    def nested(projection: Option[BroadcastValueProjection]): LogicalPlan = {
      val dpp = DynamicPruningSubquery(
        probe.output.head, build, build.output, Seq(0), onlyInBroadcast = true)(projection)
      Filter(dpp, probe)
    }
    val first = nested(Some(BroadcastValueProjection(source, Seq(source.output.head),
      source.output.head)))
    val equivalent = nested(Some(BroadcastValueProjection(equivalentSource,
      Seq(equivalentSource.output.head), equivalentSource.output.head)))
    val differentValue = nested(Some(BroadcastValueProjection(source, Seq(source.output.head),
      source.output(1))))
    val differentKeys = nested(Some(BroadcastValueProjection(source, Seq(source.output(1)),
      source.output.head)))
    val noProjection = nested(None)
    assert(first.sameResult(differentValue))
    assert(signatures.signature(first) == signatures.signature(equivalent))
    assert(signatures.signature(first) != signatures.signature(differentValue))
    assert(signatures.signature(first) != signatures.signature(differentKeys))
    assert(signatures.signature(first) != signatures.signature(noProjection))

    withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
      val cache = new DynamicPruningSubqueryCache(spark)
      val scan = LocalTableScanExec(probe.output, Nil, None)
      var builds = 0
      Seq(first, equivalent, differentValue, differentKeys, noProjection).foreach { query =>
        cache.prepareAdaptiveBroadcast(query, scan, scan.output, Seq(0), None) {
          builds += 1
          SubqueryExec("dpp", scan)
        }
      }
      assert(builds == 4)
    }
  }

  test("broadcast caches distinguish projected values and keep producers traversable") {
    withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
      for (adaptive <- Seq(false, true)) {
        val cache = new DynamicPruningSubqueryCache(spark)
        val query = LocalRelation(
          AttributeReference("a", LongType)(), AttributeReference("b", LongType)())
        val scan = LocalTableScanExec(query.output, Nil, None)
        var builds = 0
        def producer(value: Expression): ProjectedBroadcastValueSubqueryExec = {
          builds += 1
          ProjectedBroadcastValueSubqueryExec("dpp", value, scan)
        }
        def prepare(value: Expression)(build: => BaseSubqueryExec): BaseSubqueryExec = {
          if (adaptive) {
            cache.prepareAdaptiveBroadcast(query, scan, Seq(query.output.head), Seq(0),
              Some(value))(build)
          } else {
            cache.prepareBroadcast(query, Seq(query.output.head), Seq(0), Some(value))(build)
          }
        }
        val first = prepare(query.output.head) { producer(query.output.head) }
        val repeated = prepare(query.output.head) { fail("projected producer constructed twice") }
        val different = prepare(query.output(1)) { producer(query.output(1)) }
        assert(builds == 2)
        assert(first ne repeated)
        assert(first.child eq repeated.child)
        assert(different.asInstanceOf[ProjectedBroadcastValueSubqueryExec].valueExpression ==
          query.output(1))
      }
    }
  }

  test("adaptive projection compilation reuses equivalent inputs and distinguishes keys") {
    withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
      val cache = new DynamicPruningSubqueryCache(spark)
      val first = LocalRelation(
        AttributeReference("a", LongType)(), AttributeReference("b", LongType)())
      val second = LocalRelation(
        AttributeReference("x", LongType)(), AttributeReference("y", LongType)())
      var builds = 0
      def build(query: LogicalPlan): LocalTableScanExec = {
        builds += 1
        LocalTableScanExec(query.output, Nil, None)
      }
      val original = cache.compileAdaptiveProjection(
        first, Seq(first.output.head), first.output(1)) { build(first) }
      val reused = cache.compileAdaptiveProjection(
        second, Seq(second.output.head), second.output(1)) {
        fail("equivalent projection compiled twice")
      }
      assert(reused eq original)
      assert(reused.bindKeys(second, second.output) == first.output)
      cache.compileAdaptiveProjection(second, Seq(second.output.head), second.output.head) {
        build(second)
      }
      cache.compileAdaptiveProjection(second, Seq(second.output(1)), second.output(1)) {
        build(second)
      }
      assert(builds == 3)
    }
  }

  test("hint signature construction visits shared nested plans once and memoizes later lookups") {
    val signatures = new DynamicPruningSubqueryCache.HintSignatureCache
    val relation = LocalRelation(AttributeReference("k", LongType)())
    val hinted = Join(relation, relation, Inner, None,
      JoinHint(None, Some(HintInfo(Some(BROADCAST)))))
    var visits = 0
    val depth = 12
    val root = (0 until depth).foldLeft[LogicalPlan](hinted) { (child, _) =>
      val dpp = DynamicPruningSubquery(
        relation.output.head, child, Seq(child.output.head), Seq(0), true)()
      CountingPlan(child, And(dpp, dpp))(() => visits += 1)
    }
    val signature = signatures.signature(root)
    assert(signature ne root)
    val initialVisits = visits
    assert(initialVisits <= 16 * depth)
    assert(signatures.signature(root) eq signature)
    assert(visits == initialVisits)
    val copied = root.asInstanceOf[CountingPlan]
    val equivalent = CountingPlan(copied.child, copied.expression)(() => visits += 1)
    assert(signatures.signature(equivalent) == signature)
    assert(visits <= initialVisits + 16)

    val dpp = DynamicPruningSubquery(
      relation.output.head, hinted, Seq(hinted.output.head), Seq(0), true)()
    def repeatedExpression(visit: () => Unit): Expression = {
      (0 until depth).foldLeft[Expression](dpp) { (child, _) =>
        CountingExpression(child, child)(visit)
      }
    }
    // Spark's existing expression normalization can revisit shared expressions. Measure that
    // baseline separately: collecting hints must add only linear work and no work on later hits.
    var baselineVisits = 0
    val baseline = Filter(repeatedExpression(() => baselineVisits += 1), relation)
    val baselineSignature = baseline.canonicalized
    var expressionVisits = 0
    val filter = Filter(repeatedExpression(() => expressionVisits += 1), relation)
    val expressionSignature = signatures.signature(filter)
    val initialExpressionVisits = expressionVisits
    assert(expressionSignature != baselineSignature)
    assert(initialExpressionVisits <= baselineVisits + 16 * depth)
    assert(signatures.signature(filter) eq expressionSignature)
    assert(expressionVisits == initialExpressionVisits)
  }

  test("commutative predicates cannot move a hint to a different subquery in the cache key") {
    def query(size: Int, hinted: Boolean): LogicalPlan = {
      val hint = if (hinted) "/*+ MERGE(r) */" else ""
      spark.sql(s"SELECT $hint l.id FROM range($size) l " +
        s"JOIN range($size) r ON l.id = r.id").queryExecution.optimizedPlan
    }
    def predicate(size: Int, hinted: Boolean): Expression = {
      GreaterThan(ScalarSubquery(query(size, hinted)), Literal(0L))
    }
    val relation = LocalRelation(AttributeReference("k", LongType)())
    val first = Filter(And(predicate(10, true), predicate(20, false)), relation)
    val second = Filter(And(predicate(20, true), predicate(10, false)), relation)
    assert(first.sameResult(second))
    withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
      val cache = new DynamicPruningSubqueryCache(spark)
      var builds = 0
      for (query <- Seq(first, second)) {
        cache.planQuery(query) {
          builds += 1
          LocalTableScanExec(query.output, Nil, None)
        }
      }
      assert(builds == 2)
    }
  }

  test("failed and cyclic builds do not publish partial cache entries") {
    val cache = new DynamicPruningSubqueryCache.ConstructionCache[String, Int]
    intercept[IllegalStateException] {
      cache.getOrBuild("failed") { throw new IllegalStateException("expected") }
    }
    assert(cache.getOrBuild("failed") { 1 } == 1)
    intercept[IllegalArgumentException] {
      cache.getOrBuild("cycle") { cache.getOrBuild("cycle") { 0 } }
    }
    assert(cache.getOrBuild("cycle") { 2 } == 2)
    assert(cache.getOrBuild("failed") { fail("a completed entry was rebuilt") } == 1)
  }

  test("concurrent requests share one builder without holding the cache monitor") {
    val pool = Executors.newFixedThreadPool(3)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    val started = new CountDownLatch(1)
    val waiterStarted = new CountDownLatch(1)
    val waiterThread = new AtomicReference[Thread]
    val release = new CountDownLatch(1)
    val cache = new DynamicPruningSubqueryCache.ConstructionCache[String, Int]
    try {
      val first = Future {
        cache.getOrBuild("shared") {
          started.countDown()
          assert(release.await(30, TimeUnit.SECONDS))
          cache.getOrBuild("nested") { 1 }
        }
      }
      assert(started.await(10, TimeUnit.SECONDS))
      val second = Future {
        waiterThread.set(Thread.currentThread())
        waiterStarted.countDown()
        cache.getOrBuild("shared") { fail("the same input was constructed concurrently") }
      }
      val unrelated = Future { cache.getOrBuild("unrelated") { 2 } }
      assert(ThreadUtils.awaitResult(unrelated, 10.seconds) == 2)
      assert(waiterStarted.await(10, TimeUnit.SECONDS))
      eventually(timeout(10.seconds), interval(10.millis)) {
        val thread = waiterThread.get()
        assert(thread.getState == Thread.State.WAITING ||
          thread.getState == Thread.State.TIMED_WAITING)
        assert(thread.getStackTrace.exists(_.getMethodName == "getOrBuild"))
        assert(!second.isCompleted)
      }
      release.countDown()
      assert(ThreadUtils.awaitResult(first, 10.seconds) == 1)
      assert(ThreadUtils.awaitResult(second, 10.seconds) == 1)
    } finally {
      release.countDown()
      pool.shutdownNow()
    }
  }

}

private object DynamicPruningSubqueryCacheSuite {
  case class CountingPlan(child: LogicalPlan, expression: Expression)(visit: () => Unit)
      extends LogicalPlan {
    override def otherCopyArgs: Seq[AnyRef] = Seq(visit)
    override def children: Seq[LogicalPlan] = {
      visit()
      Seq(child)
    }
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
      copy(child = newChildren.head)(visit)
    }
  }

  case class CountingExpression(left: Expression, right: Expression)(visit: () => Unit)
      extends Expression with Unevaluable {
    override def otherCopyArgs: Seq[AnyRef] = Seq(visit)
    override def children: Seq[Expression] = {
      visit()
      Seq(left, right)
    }
    override def dataType: DataType = BooleanType
    override def nullable: Boolean = false
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): Expression = {
      copy(left = newChildren(0), right = newChildren(1))(visit)
    }
  }
}
