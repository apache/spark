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

package org.apache.spark.sql.catalyst.analysis

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{KllSketchAggBigint, Sum}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{CollectMetrics, Join, JoinHint, LateralJoin, LocalRelation, OneRowRelation}
import org.apache.spark.sql.types.{DataType, DoubleType}

class NondeterministicExtractionCostSuite extends AnalysisTest {
  private case class CanonicalizationCounted(child: Expression, visits: AtomicInteger)
      extends UnaryExpression with Unevaluable {
    override def dataType: DataType = child.dataType
    override def nullable: Boolean = child.nullable
    override lazy val canonicalized: Expression = {
      visits.incrementAndGet()
      this
    }
    override protected def withNewChildInternal(newChild: Expression): Expression =
      copy(child = newChild)
  }

  test("ASOF validation classifies distinct queries sharing one KLL plan once") {
    def work(width: Int): Int = {
      val visits = new AtomicInteger
      val value = (1 to 32).foldLeft[Expression](
        KllSketchAggBigint(Literal(1L)).toAggregateExpression()) { (child, _) =>
        Counted(child, visits)
      }
      val query = OneRowRelation().groupBy()(value.as("s"))
      val condition = Seq.fill(width)(Exists(query)).reduceLeft[Expression](And)
      val join = org.apache.spark.sql.catalyst.plans.logical.AsOfJoin(
        OneRowRelation(), OneRowRelation(), Literal.TrueLiteral, Some(condition), Inner,
        None, org.apache.spark.sql.catalyst.plans.Backward)
      join.treePatternBits
      query.deterministic
      visits.set(0)
      assert(ValidateSubqueryExpression.allowsNondeterministicJoinSubqueries(join))
      visits.get
    }
    val one = work(1)
    val many = work(32)
    assert(many <= one * 4, s"one plan=$one visits, repeated shared plan=$many")
  }

  test("join side selection reuses one wide operand reference membership") {
    def work(occurrences: Int): Int = {
      val hashes = new AtomicInteger
      val fields = (1 to 64).map { i =>
        val id = new ExprId(i.toLong, NamedExpression.newExprId.jvmId) {
          override def equals(other: Any): Boolean = super.equals(other)
          override def hashCode(): Int = {
            hashes.incrementAndGet()
            super.hashCode()
          }
        }
        AttributeReference(s"right_$i", DoubleType)(id)
      }
      val scalar = ScalarSubquery(OneRowRelation().select(Rand(0).as("n")))
      val operand = (fields ++ Seq.fill(occurrences)(scalar)).reduceLeft[Expression](Add(_, _))
      val target = $"target".double
      val plan = Join(LocalRelation(target), LocalRelation(fields), Inner,
        Some(EqualTo(target, operand)), JoinHint.NONE)
      plan.resolved
      plan.treePatternBits
      plan.foreach(_.outputSet)
      operand.foreach { e => e.references; e.deterministic; e.canonicalized }
      hashes.set(0)
      PullOutNondeterministic.applyLocally(plan)
      hashes.get
    }
    val one = work(1)
    val many = work(32)
    assert(many <= one * 4, s"one=$one membership hashes, repeated=$many")
  }

  test("retained-only filters skip KLL output classification") {
    val visits = new AtomicInteger
    val value = (1 to 32).foldLeft[Expression](
      KllSketchAggBigint(Literal(1L)).toAggregateExpression()) { (child, _) =>
      Counted(child, visits)
    }
    val query = OneRowRelation().groupBy()(value.as("s"))
    val fields = (1 to 16).map(i => ScalarSubquery(query).as(s"s$i"))
    val project = OneRowRelation().select(fields: _*)
    val filter = org.apache.spark.sql.catalyst.plans.logical.Filter(
      IsNotNull(project.output.head), project)
    filter.treePatternBits
    query.deterministic
    fields.foreach(_.deterministic)
    visits.set(0)
    assert(org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughNonJoin(filter) eq filter)
    assert(visits.get == 0, s"no-op field classification visits=${visits.get}")
  }

  test("filtered aggregate and lateral extraction bound changed-suffix comparisons") {
    for (depth <- Seq(32, 64); lateral <- Seq(false, true)) {
      val comparisons = new AtomicInteger
      val scalar = ScalarSubquery(OneRowRelation().groupBy()(
        KllSketchGetNBigint(KllSketchAggBigint(Literal(1L)).toAggregateExpression()).as("n")))
      val value = if (lateral) EqualTo(scalar, Literal(1L)) else {
        Sum(scalar).toAggregateExpression(isDistinct = false, filter = Some(Literal.TrueLiteral))
      }
      val wrapped = (1 to depth).foldLeft[Expression](value) { (child, _) =>
        EqualityCounted(child, comparisons)
      }
      val plan = if (lateral) LateralJoin(OneRowRelation(), LateralSubquery(OneRowRelation()),
        Inner, Some(wrapped)) else OneRowRelation().groupBy()(wrapped.as("n"))
      plan.resolved
      plan.treePatternBits
      wrapped.foreach { e => e.deterministic; e.canonicalized }
      comparisons.set(0)
      PullOutNondeterministic.applyLocally(plan)
      assert(comparisons.get <= depth * 8,
        s"lateral=$lateral depth=$depth comparisons=${comparisons.get}")
    }
  }

  test("dependency staging shares strict forwarded output prefixes") {
    val bean = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    assume(bean.isThreadAllocatedMemorySupported)
    bean.setThreadAllocatedMemoryEnabled(true)
    val threadId = Thread.currentThread().threadId()
    for (array <- Seq(false, true)) {
      def allocation(width: Int, depth: Int): Long = {
        val attributes = (1 to width).map(i => AttributeReference(s"a$i", DoubleType)())
        val output = if (array) scala.collection.immutable.ArraySeq.from(attributes)
          else attributes.toList
        val input = LocalRelation(output)
        input.treePatternBits
        val scalar = ScalarSubquery(OneRowRelation().select(Rand(0).as("n")))
        val expression = (1 to depth).foldLeft[Expression](scalar) { (child, _) =>
          ScalaUDF((n: java.lang.Double) => n, DoubleType, Seq(child))
        }
        val keys = new NondeterministicExpressionCollection.ExpressionKeys
        val bindings = NondeterministicExpressionCollection.getNondeterministicToAttributes(
          Seq(expression), keys)
        val before = bean.getThreadAllocatedBytes(threadId)
        val result = NondeterministicExpressionCollection.project(input, bindings, keys)
        val bytes = bean.getThreadAllocatedBytes(threadId) - before
        assert(result.output.take(output.size).map(_.exprId) == output.map(_.exprId))
        bytes
      }
      allocation(64, 4)
      // Subtract per-binding construction: the additional cost of forwarding a wide input
      // should not grow with the number of stages that forward it.
      def widthCost(depth: Int): Long = {
        Seq.fill(3)(allocation(4096, depth)).min - Seq.fill(3)(allocation(64, depth)).min
      }
      val small = widthCost(16)
      val large = widthCost(64)
      assert(small > 0 && large < small * 2,
        s"array=$array depth16=$small extra bytes, depth64=$large extra bytes")
    }
  }

  test("generated FILTER inputs do not erase an equal user conditional during legacy restoration") {
    val predicate = $"p".boolean
    val scalar = ScalarSubquery(OneRowRelation().groupBy()(
      KllSketchGetNBigint(KllSketchAggBigint(Literal(1L)).toAggregateExpression()).as("n")))
    val generated = KllFilteredAggregateInput(predicate, scalar)
    val written = If(predicate, scalar, Literal.create(null, scalar.dataType))
    val bindings = NondeterministicExpressionCollection.getNondeterministicToAttributes(
      Seq(generated, written))
    // Their evaluated values agree, but only the compiler-inserted guard can be removed when
    // restoring an aggregate input under legacy checkpoint semantics.
    assert(bindings.size == 2, s"compiler and user guards shared one binding: $bindings")
    val values = bindings.values().toArray.toSeq.map(_.asInstanceOf[Alias].child)
    assert(values.exists(_.isInstanceOf[KllFilteredAggregateInput]))
    assert(values.exists(_.isInstanceOf[If]))
  }

  test("equal join reference memberships reuse completed destination walks") {
    def work(width: Int): Int = {
      val visits = new AtomicInteger
      val value = $"value".double
      val target = $"target".double
      val input = (1 to 16).foldLeft[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan](
        LocalRelation(value)) { (left, i) =>
        val right = new LocalRelation(Seq($"side".double)) {
          override def outputSet: AttributeSet = {
            visits.incrementAndGet()
            super.outputSet
          }
        }
        Join(left, right, Inner, None, JoinHint.NONE)
      }
      val scalar = ScalarSubquery(OneRowRelation().select(Rand(0).as("n")))
      val condition = (1 to width).map { i =>
        EqualTo(Add(Add(value, scalar), Literal(i.toDouble)), target)
      }.reduceLeft[Expression](And)
      val plan = Join(input, LocalRelation(target), Inner, Some(condition), JoinHint.NONE)
      plan.resolved
      plan.treePatternBits
      plan.foreach(_.outputSet)
      condition.foreach { e => e.references; e.deterministic; e.canonicalized }
      visits.set(0)
      PullOutNondeterministic.applyLocally(plan)
      visits.get
    }
    val small = work(1)
    val large = work(32)
    assert(large <= small * 4, s"one operand=$small walks, equal memberships=$large")
  }

  test("KLL determinism probes bound comparisons through expressions and nested plans") {
    for (depth <- Seq(32, 64); nested <- Seq(false, true)) {
      val comparisons = new AtomicInteger
      val aggregate = KllSketchAggBigint(Literal(1L)).toAggregateExpression()
      val expression = (1 to depth).foldLeft[Expression](aggregate) { (child, _) =>
        EqualityCounted(child, comparisons)
      }
      val query = OneRowRelation().groupBy()(expression.as("s"))
      val tested = if (nested) ScalarSubquery(query) else expression
      tested.treePatternBits
      tested.deterministic
      comparisons.set(0)
      assert(ValidateSubqueryExpression.deterministicWithoutKllRandomness(tested))
      assert(!tested.deterministic)
      assert(comparisons.get <= depth * 8,
        s"nested=$nested depth=$depth comparisons=${comparisons.get}")
      comparisons.set(0)
      assert(ValidateSubqueryExpression.isIndependentKllScalar(ScalarSubquery(query)))
      assert(comparisons.get <= depth * 8,
        s"plan probe depth=$depth comparisons=${comparisons.get}")
    }
  }

  test("identity summaries skip query-free sibling expression arms") {
    val visits = new AtomicInteger
    val argument = (1 to 64).foldLeft[Expression](Literal(1D)) { (child, _) =>
      Counted(child, visits)
    }
    val scalar = ScalarSubquery(OneRowRelation().groupBy()(
      KllSketchGetNBigint(KllSketchAggBigint(Literal(1L)).toAggregateExpression()).as("n")))
    val udf = ScalaUDF((a: Double, n: Long) => a + n, DoubleType, Seq(argument, scalar))
    udf.treePatternBits
    udf.deterministic
    udf.canonicalized
    visits.set(0)
    val key = new NondeterministicExpressionCollection.ExpressionKeys()(udf)
    assert(key._2.ids == Vector(scalar.exprId))
    assert(visits.get == 0, s"ordinary summary arm visits=${visits.get}")
  }

  test("random projections skip subquery pushdown classification") {
    for (branching <- Seq(false, true)) {
      val visits = new AtomicInteger
      val random = (1 to 64).foldLeft[Expression](Rand(0)) { (child, _) =>
        Counted(if (branching) Add(child, Rand(1)) else child, visits)
      }
      val project = OneRowRelation().select(random.as("random"))
      val plan = org.apache.spark.sql.catalyst.plans.logical.Filter(Literal.TrueLiteral, project)
      plan.treePatternBits
      plan.deterministic
      visits.set(0)
      val result = org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughNonJoin(plan)
      assert(result eq plan)
      assert(visits.get == 0, s"branching=$branching classification visits=${visits.get}")
    }
  }

  test("dependency staging skips deterministic argument prefixes") {
    val visits = new AtomicInteger
    val argument = (1 to 64).foldLeft[Expression](Literal(1D)) { (child, _) =>
      CanonicalizationCounted(child, visits)
    }
    val scalar = ScalarSubquery(OneRowRelation().groupBy()(
      KllSketchGetNBigint(KllSketchAggBigint(Literal(1L)).toAggregateExpression()).as("n")))
    val udf = ScalaUDF((a: Double, b: Double, n: Long) => a + b + n, DoubleType,
      Seq(argument, Rand(0), scalar))
    val keys = new NondeterministicExpressionCollection.ExpressionKeys
    val bindings = NondeterministicExpressionCollection.getNondeterministicToAttributes(
      Seq(udf), keys)
    visits.set(0)
    val projected = NondeterministicExpressionCollection.project(OneRowRelation(), bindings, keys)
    assert(visits.get == 0, s"deterministic prefixes canonicalized ${visits.get} times")
    assert(projected.children.head.isInstanceOf[
      org.apache.spark.sql.catalyst.plans.logical.Project])
  }

  test("dependency staging skips nonbinding nondeterministic arithmetic prefixes") {
    val visits = new AtomicInteger
    val argument = (1 to 64).foldLeft[Expression](Rand(0)) { (child, i) =>
      CanonicalizationCounted(Add(child, Literal(i.toDouble)), visits)
    }
    val scalar = ScalarSubquery(OneRowRelation().groupBy()(
      KllSketchGetNBigint(KllSketchAggBigint(Literal(1L)).toAggregateExpression()).as("n")))
    val udf = ScalaUDF((value: Double, n: Long) => value + n, DoubleType, Seq(argument, scalar))
    val keys = new NondeterministicExpressionCollection.ExpressionKeys
    val bindings = NondeterministicExpressionCollection.getNondeterministicToAttributes(
      Seq(udf), keys)
    visits.set(0)
    val projected = NondeterministicExpressionCollection.project(OneRowRelation(), bindings, keys)
    assert(visits.get == 0, s"nonbinding prefixes canonicalized ${visits.get} times")
    assert(projected.children.head.isInstanceOf[
      org.apache.spark.sql.catalyst.plans.logical.Project])
  }

  test("aggregate scalar extraction skips unrelated deterministic argument keys") {
    val visits = new AtomicInteger
    val value = $"value".double
    val argument = (1 to 64).foldLeft[Expression](value) { (child, _) =>
      CanonicalizationCounted(Add(child, value), visits)
    }
    val aggregate = KllSketchAggBigint(Literal(1L)).toAggregateExpression()
    val scalar = ScalarSubquery(OneRowRelation().groupBy()(
      KllSketchGetNBigint(aggregate).as("n")))
    val plan = LocalRelation(value).groupBy()(
      Sum(scalar).toAggregateExpression().as("kll"),
      Sum(argument).toAggregateExpression().as("ordinary"))
    PullOutNondeterministic(plan)
    assert(visits.get == 0, s"deterministic sibling prefixes canonicalized ${visits.get} times")
  }

  private case class EqualityCounted(child: Expression, comparisons: AtomicInteger)
      extends Expression with Unevaluable {
    override def children: Seq[Expression] = Seq(child)
    override def dataType: DataType = child.dataType
    override def nullable: Boolean = true
    override def equals(other: Any): Boolean = {
      comparisons.incrementAndGet()
      other match {
        case that: EqualityCounted => (comparisons eq that.comparisons) && child == that.child
        case _ => false
      }
    }
    override def hashCode(): Int = 31 * child.hashCode() + comparisons.hashCode()
    override protected def withNewChildrenInternal(children: IndexedSeq[Expression]): Expression =
      copy(child = children.head)
  }

  test("validation rewrites bound equality comparisons on changed wrapper suffixes") {
    for (depth <- Seq(32, 64); mode <- Seq("metric", "extraction", "predicate")) {
      val comparisons = new AtomicInteger
      val value = $"value".long
      val query = OneRowRelation().select(Rand(0).as("n"))
      val base: Expression = mode match {
        case "metric" => GreaterThan(Sum(value).toAggregateExpression(), Literal(0L))
        case "extraction" => GreaterThan(ScalarSubquery(query), Literal(0D))
        case _ => Exists(query)
      }
      val expression = (1 to depth).foldLeft(base) { (child, _) =>
        EqualityCounted(child, comparisons)
      }
      expression.treePatternBits
      expression.deterministic
      comparisons.set(0)
      mode match {
        case "metric" => getAnalyzer.checkAnalysis(CollectMetrics("cost",
          Seq(expression.as("n")), LocalRelation(value), 0L))
        case "extraction" => assert(ExprUtils.isDeterministicAfterSubqueryExtraction(expression))
        case _ => assert(ExprUtils.isDeterministicAfterPredicateSubqueryRewrite(expression))
      }
      assert(comparisons.get <= depth * 8,
        s"mode=$mode depth=$depth comparisons=${comparisons.get}")
    }
  }

  test("wide join extraction bounds conjunct and reference-union allocation") {
    val bean = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    assume(bean.isThreadAllocatedMemorySupported)
    bean.setThreadAllocatedMemoryEnabled(true)
    val threadId = Thread.currentThread().threadId()
    for (uniqueReferences <- Seq(false, true)) {
      def allocation(width: Int): Long = {
        val attributes = (1 to width).map(i => AttributeReference(s"a$i", DoubleType)())
        val target = AttributeReference("target", DoubleType)()
        val scalar = ScalarSubquery(OneRowRelation().select(Rand(0).as("n")))
        val condition = attributes.map { attribute =>
          EqualTo(Add(if (uniqueReferences) attribute else attributes.head, scalar), target)
        }.reduceLeft[Expression](And)
        val plan = Join(LocalRelation(attributes), LocalRelation(target), Inner,
          Some(condition), JoinHint.NONE)
        plan.treePatternBits
        condition.foreach { expression =>
          expression.references
          expression.deterministic
          expression.canonicalized
        }
        val before = bean.getThreadAllocatedBytes(threadId)
        PullOutNondeterministic(plan)
        bean.getThreadAllocatedBytes(threadId) - before
      }
      allocation(32)
      val small = Seq.fill(3)(allocation(256)).min
      val large = Seq.fill(3)(allocation(1024)).min
      assert(large < small * 8,
        s"uniqueReferences=$uniqueReferences 256=$small bytes,1024=$large bytes")
    }
  }

  private case class Counted(child: Expression, visits: AtomicInteger)
      extends Expression with Unevaluable {
    override def children: Seq[Expression] = {
      visits.incrementAndGet()
      Seq(child)
    }
    override def dataType: DataType = child.dataType
    override def nullable: Boolean = false
    override lazy val deterministic: Boolean = child.deterministic
    override lazy val canonicalized: Expression = this
    override protected def withNewChildrenInternal(children: IndexedSeq[Expression]): Expression =
      copy(child = children.head)
  }

  test("subquery-free aggregates and joins skip extraction searches") {
    for (join <- Seq(false, true)) {
      val visits = new AtomicInteger
      val argument = (1 to 64).foldLeft[Expression](Literal(1D)) { (child, _) =>
        Counted(child, visits)
      }
      val input = OneRowRelation()
      val plan = if (join) Join(input, input, Inner,
        Some(EqualTo(argument, Literal(1D))), JoinHint.NONE)
      else input.groupBy()(Sum(argument).toAggregateExpression().as("s"))
      plan.resolved
      plan.treePatternBits
      plan.expressions.foreach(_.deterministic)
      visits.set(0)
      val result = PullOutNondeterministic.applyLocally.applyOrElse(plan,
        identity[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan])
      assert(result eq plan)
      assert(visits.get == 0, s"join=$join unrelated expressions visited ${visits.get} times")
    }
  }

  private case class NullabilityCounted(child: Expression, visits: AtomicInteger)
      extends UnaryExpression with Unevaluable {
    override def dataType: DataType = child.dataType
    override def nullable: Boolean = {
      visits.incrementAndGet()
      child.nullable
    }
    override lazy val canonicalized: Expression = this
    override protected def withNewChildInternal(newChild: Expression): Expression =
      copy(child = newChild)
  }

  test("KLL arithmetic guards avoid repeated nonnullable prefix scans") {
    val visits = new AtomicInteger
    val scalar = ScalarSubquery(OneRowRelation().groupBy()(
      KllSketchGetNBigint(KllSketchAggBigint(Literal(1L)).toAggregateExpression()).as("n")))
    val expression = (1 to 64).foldLeft[Expression](Coalesce(Seq(scalar, Literal(0L)))) {
      (child, _) => Subtract(NullabilityCounted(child, visits), Literal(1L))
    }
    expression.treePatternBits
    expression.deterministic
    visits.set(0)
    NondeterministicExpressionCollection.getNondeterministicToAttributes(Seq(expression))
    assert(visits.get <= 128, s"guard nullability visits=${visits.get}")
  }

  test("dependency Projects do not reconvert already forwarded output attributes") {
    val visits = new AtomicInteger
    val value = new AttributeReference("value", org.apache.spark.sql.types.LongType)() {
      override def toAttribute: Attribute = {
        visits.incrementAndGet()
        this
      }
    }
    val scalar = ScalarSubquery(OneRowRelation().groupBy()(
      KllSketchGetNBigint(KllSketchAggBigint(Literal(1L)).toAggregateExpression()).as("n")))
    val expression = (1 to 32).foldLeft[Expression](scalar) { (child, _) =>
      ScalaUDF((n: java.lang.Long) => n, org.apache.spark.sql.types.LongType, Seq(child))
    }
    val keys = new NondeterministicExpressionCollection.ExpressionKeys
    val bindings = NondeterministicExpressionCollection.getNondeterministicToAttributes(
      Seq(expression), keys)
    visits.set(0)
    NondeterministicExpressionCollection.project(LocalRelation(value), bindings, keys)
    assert(visits.get <= 1, s"forwarded attribute conversions=${visits.get}")
  }

  test("subquery-free arithmetic guard checks avoid prefix nullability scans") {
    val visits = new AtomicInteger
    val expression = (1 to 64).foldLeft[Expression](Rand(0)) { (child, _) =>
      Subtract(NullabilityCounted(child, visits), Literal(1D))
    }
    expression.treePatternBits
    expression.deterministic
    visits.set(0)
    NondeterministicExpressionCollection.getNondeterministicToAttributes(Seq(expression))
    assert(visits.get <= 128, s"guard nullability visits=${visits.get}")
  }

  test("join scalar placement classifies forwarding Projects once") {
    def work(occurrences: Int): Int = {
      val visits = new AtomicInteger
      val fields = (1 to 128).map(i => AttributeReference(s"a$i", DoubleType)())
      val counted = new scala.collection.immutable.AbstractSeq[NamedExpression] {
        override def length: Int = fields.length
        override def iterator: Iterator[NamedExpression] = fields.iterator.map { field =>
          visits.incrementAndGet()
          field
        }
        override def apply(index: Int): NamedExpression = {
          visits.incrementAndGet()
          fields(index)
        }
      }
      val input = org.apache.spark.sql.catalyst.plans.logical.Project(
        counted, LocalRelation(fields))
      val target = $"target".double
      val scalar = ScalarSubquery(OneRowRelation().select(Rand(0).as("n")))
      val condition = Seq.fill(occurrences)(EqualTo(Add(fields.head, scalar), target))
        .reduceLeft[Expression](And)
      val plan = Join(input, LocalRelation(target), Inner, Some(condition), JoinHint.NONE)
      plan.resolved
      plan.treePatternBits
      input.outputSet
      condition.foreach { e => e.references; e.deterministic; e.canonicalized }
      visits.set(0)
      PullOutNondeterministic(plan)
      visits.get
    }
    val small = work(1)
    val large = work(32)
    assert(large <= small * 4, s"one=$small projected fields, repeated=$large")
  }

  test("lateral extraction classifies a repeated scalar plan once") {
    def work(width: Int): Int = {
      val visits = new AtomicInteger
      val expression = (1 to 32).foldLeft[Expression](
        KllSketchAggBigint(Literal(1L)).toAggregateExpression()) { (child, _) =>
        Counted(child, visits)
      }
      val scalar = ScalarSubquery(OneRowRelation().groupBy()(expression.as("s")))
      val value = $"value".binary
      val condition = Seq.fill(width)(EqualTo(value, scalar)).reduceLeft[Expression](And)
      val plan = LateralJoin(LocalRelation(value), LateralSubquery(OneRowRelation()),
        Inner, Some(condition))
      plan.resolved
      scalar.plan.treePatternBits
      scalar.canonicalized
      visits.set(0)
      PullOutNondeterministic(plan)
      visits.get
    }
    val small = work(1)
    val large = work(32)
    assert(large <= small * 4, s"one=$small visits, repeated=$large visits")
  }

  test("legacy restoration does not revisit already expanded binding values") {
    val visits = new AtomicInteger
    val value = (1 to 64).foldLeft[Expression](Literal(1L)) { (child, _) =>
      Counted(child, visits)
    }
    val attribute = $"binding".long
    val references = CreateArray(Seq.fill(64)(attribute))
    value.treePatternBits
    visits.set(0)
    val restored = NondeterministicExpressionCollection.restoreBindingReferences(
      references, Map(attribute.exprId -> value))
    assert(restored.children.size == 64)
    assert(restored.children.forall(_ eq value))
    assert(visits.get == 0, s"expanded binding values revisited ${visits.get} times")
    assert(NondeterministicExpressionCollection.restoreBindingReferences(
      references, Map.empty) eq references)
  }

  test("scalar-free replacement keys visit expression nodes linearly") {
    Seq(32, 64).foreach { depth =>
      val visits = new AtomicInteger
      val nodes = (1 to depth).scanLeft[Expression](Literal(1L)) { (child, _) =>
        Counted(child, visits)
      }
      nodes.last.treePatternBits
      visits.set(0)
      val keys = new NondeterministicExpressionCollection.ExpressionKeys
      nodes.foreach(keys.apply)
      assert(visits.get <= depth * 4, s"depth=$depth visits=${visits.get}")
    }
  }

  test("observed aggregate wrapper validation visits expression nodes linearly") {
    Seq(32, 64).foreach { depth =>
      val visits = new AtomicInteger
      val value = $"value".long
      val aggregate = KllSketchAggBigint(value).toAggregateExpression()
      val metric = (1 to depth).foldLeft[Expression](KllSketchGetNBigint(aggregate)) {
        (child, _) => Counted(child, visits)
      }
      val plan = CollectMetrics("metric", Seq(metric.as("n")), LocalRelation(value), 0L)
      plan.treePatternBits
      visits.set(0)
      getAnalyzer.checkAnalysis(plan)
      assert(visits.get <= depth * 20, s"depth=$depth visits=${visits.get}")
    }
  }

  test("subquery-containing extraction keys do not rescan every wrapper suffix") {
    Seq(64, 128).foreach { depth =>
      val visits = new AtomicInteger
      val scalar = ScalarSubquery(OneRowRelation().select(Rand(0).as("value")))
      val expression = (1 to depth).foldLeft[Expression](scalar) { (child, _) =>
        Counted(child, visits)
      }
      val value = $"value".long
      val plan = LocalRelation(value).groupBy(expression)(count(Literal(1)).as("n"))
      plan.treePatternBits
      visits.set(0)
      PullOutNondeterministic(plan)
      assert(visits.get <= depth * 30, s"depth=$depth visits=${visits.get}")
    }
  }

  test("nested IN operand validation does not repeat completed checks exponentially") {
    Seq(32, 64, 128).foreach { depth =>
      val visits = new AtomicInteger
      val query = ListQuery(OneRowRelation().select((Rand(0) > Literal(0.5)).as("b")),
        numCols = 1)
      val expression = (1 to depth).foldLeft[Expression](Rand(0) > Literal(0.5)) {
        (child, _) => Counted(InSubquery(Seq(child), query), visits)
      }
      expression.deterministic
      expression.treePatternBits
      visits.set(0)
      assert(!ExprUtils.isDeterministicAfterSubqueryExtraction(expression))
      assert(visits.get <= depth * 20, s"depth=$depth visits=${visits.get}")
    }
  }

  test("subquery identity summaries share storage across growing prefixes") {
    val bean = ManagementFactory.getThreadMXBean
      .asInstanceOf[com.sun.management.ThreadMXBean]
    if (!bean.isThreadAllocatedMemorySupported) cancel("Allocation counters unavailable")
    bean.setThreadAllocatedMemoryEnabled(true)
    val threadId = Thread.currentThread().threadId()
    def allocation(width: Int): Long = {
      val query = OneRowRelation().select(Rand(0).as("n"))
      val nodes = (1 to width).scanLeft[Expression](Literal(0D)) { (child, _) =>
        Subtract(child, ScalarSubquery(query))
      }
      nodes.last.treePatternBits
      nodes.foreach(_.canonicalized)
      nodes.foreach(_.deterministic)
      val before = bean.getThreadAllocatedBytes(threadId)
      val keys = new NondeterministicExpressionCollection.ExpressionKeys
      nodes.foreach(keys.apply)
      val bytes = bean.getThreadAllocatedBytes(threadId) - before
      assert(keys(nodes.last)._2.size == width)
      bytes
    }
    allocation(16)
    val small = Seq.fill(3)(allocation(128)).min
    val large = Seq.fill(3)(allocation(512)).min
    assert(large < small * 8, s"128=$small bytes,512=$large bytes")
  }

  test("growing identity keys bound hashing work during map consumption") {
    Seq(64, 128, 256).foreach { width =>
      val hashes = new AtomicInteger
      val query = OneRowRelation().select(Rand(0).as("n"))
      val nodes = (1 to width).scanLeft[Expression](Literal(0D)) { (child, i) =>
        val id = new ExprId(i.toLong, NamedExpression.newExprId.jvmId) {
          override def equals(other: Any): Boolean = super.equals(other)
          override def hashCode(): Int = {
            hashes.incrementAndGet()
            super.hashCode()
          }
        }
        Subtract(child, ScalarSubquery(query, exprId = id))
      }
      nodes.last.treePatternBits
      nodes.foreach(_.canonicalized)
      nodes.foreach(_.deterministic)
      hashes.set(0)
      val keys = new NondeterministicExpressionCollection.ExpressionKeys
      val map = new java.util.HashMap[Any, Expression]
      nodes.foreach(node => map.put(keys(node), node))
      nodes.foreach(node => assert(map.get(keys(node)) eq node))
      assert(hashes.get <= width * 8, s"width=$width hashes=${hashes.get}")
    }
  }

  test("accepted nested IN operands share validity work within aggregate extraction") {
    Seq(32, 64, 128).foreach { depth =>
      val visits = new AtomicInteger
      val value = $"value".long
      val aggregate = KllSketchAggBigint(Literal(1L)).toAggregateExpression()
      val query = ListQuery(OneRowRelation().groupBy()(
        (KllSketchGetNBigint(aggregate) > Literal(0L)).as("b")), numCols = 1)
      val expression = (1 to depth).foldLeft[Expression](Literal.TrueLiteral) { (child, i) =>
        val in = InSubquery(Seq(child), query.copy(exprId = NamedExpression.newExprId))
        Counted(if (i % 2 == 0) Not(in) else in, visits)
      }
      val plan = LocalRelation(value).groupBy()(count(expression).as("n"))
      plan.treePatternBits
      expression.deterministic
      expression.canonicalized
      visits.set(0)
      val transformed = PullOutNondeterministic(plan)
      assert(visits.get <= depth * 100, s"depth=$depth visits=${visits.get}")
      val expanded = scala.collection.mutable.Map.empty[ExprId, Expression]
      val projects = transformed.collect {
        case p: org.apache.spark.sql.catalyst.plans.logical.Project => p
      }
      assert(projects.nonEmpty)
      projects.reverse.foreach { project =>
        assert(project.projectList.forall(_.references.subsetOf(project.child.outputSet)))
        project.projectList.foreach {
          case alias: Alias if alias.isNondeterministicSubqueryBinding =>
            expanded(alias.exprId) = NondeterministicExpressionCollection.restoreBindingReferences(
              alias.child, expanded)
          case _ =>
        }
      }
      assert(expanded.size >= depth)
      val rewritten = transformed.asInstanceOf[
        org.apache.spark.sql.catalyst.plans.logical.Aggregate]
      val restored = rewritten.mapExpressions { expression =>
        NondeterministicExpressionCollection.restoreBindingReferences(expression, expanded)
      }.copy(child = plan.children.head)
      comparePlans(restored, plan)
    }
  }

  test("identity key equality separates ordered identities despite hash collisions") {
    val first = ExprId(0L)
    val second = ExprId(4294967297L)
    assert(first.hashCode() == second.hashCode())
    val query = OneRowRelation().select(Rand(0).as("n"))
    val a = ScalarSubquery(query, exprId = first)
    val b = ScalarSubquery(query, exprId = second)
    val keys = new NondeterministicExpressionCollection.ExpressionKeys
    val forward = keys(Subtract(a, b))
    val backward = keys(Subtract(b, a))
    assert(forward.hashCode() == backward.hashCode())
    assert(forward != backward)
    val map = Map(forward -> 1, backward -> 2)
    assert(map(keys(Subtract(a.copy(), b.copy()))) == 1)
    assert(map(keys(Subtract(b.copy(), a.copy()))) == 2)
    assert(keys(Literal(1))._2.size == 0)
  }
}
