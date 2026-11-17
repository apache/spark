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
import org.apache.spark.sql.catalyst.plans.logical.{CollectMetrics, Join, JoinHint, LocalRelation, OneRowRelation}
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

  test("dependency staging skips deterministic argument prefixes") {
    val visits = new AtomicInteger
    val argument = (1 to 64).foldLeft[Expression](Literal(1D)) { (child, _) =>
      CanonicalizationCounted(child, visits)
    }
    val udf = ScalaUDF((a: Double, b: Double) => a + b, DoubleType,
      Seq(argument, Rand(0)))
    val keys = new NondeterministicExpressionCollection.ExpressionKeys
    val bindings = NondeterministicExpressionCollection.getNondeterministicToAttributes(
      Seq(udf), keys)
    visits.set(0)
    NondeterministicExpressionCollection.project(OneRowRelation(), bindings, keys)
    assert(visits.get == 0, s"deterministic prefixes canonicalized ${visits.get} times")
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

  test("scalar-free replacement keys visit expression nodes linearly") {
    Seq(32, 64).foreach { depth =>
      val visits = new AtomicInteger
      val nodes = (1 to depth).scanLeft[Expression](Literal(1L)) { (child, _) =>
        Counted(child, visits)
      }
      nodes.last.treePatternBits
      visits.set(0)
      nodes.foreach(NondeterministicExpressionCollection.getExpressionKey)
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
      PullOutNondeterministic(plan)
      assert(visits.get <= depth * 100, s"depth=$depth visits=${visits.get}")
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
