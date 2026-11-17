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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.KllSketchAggBigint
import org.apache.spark.sql.catalyst.plans.logical.{CollectMetrics, LocalRelation, OneRowRelation}
import org.apache.spark.sql.types.DataType

class NondeterministicExtractionCostSuite extends AnalysisTest {
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
    Seq(8, 12).foreach { depth =>
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
      assert(visits.get <= depth * 100, s"depth=$depth visits=${visits.get}")
    }
  }
}
