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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.IntegerType

class MemoProbeSuite extends SparkFunSuite {

  // A stand-in for a stateful generator: each evaluation returns the next integer, so a second
  // evaluation within one row is directly observable.
  case class Counter() extends LeafExpression with Nondeterministic {
    @transient private var n = 0
    override def stateful: Boolean = true
    override def dataType = IntegerType
    override def nullable: Boolean = false
    override protected def initializeInternal(partitionIndex: Int): Unit = {}
    override protected def evalInternal(input: InternalRow): Any = { n += 1; n }
    override protected def doGenCode(ctx: codegen.CodegenContext, ev: codegen.ExprCode) =
      throw new UnsupportedOperationException
  }

  test("probe: a With memoizes its definition per evaluation") {
    val counter = Counter()
    counter.initialize(0)
    // `ref + ref` -- the shape `BETWEEN` produces. Memoized, both references read one value, so the
    // sum is 2n on the nth evaluation; inlined it would be n + (n+1).
    val w = With(counter) { case Seq(ref) => Add(ref, ref) }
    val got = (1 to 4).map(_ => w.eval(InternalRow.empty))
    // scalastyle:off println
    println(s"PROBE memoized ref+ref over 4 rows = $got  (expect 2,4,6,8)")
    // scalastyle:on println
    assert(got == Seq(2, 4, 6, 8))
  }

  test("probe: a definition is not evaluated when no reference is reached") {
    val counter = Counter()
    counter.initialize(0)
    // The `With` is the false branch, so evaluating the `If` on a true predicate must not advance
    // the counter -- this is the property a guard could only approximate.
    val w = With(counter) { case Seq(ref) => Add(ref, ref) }
    val cond = If(Literal.TrueLiteral, Literal(-1), w)
    val skipped = (1 to 3).map(_ => cond.eval(InternalRow.empty))
    val thenReached = With(counter) { case Seq(ref) => Add(ref, ref) }.eval(InternalRow.empty)
    // scalastyle:off println
    println(s"PROBE branch not taken = $skipped, first value afterwards = $thenReached (expect 2)")
    // scalastyle:on println
    assert(skipped == Seq(-1, -1, -1))
    assert(thenReached == 2, "the counter advanced on rows that did not reach the branch")
  }

  test("probe: short-circuit -- a reference behind AND is not read when the left side is false") {
    val counter = Counter()
    counter.initialize(0)
    // `false AND (ref >= 1 AND ref <= 100)`: `And` short-circuits, so the reference is never read
    // and the counter must not advance. Neither a guard nor pre-evaluation can express this.
    val w = With(counter) { case Seq(ref) =>
      And(GreaterThanOrEqual(ref, Literal(1)), LessThanOrEqual(ref, Literal(100)))
    }
    val shortCircuited = And(Literal.FalseLiteral, w)
    val got = (1 to 3).map(_ => shortCircuited.eval(InternalRow.empty))
    val afterwards = With(counter) { case Seq(ref) => Add(ref, ref) }.eval(InternalRow.empty)
    // scalastyle:off println
    println(s"PROBE short-circuited = $got, first value afterwards = $afterwards (expect 2)")
    // scalastyle:on println
    assert(afterwards == 2, "the counter advanced behind a short-circuited AND")
  }

  test("probe: nested With") {
    val outer = Counter()
    val inner = Counter()
    outer.initialize(0)
    inner.initialize(0)
    // outer ref used twice, and one use wraps an inner `With` also using its ref twice.
    val w = With(outer) { case Seq(o) =>
      Add(o, With(inner) { case Seq(i) => Add(Add(i, i), o) })
    }
    val got = (1 to 3).map(_ => w.eval(InternalRow.empty))
    // scalastyle:off println
    println(s"PROBE nested = $got  (expect 4,8,12: o + (2i + o) with o == i == n)")
    // scalastyle:on println
    assert(got == Seq(4, 8, 12))
  }

  test("probe: a rewritten With keeps working") {
    val counter = Counter()
    counter.initialize(0)
    val w = With(counter) { case Seq(ref) => Add(ref, ref) }
    // A rule transforming the tree produces new nodes; the result must still memoize.
    val rewritten = w.transformUp { case a: Add => a }.asInstanceOf[With]
    val got = (1 to 3).map(_ => rewritten.eval(InternalRow.empty))
    // scalastyle:off println
    println(s"PROBE after transform = $got  (expect 2,4,6)")
    // scalastyle:on println
    assert(got == Seq(2, 4, 6))
  }
}
