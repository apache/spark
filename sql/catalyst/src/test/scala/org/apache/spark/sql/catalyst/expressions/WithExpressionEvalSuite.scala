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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode, GenerateMutableProjection}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}

/**
 * Evaluation of [[With]] and the memoization it gives a [[CommonExpressionRef]]. The rewrite that
 * decides which `With`s reach evaluation at all is covered by `RewriteWithExpressionSuite`.
 */
class WithExpressionEvalSuite extends SparkFunSuite with SQLHelper {

  /**
   * A stand-in for a stateful generator: every evaluation returns the next integer, so a second
   * evaluation within one row is directly observable. Only used on the interpreted path.
   */
  private case class Counter() extends LeafExpression with Nondeterministic {
    @transient private var n = 0
    override def stateful: Boolean = true
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = false
    override protected def initializeInternal(partitionIndex: Int): Unit = {}
    override protected def evalInternal(input: InternalRow): Any = { n += 1; n }
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      throw new UnsupportedOperationException
  }

  private def counter(): Counter = {
    val c = Counter()
    c.initialize(0)
    c
  }

  /**
   * A node that has to be evaluated interpretively even when its parent is generated, so that a
   * reference below it takes the interpreted path out of generated code.
   */
  private case class Fallback(child: Expression) extends UnaryExpression with CodegenFallback {
    override def dataType: DataType = child.dataType
    override def eval(input: InternalRow): Any = child.eval(input)
    override protected def withNewChildInternal(newChild: Expression): Fallback =
      copy(child = newChild)
  }

  test("a definition is evaluated once per row however many references read it") {
    // `ref + ref` is the shape `BETWEEN` produces. Memoized, both references read one value, so the
    // sum is 2n on the nth row; inlined it would be n + (n + 1).
    val w = With(counter()) { case Seq(ref) => Add(ref, ref) }
    assert((1 to 4).map(_ => w.eval(InternalRow.empty)) == Seq(2, 4, 6, 8))
  }

  test("a definition is not evaluated on a row that reaches no reference") {
    val c = counter()
    // The branch is inside the `With`, so the `With` is entered on every row and does clear its
    // cells. What it must not do is evaluate the definition before a reference is reached; putting
    // the branch outside would only test that `If` does not evaluate the arm it did not take.
    val w = With(c) { case Seq(ref) => If(Literal.TrueLiteral, Literal(-1), Add(ref, ref)) }
    assert((1 to 3).map(_ => w.eval(InternalRow.empty)) == Seq(-1, -1, -1))
    // The counter is still at 0, so the first row that does reach a reference sees 1.
    assert(With(c) { case Seq(ref) => Add(ref, ref) }.eval(InternalRow.empty) == 2)
  }

  test("a reference behind a short-circuiting operator is not read when the left side is false") {
    // Neither pre-evaluating the definition into a project nor guarding that column by the branch
    // can express this: both evaluate on every row the branch is reached on, while `And`
    // short-circuits before the reference. The `And` is inside the `With` so that the `With` is
    // entered and only the reference is skipped.
    val c = counter()
    val w = With(c) { case Seq(ref) =>
      And(Literal.FalseLiteral, GreaterThanOrEqual(ref, Literal(1)))
    }
    assert((1 to 3).map(_ => w.eval(InternalRow.empty)) == Seq(false, false, false))
    assert(With(c) { case Seq(ref) => Add(ref, ref) }.eval(InternalRow.empty) == 2)
  }

  test("a nested With memoizes each scope separately") {
    val outer = counter()
    val inner = counter()
    // The outer reference is read twice, and one of its uses wraps an inner `With` whose own
    // reference is also read twice, so each row is `o + (2i + o)` with `o` and `i` both n.
    val w = With(outer) { case Seq(o) =>
      Add(o, With(inner) { case Seq(i) => Add(Add(i, i), o) })
    }
    assert((1 to 3).map(_ => w.eval(InternalRow.empty)) == Seq(4, 8, 12))
  }

  test("a With rebuilt by a rule still memoizes") {
    val w = With(counter()) { case Seq(ref) => Add(ref, ref) }
    // Evaluate the original first. A rule rebuilds the definition, and therefore its cell, while
    // handing the new `With` the original's reference objects, so an implementation that bound the
    // references once would leave the rebuilt `With` reading this evaluation's cell.
    assert(w.eval(InternalRow.empty) == 2)
    // The rule has to hand back a node that is not `==` the one it replaces, or `transformUp` keeps
    // the original -- `Counter()` is a case class with no parameters, so a fresh one compares equal
    // to the old one. Wrapping it changes the tree while leaving the definition's value sequence,
    // its data type and its nullability alone, so the references are carried over rather than
    // rebuilt, which is the shape that needs the per-evaluation rebinding.
    val rewritten = w.transformUp { case _: Counter => Add(counter(), Literal(0)) }
      .asInstanceOf[With]
    assert(rewritten ne w, "the rule has to have rebuilt something")
    assert((1 to 3).map(_ => rewritten.eval(InternalRow.empty)) == Seq(2, 4, 6))
  }

  test("SPARK-58902: a copy of a With owns its references and its cells") {
    // The definition is deliberately not stateful, which is what makes
    // `CommonExpressionDef.stateful` load-bearing: `mapChildren` finds nothing changed below the
    // definition, so without the override the copy is handed the original definition object, cell
    // included. A `Rand` definition would hide that, since copying it changes the definition's
    // child and forces a rebuild anyway.
    // The references are stateful leaves, and `LeafLike.withNewChildrenInternal` returns `this`, so
    // without the override on `CommonExpressionRef` the two `With`s would share one set of
    // reference objects while owning two cells.
    val w1 = With(Literal(1)) { case Seq(ref) => Add(ref, ref) }
    val w2 = w1.freshCopyIfContainsStatefulExpression().asInstanceOf[With]
    def refOf(e: Expression): CommonExpressionRef =
      e.collect { case r: CommonExpressionRef => r }.head
    assert(w1 ne w2, "a stateful With has to be copied")
    assert(refOf(w1.child) ne refOf(w2.child), "the copy has to own its references")
    assert(w1.defs.head.cell ne w2.defs.head.cell, "the copy has to own its cell")
  }

  test("SPARK-58902: two Withs over one set of references each read their own definition") {
    // The shape a rule produces when it rewrites only the definition: `mapChildren` hands back the
    // same `child` object with a new definition, so `withNewChildrenInternal` builds a `With` over
    // the original's references -- and a rebuilt reference compares equal to the one it replaces,
    // so `transform` cannot hand the new `With` references of its own. Giving the two `With`s
    // visibly different definitions shows which cell the references actually read.
    val w1 = With(counter()) { case Seq(ref) => Add(ref, ref) }
    val w2 = w1.withNewChildren(
      IndexedSeq(w1.child, CommonExpressionDef(Literal(100), w1.defs.head.id))).asInstanceOf[With]

    assert(w1.eval(InternalRow.empty) == 2, "w1 reads its own counter")
    assert(w2.eval(InternalRow.empty) == 200, "w2 reads its own literal")
    assert(w1.eval(InternalRow.empty) == 4, "w1 read the value w2 memoized")
  }

  test("SPARK-58902: a definition's code is generated once however many references read it") {
    // This definition is short and holds no `With`, so it is emitted inline: every reference emits
    // it inside its own `if (!computed)` guard, and the text appears once per reference -- but it
    // has to be the same text, generated once, or each copy calls `addMutableState` again and the
    // definition ends up owning one counter per reference. Two references in mutually exclusive
    // positions would then draw from two counters sitting at the same position in their sequences,
    // and hand out one value on two rows.
    val ctx = new CodegenContext
    With(MonotonicallyIncreasingID()) { case Seq(ref) => Add(ref, ref) }.genCode(ctx)
    val counters = ctx.inlinedMutableStates.count { case (_, name) => name.startsWith("count") }
    assert(counters == 1, s"the definition was generated $counters times")
  }

  test("SPARK-58902: nesting does not multiply a definition's body when it can go in a method") {
    // Each level of nested `With`s reads its definition twice, so a reference that pastes the body
    // doubles it per level. What already stopped that from running away is
    // `Expression.reduceCodeSize`, which hoists into a method whichever node's code first passes
    // `methodSplitThreshold` as generation walks up: measured with the 237-character leaf below and
    // the default threshold, the count settled at 4 from depth 2 on rather than doubling -- where
    // it settles is where the threshold catches the doubling, so it is a property of those two
    // numbers and not a law. Emitting the body into a method here makes it 2 at any depth, so the
    // bound no longer grows with depth wherever the threshold sits.
    //
    // What is not covered here is the shape where the input arrives as local variables, which is
    // what a whole-stage `Project` or `Filter` hands an expression: `currentVars` is set, so
    // neither this method nor `reduceCodeSize` applies and the body is pasted per reference -- 2,
    // 4, 8, ... 256 at depths 1 to 8. A bare `CodegenContext` has `INPUT_ROW` set and `currentVars`
    // null, which is what a method needs.
    val marker = 1234567
    def nested(depth: Int): Expression = {
      val leaf: Expression = Add(BoundReference(0, IntegerType, nullable = false), Literal(marker))
      (1 to depth).foldLeft(leaf) { (inner, _) =>
        With(inner) { case Seq(ref) => Add(ref, ref) }
      }
    }
    def markerCount(depth: Int): Int = {
      val ctx = new CodegenContext
      val source = nested(depth).genCode(ctx).code.toString + ctx.declareAddedFunctions()
      marker.toString.r.findAllMatchIn(source).size
    }
    // The threshold decides whether the innermost body is inlined at all, so pin it rather than
    // depend on the default: below the length of that body the fill would go into a method of its
    // own and the count would be 1, for a reason that has nothing to do with nesting.
    withSQLConf(SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1024") {
      (1 to 6).foreach { depth =>
        val count = markerCount(depth)
        assert(count == 2, s"the innermost body was emitted $count times at depth $depth")
      }
      // The values still come out right: each level doubles what the one below it produced. Four
      // levels are enough to show that and keep the product inside Int, which ANSI `Add` would
      // raise on rather than wrap past depth 10.
      val proj = GenerateMutableProjection.generate(Seq(nested(4)))
      assert(proj(InternalRow(1)).getInt(0) == (1 + marker) * 16)
    }
  }

  test("SPARK-58902: a nested With sharing a reference object restores the outer binding") {
    // Two `With`s over one reference object, the inner one nested in the outer one's child and
    // redefining the id the outer one defines. Only a caller building the case class directly
    // produces this -- `withNewChildrenInternal` shares references between a `With` and its
    // replacement, which are siblings, and no rule nests a redefinition of a live id -- so this is
    // hardening rather than a reachable wrong answer. Binding on entry alone is not enough: the
    // inner `With` rebinds the shared reference to its own definition, so a read after the inner
    // one returned would answer with the inner definition unless the outer binding is put back.
    val id = new CommonExpressionId()
    val outerDef = CommonExpressionDef(counter(), id)
    val innerDef = CommonExpressionDef(Literal(10), id)
    val ref = new CommonExpressionRef(outerDef)
    val inner = With(Add(ref, ref), Seq(innerDef))
    val outer = With(Add(Add(ref, inner), ref), Seq(outerDef))
    // The outer definition counts, the inner one does not, so row n is n + (10 + 10) + n. A counter
    // is what makes the repetition worth something: leaving the inner binding in place gives
    // n + 20 + 10, and losing memoization of the outer definition gives n + 20 + (n + 1).
    assert((1 to 3).map(_ => outer.eval(InternalRow.empty)) == Seq(22, 24, 26))

    // The generated path never has to answer this: one id in two nested scopes is refused while
    // generating, so it cannot quietly disagree with the values above.
    val literalOuter = CommonExpressionDef(Literal(1), new CommonExpressionId())
    val literalRef = new CommonExpressionRef(literalOuter)
    val nested = With(
      Add(literalRef, With(Add(literalRef, literalRef),
        Seq(CommonExpressionDef(Literal(10), literalOuter.id)))),
      Seq(literalOuter))
    val generated = intercept[SparkException](GenerateMutableProjection.generate(Seq(nested)))
    assert(generated.getMessage.contains("is already being generated"))
  }

  test("SPARK-58902: a definition that references its own id fails without recursing") {
    // Only a caller building the case class directly can produce this, and it is caught rather than
    // left to recurse: generating the definition re-enters the same slot, and evaluating it reaches
    // a reference `refsToBind` never bound, since that scan only covers `child`.
    val id = new CommonExpressionId()
    val proto = CommonExpressionDef(Literal(1), id)
    val selfDef = CommonExpressionDef(Add(new CommonExpressionRef(proto), Literal(1)), id)
    val w = With(new CommonExpressionRef(proto), Seq(selfDef))

    val generated = intercept[SparkException] {
      GenerateMutableProjection.generate(Seq(w))
    }
    assert(generated.getMessage.contains("references it"))
    val interpreted = intercept[SparkException](w.eval(InternalRow.empty))
    assert(interpreted.getMessage.contains("outside its With"))
  }

  test("SPARK-58902: the generated path does not compute a definition on a row that skips it") {
    // The interpreted no-reference tests use `Counter`, which has no codegen, and the end-to-end
    // skip case never enters the `With` at all. Here the `With` is entered on every row and the
    // branch inside it decides whether a reference is reached, so an eager fill would show up as
    // ids consumed on the rows that take the other arm.
    val id = MonotonicallyIncreasingID()
    val w = With(id) { case Seq(ref) =>
      If(BoundReference(0, BooleanType, nullable = false), ref, Literal(-1L))
    }
    val proj = GenerateMutableProjection.generate(Seq(w))
    proj.initialize(0)
    val got = Seq(false, true, false, true).map { reached =>
      proj(InternalRow(reached)).getLong(0)
    }
    // Only the rows that reach the reference take an id, so the ids stay 0 and 1.
    assert(got == Seq(-1L, 0L, -1L, 1L))
  }

  test("SPARK-58902: two surviving definitions in one With keep their own state") {
    // Every other runtime case has a single definition, so a slot that aliased two definitions, or
    // a clear that reset the wrong one, would go unnoticed. The two are scaled differently to make
    // which slot answered visible in the value.
    def build(): Expression = With(
      MonotonicallyIncreasingID(),
      Multiply(MonotonicallyIncreasingID(), Literal(100L))) { case Seq(a, b) =>
        Add(Add(a, a), Add(b, b))
      }
    // Row n reads a = n and b = 100n once each, so the sum is 2n + 200n. Aliasing the two slots
    // would give 4n or 400n, and clearing only one of them would drift after the first row.
    val proj = GenerateMutableProjection.generate(Seq(build()))
    proj.initialize(0)
    assert((0 until 3).map(_ => proj(InternalRow.empty).getLong(0)) == Seq(0L, 202L, 404L))

    val interpreted = build()
    interpreted.foreach {
      case n: Nondeterministic => n.initialize(0)
      case _ =>
    }
    assert((0 until 3).map(_ => interpreted.eval(InternalRow.empty)) == Seq(0L, 202L, 404L))
  }

  test("SPARK-58902: a refused scope leaves no slot behind in the context") {
    // The duplicate-id check throws partway through allocating, after earlier definitions of the
    // same `With` are already in scope. If those were left there, a later reference to one of those
    // ids in the same context would resolve an orphan and generate code from it instead of saying
    // the id is not in scope.
    val ctx = new CodegenContext
    val id = new CommonExpressionId()
    val w = With(Literal(0), Seq(CommonExpressionDef(Literal(1), id), CommonExpressionDef(
      Literal(2), id)))
    val refused = intercept[SparkException](w.genCode(ctx))
    assert(refused.getMessage.contains("is already being generated"))
    val gone = intercept[SparkException](ctx.getCommonExpr(id.id))
    assert(gone.getMessage.contains("is not in scope"))
  }

  test("SPARK-58902: a reference under a CodegenFallback still memoizes") {
    // The generated code clears the codegen flags, not the cells, so a reference reached through a
    // `CodegenFallback`'s `eval` cannot use them: the whole `With` falls back to `eval`, which
    // binds and clears the cells itself. Generating part of it would be worse than either -- a
    // definition reached from both sides would hold one value in the slots and another in the cell.
    val w = With(counter()) { case Seq(ref) => Add(Fallback(ref), Fallback(ref)) }
    val proj = GenerateMutableProjection.generate(Seq(w))
    proj.initialize(0)
    // Both references read one value per row, so the sum is 2n rather than n + (n + 1).
    assert((1 to 4).map(_ => proj(InternalRow.empty).getInt(0)) == Seq(2, 4, 6, 8))
  }

  test("SPARK-58902: a reference under a nested With that falls back still memoizes") {
    // The inner `With` would fall back on its own, because one of its own references sits under a
    // `Fallback`. The outer one holds no `CodegenFallback` of its own, so it can only notice by
    // asking the inner `With` whether it fell back -- and it has to, since its reference `o` sits
    // inside the inner subtree and would be reached interpretively with no cell bound.
    val outer = counter()
    val inner = counter()
    val w = With(outer) { case Seq(o) =>
      Add(o, With(inner) { case Seq(i) => Add(Fallback(i), o) })
    }
    val proj = GenerateMutableProjection.generate(Seq(w))
    proj.initialize(0)
    // `o` and `i` are both n on the nth row and `o` is read twice, so each row is o + (i + o).
    assert((1 to 3).map(_ => proj(InternalRow.empty).getInt(0)) == Seq(3, 6, 9))
  }
}
