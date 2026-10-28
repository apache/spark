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
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.types.{ByteType, DataType, DecimalType, DoubleType, IntegerType, LongType, ShortType, StringType}

/**
 * The promotion rule on its own, without a session. The point of these is that the widths are
 * derived arithmetic rather than a lookup table someone can quietly edit: each expectation below
 * is the narrowest Catalyst type that holds the worst case the *input* types allow, so if the
 * rule drifts the numbers stop lining up.
 */
class PythonNumericPromotionSuite extends SparkFunSuite {

  private def dec(p: Int): DataType = DecimalType(p, 0)

  private val ansi = NumericEvalContext(EvalMode.ANSI, allowDecimalPrecisionLoss = true)
  private val legacy = NumericEvalContext(EvalMode.LEGACY, allowDecimalPrecisionLoss = true)

  test("addition widens far enough for the sum of the operands' extremes") {
    // Two tinyints reach 256, which a tinyint cannot hold but a smallint can. Two bigints reach
    // 2**64, which needs 20 digits -- one more than LongType has.
    assert(PythonNumericPromotion.forAddition(ByteType, ByteType) === Some(ShortType))
    assert(PythonNumericPromotion.forAddition(ShortType, ShortType) === Some(IntegerType))
    assert(PythonNumericPromotion.forAddition(IntegerType, IntegerType) === Some(LongType))
    // Two bigints reach 2**64, which nothing integral holds, so they are left alone -- LongType
    // is the ceiling on purpose, see narrowestFor.
    assert(PythonNumericPromotion.forAddition(LongType, LongType).isEmpty)
    assert(PythonNumericPromotion.forAddition(ByteType, LongType).isEmpty)
  }

  test("multiplication widens by the product, which climbs much faster") {
    // 128 * 128 = 16384, so two tinyints still fit a smallint, and two ints reach 2**62, which
    // needs the whole of LongType. Two bigints reach 2**126, which nothing integral holds -- and
    // LongType is the ceiling on purpose, see narrowestFor -- so they are left alone.
    assert(PythonNumericPromotion.forMultiplication(ByteType, ByteType) === Some(ShortType))
    assert(PythonNumericPromotion.forMultiplication(IntegerType, IntegerType) === Some(LongType))
    assert(PythonNumericPromotion.forMultiplication(LongType, LongType).isEmpty)
  }

  test("negation needs one more value than the type holds") {
    // Two's complement reaches one further down than up, so `abs` of a width's minimum does not
    // fit that width -- the case that made `abs(x)` raise on a smallint holding -32768.
    assert(PythonNumericPromotion.forNegation(ByteType) === Some(ShortType))
    assert(PythonNumericPromotion.forNegation(ShortType) === Some(IntegerType))
    assert(PythonNumericPromotion.forNegation(IntegerType) === Some(LongType))
    // And 2**63 overruns Long.MaxValue by exactly one, so bigint has nowhere to go: abs of
    // Long.MinValue keeps raising, which no amount of intermediate widening could fix anyway
    // since the value does not fit a LongType return either.
    assert(PythonNumericPromotion.forNegation(LongType).isEmpty)
  }

  test("nothing is promoted when there is nothing to gain or nothing to promote") {
    // Doubles saturate to infinity, which is what Python does too, so there is no overflow to
    // avoid and no reason to touch them.
    assert(PythonNumericPromotion.forAddition(DoubleType, DoubleType).isEmpty)
    assert(PythonNumericPromotion.forMultiplication(IntegerType, DoubleType).isEmpty)
    assert(PythonNumericPromotion.forNegation(DoubleType).isEmpty)
    // Non-numerics are not our business at all.
    assert(PythonNumericPromotion.forAddition(StringType, StringType).isEmpty)
    // A decimal operand is not promoted at all -- decimal arithmetic needs the coercion rule
    // our replacement never sees, so we leave it to the ordinary operator.
    assert(PythonNumericPromotion.forNegation(dec(38)).isEmpty)
  }

  test("a chain climbs until it runs out of integral room, then gives up") {
    // int32 * int32 lands on LongType, and multiplying that again has nowhere left to go. Giving
    // up leaves the operator to raise on overflow exactly as it did before promotion existed --
    // no worse, and never a wrong answer.
    assert(PythonNumericPromotion.forMultiplication(ByteType, ByteType) === Some(ShortType))
    assert(PythonNumericPromotion.forMultiplication(ShortType, ShortType) === Some(IntegerType))
    assert(PythonNumericPromotion.forMultiplication(IntegerType, IntegerType) === Some(LongType))
    assert(PythonNumericPromotion.forMultiplication(LongType, IntegerType).isEmpty)
  }

  test("the widened form casts both operands, not the result") {
    // The whole bug this fixes: `a + b` on two int columns already *reported* bigint, because
    // the transpiler casts to the declared return type at the end. The addition still happened
    // in IntegerType and overflowed long before that cast. So assert the casts are on the way in.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    PythonNumericPromotion.widened(
      a, b, PythonNumericPromotion.forAddition, ansi, Add(_, _, ansi)) match {
      case Add(Cast(l, LongType, _, _), Cast(r, LongType, _, _), _) =>
        assert(l === a && r === b)
      case other =>
        fail(s"expected both operands cast to bigint before the Add, got $other")
    }
  }

  test("the widened form leaves the operator alone when no promotion applies") {
    val a = AttributeReference("a", DoubleType)()
    val b = AttributeReference("b", DoubleType)()
    val result = PythonNumericPromotion.widened(
      a, b, PythonNumericPromotion.forAddition, ansi, Add(_, _, ansi))
    assert(result === Add(a, b, ansi), "a double add should not collect casts it cannot use")
  }

  test("promotion always widens, so there is nothing to filter") {
    // `promote` used to drop a target that matched an input type, on the theory that a body whose
    // worst case already fits gains nothing. That case cannot arise: `narrowestFor(m)` answers T
    // only when m <= T.MaxValue, one below `magnitude(T)`, while every worst case here is at
    // least the widest input's magnitude. Asserted over the whole cross product so that a future
    // magnitude or width edit cannot quietly reintroduce the possibility.
    val types = Seq(ByteType, ShortType, IntegerType, LongType)
    for (l <- types; r <- types) {
      PythonNumericPromotion.forAddition(l, r).foreach { t =>
        assert(t != l && t != r, s"forAddition($l, $r) returned an operand type")
      }
      PythonNumericPromotion.forMultiplication(l, r).foreach { t =>
        assert(t != l && t != r, s"forMultiplication($l, $r) returned an operand type")
      }
    }
    types.foreach { t =>
      PythonNumericPromotion.forNegation(t).foreach { w =>
        assert(w != t, s"forNegation($t) returned its own operand type")
      }
    }
  }

  test("the replacement is built once the children resolve, and then reused") {
    // The `def` this replaced rebuilt the whole subtree on every `dataType` read, because
    // RuntimeReplaceable derives `dataType` and `nullable` straight from `replacement`. Identity
    // is the assertion that catches a revert: a `def` hands back a fresh tree each call.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val add = PythonPromotingAdd(a, b)
    assert(add.childrenResolved, "attribute references should count as resolved")
    assert(add.replacement eq add.replacement, "a resolved replacement should be cached")
    assert(add.dataType === LongType, "two int operands should evaluate in bigint")
  }

  test("an unresolved child is reported around rather than promoted") {
    // Not an `UnresolvedAttribute`: `plain` asks its operands for `dataType`, so that would throw
    // rather than fall to `unpromoted`. The case that actually reaches it is a child that reports
    // a dataType while its own type check fails -- `ShiftLeft` wants an int on the left.
    val b = AttributeReference("b", ByteType)()
    val bad = ShiftLeft(b, b)
    assert(!bad.resolved, "ShiftLeft on two tinyints should fail its type check")
    val add = PythonPromotingAdd(bad, Literal(1.toByte))
    assert(!add.childrenResolved)
    // Unpromoted, but still aligned -- an unaligned replacement does not resolve at all.
    assert(add.replacement === Add(bad, Literal(1.toByte)))
  }

  test("plain aligns an operand pair the promotion rule declines to widen") {
    // `x + 1` on a bigint: forAddition gives up (2**63 + 2**31 overruns LongType), so this goes
    // through `plain`, whose whole job is that the replacement resolves. Unaligned, CheckAnalysis
    // reports INTERNAL_ERROR -- a broken query rather than a fallback to interpreted Python.
    val a = AttributeReference("a", LongType)()
    val add = PythonPromotingAdd(a, Literal(1))
    assert(PythonNumericPromotion.forAddition(LongType, IntegerType).isEmpty)
    assert(add.replacement.resolved, s"unaligned replacement: ${add.replacement}")
    assert(add.dataType === LongType)
  }

  test("subtraction keeps its operands in order through withNewChildren") {
    // A left/right swap here is invisible in a plan and wrong in every row.
    val a = AttributeReference("a", ByteType)()
    val b = AttributeReference("b", ByteType)()
    val swapped = PythonPromotingSubtract(a, a).withNewChildren(Seq(a, b))
    assert(swapped === PythonPromotingSubtract(a, b))
    swapped.asInstanceOf[PythonPromotingSubtract].replacement match {
      case Subtract(Cast(l, ShortType, _, _), Cast(r, ShortType, _, _), _) =>
        assert(l === a && r === b, "operands should keep their order")
      case other => fail(s"expected a widened subtract, got $other")
    }
  }

  test("the stored eval context is what the replacement carries") {
    // Not the ambient conf. The replacement is built lazily, so without a stored context the
    // operator's eval mode would be decided by whenever something first asked for our dataType.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    Seq(ansi, legacy).foreach { context =>
      PythonPromotingAdd(a, b, context).replacement match {
        case Add(_, _, ctx) => assert(ctx === context, s"expected $context on the Add")
        case other => fail(s"expected an Add, got $other")
      }
      // The widening casts carry it too. Behaviour is identical either way -- these only ever
      // widen -- but a differing evalMode would split the canonical form.
      PythonPromotingAdd(a, b, context).replacement.collect { case c: Cast => c }.foreach { c =>
        assert(c.evalMode === context.evalMode, "a widening cast should carry the same mode")
      }
    }
  }

  test("the eval context makes the canonical form deterministic") {
    // The point of storing it. Two nodes built with the same context agree no matter when their
    // replacement is forced; before, canonical form was a function of force ordering, which
    // silently cost subexpression elimination and plan dedup.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    assert(PythonPromotingAdd(a, b, ansi).canonicalized === PythonPromotingAdd(a, b, ansi)
      .canonicalized)
    assert(PythonPromotingAdd(a, b, ansi).semanticEquals(PythonPromotingAdd(a, b, ansi)))
    // And two genuinely different contexts stay distinguishable rather than colliding.
    assert(PythonPromotingAdd(a, b, ansi).canonicalized !==
      PythonPromotingAdd(a, b, legacy).canonicalized)
  }

  test("the eval context survives withNewChildren") {
    // "Copying an expression" is the case NumericEvalContext's own scaladoc calls out.
    val a = AttributeReference("a", ByteType)()
    val b = AttributeReference("b", ByteType)()
    val copied = PythonPromotingSubtract(a, a, legacy).withNewChildren(Seq(a, b))
    assert(copied.asInstanceOf[PythonPromotingSubtract].evalContext === legacy)
    val negated = PythonPromotingNegate(a, legacy).withNewChildren(Seq(b))
    assert(negated.asInstanceOf[PythonPromotingNegate].evalContext === legacy)
  }

  test("the unary operators map the eval mode onto failOnError") {
    // `Abs` and `UnaryMinus` predate NumericEvalContext and take a Boolean. TRY counts as
    // failing, the same way BinaryArithmetic reads it: it evaluates as though it would fail and
    // captures the error afterwards.
    val a = AttributeReference("a", ByteType)()
    val tryMode = NumericEvalContext(EvalMode.TRY, allowDecimalPrecisionLoss = true)
    Seq(ansi -> true, tryMode -> true, legacy -> false).foreach { case (context, expected) =>
      PythonPromotingAbs(a, context).replacement match {
        case Abs(_, failOnError) => assert(failOnError === expected, s"for $context")
        case other => fail(s"expected an Abs, got $other")
      }
      PythonPromotingNegate(a, context).replacement match {
        case UnaryMinus(_, failOnError) => assert(failOnError === expected, s"for $context")
        case other => fail(s"expected a UnaryMinus, got $other")
      }
    }
  }

  test("the function registry can still build these from children alone") {
    // Adding a defaulted `evalContext` changed the primary constructor's arity, and
    // FunctionRegistryBase.build looks for a constructor whose parameters are *all* Expression.
    // Without the two-child secondary constructor this fails at the call site, not at startup,
    // so every transpiled UDF breaks while the suite above stays green.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    Seq(
      ("python_promoting_add", Seq(a, b)),
      ("python_promoting_subtract", Seq(a, b)),
      ("python_promoting_multiply", Seq(a, b)),
      ("python_promoting_abs", Seq(a)),
      ("python_promoting_negate", Seq(a))).foreach { case (name, children) =>
      val built = FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name), children)
      assert(built.isInstanceOf[PythonPromotingArithmetic], s"$name built ${built.getClass}")
      assert(built.children === children, s"$name lost its children")
    }
  }

  test("a deep chain of promoting nodes stays cheap to type") {
    // The regression guard for the rebuild-per-read blowup: nested nodes each asked their child
    // for a type, which rebuilt that child's subtree, which asked again. Measured before the fix:
    // 3.5s at 13 nodes and no answer at all in nine minutes at 19. `x ** 8` alone lowers to seven
    // nested multiplies, so this is a shape real UDFs reach. If it regresses, this test hangs.
    val a = AttributeReference("a", ByteType)()
    val deep = (1 to 24).foldLeft[Expression](a)((acc, _) => PythonPromotingMultiply(acc, a))
    // Byte * Byte lands on Short, Short * Byte on Int, Int * Byte on Long, and from there the
    // rule runs out of integral room and leaves the multiply where it is.
    assert(deep.dataType === LongType)
    assert(deep.resolved)
  }
}
