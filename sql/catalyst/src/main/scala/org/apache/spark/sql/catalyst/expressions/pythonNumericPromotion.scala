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

import org.apache.spark.sql.types.{ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType}

/**
 * Widening rules that give transpiled Python UDF arithmetic Python's overflow behaviour.
 *
 * A Python `int` has no width -- it is a digit array that grows per value, so `x * 100` on a
 * tinyint column returns 12700 and thinks nothing of it. Catalyst picks one type per column
 * before it sees any data, and `Multiply` keeps its operands' type, so the same expression
 * raises ARITHMETIC_OVERFLOW under ANSI. The interpreted Python UDF path does not have this
 * problem, because it converts each row's result to the UDF's declared return type
 * individually -- input widths never enter into it.
 *
 * We cannot reproduce "per value" with a static type, but we do not need to. Python needs
 * unbounded integers because a Python int is unbounded; a Spark column is 8, 16, 32 or 64 bits,
 * and that bound propagates through the expression. So the worst case over every row the column
 * could possibly hold is computable from the input types alone, with no data:
 *
 *   tinyint + tinyint  ->  256      ->  ShortType
 *   abs(smallint)      ->  32768    ->  IntegerType
 *   int32 + int32      ->  2**32    ->  LongType
 *   int32 * int32      ->  2**62    ->  LongType
 *   int64 + int64      ->  2**64    ->  nothing fits; left alone (see narrowestFor)
 *
 * Widening the *operands* to that type is what makes the difference; widening the result does
 * not, and is what the lowerings did before. `a + b` on two int columns already reported
 * LongType, because the transpiler casts to the declared return type at the end -- but the
 * addition happened in IntegerType and overflowed long before that cast was reached.
 *
 * Two deliberate limits:
 *
 *  - `LongType` is the ceiling, so anything whose worst case needs more than 64 bits is left
 *    alone and raises on overflow exactly as it did before this rule existed -- no worse, and
 *    never a wrong answer. `narrowestFor` explains why decimal is not an option here. In practice
 *    this means bigint operands: every narrower width promotes cleanly.
 *  - Only integral inputs are promoted. Doubles need nothing: they saturate to infinity, which
 *    is what Python does too, so there is no overflow to avoid.
 *
 * Note what this deliberately does NOT do. Once the result reaches the UDF's declared return
 * type, `EvaluatePython.makeFromJava` narrows it with `.toByte`/`.toShort`/`.toInt` -- two's
 * complement truncation, so an interpreted UDF declared `-> ByteType` turns 500 into -12 without
 * a word. Matching the interpreted path there would mean reproducing silent data corruption, so
 * the transpiled path keeps raising instead. Promotion is about not failing on an intermediate
 * that Python would have carried; it is not licence to invent a wrong answer.
 */
object PythonNumericPromotion {

  /**
   * The largest magnitude a value of `dt` can have, or None if `dt` is not an integral type we
   * promote. Note these are magnitudes, not maxima -- two's complement reaches one further in
   * the negative direction, and `abs(Byte.MinValue)` is the case that cares.
   */
  private def magnitude(dt: DataType): Option[BigInt] = dt match {
    case ByteType => Some(BigInt(1) << 7)
    case ShortType => Some(BigInt(1) << 15)
    case IntegerType => Some(BigInt(1) << 31)
    case LongType => Some(BigInt(1) << 63)
    // Decimal is refused outright rather than half-supported. Ranking it by precision looked
    // honest but was worse than declining: a decimal(18,0) pair promoted to bigint, silently
    // losing the decimal type, while decimal(19,0) and any non-zero scale fell through to an
    // unaligned replacement and INTERNAL_ERROR. Returning None sends every decimal to `plain`,
    // which keeps its type. (A decimal column cannot reach a transpiled UDF anyway --
    // ResolveTranspiledPythonUDFOptions excludes it from every numeric category -- so this only
    // matters for a direct SQL call.)
    case _ => None
  }

  /**
   * The narrowest integral type that holds every value up to `m` in magnitude, or None when
   * nothing does.
   *
   * `LongType` is the ceiling, and that is a limitation of how this is built rather than of
   * Spark. `DecimalType(38, 0)` would reach further and would cover a single bigint multiply
   * exactly, 2**126 being 38 digits. An earlier version of this comment claimed a decimal
   * replacement could not resolve because `DecimalPrecision` coercion is what caps a product's
   * precision at 38; that is wrong. `Multiply` caps it itself, in `resultDecimalType`, and
   * `Multiply(cast(a, decimal(38,0)), cast(b, decimal(38,0)))` resolves standalone. What actually
   * failed was `Add(decimal, int_literal)`, because the hand-rolled alignment in `widestOf` does
   * not rank DecimalType -- a gap in this file, not in Catalyst.
   *
   * Ranking decimal here would then have to reproduce Spark's decimal type coercion by hand,
   * which is the wrong trade. The right shape is to widen in an analyzer rule instead of inside
   * the expression: `ResolveTranspiledPythonUDFOptions` already runs in the Resolution batch
   * alongside `typeCoercionRules`, so a rewrite there is coerced on the next fixed-point
   * iteration and needs none of `plain`, `widestOf`, or a lazily-computed replacement. Until
   * then `x * y` on two bigints raises on overflow exactly as it did before promotion existed.
   */
  private def narrowestFor(m: BigInt): Option[DataType] = {
    if (m <= Byte.MaxValue) Some(ByteType)
    else if (m <= Short.MaxValue) Some(ShortType)
    else if (m <= Int.MaxValue) Some(IntegerType)
    else if (m <= Long.MaxValue) Some(LongType)
    else None
  }

  /**
   * The type to evaluate `worst`-bounded arithmetic in, given the operand types. Returns None
   * when nothing needs to change: a non-integral operand, or a worst case too wide for any
   * Catalyst type, both of which leave the operator alone.
   */
  private def promote(inputs: Seq[DataType], worst: Seq[BigInt] => BigInt): Option[DataType] = {
    val magnitudes = inputs.map(magnitude)
    if (magnitudes.exists(_.isEmpty)) {
      None
    } else {
      narrowestFor(worst(magnitudes.map(_.get))).filter { widened =>
        // Only report a promotion that actually widens. A body whose worst case already fits the
        // operands' own type gains nothing from a cast, and emitting one would just grow the plan.
        inputs.exists(_ != widened)
      }
    }
  }

  /** Promotion target for `a + b` / `a - b`: the operands' magnitudes can only add. */
  def forAddition(left: DataType, right: DataType): Option[DataType] =
    promote(Seq(left, right), ms => ms(0) + ms(1))

  /** Promotion target for `a * b`: the magnitudes multiply. */
  def forMultiplication(left: DataType, right: DataType): Option[DataType] =
    promote(Seq(left, right), ms => ms(0) * ms(1))

  /**
   * Promotion target for `abs(a)` and unary minus. Both need exactly one more value than the
   * type holds, because two's complement has no positive counterpart for the minimum -- which
   * is why `abs(x)` on a smallint holding -32768 raises where Python answers 32768.
   */
  def forNegation(child: DataType): Option[DataType] = promote(Seq(child), ms => ms(0))

  /**
   * The operator over its operands as given -- what we report before the children resolve and a
   * width can be chosen. The one thing it must do is align the operand types: a replacement never
   * goes through type coercion (`InheritAnalysisRules` is what normally arranges that, and we
   * cannot use it, since building ours needs the children's types), so an unaligned `Add(bigint,
   * int)` does not resolve and CheckAnalysis reports INTERNAL_ERROR -- a broken query rather than
   * a fallback. `x + 1` on a bigint column is enough to reach it, the literal being an int.
   */
  def plain(
      left: Expression,
      right: Expression,
      op: (Expression, Expression) => Expression): Expression = {
    val (l, r) = (left, right)
    // Align the operand types ourselves rather than emitting `Add(bigint, int)` and hoping. A
    // replacement has to be resolvable *as written*: `InheritAnalysisRules` is what normally
    // hands a replacement to type coercion, and we cannot use it (building ours needs the
    // children's types, which are not known in the constructor). So an unaligned replacement
    // never gets coerced and CheckAnalysis rejects it with INTERNAL_ERROR -- which is a broken
    // query, not a fallback. `x + 1` on a bigint column is enough to hit it, since the literal
    // is an int.
    //
    // Widest-of-two is the whole rule here, and it is safe because these operators are only
    // emitted for numeric operands (see `_promoting_if_numeric` on the Python side), and it is
    // Spark's own numeric precedence, so it agrees with what coercion would have done. Note the
    // gate really is *numeric* and not *integral*: an unannotated parameter's category is plain
    // "numeric", so an integral-only gate would refuse the commonest body of all.
    widestOf(l.dataType, r.dataType) match {
      case Some(t) if l.dataType != t || r.dataType != t => op(Cast(l, t), Cast(r, t))
      case _ => op(l, r)
    }
  }

  /**
   * The wider of two numeric types, or None if either is not one we rank.
   *
   * This is Spark's own numeric precedence, and it covers the float types as well as the integral
   * ones on purpose: `plain` uses it to make a replacement resolvable, and `x / 2.0 + 1` needs
   * aligning just as much as `x + 1` on a bigint does. DecimalType is absent because a decimal
   * column never reaches these expressions -- ResolveTranspiledPythonUDFOptions keeps it out of
   * every numeric category -- and because we never produce one ourselves.
   */
  private def widestOf(left: DataType, right: DataType): Option[DataType] = {
    val rank = Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)
    (rank.indexOf(left), rank.indexOf(right)) match {
      case (l, r) if l >= 0 && r >= 0 => Some(rank(math.max(l, r)))
      case _ => None
    }
  }

  /**
   * `op(cast(left, t), cast(right, t))` for the promoted `t`, or `plain` when nothing needs
   * widening. Lives here so the "cast both sides, then delegate" step exists once rather than in
   * each operator below.
   */
  def widened(
      left: Expression,
      right: Expression,
      target: (DataType, DataType) => Option[DataType],
      op: (Expression, Expression) => Expression): Expression = {
    val (l, r) = (left, right)
    target(l.dataType, r.dataType) match {
      case Some(t) => op(Cast(l, t), Cast(r, t))
      // No promotion to apply, but the operands may still disagree, and an unaligned replacement
      // does not resolve -- so fall through to `plain`, which aligns them.
      case None => plain(l, r, op)
    }
  }

}

/**
 * Arithmetic that evaluates at a width wide enough for Python's semantics, per
 * [[PythonNumericPromotion]]. Only meant to be emitted by the Python UDF transpiler, which
 * reaches them through `call_function` the same way it reaches `div` -- hence the registry
 * entries. Hand-written SQL should stay away: there, `a + b` overflowing is the documented ANSI
 * contract, and quietly getting a decimal back instead would be a surprise, which is why the
 * names are deliberately unlovely rather than `python_add`.
 *
 * These are [[RuntimeReplaceable]] rather than hand-written arithmetic because the whole point is
 * to run the *existing* operator at a wider type -- writing fresh `eval` and `doGenCode` bodies
 * would be a second implementation of Add to keep in step with the first. The replacement is
 * recomputed rather than cached, because it depends on the children's types -- see the note on
 * `replacement` below.
 */
trait PythonPromotingArithmetic extends RuntimeReplaceable {
  /**
   * Deliberately a `def` and not a `lazy val`. `replacement` is derived from the children's
   * types, and a parent doing type coercion can ask for our `dataType` -- and so force this --
   * while those children are still unresolved. A `lazy val` would cache whatever the unresolved
   * tree happened to say and never revisit it, which showed up as every *compound* body falling
   * back (`(x + 1) // 3`) while a bare `x + y` lowered fine. Recomputing is cheap; the promotion
   * rule is a few BigInt comparisons.
   */
  override def replacement: Expression = if (childrenResolved) promoted else unpromoted

  /** The widened form, safe to build only once the children's types are known. */
  protected def promoted: Expression

  /**
   * What to report when a width cannot be chosen yet.
   *
   * Not, despite the obvious reading, for a child that is unresolved in the
   * `UnresolvedAttribute` sense: `plain` asks its operands for `dataType`, so such a child would
   * throw `UnresolvedException` rather than be reported around. The case this actually serves is
   * a child that is unresolved because its *own* type check failed while still reporting a
   * dataType -- `ShiftLeft(tinyint, tinyint)`, say. Nothing reaches the throwing path through the
   * SQL parser today, since `ResolveFunctions` waits for `childrenResolved`, but it is a trap for
   * a single-pass resolver or a hand-built plan.
   */
  protected def unpromoted: Expression
}

case class PythonPromotingAdd(left: Expression, right: Expression)
  extends BinaryExpression with PythonPromotingArithmetic {
  override protected def promoted: Expression =
    PythonNumericPromotion.widened(
      left, right, PythonNumericPromotion.forAddition, Add(_, _))
  override protected def unpromoted: Expression =
    PythonNumericPromotion.plain(left, right, Add(_, _))
  override def prettyName: String = "python_promoting_add"
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): PythonPromotingAdd =
    copy(left = newLeft, right = newRight)
}

case class PythonPromotingSubtract(left: Expression, right: Expression)
  extends BinaryExpression with PythonPromotingArithmetic {
  override protected def promoted: Expression =
    PythonNumericPromotion.widened(
      left, right, PythonNumericPromotion.forAddition, Subtract(_, _))
  override protected def unpromoted: Expression =
    PythonNumericPromotion.plain(left, right, Subtract(_, _))
  override def prettyName: String = "python_promoting_subtract"
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): PythonPromotingSubtract =
    copy(left = newLeft, right = newRight)
}

case class PythonPromotingMultiply(left: Expression, right: Expression)
  extends BinaryExpression with PythonPromotingArithmetic {
  override protected def promoted: Expression =
    PythonNumericPromotion.widened(
      left, right, PythonNumericPromotion.forMultiplication, Multiply(_, _))
  override protected def unpromoted: Expression =
    PythonNumericPromotion.plain(left, right, Multiply(_, _))
  override def prettyName: String = "python_promoting_multiply"
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): PythonPromotingMultiply =
    copy(left = newLeft, right = newRight)
}

case class PythonPromotingNegate(child: Expression)
  extends UnaryExpression with PythonPromotingArithmetic {
  // `UnaryMinus` keeps its operand's type and negates exactly, so `-x` on an int column holding
  // Integer.MinValue raised where Python answers 2147483648. Same rule as `abs` -- two's
  // complement has no positive counterpart for a width's minimum -- and it was an oversight that
  // `forNegation`'s scaladoc said it covered unary minus while nothing called it for that.
  override protected def promoted: Expression =
    PythonNumericPromotion.forNegation(child.dataType) match {
      case Some(widened) => UnaryMinus(Cast(child, widened))
      case None => UnaryMinus(child)
    }
  override protected def unpromoted: Expression = UnaryMinus(child)
  override def prettyName: String = "python_promoting_negate"
  override protected def withNewChildInternal(newChild: Expression): PythonPromotingNegate =
    copy(child = newChild)
}

case class PythonPromotingAbs(child: Expression)
  extends UnaryExpression with PythonPromotingArithmetic {
  override protected def promoted: Expression =
    // Built directly rather than through the binary `widened`: passing the child into both slots
    // evaluated it twice, which on a nested `abs` cost 2^(depth-1) leaf type reads and built a
    // `Cast` that `op` then discarded.
    PythonNumericPromotion.forNegation(child.dataType) match {
      case Some(widened) => Abs(Cast(child, widened))
      case None => Abs(child)
    }
  override protected def unpromoted: Expression =
    Abs(child)
  override def prettyName: String = "python_promoting_abs"
  override protected def withNewChildInternal(newChild: Expression): PythonPromotingAbs =
    copy(child = newChild)
}

