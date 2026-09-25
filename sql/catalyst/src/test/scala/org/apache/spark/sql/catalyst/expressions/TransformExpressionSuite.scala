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
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

class TransformExpressionSuite extends SparkFunSuite {

  /**
   * A bound function with a stable canonical name and no `equals` of its own. A plain class, NOT a
   * case class: a case class would bring structural `equals`, which is the connector-provided
   * comparison these tests are trying to do without.
   */
  private class NamedFunction(canonical: String) extends ScalarFunction[Int] {
    override def inputTypes(): Array[DataType] = Array(IntegerType)
    override def resultType(): DataType = IntegerType
    override def name(): String = canonical
    override def canonicalName(): String = canonical
  }

  /** Honours the contract in `BoundFunction#equals`. */
  private class ComparableFunction extends NamedFunction("test.comparable") {
    override def equals(other: Any): Boolean = other.isInstanceOf[ComparableFunction]
    override def hashCode(): Int = canonicalName().hashCode
  }

  private val a = AttributeReference("a", IntegerType)()
  private val b = AttributeReference("b", IntegerType)()

  private def bucket(function: BoundFunction, child: Expression, numBuckets: Int = 4) =
    TransformExpression(function, Seq(Literal(numBuckets), child))

  test("SPARK-58769: expression equality follows the function's own equals") {
    // Spark does not derive transform identity itself -- it defers to the connector, because only
    // the connector knows which of its state matters. Separate instances of a function that does
    // not implement `equals` therefore yield expressions that do not compare equal. See
    // BoundFunction#equals.
    assert(
      bucket(new NamedFunction("test.bucket"), a) != bucket(new NamedFunction("test.bucket"), a),
      "no equals on the function means no equality across binds")

    val shared = new NamedFunction("test.bucket")
    assert(bucket(shared, a) == bucket(shared, a), "a shared instance is equal either way")

    // A function that does implement it gets the deduplication.
    val left = bucket(new ComparableFunction, a)
    val right = bucket(new ComparableFunction, a)
    assert(left.function ne right.function, "the fixture must use distinct function instances")
    assert(left == right)
    assert(left.semanticEquals(right))
    assert(ExpressionSet(Seq(left, right)).size == 1)
  }

  test("SPARK-58769: the two comparisons agree for a function that follows the contract") {
    // `equals` is the finer comparison and the canonical name the coarser one, so a function that
    // overrides both answers both consistently. They still differ in what they take into account:
    // `isSameFunction` ignores the arguments, because a join compares bucket(4, left.id) against
    // bucket(4, right.id) and recovers the positions separately, while equality does not.
    val left = bucket(new ComparableFunction, a)
    val right = bucket(new ComparableFunction, b)
    assert(left.function ne right.function, "the fixture must use distinct function instances")
    assert(left.function == right.function)
    assert(left.function.canonicalName() == right.function.canonicalName())
    assert(left.isSameFunction(right), "the same partition function, arguments aside")
    assert(left != right, "but not the same expression, since the arguments differ")
  }

  test("SPARK-58769: the function's equals does not override the arguments") {
    // A coarse comparison on the connector's side does not have to carry the whole identity: the
    // transform's arguments and bucket count are compared separately, by Spark.
    assert(bucket(new ComparableFunction, a) != bucket(new ComparableFunction, b),
      "different argument")
    assert(bucket(new ComparableFunction, a, 4) != bucket(new ComparableFunction, a, 8),
      "different bucket count")
  }

  test("SPARK-59121: hasSameReducedKeys recognises the pairing that reduced both sides") {
    // A join that reduces both sides leaves their keys in a space neither transform names, and the
    // pairing that produced it is the only thing that tells two such spaces apart.
    val fn = new NamedFunction("test.bucket")
    val b12 = bucket(fn, a, 12)
    val b8 = bucket(fn, a, 8)
    val b18 = bucket(fn, a, 18)
    val left = b12.reducedTogetherWith(b8)
    val right = b8.reducedTogetherWith(b12)

    assert(TransformExpression.hasReducedKeys(left) && TransformExpression.hasReducedKeys(right))
    assert(left.hasSameReducedKeys(left), "an expression shares its own key space")
    assert(left.hasSameReducedKeys(right), "the two sides of one reduce share theirs")
    assert(right.hasSameReducedKeys(left), "and the relation is symmetric")

    assert(!left.hasSameReducedKeys(b12.reducedTogetherWith(b18)),
      "another pairing is another key space")
    assert(!left.hasSameReducedKeys(b12), "an unreduced side never shares one")
    assert(!b12.hasSameReducedKeys(left))
    assert(!b12.hasSameReducedKeys(b8), "nor do two unreduced ones")

    // The marker rides on the expression, so it survives the attribute rewrites a projection and
    // `GroupPartitionsExec` apply to a reported partitioning.
    assert(left.hasSameReducedKeys(left.withReference(b)))
  }

  test("SPARK-50593: hasSameReducedKeys discriminates non-bucket (truncate-style) reductions") {
    // Truncate reductions onto different key spaces must not compare equal: 3 with 5 lands on
    // lcm 15, and 7 with 11 on lcm 77. They would if identity ignored the width.
    val fn = new NamedFunction("test.truncate")
    def truncate(width: Int): TransformExpression = bucket(fn, a, width)

    val trunc3x5 = truncate(3).reducedTogetherWith(truncate(5))
    val trunc5x3 = truncate(5).reducedTogetherWith(truncate(3))
    val trunc7x11 = truncate(7).reducedTogetherWith(truncate(11))

    assert(trunc3x5.hasSameReducedKeys(trunc5x3), "the two sides of one reduce share their space")
    assert(!trunc3x5.hasSameReducedKeys(trunc7x11),
      "different truncate widths must reduce onto different key spaces")
    assert(!trunc7x11.hasSameReducedKeys(trunc3x5), "and symmetrically")
  }
  test("SPARK-50593: transform identity is per-argument-position, not a bag of literals") {
    // `KeyedPartitioning.supportsExpressions` requires one *column* child, not one child, so a
    // multi-argument transform reaches identity comparison. If identity collected only the literal
    // values and dropped their positions, `truncate(col, 2)` and `truncate(2, col)` would share a
    // TransformFunctionId: `isSameFunction` would answer true, `isCompatible` would short-circuit
    // on it, and `sameArgumentLayout` -- the guard that exists for exactly this -- would never run,
    // because it only executes inside `reducer`. The pair would then be treated as lined up as it
    // stands and joined with no reduce, pairing partitions that do not hold the same rows.
    val fn = new NamedFunction("test.truncate")
    val colThenLit = TransformExpression(fn, Seq(a, Literal(2)))
    val litThenCol = TransformExpression(fn, Seq(Literal(2), b))

    assert(colThenLit.functionId != litThenCol.functionId,
      "a literal in a different argument position is a different identity")
    assert(!colThenLit.isSameFunction(litThenCol),
      "differing argument layouts are not the same function")
    assert(!litThenCol.isSameFunction(colThenLit), "and symmetrically")
    assert(!colThenLit.isCompatible(litThenCol),
      "isCompatible must not short-circuit two differing layouts into compatible")

    // Same layout, different column: still the same function -- column identity is reconciled
    // separately, through `keyPositions`.
    assert(colThenLit.isSameFunction(TransformExpression(fn, Seq(b, Literal(2)))))
    // Same layout, different literal: still distinguished.
    assert(!colThenLit.isSameFunction(TransformExpression(fn, Seq(a, Literal(3)))))
    // A zero-argument transform is unaffected.
    val days = new NamedFunction("test.days")
    assert(TransformExpression(days, Seq(a)).isSameFunction(TransformExpression(days, Seq(b))))

    // Nested transforms are compared recursively: the same outer function and literal over a
    // different inner transform is not the same function, although both literal lists are [4].
    val years = new NamedFunction("test.years")
    val outer = new NamedFunction("test.bucket")
    val overYears = TransformExpression(outer, Seq(Literal(4), TransformExpression(years, Seq(a))))
    val overDays = TransformExpression(outer, Seq(Literal(4), TransformExpression(days, Seq(a))))
    assert(!overYears.isSameFunction(overDays), "nested transforms must be compared recursively")
    assert(overYears.isSameFunction(
      TransformExpression(outer, Seq(Literal(4), TransformExpression(years, Seq(b))))),
      "the same nested function over a different column is still the same function")

    // A non-reference slot is never "the same", even against an identical one: `c + 1` is not a
    // partition transform argument SPJ can reason about.
    val plusOne = TransformExpression(outer, Seq(Literal(4), Add(a, Literal(1))))
    assert(!plusOne.isSameFunction(TransformExpression(outer, Seq(Literal(4), Add(a, Literal(1))))),
      "a non-reference slot is not comparable, so not the same function")
  }

  test("SPARK-50593: a retargeted struct-field column slot is not reducible, and does not throw") {
    // `withReference` retargets a transform at the other side's key. A GetStructField column slot
    // retargeted at a non-struct attribute leaves GetStructField(intCol, 0), whose `dataType`
    // raises INTERNAL_ERROR rather than answering. `argsMatchInputTypes` is a gate -- it has to
    // report "not reducible" and let the join shuffle, not fail the query.
    val structAttr = AttributeReference("s", StructType(Seq(StructField("f", IntegerType))))()
    val sf = GetStructField(structAttr, 0)
    // Two declared input types, so the arity short-circuit does not hide the dataType read.
    val fn = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "test.truncate2"
      override def canonicalName(): String = name()
    }
    val t = TransformExpression(fn, Seq(sf, Literal(3)))
    val retargeted = t.withReference(AttributeReference("id", IntegerType)())

    assert(!retargeted.argsMatchInputTypes,
      "an unreadable child type must read as not matching, not raise")

    // The same slot retargeted at a compatible struct still matches.
    val otherStruct = AttributeReference("s2", StructType(Seq(StructField("f", IntegerType))))()
    assert(t.withReference(otherStruct).argsMatchInputTypes,
      "a compatible retarget still matches")
  }

  test("SPARK-50593: hasSameReducedKeys tells nested transforms apart, like isSameFunction") {
    // A nested transform is part of the identity, so bucket(4, years(c)) and bucket(4, days(c))
    // reduce onto different key spaces, just as isSameFunction tells them apart.
    val outer = new NamedFunction("test.bucket")
    val years = new NamedFunction("test.years")
    val days = new NamedFunction("test.days")
    val partner = bucket(outer, a, 8)
    val overYears = TransformExpression(outer, Seq(Literal(4), TransformExpression(years, Seq(a))))
    val overDays = TransformExpression(outer, Seq(Literal(4), TransformExpression(days, Seq(a))))

    assert(!overYears.isSameFunction(overDays))
    assert(!overYears.reducedTogetherWith(partner)
      .hasSameReducedKeys(overDays.reducedTogetherWith(partner)),
      "a different nested transform is a different reduced key space")
    assert(overYears.reducedTogetherWith(partner)
      .hasSameReducedKeys(overYears.withReference(b).reducedTogetherWith(partner)),
      "the same nested transform over another column is the same key space")
  }

  test("SPARK-50593: a transform with a non-reference argument has no identity") {
    val fn = new NamedFunction("test.bucket")
    val plusOne = TransformExpression(fn, Seq(Literal(4), Add(a, Literal(1))))
    assert(plusOne.functionId.isEmpty)
    assert(!plusOne.isSameFunction(plusOne), "never the same, not even as itself")
    // Nesting one inside a transform takes the identity away from the outer one too.
    val nested = TransformExpression(fn, Seq(Literal(4), TransformExpression(fn, Seq(Add(a, a)))))
    assert(nested.functionId.isEmpty)

    // Recording a reduce needs an identity on both sides: reducedWith stores the partner's, and
    // silently skipping the mark would report reduced keys as raw. So it fails loudly.
    val ok = bucket(fn, a, 8)
    Seq(() => plusOne.reducedTogetherWith(ok), () => ok.reducedTogetherWith(plusOne)).foreach { f =>
      val e = intercept[SparkException](f())
      assert(e.getMessage.contains("without an identity"))
    }
  }

  test("SPARK-50593: isSameFunction and hasSameReducedKeys agree on every pair") {
    // The two questions are answered from one value, `functionId`. Pin that they cannot drift:
    // for every pair of transforms that have an identity, "same function" and "reduced onto the
    // same key space with the same partner" must give the same answer.
    val bucketFn = new NamedFunction("test.bucket")
    val truncFn = new NamedFunction("test.truncate")
    val years = new NamedFunction("test.years")
    val days = new NamedFunction("test.days")
    val s = AttributeReference("s", StructType(Seq(StructField("f", IntegerType))))()
    val shapes: Seq[TransformExpression] = Seq(
      bucket(bucketFn, a, 4), bucket(bucketFn, b, 4), bucket(bucketFn, a, 8),
      TransformExpression(truncFn, Seq(a, Literal(2))),
      TransformExpression(truncFn, Seq(Literal(2), b)),
      TransformExpression(truncFn, Seq(a, Literal(3))),
      TransformExpression(bucketFn, Seq(Literal(4), GetStructField(s, 0))),
      TransformExpression(bucketFn, Seq(Literal(4), TransformExpression(years, Seq(a)))),
      TransformExpression(bucketFn, Seq(Literal(4), TransformExpression(days, Seq(b)))),
      TransformExpression(years, Seq(a)), TransformExpression(days, Seq(a)))
    assert(shapes.forall(_.functionId.isDefined), "the fixture needs identities throughout")
    val partner = bucket(bucketFn, a, 16)
    for (x <- shapes; y <- shapes) {
      val sameFunction = x.isSameFunction(y)
      val sameKeys =
        x.reducedTogetherWith(partner).hasSameReducedKeys(y.reducedTogetherWith(partner))
      assert(sameFunction == sameKeys, s"disagree on $x vs $y: $sameFunction vs $sameKeys")
    }
  }

}
