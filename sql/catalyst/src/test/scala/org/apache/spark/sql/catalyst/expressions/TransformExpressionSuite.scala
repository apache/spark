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
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction}
import org.apache.spark.sql.types.{DataType, IntegerType}

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
    TransformExpression(function, Seq(child), Some(numBuckets))

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
}
