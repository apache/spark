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

package org.apache.spark.sql.catalyst

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, AttributeSet, InterpretedMutableProjection, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.DataTypes

class PartitioningSuite extends SparkFunSuite {
  test("HashPartitioning compatibility should be sensitive to expression ordering (SPARK-9785)") {
    val expressions = Seq(Literal(2), Literal(3))
    // Consider two HashPartitionings that have the same _set_ of hash expressions but which are
    // created with different orderings of those expressions:
    val partitioningA = HashPartitioning(expressions, 100)
    val partitioningB = HashPartitioning(expressions.reverse, 100)
    // These partitionings are not considered equal:
    assert(partitioningA != partitioningB)
    // However, they both satisfy the same clustered distribution:
    val distribution = ClusteredDistribution(expressions)
    assert(partitioningA.satisfies(distribution))
    assert(partitioningB.satisfies(distribution))
    // These partitionings compute different hashcodes for the same input row:
    def computeHashCode(partitioning: HashPartitioning): Int = {
      val hashExprProj = new InterpretedMutableProjection(partitioning.expressions, Seq.empty)
      hashExprProj.apply(InternalRow.empty).hashCode()
    }
    assert(computeHashCode(partitioningA) != computeHashCode(partitioningB))
    // Thus, these partitionings are incompatible:
    assert(!partitioningA.compatibleWith(partitioningB))
    assert(!partitioningB.compatibleWith(partitioningA))
    assert(!partitioningA.guarantees(partitioningB))
    assert(!partitioningB.guarantees(partitioningA))

    // Just to be sure that we haven't cheated by having these methods always return false,
    // check that identical partitionings are still compatible with and guarantee each other:
    assert(partitioningA === partitioningA)
    assert(partitioningA.guarantees(partitioningA))
    assert(partitioningA.compatibleWith(partitioningA))
  }

  test("restriction of Partitioning works") {
    val n = 5

    val a1 = AttributeReference("a1", DataTypes.IntegerType)()
    val a2 = AttributeReference("a2", DataTypes.IntegerType)()
    val a3 = AttributeReference("a3", DataTypes.IntegerType)()

    val hashPartitioning = HashPartitioning(Seq(a1, a2), n)

    assert(hashPartitioning.restrict(AttributeSet(Seq())) === UnknownPartitioning(n))
    assert(hashPartitioning.restrict(AttributeSet(Seq(a1))) === UnknownPartitioning(n))
    assert(hashPartitioning.restrict(AttributeSet(Seq(a1, a2))) === hashPartitioning)
    assert(hashPartitioning.restrict(AttributeSet(Seq(a1, a2, a3))) === hashPartitioning)

    val so1 = SortOrder(a1, Ascending)
    val so2 = SortOrder(a2, Ascending)

    val rangePartitioning1 = RangePartitioning(Seq(so1), n)
    val rangePartitioning2 = RangePartitioning(Seq(so1, so2), n)

    assert(rangePartitioning2.restrict(AttributeSet(Seq())) == UnknownPartitioning(n))
    assert(rangePartitioning2.restrict(AttributeSet(Seq(a1))) == UnknownPartitioning(n))
    assert(rangePartitioning2.restrict(AttributeSet(Seq(a1, a2))) === rangePartitioning2)
    assert(rangePartitioning2.restrict(AttributeSet(Seq(a1, a2, a3))) === rangePartitioning2)

    assert(SinglePartition.restrict(AttributeSet(a1)) === SinglePartition)

    val all = Seq(hashPartitioning, rangePartitioning1, rangePartitioning2)
    val partitioningCollection = PartitioningCollection(all)

    assert(partitioningCollection.restrict(AttributeSet(Seq())) == UnknownPartitioning(n))
    assert(partitioningCollection.restrict(AttributeSet(Seq(a1))) == rangePartitioning1)
    assert(partitioningCollection.restrict(AttributeSet(Seq(a1, a2))) == partitioningCollection)
    assert(partitioningCollection.restrict(AttributeSet(Seq(a1, a2, a3))) == partitioningCollection)

  }
}
