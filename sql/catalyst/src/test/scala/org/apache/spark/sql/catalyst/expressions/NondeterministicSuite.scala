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
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, KeyGroupedPartitioning, RangePartitioning}

class NondeterministicSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("MonotonicallyIncreasingID") {
    checkEvaluation(MonotonicallyIncreasingID(), 0L)
  }

  test("SparkPartitionID") {
    checkEvaluation(SparkPartitionID(), 0)
  }

  test("InputFileName") {
    checkEvaluation(InputFileName(), "")
  }

  test("SPARK-51016: has Indeterministic Component") {
    def assertIndeterminancyComponent(expression: Expression): Unit =
      assert(prepareEvaluation(expression).hasIndeterminism)

    assertIndeterminancyComponent(MonotonicallyIncreasingID())
    val alias = Alias(Multiply(MonotonicallyIncreasingID(), Literal(100L)), "al1")()
    assertIndeterminancyComponent(alias)
    // For the attribute created from an Alias with deterministic flag false, the attribute would
    // carry forward that information from Alias, via the hasIndeterminism flag value being true.
    assertIndeterminancyComponent(alias.toAttribute)
    // But the Attribute's deterministic flag would be true ( implying it does not carry forward
    // that inDeterministic nature of evaluated quantity which Attribute represents)
    assert(prepareEvaluation(alias.toAttribute).deterministic)

    assertIndeterminancyComponent(Multiply(alias.toAttribute, Literal(1000L)))
    assertIndeterminancyComponent(
      HashPartitioning(Seq(Multiply(MonotonicallyIncreasingID(), Literal(100L))), 5))
    assertIndeterminancyComponent(HashPartitioning(Seq(alias.toAttribute), 5))
    assertIndeterminancyComponent(
      RangePartitioning(Seq(SortOrder.apply(alias.toAttribute, Descending)), 5))
    assertIndeterminancyComponent(KeyGroupedPartitioning(Seq(alias.toAttribute), 5))
  }

  test("SPARK-51016: has Deterministic Component") {
    def assertNoIndeterminancyComponent(expression: Expression): Unit =
      assert(!prepareEvaluation(expression).hasIndeterminism)

    assertNoIndeterminancyComponent(Literal(1000L))
    val alias = Alias(Multiply(Literal(10000L), Literal(100L)), "al1")()
    assertNoIndeterminancyComponent(alias)
    assertNoIndeterminancyComponent(alias.toAttribute)
    assertNoIndeterminancyComponent(
      HashPartitioning(Seq(Multiply(Literal(10L), Literal(100L))), 5))
    assertNoIndeterminancyComponent(HashPartitioning(Seq(alias.toAttribute), 5))
    assertNoIndeterminancyComponent(
      RangePartitioning(Seq(SortOrder.apply(alias.toAttribute, Descending)), 5))
    assertNoIndeterminancyComponent(KeyGroupedPartitioning(Seq(alias.toAttribute), 5))
  }


}
