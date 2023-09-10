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

package org.apache.spark.sql.streaming

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, GreaterThan, LessThan}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.JoinConditionSplitPredicates
import org.apache.spark.sql.types._

class StreamingSymmetricHashJoinHelperSuite extends StreamTest {
  import org.apache.spark.sql.functions._

  val leftAttributeA = AttributeReference("a", IntegerType)()
  val leftAttributeB = AttributeReference("b", IntegerType)()
  val rightAttributeC = AttributeReference("c", IntegerType)()
  val rightAttributeD = AttributeReference("d", IntegerType)()

  val left = new LocalTableScanExec(Seq(leftAttributeA, leftAttributeB), Seq())
  val right = new LocalTableScanExec(Seq(rightAttributeC, rightAttributeD), Seq())

  test("empty") {
    val split = JoinConditionSplitPredicates(None, left, right)
    assert(split.leftSideOnly.isEmpty)
    assert(split.rightSideOnly.isEmpty)
    assert(split.bothSides.isEmpty)
    assert(split.full.isEmpty)
  }

  test("only literals") {
    // Literal-only conjuncts end up on the left side because that's the first bucket they fit in.
    // There's no semantic reason they couldn't be in any bucket.
    val predicate =
      And(
        And(
          LessThan(lit(1).expr, lit(5).expr),
          LessThan(lit(6).expr, lit(7).expr)),
        EqualTo(lit(0).expr, lit(-1).expr))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(predicate))
    assert(split.rightSideOnly.contains(predicate))
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only left") {
    val predicate =
      And(
        And(
          GreaterThan(leftAttributeA, lit(1).expr),
          GreaterThan(leftAttributeB, lit(5).expr)),
        LessThan(leftAttributeA, leftAttributeB))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(predicate))
    assert(split.rightSideOnly.isEmpty)
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only right") {
    val predicate =
      And(
        And(
          GreaterThan(rightAttributeC, lit(1).expr),
          GreaterThan(rightAttributeD, lit(5).expr)),
        LessThan(rightAttributeD, rightAttributeC))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.isEmpty)
    assert(split.rightSideOnly.contains(predicate))
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("mixed conjuncts") {
    val predicate =
      And(
        And(
          And(
            GreaterThan(leftAttributeA, leftAttributeB),
            GreaterThan(rightAttributeC, rightAttributeD)),
          EqualTo(leftAttributeA, rightAttributeC)),
        EqualTo(lit(1).expr, lit(1).expr))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      And(GreaterThan(leftAttributeA, leftAttributeB), EqualTo(lit(1).expr, lit(1).expr))))
    assert(split.rightSideOnly.contains(
      And(GreaterThan(rightAttributeC, rightAttributeD), EqualTo(lit(1).expr, lit(1).expr))))
    assert(split.bothSides.contains(EqualTo(leftAttributeA, rightAttributeC)))
    assert(split.full.contains(predicate))
  }

  test("conjuncts after nondeterministic") {
    val predicate =
      And(
        And(
          And(
            And(
              GreaterThan(rand(9).expr, lit(0).expr),
              GreaterThan(leftAttributeA, leftAttributeB)),
            GreaterThan(rightAttributeC, rightAttributeD)),
          EqualTo(leftAttributeA, rightAttributeC)),
        EqualTo(lit(1).expr, lit(1).expr))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      And(GreaterThan(leftAttributeA, leftAttributeB), EqualTo(lit(1).expr, lit(1).expr))))
    assert(split.rightSideOnly.contains(
      And(GreaterThan(rightAttributeC, rightAttributeD), EqualTo(lit(1).expr, lit(1).expr))))
    assert(split.bothSides.contains(
      And(EqualTo(leftAttributeA, rightAttributeC), GreaterThan(rand(9).expr, lit(0).expr))))
    assert(split.full.contains(predicate))
  }


  test("conjuncts before nondeterministic") {
    val randCol = rand()
    val predicate =
      And(
        And(
          And(
            And(
              GreaterThan(leftAttributeA, leftAttributeB),
              GreaterThan(rightAttributeC, rightAttributeD)),
            EqualTo(leftAttributeA, rightAttributeC)),
          EqualTo(lit(1).expr, lit(1).expr)),
        GreaterThan(randCol.expr, lit(0).expr))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      And(GreaterThan(leftAttributeA, leftAttributeB), EqualTo(lit(1).expr, lit(1).expr))))
    assert(split.rightSideOnly.contains(
      And(GreaterThan(rightAttributeC, rightAttributeD), EqualTo(lit(1).expr, lit(1).expr))))
    assert(split.bothSides.contains(
      And(EqualTo(leftAttributeA, rightAttributeC), GreaterThan(randCol.expr, lit(0).expr))))
    assert(split.full.contains(predicate))
  }
}
