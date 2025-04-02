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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.JoinConditionSplitPredicates
import org.apache.spark.sql.types._

class StreamingSymmetricHashJoinHelperSuite extends StreamTest {
  val leftAttributeA = AttributeReference("a", IntegerType)()
  val leftAttributeB = AttributeReference("b", IntegerType)()
  val rightAttributeC = AttributeReference("c", IntegerType)()
  val rightAttributeD = AttributeReference("d", IntegerType)()

  val left = new LocalTableScanExec(Seq(leftAttributeA, leftAttributeB), Seq(), None)
  val right = new LocalTableScanExec(Seq(rightAttributeC, rightAttributeD), Seq(), None)

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
    val predicate = Literal(1) < Literal(5) && Literal(6) < Literal(7) && Literal(0) === Literal(-1)
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(predicate))
    assert(split.rightSideOnly.contains(predicate))
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only left") {
    val predicate =
      leftAttributeA > Literal(1) && leftAttributeB > Literal(5) && leftAttributeA < leftAttributeB
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(predicate))
    assert(split.rightSideOnly.isEmpty)
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only right") {
    val predicate = rightAttributeC > Literal(1) && rightAttributeD > Literal(5) &&
      rightAttributeD < rightAttributeC
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.isEmpty)
    assert(split.rightSideOnly.contains(predicate))
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("mixed conjuncts") {
    val predicate =
      (leftAttributeA > leftAttributeB
        && rightAttributeC > rightAttributeD
        && leftAttributeA === rightAttributeC
        && Literal(1) === Literal(1))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      leftAttributeA > leftAttributeB && Literal(1) === Literal(1)))
    assert(split.rightSideOnly.contains(
      rightAttributeC > rightAttributeD && Literal(1) === Literal(1)))
    assert(split.bothSides.contains((leftAttributeA === rightAttributeC)))
    assert(split.full.contains(predicate))
  }

  test("conjuncts after nondeterministic") {
    val predicate =
      (rand(9) > Literal(0)
        && leftAttributeA > leftAttributeB
        && rightAttributeC > rightAttributeD
        && leftAttributeA === rightAttributeC
        && Literal(1) === Literal(1))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      leftAttributeA > leftAttributeB && Literal(1) === Literal(1)))
    assert(split.rightSideOnly.contains(
      rightAttributeC > rightAttributeD && Literal(1) === Literal(1)))
    assert(split.bothSides.contains(
      leftAttributeA === rightAttributeC && rand(9).expr > Literal(0)))
    assert(split.full.contains(predicate))
  }


  test("conjuncts before nondeterministic") {
    val randAttribute = rand(0)
    val predicate =
      (leftAttributeA > leftAttributeB
        && rightAttributeC > rightAttributeD
        && leftAttributeA === rightAttributeC
        && Literal(1) === Literal(1)
        && randAttribute > Literal(0))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      leftAttributeA > leftAttributeB && Literal(1) === Literal(1)))
    assert(split.rightSideOnly.contains(
      rightAttributeC > rightAttributeD && Literal(1) === Literal(1)))
    assert(split.bothSides.contains(
      leftAttributeA === rightAttributeC && randAttribute > Literal(0)))
    assert(split.full.contains(predicate))
  }
}
