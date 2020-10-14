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

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.JoinConditionSplitPredicates
import org.apache.spark.sql.types._

class StreamingSymmetricHashJoinHelperSuite extends StreamTest {
  import org.apache.spark.sql.functions._

  val leftAttributeA = AttributeReference("a", IntegerType)()
  val leftAttributeB = AttributeReference("b", IntegerType)()
  val rightAttributeC = AttributeReference("c", IntegerType)()
  val rightAttributeD = AttributeReference("d", IntegerType)()
  val leftColA = new Column(leftAttributeA)
  val leftColB = new Column(leftAttributeB)
  val rightColC = new Column(rightAttributeC)
  val rightColD = new Column(rightAttributeD)

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
    val predicate = (lit(1) < lit(5) && lit(6) < lit(7) && lit(0) === lit(-1)).expr
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(predicate))
    assert(split.rightSideOnly.contains(predicate))
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only left") {
    val predicate = (leftColA > lit(1) && leftColB > lit(5) && leftColA < leftColB).expr
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(predicate))
    assert(split.rightSideOnly.isEmpty)
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only right") {
    val predicate = (rightColC > lit(1) && rightColD > lit(5) && rightColD < rightColC).expr
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.isEmpty)
    assert(split.rightSideOnly.contains(predicate))
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("mixed conjuncts") {
    val predicate =
      (leftColA > leftColB
        && rightColC > rightColD
        && leftColA === rightColC
        && lit(1) === lit(1)).expr
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains((leftColA > leftColB && lit(1) === lit(1)).expr))
    assert(split.rightSideOnly.contains((rightColC > rightColD && lit(1) === lit(1)).expr))
    assert(split.bothSides.contains((leftColA === rightColC).expr))
    assert(split.full.contains(predicate))
  }

  test("conjuncts after nondeterministic") {
    val predicate =
      (rand(9) > lit(0)
        && leftColA > leftColB
        && rightColC > rightColD
        && leftColA === rightColC
        && lit(1) === lit(1)).expr
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains((leftColA > leftColB && lit(1) === lit(1)).expr))
    assert(split.rightSideOnly.contains((rightColC > rightColD && lit(1) === lit(1)).expr))
    assert(split.bothSides.contains((leftColA === rightColC && rand(9) > lit(0)).expr))
    assert(split.full.contains(predicate))
  }


  test("conjuncts before nondeterministic") {
    val randCol = rand()
    val predicate =
      (leftColA > leftColB
        && rightColC > rightColD
        && leftColA === rightColC
        && lit(1) === lit(1)
        && randCol > lit(0)).expr
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains((leftColA > leftColB && lit(1) === lit(1)).expr))
    assert(split.rightSideOnly.contains((rightColC > rightColD && lit(1) === lit(1)).expr))
    assert(split.bothSides.contains((leftColA === rightColC && randCol > lit(0)).expr))
    assert(split.full.contains(predicate))
  }
}
