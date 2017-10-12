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
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.{LeafExecNode, LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.JoinConditionSplitPredicates
import org.apache.spark.sql.types.DataTypes

class StreamingSymmetricHashJoinHelperSuite extends StreamTest {
  import org.apache.spark.sql.functions._

  val attributeA = AttributeReference("a", DataTypes.IntegerType)()
  val attributeB = AttributeReference("b", DataTypes.IntegerType)()
  val attributeC = AttributeReference("c", DataTypes.IntegerType)()
  val attributeD = AttributeReference("d", DataTypes.IntegerType)()
  val colA = new Column(attributeA)
  val colB = new Column(attributeB)
  val colC = new Column(attributeC)
  val colD = new Column(attributeD)

  val left = new LocalTableScanExec(Seq(attributeA, attributeB), Seq())
  val right = new LocalTableScanExec(Seq(attributeC, attributeD), Seq())

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
    assert(split.rightSideOnly.isEmpty)
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only left") {
    val predicate = (colA > lit(1) && colB > lit(5) && colA < colB).expr
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(predicate))
    assert(split.rightSideOnly.isEmpty)
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only right") {
    val predicate = (colC > lit(1) && colD > lit(5) && colD < colC).expr
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.isEmpty)
    assert(split.rightSideOnly.contains(predicate))
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("mixed conjuncts") {
    val predicate = (colA > colB && colC > colD && colA === colC && lit(1) === lit(1)).expr
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains((colA > colB && lit(1) === lit(1)).expr))
    assert(split.rightSideOnly.contains((colC > colD).expr))
    assert(split.bothSides.contains((colA === colC).expr))
    assert(split.full.contains(predicate))
  }

  test("conjuncts after nondeterministic") {
    // All conjuncts after a nondeterministic conjunct shouldn't be split because they don't
    // commute across it.
    val predicate =
      (rand() > lit(0) && colA > colB && colC > colD && colA === colC && lit(1) === lit(1)).expr
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.isEmpty)
    assert(split.rightSideOnly.isEmpty)
    assert(split.bothSides.contains(predicate))
    assert(split.full.contains(predicate))
  }


  test("conjuncts before nondeterministic") {
    val randCol = rand()
    val predicate =
      (colA > colB && colC > colD && colA === colC && lit(1) === lit(1) && randCol > lit(0)).expr
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains((colA > colB && lit(1) === lit(1)).expr))
    assert(split.rightSideOnly.contains((colC > colD).expr))
    assert(split.bothSides.contains((colA === colC && randCol > lit(0)).expr))
    assert(split.full.contains(predicate))
  }
}
