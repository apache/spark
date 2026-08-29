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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, EqualTo, IsNull, Or}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, HintInfo, Join, JoinHint, LeafNode, NO_BROADCAST_HASH, SHUFFLE_HASH, Statistics}
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.map.BytesToBytesMap

class JoinSelectionHelperSuite extends PlanTest with JoinSelectionHelper {

  private case class UnknownRowCountTestPlan(override val output: Seq[Attribute])
      extends LeafNode {
    override def computeStats(): Statistics = Statistics(sizeInBytes = 1000)
  }

  private val left = StatsTestPlan(
    outputList = Seq($"a".int, $"b".int, $"c".int),
    rowCount = 20000000,
    size = Some(20000000),
    attributeStats = AttributeMap(Seq()))

  private val right = StatsTestPlan(
    outputList = Seq($"d".int),
    rowCount = 1000,
    size = Some(1000),
    attributeStats = AttributeMap(Seq()))

  private val join = Join(left, right, Inner, None, JoinHint(None, None))

  private val hintBroadcast = Some(HintInfo(Some(BROADCAST)))
  private val hintNotToBroadcast = Some(HintInfo(Some(NO_BROADCAST_HASH)))
  private val hintShuffleHash = Some(HintInfo(Some(SHUFFLE_HASH)))

  test("getBroadcastBuildSide (hintOnly = true) return BuildLeft with only a left hint") {
    val broadcastSide = getBroadcastBuildSide(
      join.copy(hint = JoinHint(hintBroadcast, None)),
      hintOnly = true,
      SQLConf.get
    )
    assert(broadcastSide === Some(BuildLeft))
  }

  test("getBroadcastBuildSide (hintOnly = true) return BuildRight with only a right hint") {
    val broadcastSide = getBroadcastBuildSide(
      join.copy(hint = JoinHint(None, hintBroadcast)),
      hintOnly = true,
      SQLConf.get
    )
    assert(broadcastSide === Some(BuildRight))
  }

  test("getBroadcastBuildSide (hintOnly = true) return smaller side with both having hints") {
    val broadcastSide = getBroadcastBuildSide(
      join.copy(hint = JoinHint(hintBroadcast, hintBroadcast)),
      hintOnly = true,
      SQLConf.get
    )
    assert(broadcastSide === Some(BuildRight))
  }

  test("getBroadcastBuildSide (hintOnly = true) return None when no side has a hint") {
    val broadcastSide = getBroadcastBuildSide(
      join.copy(hint = JoinHint(None, None)),
      hintOnly = true,
      SQLConf.get
    )
    assert(broadcastSide === None)
  }

  test("getBroadcastBuildSide (hintOnly = false) return BuildRight when right is broadcastable") {
    val broadcastSide = getBroadcastBuildSide(
      join.copy(hint = JoinHint(None, None)),
      hintOnly = false,
      SQLConf.get
    )
    assert(broadcastSide === Some(BuildRight))
  }

  test("getBroadcastBuildSide (hintOnly = false) return None when right has no broadcast hint") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      val broadcastSide = getBroadcastBuildSide(
        join.copy(hint = JoinHint(None, hintNotToBroadcast)),
        hintOnly = false,
        SQLConf.get
      )
      assert(broadcastSide === None)
    }
  }

  test("getShuffleHashJoinBuildSide (hintOnly = true) return BuildLeft with only a left hint") {
    val broadcastSide = getShuffleHashJoinBuildSide(
      join.copy(hint = JoinHint(hintShuffleHash, None)),
      hintOnly = true,
      SQLConf.get
    )
    assert(broadcastSide === Some(BuildLeft))
  }

  test("getShuffleHashJoinBuildSide (hintOnly = true) return BuildRight with only a right hint") {
    val broadcastSide = getShuffleHashJoinBuildSide(
      join.copy(hint = JoinHint(None, hintShuffleHash)),
      hintOnly = true,
      SQLConf.get
    )
    assert(broadcastSide === Some(BuildRight))
  }

  test("getShuffleHashJoinBuildSide (hintOnly = true) return smaller side when both have hints") {
    val broadcastSide = getShuffleHashJoinBuildSide(
      join.copy(hint = JoinHint(hintShuffleHash, hintShuffleHash)),
      hintOnly = true,
      SQLConf.get
    )
    assert(broadcastSide === Some(BuildRight))
  }

  test("getShuffleHashJoinBuildSide (hintOnly = true) return None when no side has a hint") {
    val broadcastSide = getShuffleHashJoinBuildSide(
      join.copy(hint = JoinHint(None, None)),
      hintOnly = true,
      SQLConf.get
    )
    assert(broadcastSide === None)
  }

  test("getShuffleHashJoinBuildSide (hintOnly = false) return BuildRight when right is smaller") {
    val broadcastSide = getBroadcastBuildSide(
      join.copy(hint = JoinHint(None, None)),
      hintOnly = false,
      SQLConf.get
    )
    assert(broadcastSide === Some(BuildRight))
  }

  test("getSmallerSide should return BuildRight") {
    assert(getSmallerSide(left, right) === BuildRight)
  }

  test("canBroadcastBySize should return true if the plan size is less than 10MB") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      assert(canBroadcastBySize(left, SQLConf.get) === false)
      assert(canBroadcastBySize(right, SQLConf.get) === true)
    }
  }

  test("canPlanAsBroadcastHashJoin should respect NAAJ nested-loop build side") {
    val leftKey = left.output.head
    val rightKey = right.output.head
    val condition = Or(EqualTo(leftKey, rightKey), IsNull(EqualTo(leftKey, rightKey)))
    val nullAwareAntiJoin = Join(left, right, LeftAnti, Some(condition), JoinHint.NONE)
    val smallLeft = left.copy(rowCount = 1000, size = Some(1000))
    val largeRight = right.copy(rowCount = 20000000, size = Some(20000000))

    withSQLConf(
      SQLConf.OPTIMIZE_NULL_AWARE_ANTI_JOIN.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      assert(canPlanAsBroadcastHashJoin(nullAwareAntiJoin, SQLConf.get))
      assert(canPlanAsBroadcastHashJoin(nullAwareAntiJoin.copy(right = largeRight), SQLConf.get))
      assert(!canPlanAsBroadcastHashJoin(
        nullAwareAntiJoin.copy(left = smallLeft, right = largeRight), SQLConf.get))
      assert(!canPlanAsBroadcastHashJoin(
        nullAwareAntiJoin.copy(hint = JoinHint(hintBroadcast, None)), SQLConf.get))
    }
  }

  test("canPlanAsBroadcastHashJoin should respect the NAAJ hashed relation row limit") {
    val maxBroadcastHashRows = (BytesToBytesMap.MAX_CAPACITY / 1.5).toLong
    val leftKey = left.output.head
    val rightKey = right.output.head
    val condition = Or(EqualTo(leftKey, rightKey), IsNull(EqualTo(leftKey, rightKey)))
    val nullAwareAntiJoin = Join(left, right, LeftAnti, Some(condition), JoinHint.NONE)
    val rightBelowLimit = right.copy(rowCount = maxBroadcastHashRows - 1)
    val rightAtLimit = right.copy(rowCount = maxBroadcastHashRows)

    val unknownRight = UnknownRowCountTestPlan(Seq($"unknownRight".int))
    val unknownCondition = Or(
      EqualTo(leftKey, unknownRight.output.head),
      IsNull(EqualTo(leftKey, unknownRight.output.head)))
    val unknownRowCountJoin = Join(
      left, unknownRight, LeftAnti, Some(unknownCondition), JoinHint.NONE)

    val longLeft = StatsTestPlan(
      Seq($"longLeft".long), 20000000, AttributeMap(Seq()), Some(20000000))
    val longRight = StatsTestPlan(
      Seq($"longRight".long), maxBroadcastHashRows, AttributeMap(Seq()), Some(1000))
    val longCondition = Or(
      EqualTo(longLeft.output.head, longRight.output.head),
      IsNull(EqualTo(longLeft.output.head, longRight.output.head)))
    val longKeyJoin = Join(longLeft, longRight, LeftAnti, Some(longCondition), JoinHint.NONE)

    withSQLConf(
      SQLConf.OPTIMIZE_NULL_AWARE_ANTI_JOIN.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      assert(canPlanAsBroadcastHashJoin(
        nullAwareAntiJoin.copy(right = rightBelowLimit), SQLConf.get))
      assert(!canPlanAsBroadcastHashJoin(
        nullAwareAntiJoin.copy(right = rightAtLimit), SQLConf.get))
      assert(canPlanAsBroadcastHashJoin(unknownRowCountJoin, SQLConf.get))
      assert(canPlanAsBroadcastHashJoin(longKeyJoin, SQLConf.get))
    }
  }

  test("getBroadcastHashJoinBuildSide returns the hinted side") {
    val equiJoin = join.copy(condition = Some(EqualTo(left.output.head, right.output.head)))
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      assert(getBroadcastHashJoinBuildSide(
        equiJoin.copy(hint = JoinHint(hintBroadcast, None)), SQLConf.get) === Some(BuildLeft))
      assert(getBroadcastHashJoinBuildSide(
        equiJoin.copy(hint = JoinHint(None, hintBroadcast)), SQLConf.get) === Some(BuildRight))
      // Both sides hinted: the smaller one wins, as in `getBroadcastBuildSide`.
      assert(getBroadcastHashJoinBuildSide(
        equiJoin.copy(hint = JoinHint(hintBroadcast, hintBroadcast)), SQLConf.get) ===
        Some(BuildRight))
    }
  }

  test("getBroadcastHashJoinBuildSide falls back to the smaller side") {
    val equiJoin = join.copy(condition = Some(EqualTo(left.output.head, right.output.head)))
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      // Only the right side is under the threshold, so it is the only candidate.
      assert(getBroadcastHashJoinBuildSide(equiJoin, SQLConf.get) === Some(BuildRight))
    }
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      assert(getBroadcastHashJoinBuildSide(equiJoin, SQLConf.get).isEmpty)
    }
  }

  test("getBroadcastHashJoinBuildSide returns None when a shuffle hash hint applies") {
    val equiJoin = join.copy(condition = Some(EqualTo(left.output.head, right.output.head)))
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      assert(getBroadcastHashJoinBuildSide(
        equiJoin.copy(hint = JoinHint(None, hintShuffleHash)), SQLConf.get).isEmpty)
    }
  }

  test("getBroadcastHashJoinBuildSide returns None without equi-join keys") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      assert(getBroadcastHashJoinBuildSide(join, SQLConf.get).isEmpty)
    }
  }

  test("getBroadcastHashJoinBuildSide builds from the right for a null-aware anti join") {
    val leftKey = left.output.head
    val rightKey = right.output.head
    val condition = Or(EqualTo(leftKey, rightKey), IsNull(EqualTo(leftKey, rightKey)))
    val nullAwareAntiJoin = Join(left, right, LeftAnti, Some(condition), JoinHint.NONE)

    withSQLConf(
      SQLConf.OPTIMIZE_NULL_AWARE_ANTI_JOIN.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      assert(getBroadcastHashJoinBuildSide(nullAwareAntiJoin, SQLConf.get) === Some(BuildRight))
    }
  }

}
