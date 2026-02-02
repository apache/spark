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
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, HintInfo, Join, JoinHint, NO_BROADCAST_HASH, SHUFFLE_HASH}
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.internal.SQLConf

class JoinSelectionHelperSuite extends PlanTest with JoinSelectionHelper {

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

}
