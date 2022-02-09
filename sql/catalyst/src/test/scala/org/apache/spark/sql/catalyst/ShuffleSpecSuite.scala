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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.internal.SQLConf

class ShuffleSpecSuite extends SparkFunSuite with SQLHelper {
  protected def checkCompatible(
      left: ShuffleSpec,
      right: ShuffleSpec,
      expected: Boolean): Unit = {
    val actual = left.isCompatibleWith(right)
    if (actual != expected) {
      fail(
        s"""
           |== Left ShuffleSpec
           |$left
           |== Right ShuffleSpec
           |$right
           |== Is left compatible with right? ==
           |Expected $expected but got $actual
           |""".stripMargin
      )
    }
  }

  protected def checkCreatePartitioning(
      spec: ShuffleSpec,
      dist: ClusteredDistribution,
      expected: Partitioning): Unit = {
    val actual = spec.createPartitioning(dist.clustering)
    if (actual != expected) {
      fail(
        s"""
           |== ShuffleSpec
           |$spec
           |== Distribution
           |$dist
           |== Result ==
           |Expected $expected but got $actual
           |""".stripMargin
      )
    }
  }

  test("compatibility: HashShuffleSpec on both sides") {
    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      expected = true
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a"), 10), ClusteredDistribution(Seq($"a", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"a"), 10), ClusteredDistribution(Seq($"a", $"b"))),
      expected = true
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"b"), 10), ClusteredDistribution(Seq($"a", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"d"), 10), ClusteredDistribution(Seq($"c", $"d"))),
      expected = true
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"c", $"c", $"d"), 10),
        ClusteredDistribution(Seq($"c", $"d"))),
      expected = true
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b", $"a"), 10),
        ClusteredDistribution(Seq($"a", $"b", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"a", $"c", $"a"), 10),
        ClusteredDistribution(Seq($"a", $"c", $"c"))),
      expected = true
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b", $"a"), 10),
        ClusteredDistribution(Seq($"a", $"b", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"a", $"c", $"a"), 10),
        ClusteredDistribution(Seq($"a", $"c", $"d"))),
      expected = true
    )

    // negative cases
    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"c"), 5),
        ClusteredDistribution(Seq($"c", $"d"))),
      expected = false
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      expected = false
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      expected = false
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"d"), 10),
        ClusteredDistribution(Seq($"c", $"d"))),
      expected = false
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"d"), 10),
        ClusteredDistribution(Seq($"c", $"d"))),
      expected = false
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b", $"a"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      expected = false
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b", $"a"), 10),
        ClusteredDistribution(Seq($"a", $"b", $"b"))),
      expected = false
    )
  }

  test("compatibility: Only one side is HashShuffleSpec") {
    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      SinglePartitionShuffleSpec,
      expected = false
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 1),
        ClusteredDistribution(Seq($"a", $"b"))),
      SinglePartitionShuffleSpec,
      expected = true
    )

    checkCompatible(
      SinglePartitionShuffleSpec,
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 1),
        ClusteredDistribution(Seq($"a", $"b"))),
      expected = true
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      RangeShuffleSpec(10, ClusteredDistribution(Seq($"a", $"b"))),
      expected = false
    )

    checkCompatible(
      RangeShuffleSpec(10, ClusteredDistribution(Seq($"a", $"b"))),
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      expected = false
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      ShuffleSpecCollection(Seq(
        HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))))),
      expected = true
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      ShuffleSpecCollection(Seq(
        HashShuffleSpec(HashPartitioning(Seq($"a"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))))),
      expected = true
    )

    checkCompatible(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
        ClusteredDistribution(Seq($"a", $"b"))),
      ShuffleSpecCollection(Seq(
        HashShuffleSpec(HashPartitioning(Seq($"a"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        HashShuffleSpec(HashPartitioning(Seq($"a", $"b", $"c"), 10),
          ClusteredDistribution(Seq($"a", $"b", $"c"))))),
      expected = false
    )

    checkCompatible(
      ShuffleSpecCollection(Seq(
        HashShuffleSpec(HashPartitioning(Seq($"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))))),
      ShuffleSpecCollection(Seq(
        HashShuffleSpec(HashPartitioning(Seq($"a", $"b", $"c"), 10),
          ClusteredDistribution(Seq($"a", $"b", $"c"))),
        HashShuffleSpec(HashPartitioning(Seq($"d"), 10),
          ClusteredDistribution(Seq($"c", $"d"))))),
      expected = true
    )

    checkCompatible(
      ShuffleSpecCollection(Seq(
        HashShuffleSpec(HashPartitioning(Seq($"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))))),
      ShuffleSpecCollection(Seq(
        HashShuffleSpec(HashPartitioning(Seq($"a", $"b", $"c"), 10),
          ClusteredDistribution(Seq($"a", $"b", $"c"))),
        HashShuffleSpec(HashPartitioning(Seq($"c"), 10),
          ClusteredDistribution(Seq($"c", $"d"))))),
      expected = false
    )
  }

  test("compatibility: other specs") {
    checkCompatible(
      SinglePartitionShuffleSpec, SinglePartitionShuffleSpec, expected = true
    )

    checkCompatible(
      SinglePartitionShuffleSpec,
      RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))),
      expected = true
    )

    checkCompatible(
      SinglePartitionShuffleSpec,
      ShuffleSpecCollection(Seq(
        RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))), SinglePartitionShuffleSpec)),
      expected = true
    )

    checkCompatible(
      RangeShuffleSpec(10, ClusteredDistribution(Seq($"a", $"b"))),
      RangeShuffleSpec(10, ClusteredDistribution(Seq($"a", $"b"))),
      expected = false
    )

    checkCompatible(
      RangeShuffleSpec(10, ClusteredDistribution(Seq($"a", $"b"))),
      SinglePartitionShuffleSpec,
      expected = false
    )

    checkCompatible(
      RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))),
      SinglePartitionShuffleSpec,
      expected = true
    )

    checkCompatible(
      RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))),
      ShuffleSpecCollection(Seq(
        RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))), SinglePartitionShuffleSpec)),
      expected = true
    )

    checkCompatible(
      RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))),
      ShuffleSpecCollection(Seq(
        RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))),
        RangeShuffleSpec(1, ClusteredDistribution(Seq($"c", $"d"))))),
      expected = false
    )

    checkCompatible(
      ShuffleSpecCollection(Seq(
        RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))), SinglePartitionShuffleSpec)),
      SinglePartitionShuffleSpec,
      expected = true
    )

    checkCompatible(
      ShuffleSpecCollection(Seq(
        RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))), SinglePartitionShuffleSpec)),
      ShuffleSpecCollection(Seq(
        SinglePartitionShuffleSpec, RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))))),
      expected = true
    )

    checkCompatible(
      ShuffleSpecCollection(Seq(
        RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))), SinglePartitionShuffleSpec)),
      ShuffleSpecCollection(Seq(
        HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 1),
          ClusteredDistribution(Seq($"a", $"b"))),
        RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))))),
      expected = true
    )

    checkCompatible(
      ShuffleSpecCollection(Seq(
        RangeShuffleSpec(1, ClusteredDistribution(Seq($"a", $"b"))), SinglePartitionShuffleSpec)),
      ShuffleSpecCollection(Seq(
        HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 2),
          ClusteredDistribution(Seq($"a", $"b"))),
        RangeShuffleSpec(2, ClusteredDistribution(Seq($"a", $"b"))))),
      expected = false
    )
  }

  test("canCreatePartitioning") {
    val distribution = ClusteredDistribution(Seq($"a", $"b"))
    withSQLConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
      assert(HashShuffleSpec(HashPartitioning(Seq($"a"), 10), distribution).canCreatePartitioning)
    }
    withSQLConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "true") {
      assert(!HashShuffleSpec(HashPartitioning(Seq($"a"), 10), distribution)
        .canCreatePartitioning)
      assert(HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10), distribution)
        .canCreatePartitioning)
    }
    assert(SinglePartitionShuffleSpec.canCreatePartitioning)
    withSQLConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
      assert(ShuffleSpecCollection(Seq(
        HashShuffleSpec(HashPartitioning(Seq($"a"), 10), distribution),
        HashShuffleSpec(HashPartitioning(Seq($"a", $"b"), 10), distribution)))
        .canCreatePartitioning)
    }
    assert(!RangeShuffleSpec(10, distribution).canCreatePartitioning)
  }

  test("createPartitioning: HashShuffleSpec") {
    checkCreatePartitioning(
      HashShuffleSpec(HashPartitioning(Seq($"a"), 10), ClusteredDistribution(Seq($"a", $"b"))),
      ClusteredDistribution(Seq($"c", $"d")),
      HashPartitioning(Seq($"c"), 10)
    )

    checkCreatePartitioning(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b", $"a"), 10),
        ClusteredDistribution(Seq($"a", $"b", $"b"))),
      ClusteredDistribution(Seq($"a", $"c", $"c")),
      HashPartitioning(Seq($"a", $"c", $"a"), 10)
    )

    checkCreatePartitioning(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"b", $"a"), 10),
        ClusteredDistribution(Seq($"a", $"b", $"b"))),
      ClusteredDistribution(Seq($"a", $"c", $"c")),
      HashPartitioning(Seq($"a", $"c", $"a"), 10)
    )

    checkCreatePartitioning(
      HashShuffleSpec(HashPartitioning(Seq($"a", $"d"), 10),
        ClusteredDistribution(Seq($"a", $"d", $"a", $"d"))),
      ClusteredDistribution(Seq($"a", $"b", $"c", $"d")),
      HashPartitioning(Seq($"a", $"b"), 10)
    )
  }

  test("createPartitioning: other specs") {
    val distribution = ClusteredDistribution(Seq($"a", $"b"))
    checkCreatePartitioning(SinglePartitionShuffleSpec,
      distribution,
      SinglePartition
    )

    checkCreatePartitioning(SinglePartitionShuffleSpec,
      distribution,
      SinglePartition
    )

    checkCreatePartitioning(ShuffleSpecCollection(Seq(
      HashShuffleSpec(HashPartitioning(Seq($"a"), 10), distribution),
        RangeShuffleSpec(10, distribution))),
      ClusteredDistribution(Seq($"c", $"d")),
      HashPartitioning(Seq($"c"), 10)
    )

    // unsupported cases

    val msg = intercept[Exception](RangeShuffleSpec(10, distribution)
      .createPartitioning(distribution.clustering))
    assert(msg.getMessage.contains("Operation unsupported"))
  }
}
