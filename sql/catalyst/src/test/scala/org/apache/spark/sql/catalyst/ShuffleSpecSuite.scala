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

  private def testHashShuffleSpecLike(
      shuffleSpecName: String,
      create: (HashPartitioning, ClusteredDistribution) => ShuffleSpec): Unit = {

    test(s"compatibility: $shuffleSpecName on both sides") {
      checkCompatible(
        create(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        create(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        expected = true
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a"), 10), ClusteredDistribution(Seq($"a", $"b"))),
        create(HashPartitioning(Seq($"a"), 10), ClusteredDistribution(Seq($"a", $"b"))),
        expected = true
      )

      checkCompatible(
        create(HashPartitioning(Seq($"b"), 10), ClusteredDistribution(Seq($"a", $"b"))),
        create(HashPartitioning(Seq($"d"), 10), ClusteredDistribution(Seq($"c", $"d"))),
        expected = true
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        create(HashPartitioning(Seq($"c", $"c", $"d"), 10),
          ClusteredDistribution(Seq($"c", $"d"))),
        expected = true
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b", $"b"))),
        create(HashPartitioning(Seq($"a", $"d"), 10),
          ClusteredDistribution(Seq($"a", $"c", $"d"))),
        expected = true
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"b", $"a"), 10),
          ClusteredDistribution(Seq($"a", $"b", $"b"))),
        create(HashPartitioning(Seq($"a", $"c", $"a"), 10),
          ClusteredDistribution(Seq($"a", $"c", $"c"))),
        expected = true
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"b", $"a"), 10),
          ClusteredDistribution(Seq($"a", $"b", $"b"))),
        create(HashPartitioning(Seq($"a", $"c", $"a"), 10),
          ClusteredDistribution(Seq($"a", $"c", $"d"))),
        expected = true
      )

      // negative cases
      checkCompatible(
        create(HashPartitioning(Seq($"a"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        create(HashPartitioning(Seq($"c"), 5),
          ClusteredDistribution(Seq($"c", $"d"))),
        expected = false
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        create(HashPartitioning(Seq($"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        expected = false
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        create(HashPartitioning(Seq($"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        expected = false
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        create(HashPartitioning(Seq($"d"), 10),
          ClusteredDistribution(Seq($"c", $"d"))),
        expected = false
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        create(HashPartitioning(Seq($"d"), 10),
          ClusteredDistribution(Seq($"c", $"d"))),
        expected = false
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        create(HashPartitioning(Seq($"a", $"b", $"a"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        expected = false
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b", $"b"))),
        create(HashPartitioning(Seq($"a", $"b", $"a"), 10),
          ClusteredDistribution(Seq($"a", $"b", $"b"))),
        expected = false
      )
    }

    test(s"compatibility: Only one side is $shuffleSpecName") {
      checkCompatible(
        create(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        SinglePartitionShuffleSpec,
        expected = false
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"b"), 1),
          ClusteredDistribution(Seq($"a", $"b"))),
        SinglePartitionShuffleSpec,
        expected = true
      )

      checkCompatible(
        SinglePartitionShuffleSpec,
        create(HashPartitioning(Seq($"a", $"b"), 1),
          ClusteredDistribution(Seq($"a", $"b"))),
        expected = true
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        RangeShuffleSpec(10, ClusteredDistribution(Seq($"a", $"b"))),
        expected = false
      )

      checkCompatible(
        RangeShuffleSpec(10, ClusteredDistribution(Seq($"a", $"b"))),
        create(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        expected = false
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        ShuffleSpecCollection(Seq(
          create(HashPartitioning(Seq($"a", $"b"), 10),
            ClusteredDistribution(Seq($"a", $"b"))))),
        expected = true
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        ShuffleSpecCollection(Seq(
          create(HashPartitioning(Seq($"a"), 10),
            ClusteredDistribution(Seq($"a", $"b"))),
          create(HashPartitioning(Seq($"a", $"b"), 10),
            ClusteredDistribution(Seq($"a", $"b"))))),
        expected = true
      )

      checkCompatible(
        create(HashPartitioning(Seq($"a", $"b"), 10),
          ClusteredDistribution(Seq($"a", $"b"))),
        ShuffleSpecCollection(Seq(
          create(HashPartitioning(Seq($"a"), 10),
            ClusteredDistribution(Seq($"a", $"b"))),
          create(HashPartitioning(Seq($"a", $"b", $"c"), 10),
            ClusteredDistribution(Seq($"a", $"b", $"c"))))),
        expected = false
      )

      checkCompatible(
        ShuffleSpecCollection(Seq(
          create(HashPartitioning(Seq($"b"), 10),
            ClusteredDistribution(Seq($"a", $"b"))),
          create(HashPartitioning(Seq($"a", $"b"), 10),
            ClusteredDistribution(Seq($"a", $"b"))))),
        ShuffleSpecCollection(Seq(
          create(HashPartitioning(Seq($"a", $"b", $"c"), 10),
            ClusteredDistribution(Seq($"a", $"b", $"c"))),
          create(HashPartitioning(Seq($"d"), 10),
            ClusteredDistribution(Seq($"c", $"d"))))),
        expected = true
      )

      checkCompatible(
        ShuffleSpecCollection(Seq(
          create(HashPartitioning(Seq($"b"), 10),
            ClusteredDistribution(Seq($"a", $"b"))),
          create(HashPartitioning(Seq($"a", $"b"), 10),
            ClusteredDistribution(Seq($"a", $"b"))))),
        ShuffleSpecCollection(Seq(
          create(HashPartitioning(Seq($"a", $"b", $"c"), 10),
            ClusteredDistribution(Seq($"a", $"b", $"c"))),
          create(HashPartitioning(Seq($"c"), 10),
            ClusteredDistribution(Seq($"c", $"d"))))),
        expected = false
      )
    }
  }

  testHashShuffleSpecLike("HashShuffleSpec",
    (partitioning, distribution) => HashShuffleSpec(partitioning, distribution))
   testHashShuffleSpecLike("CoalescedHashShuffleSpec",
    (partitioning, distribution) => {
      val partitions = if (partitioning.numPartitions == 1) {
        Seq(CoalescedBoundary(0, 1))
      } else {
        Seq(CoalescedBoundary(0, 1), CoalescedBoundary(0, partitioning.numPartitions))
      }
      CoalescedHashShuffleSpec(HashShuffleSpec(partitioning, distribution), partitions)
  })

  test("compatibility: CoalescedHashShuffleSpec other specs") {
      val hashShuffleSpec = HashShuffleSpec(
        HashPartitioning(Seq($"a", $"b"), 10), ClusteredDistribution(Seq($"a", $"b")))
      checkCompatible(
        hashShuffleSpec,
        CoalescedHashShuffleSpec(hashShuffleSpec, Seq(CoalescedBoundary(0, 10))),
        expected = false
      )

      checkCompatible(
        CoalescedHashShuffleSpec(hashShuffleSpec,
          Seq(CoalescedBoundary(0, 5), CoalescedBoundary(5, 10))),
        CoalescedHashShuffleSpec(hashShuffleSpec,
          Seq(CoalescedBoundary(0, 5), CoalescedBoundary(5, 10))),
        expected = true
      )

      checkCompatible(
        CoalescedHashShuffleSpec(hashShuffleSpec,
          Seq(CoalescedBoundary(0, 4), CoalescedBoundary(4, 10))),
        CoalescedHashShuffleSpec(hashShuffleSpec,
          Seq(CoalescedBoundary(0, 5), CoalescedBoundary(5, 10))),
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
    assert(!SinglePartitionShuffleSpec.canCreatePartitioning)
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
