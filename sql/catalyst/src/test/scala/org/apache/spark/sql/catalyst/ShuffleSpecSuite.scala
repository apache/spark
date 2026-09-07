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

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{DirectShufflePartitionID, TransformExpression}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.connector.catalog.functions.{Reducer, ReducibleFunction, ScalarFunction}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, LongType}

class ShuffleSpecSuite extends SparkFunSuite with SQLHelper {
  private val passThrough_a_10 = ShufflePartitionIdPassThrough(DirectShufflePartitionID($"a"), 10)
  private val passThrough_b_10 = ShufflePartitionIdPassThrough(DirectShufflePartitionID($"b"), 10)
  private val passThrough_c_10 = ShufflePartitionIdPassThrough(DirectShufflePartitionID($"c"), 10)
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

    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        RangeShuffleSpec(10, distribution).createPartitioning(distribution.clustering)
      },
      condition = "UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
      parameters = Map(
        "methodName" -> "createPartitioning$",
        "className" -> "org.apache.spark.sql.catalyst.plans.physical.ShuffleSpec"))
  }

  test("compatibility: ShufflePartitionIdPassThroughSpec on both sides") {
    val ab = ClusteredDistribution(Seq($"a", $"b"))
    val cd = ClusteredDistribution(Seq($"c", $"d"))

    // Identical specs should be compatible
    checkCompatible(
      passThrough_a_10.createShuffleSpec(ab),
      passThrough_c_10.createShuffleSpec(cd),
      expected = true
    )

    // Different number of partitions should be incompatible
    checkCompatible(
      passThrough_a_10.createShuffleSpec(ab),
      ShufflePartitionIdPassThrough(DirectShufflePartitionID($"c"), 5).createShuffleSpec(cd),
      expected = false
    )

    // Mismatched key positions should be incompatible
    checkCompatible(
      passThrough_b_10.createShuffleSpec(ab),
      passThrough_c_10.createShuffleSpec(cd),
      expected = false
    )

    // Mismatched clustering keys
    checkCompatible(
      passThrough_a_10.createShuffleSpec(ClusteredDistribution(Seq($"e", $"b"))),
      passThrough_c_10.createShuffleSpec(ab),
      expected = false
    )
  }

  test("compatibility: ShufflePartitionIdPassThroughSpec vs other specs") {
    val ab = ClusteredDistribution(Seq($"a", $"b"))
    val cd = ClusteredDistribution(Seq($"c", $"d"))

    // Compatibility with SinglePartitionShuffleSpec when numPartitions is 1
    checkCompatible(
      ShufflePartitionIdPassThrough(DirectShufflePartitionID($"a"), 1).createShuffleSpec(ab),
      SinglePartitionShuffleSpec,
      expected = true
    )

    // Incompatible with SinglePartitionShuffleSpec when numPartitions > 1
    checkCompatible(
      passThrough_a_10.createShuffleSpec(ab),
      SinglePartitionShuffleSpec,
      expected = false
    )

    // Incompatible with HashShuffleSpec
    checkCompatible(
      passThrough_a_10.createShuffleSpec(ab),
      HashShuffleSpec(HashPartitioning(Seq($"c"), 10), cd),
      expected = false
    )
  }

  test("areKeysCompatible: unknown partition keys only allow a subset of the declared keys") {
    val a = $"a".int
    val distribution = ClusteredDistribution(Seq(a))
    def keyedSpec(
        keys: Seq[Int],
        hasUnknown: Boolean = false): KeyGroupedShuffleSpec = KeyGroupedShuffleSpec(
      KeyGroupedPartitioning(Seq(a), keys.length, keys.map(k => InternalRow(k)))
        .copy(mayContainUnknownPartitionKeys = hasUnknown), distribution)

    // A partitioning with unknown partition keys (e.g. a side re-shuffled onto a keyed layout by
    // `KeyGroupedShuffleSpec.createPartitioning`) only guarantees co-location for its declared
    // keys, so
    // it can only be co-partitioned with a side whose keys are a subset of the declared keys.
    val unknown12 = keyedSpec(Seq(1, 2), hasUnknown = true)
    // hasUnknown=true is still compatible with a subset (or equal) partner: every such key is
    // co-located on both sides.
    assert(unknown12.areKeysCompatible(keyedSpec(Seq(1))), "subset keys must be compatible")
    assert(unknown12.areKeysCompatible(keyedSpec(Seq(2))), "another subset key must be compatible")
    assert(unknown12.areKeysCompatible(keyedSpec(Seq(1, 2))), "equal keys must be compatible")
    assert(keyedSpec(Seq(1)).areKeysCompatible(unknown12),
      "compatibility must be symmetric for a subset partner")

    // A larger partner's keys are not all covered by the declared keys.
    assert(!unknown12.areKeysCompatible(keyedSpec(Seq(1, 2, 3))),
      "a larger key set must not be compatible with an unknown-keyed partitioning")
    assert(!keyedSpec(Seq(1, 2, 3)).areKeysCompatible(unknown12),
      "an unknown-keyed partitioning cannot cover a larger partner's keys")

    // Both sides unknown with the same declared keys: `KeyGroupedPartitioner`'s out-of-set-key
    // fallback is a deterministic hash of the key, so both sides route those keys to the same
    // partition and stay compatible, but only when the declared key order also agrees, since a
    // GroupPartitionsExec regrouping re-labels partitions by each side's declared order. Different
    // declared keys or a different order are rejected.
    assert(unknown12.areKeysCompatible(keyedSpec(Seq(1, 2), hasUnknown = true)),
      "two unknown-keyed partitionings with the same declared keys must be compatible")
    assert(!unknown12.areKeysCompatible(keyedSpec(Seq(2, 1), hasUnknown = true)),
      "two unknown-keyed partitionings must agree on the declared key order")
    assert(!unknown12.areKeysCompatible(keyedSpec(Seq(1, 2, 3), hasUnknown = true)),
      "two unknown-keyed partitionings with different declared keys must not be compatible")

    // Without the marker, key sets are not compared, only the partition expressions are.
    assert(keyedSpec(Seq(1, 2)).areKeysCompatible(keyedSpec(Seq(1, 2, 3))),
      "without unknown keys, different key sets remain expression-compatible")
  }

  test("createShuffleSpec: a marked narrowing projection yields an unusable spec") {
    val a = $"a".int
    val b = $"b".int
    val marked = KeyGroupedPartitioning(Seq(a, b), 2, Seq(InternalRow(1, 2), InternalRow(3, 4)))
      .copy(mayContainUnknownPartitionKeys = true)
    withSQLConf(
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val spec = marked.createShuffleSpec(ClusteredDistribution(Seq(a)))
        .asInstanceOf[KeyGroupedShuffleSpec]
      // The refusal returns the unprojected spec whose `b` expression maps to no clustering
      // key; that position must make the spec unusable as a shuffle template, or
      // `createPartitioning` would index an empty position set.
      assert(spec.joinKeyPositions.isEmpty)
      assert(!spec.canCreatePartitioning)
    }
  }

  test("areKeysCompatible: unknown keys require the same function, not just a compatible one") {
    // A bucket-like reducible function: like the built-in `bucket`, a pair of the same function
    // with coarser/finer bucket counts is compatible (a reducer exists) but not the same
    // function. Local to the suite because catalyst has no BucketFunction.
    class FakeBucket extends ScalarFunction[java.lang.Long]
        with ReducibleFunction[java.lang.Long, java.lang.Long] {
      override def inputTypes(): Array[DataType] = Array(LongType)
      override def resultType(): DataType = LongType
      override def name(): String = "test.fakeBucket"
      override def canonicalName(): String = name()
      override def produceResult(input: InternalRow): java.lang.Long = input.getLong(0)
      override def reducer(
          thisNumBuckets: Int,
          other: ReducibleFunction[_, _],
          otherNumBuckets: Int): Reducer[java.lang.Long, java.lang.Long] =
        if (other.isInstanceOf[FakeBucket] && thisNumBuckets != otherNumBuckets &&
            thisNumBuckets % otherNumBuckets == 0) {
          new Reducer[java.lang.Long, java.lang.Long] {
            override def reduce(v: java.lang.Long): java.lang.Long = v % otherNumBuckets
          }
        } else {
          null
        }
    }
    val fn = new FakeBucket
    val a = $"a".long
    def bucketSpec(numBuckets: Int, hasUnknown: Boolean): KeyGroupedShuffleSpec =
      KeyGroupedShuffleSpec(
        KeyGroupedPartitioning(
          Seq(TransformExpression(fn, Seq(a), Some(numBuckets))), 2,
          Seq(InternalRow(0L), InternalRow(1L)))
          .copy(mayContainUnknownPartitionKeys = hasUnknown),
        ClusteredDistribution(Seq(a)))

    // `allowCompatibleTransforms` lets a differing-bucket-count pair through
    // `isExpressionCompatible`, and both specs declare the key set {0, 1}, so any
    // refusal below can only come from the same-function gate on the marker path, not the keys.
    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      assert(!bucketSpec(4, true).areKeysCompatible(bucketSpec(8, true)),
        "two marked sides must agree on the exact function, not just a compatible one")
      assert(!bucketSpec(4, true).areKeysCompatible(bucketSpec(8, false)),
        "a marked side must not pair across bucket counts")
      assert(!bucketSpec(8, false).areKeysCompatible(bucketSpec(4, true)),
        "the refusal must be symmetric")
      // Unmarked, the compatible-transform relaxation still pairs them (pre-existing behavior).
      assert(bucketSpec(4, false).areKeysCompatible(bucketSpec(8, false)),
        "unmarked compatible transforms remain admissible")
      // Positive control: the same marked function with the same keys must stay compatible, so
      // the refusals above are the function difference's doing, not the marker path always false.
      assert(bucketSpec(4, true).areKeysCompatible(bucketSpec(4, true)),
        "identical marked functions remain compatible")
    }
  }
}
