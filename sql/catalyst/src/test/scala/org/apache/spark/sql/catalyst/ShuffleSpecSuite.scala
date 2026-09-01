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
import org.apache.spark.sql.catalyst.expressions.{Attribute, DirectShufflePartitionID, Expression, TransformExpression}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}

class ShuffleSpecSuite extends SparkFunSuite with SQLHelper {
  private val passThrough_a_10 = ShufflePartitionIdPassThrough(DirectShufflePartitionID($"a"), 10)
  private val passThrough_b_10 = ShufflePartitionIdPassThrough(DirectShufflePartitionID($"b"), 10)
  private val passThrough_c_10 = ShufflePartitionIdPassThrough(DirectShufflePartitionID($"c"), 10)

  /** A bound function with a stable canonical name, enough to build a `TransformExpression`. */
  private object TestBucketFunction extends ScalarFunction[Int] {
    override def inputTypes(): Array[DataType] = Array(IntegerType)
    override def resultType(): DataType = IntegerType
    override def name(): String = "bucket"
    override def canonicalName(): String = "test.bucket"
  }

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

  test("SPARK-59022: canCreatePartitioning: KeyedShuffleSpec requires grouped partition keys") {
    val a = $"a".int
    val distribution = ClusteredDistribution(Seq(a))
    def keyedSpec(keys: Seq[Int]): KeyedShuffleSpec = KeyedShuffleSpec(
      KeyedPartitioning(Seq(a), keys.map(k => InternalRow(k))), distribution)

    withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
      val grouped = keyedSpec(Seq(2, 1))
      assert(grouped.partitioning.isGrouped)
      assert(grouped.canCreatePartitioning,
        "unsorted keys are fine, the shuffle follows the declared order")

      // Duplicate keys mean one key spans several partitions, which a KeyGroupedPartitioner cannot
      // reproduce, so this spec must not be the target for shuffling the other child.
      val ungrouped = keyedSpec(Seq(1, 1, 2))
      assert(!ungrouped.partitioning.isGrouped)
      assert(!ungrouped.canCreatePartitioning)
    }
  }

  test("SPARK-59121: canCreatePartitioning: KeyedShuffleSpec refuses reduced keys") {
    // A `bucket(12, a)` partitioning whose keys a join reduced together with a `bucket(8, a)` one.
    // The keys are `a % 4`, which is not what evaluating `bucket(12, a)` on a row produces, so the
    // other child cannot be shuffled onto them. This replaces SPARK-59120's data-type proxy,
    // which both bucket transforms pass, since the reduced keys keep their `IntegerType`.
    val a = $"a".int
    val bucket12 = TransformExpression(TestBucketFunction, Seq(a), Some(12))
    val bucket8 = TransformExpression(TestBucketFunction, Seq(a), Some(8))
    // `builtWith` types the key row, `declared` is what the partitioning reports, so the two can be
    // made to disagree the way `createPartitioning` does.
    def divergingSpec(
        builtWith: Expression,
        declared: Expression,
        key: InternalRow): KeyedShuffleSpec =
      KeyedShuffleSpec(
        KeyedPartitioning(Seq(builtWith), Seq(key)).copy(expressions = Seq(declared)),
        ClusteredDistribution(declared.references.toSeq))
    def spec(expression: Expression): KeyedShuffleSpec =
      divergingSpec(expression, expression, InternalRow(1))

    withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
      assert(spec(bucket12).canCreatePartitioning,
        "keys the expression describes can be evaluated per row")
      assert(!spec(bucket12.reducedTogetherWith(bucket8)).canCreatePartitioning,
        "reduced keys cannot")

      // The gate no longer looks at data types, which SPARK-59120's proxy did. That proxy had to
      // compare struct keys by shape, because `createPartitioning` puts the other child's
      // expressions over these keys and the two sides can name a field differently. This builds
      // that divergence, with the key row at `struct<f>` and the partitioning declaring
      // `struct<g>`.
      def struct(field: String): Attribute =
        $"a".struct(new StructType().add(field, IntegerType))
      assert(divergingSpec(struct("f"), struct("g"), InternalRow(InternalRow(1)))
        .canCreatePartitioning,
        "a key row reads the same either way, so the field names must not matter")
    }
  }

  test("SPARK-59120: createShuffleSpec sorts the projected keys at their built-with types") {
    val a = $"a".timestamp
    val b = $"b".int
    // A reducer left `(year, bucket)` values, both `IntegerType`, under expressions that still
    // declare `(TimestampType, IntegerType)`. Projecting to the `a` position sorts them. An
    // ordering built from the declared types reads the year as a Long and throws.
    val reduced =
      KeyedPartitioning(Seq($"a".int, b), Seq(InternalRow(2021, 1), InternalRow(2020, 0)))
        .copy(expressions = Seq(a, b))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val spec = reduced.createShuffleSpec(ClusteredDistribution(Seq(a)))
        .asInstanceOf[KeyedShuffleSpec]
      assert(spec.joinKeyPositions === Some(Seq(0)))
      assert(spec.partitioning.partitionKeys.map(_.row.getInt(0)) === Seq(2020, 2021))
    }
  }

  test("areKeysCompatible: unknown partition keys only allow a subset of the declared keys") {
    val a = $"a".int
    val distribution = ClusteredDistribution(Seq(a))
    def keyedSpec(
        keys: Seq[Int],
        hasUnknown: Boolean = false): KeyedShuffleSpec = KeyedShuffleSpec(
      KeyedPartitioning(Seq(a), keys.map(k => InternalRow(k)))
        .copy(mayContainUnknownPartitionKeys = hasUnknown), distribution)

    // A partitioning with unknown partition keys (e.g. a side re-shuffled onto a keyed layout by
    // `KeyedShuffleSpec.createPartitioning`) only guarantees co-location for its declared keys, so
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
    // partition and stay compatible -- but only when the declared key order also agrees, since a
    // GroupPartitionsExec regrouping re-labels partitions by each side's declared order. Different
    // declared keys or a different order are rejected.
    assert(unknown12.areKeysCompatible(keyedSpec(Seq(1, 2), hasUnknown = true)),
      "two unknown-keyed partitionings with the same declared keys must be compatible")
    assert(!unknown12.areKeysCompatible(keyedSpec(Seq(2, 1), hasUnknown = true)),
      "two unknown-keyed partitionings must agree on the declared key order")
    assert(!unknown12.areKeysCompatible(keyedSpec(Seq(1, 2, 3), hasUnknown = true)),
      "two unknown-keyed partitionings with different declared keys must not be compatible")

    // Without unknown keys, key sets are not compared -- only the partition expressions are.
    assert(keyedSpec(Seq(1, 2)).areKeysCompatible(keyedSpec(Seq(1, 2, 3))),
      "without unknown keys, different key sets remain expression-compatible")
  }

  test("areKeysCompatible: unknown partition keys require the same partition functions") {
    // A stand-in for a connector partition function such as `bucket`: only the canonical name
    // matters here, since `TransformExpression.isSameFunction` compares names and bucket counts.
    val bucketFn = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(LongType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "test.bucket"
      override def canonicalName(): String = "test.bucket"
    }
    def bucket(numBuckets: Int, expr: Expression): TransformExpression =
      TransformExpression(bucketFn, Seq(expr), Some(numBuckets))

    val a = $"a".long
    val distribution = ClusteredDistribution(Seq(a))
    def bucketSpec(
        keys: Seq[Long],
        hasUnknown: Boolean = false): KeyedShuffleSpec = KeyedShuffleSpec(
      KeyedPartitioning(Seq(bucket(4, a)), keys.map(k => InternalRow(k)))
        .copy(mayContainUnknownPartitionKeys = hasUnknown), distribution)
    def keyedSpec(
        keys: Seq[Long],
        hasUnknown: Boolean = false): KeyedShuffleSpec = KeyedShuffleSpec(
      KeyedPartitioning(Seq(a), keys.map(k => InternalRow(k)))
        .copy(mayContainUnknownPartitionKeys = hasUnknown), distribution)

    // `isExpressionCompatible` admits an identity-vs-transform pair when compatible transforms
    // are allowed, but then the two sides' partition keys live in different domains: raw values
    // on the identity side and bucket ids on the transform side. The unknown-keyed subset test
    // would compare unrelated numbers, so the marker path must require the same partition
    // function per position instead.
    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      assert(!keyedSpec(Seq(1), hasUnknown = true).areKeysCompatible(bucketSpec(Seq(0, 1))),
        "an unknown-keyed identity partitioning must not pair with a transform partitioning")
      assert(!bucketSpec(Seq(0, 1)).areKeysCompatible(keyedSpec(Seq(1), hasUnknown = true)),
        "the incompatibility must be symmetric")
      assert(!keyedSpec(Seq(1), hasUnknown = true).areKeysCompatible(
          bucketSpec(Seq(0, 1), hasUnknown = true)),
        "the identity-vs-transform pair must not pair even when both sides are unknown-keyed")
      // Two unmarked sides keep the pre-existing behavior: the pair stays admissible, and
      // `EnsureRequirements` computes reducers to reconcile the two key domains.
      assert(keyedSpec(Seq(1)).areKeysCompatible(bucketSpec(Seq(0, 1))),
        "unmarked identity-vs-transform pairs remain admissible")
    }
  }

  test("areKeysCompatible: incompatibility without unknown partition keys") {
    val a = $"a".int
    val b = $"b".int
    val distribution = ClusteredDistribution(Seq(a))

    // Different arity: the two partitionings partition on a different number of keys.
    val single = KeyedShuffleSpec(
      KeyedPartitioning(Seq(a), Seq(InternalRow(1), InternalRow(2))), distribution)
    val double = KeyedShuffleSpec(
      KeyedPartitioning(Seq(a, b), Seq(InternalRow(1, 1), InternalRow(2, 2))), distribution)
    assert(!single.areKeysCompatible(double), "different arity must not be compatible")

    // Non-overlapping key positions: the partition keys map to disjoint clustering key positions.
    val ab = ClusteredDistribution(Seq(a, b))
    val onA = KeyedShuffleSpec(
      KeyedPartitioning(Seq(a), Seq(InternalRow(1), InternalRow(2))), ab)
    val onB = KeyedShuffleSpec(
      KeyedPartitioning(Seq(b), Seq(InternalRow(1), InternalRow(2))), ab)
    assert(!onA.areKeysCompatible(onB),
      "partition keys must overlap on the clustering keys to be compatible")
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

  test("compatibility: NullAwareHashShuffleSpec") {
    val spreadAB = ClusteredDistribution(Seq($"a", $"b"), allowNullKeySpreading = true)
    val spreadCD = ClusteredDistribution(Seq($"c", $"d"), allowNullKeySpreading = true)
    val regularAB = ClusteredDistribution(Seq($"a", $"b"))

    val nullAwareAB = NullAwareHashShuffleSpec(
      NullAwareHashPartitioning(Seq($"a", $"b"), 10), spreadAB)
    val nullAwareCD = NullAwareHashShuffleSpec(
      NullAwareHashPartitioning(Seq($"c", $"d"), 10), spreadCD)
    val regularABSpec = HashShuffleSpec(
      HashPartitioning(Seq($"a", $"b"), 10), regularAB)
    val spreadABHashSpec = HashShuffleSpec(
      HashPartitioning(Seq($"a", $"b"), 10), spreadAB)

    checkCompatible(nullAwareAB, nullAwareCD, expected = true)
    checkCompatible(nullAwareAB, SinglePartitionShuffleSpec, expected = false)
    checkCompatible(
      NullAwareHashShuffleSpec(NullAwareHashPartitioning(Seq($"a", $"b"), 1), spreadAB),
      SinglePartitionShuffleSpec,
      expected = true)
    checkCompatible(nullAwareAB, regularABSpec, expected = false)
    checkCompatible(nullAwareAB, spreadABHashSpec, expected = true)
    checkCompatible(spreadABHashSpec, nullAwareAB, expected = true)
  }

  test("canCreatePartitioning: NullAwareHashShuffleSpec") {
    val spreadDistribution =
      ClusteredDistribution(Seq($"a", $"b"), allowNullKeySpreading = true)
    val partialSpec = NullAwareHashShuffleSpec(
      NullAwareHashPartitioning(Seq($"a"), 10), spreadDistribution)
    val fullSpec = NullAwareHashShuffleSpec(
      NullAwareHashPartitioning(Seq($"a", $"b"), 10), spreadDistribution)

    withSQLConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
      assert(partialSpec.canCreatePartitioning)
    }
    withSQLConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "true") {
      assert(!partialSpec.canCreatePartitioning)
      assert(fullSpec.canCreatePartitioning)
    }
  }

  test("createPartitioning: NullAwareHashShuffleSpec") {
    checkCreatePartitioning(
      NullAwareHashShuffleSpec(
        NullAwareHashPartitioning(Seq($"a"), 10),
        ClusteredDistribution(Seq($"a", $"b"), allowNullKeySpreading = true)),
      ClusteredDistribution(Seq($"c", $"d"), allowNullKeySpreading = true),
      NullAwareHashPartitioning(Seq($"c"), 10)
    )

    checkCreatePartitioning(
      HashShuffleSpec(
        HashPartitioning(Seq($"a"), 10),
        ClusteredDistribution(Seq($"a", $"b"), allowNullKeySpreading = true)),
      ClusteredDistribution(Seq($"c", $"d"), allowNullKeySpreading = true),
      NullAwareHashPartitioning(Seq($"c"), 10)
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
}
