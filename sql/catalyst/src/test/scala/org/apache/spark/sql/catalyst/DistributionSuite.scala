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
/* Implicit conversions */
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.physical._

class DistributionSuite extends SparkFunSuite {

  protected def checkSatisfied(
      inputPartitioning: Partitioning,
      requiredDistribution: Distribution,
      satisfied: Boolean): Unit = {
    if (inputPartitioning.satisfies(requiredDistribution) != satisfied) {
      fail(
        s"""
        |== Input Partitioning ==
        |$inputPartitioning
        |== Required Distribution ==
        |$requiredDistribution
        |== Does input partitioning satisfy required distribution? ==
        |Expected $satisfied got ${inputPartitioning.satisfies(requiredDistribution)}
        """.stripMargin)
    }
  }

  protected def checkCompatible(
      left: Partitioning,
      right: Partitioning,
      compatible: Boolean,
      leftDistribution: Distribution = UnspecifiedDistribution,
      rightDistribution: Distribution = UnspecifiedDistribution): Unit = {
    val actual = left.isCompatibleWith(leftDistribution, right, rightDistribution)
    if (actual != compatible) {
      fail(
        s"""
           |== Left Partitioning ==
           |$left
           |== Right Partitioning ==
           |$right
           |== Is left partitioning compatible with right partitioning? ==
           |Expected $compatible but got $actual
           |""".stripMargin)
    }
  }

  protected def checkPartitionCollectionCompatible(
      left: Partitioning,
      right: Partitioning,
      compatible: Boolean,
      leftDistribution: Distribution = UnspecifiedDistribution,
      rightDistribution: Distribution = UnspecifiedDistribution): Unit = {
    checkCompatible(left, right, compatible, leftDistribution, rightDistribution)
    checkCompatible(right, left, compatible, rightDistribution, leftDistribution)
  }

  test("UnspecifiedDistribution and AllTuples") {
    // all partitioning can satisfy UnspecifiedDistribution
    checkSatisfied(
      UnknownPartitioning(-1),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      RoundRobinPartitioning(10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      SinglePartition,
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a"), 10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc), 10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      BroadcastPartitioning(IdentityBroadcastMode),
      UnspecifiedDistribution,
      true)

    // except `BroadcastPartitioning`, all other partitioning can satisfy AllTuples if they have
    // only one partition.
    checkSatisfied(
      UnknownPartitioning(1),
      AllTuples,
      true)

    checkSatisfied(
      UnknownPartitioning(10),
      AllTuples,
      false)

    checkSatisfied(
      RoundRobinPartitioning(1),
      AllTuples,
      true)

    checkSatisfied(
      RoundRobinPartitioning(10),
      AllTuples,
      false)

    checkSatisfied(
      SinglePartition,
      AllTuples,
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a"), 1),
      AllTuples,
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a"), 10),
      AllTuples,
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc), 1),
      AllTuples,
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc), 10),
      AllTuples,
      false)

    checkSatisfied(
      BroadcastPartitioning(IdentityBroadcastMode),
      AllTuples,
      false)
  }

  test("SinglePartition is the output partitioning") {
    // SinglePartition can satisfy all the distributions except `BroadcastDistribution`
    checkSatisfied(
      SinglePartition,
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      SinglePartition,
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      true)

    checkSatisfied(
      SinglePartition,
      BroadcastDistribution(IdentityBroadcastMode),
      false)
  }

  test("HashPartitioning is the output partitioning") {
    // HashPartitioning can satisfy ClusteredDistribution iff its hash expressions are a subset of
    // the required clustering expressions.
    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      HashPartitioning(Seq($"b", $"c"), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"b", $"c")),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"d", $"e")),
      false)

    // HashPartitioning cannot satisfy OrderedDistribution
    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 1),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      false) // TODO: this can be relaxed.

    checkSatisfied(
      HashPartitioning(Seq($"b", $"c"), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      false)
  }

  test("RangePartitioning is the output partitioning") {
    // RangePartitioning can satisfy OrderedDistribution iff its ordering is a prefix
    // of the required ordering, or the required ordering is a prefix of its ordering.
    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc, $"d".desc)),
      true)

    // TODO: We can have an optimization to first sort the dataset
    // by a.asc and then sort b, and c in a partition. This optimization
    // should tradeoff the benefit of a less number of Exchange operators
    // and the parallelism.
    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".desc, $"c".asc)),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"b".asc, $"a".asc)),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"d".desc)),
      false)

    // RangePartitioning can satisfy ClusteredDistribution iff its ordering expressions are a subset
    // of the required clustering expressions.
    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"c", $"b", $"a")),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"b", $"c", $"a", $"d")),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b")),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"c", $"d")),
      false)
  }

  test("Partitioning.numPartitions must match Distribution.requiredNumPartitions to satisfy it") {
    checkSatisfied(
      SinglePartition,
      ClusteredDistribution(Seq($"a", $"b", $"c"), Some(10)),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c"), Some(5)),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c"), Some(5)),
      false)
  }

  test("Compatibility: SinglePartition and HashPartitioning") {
    checkCompatible(
      SinglePartition,
      SinglePartition,
      compatible = true)

    checkCompatible(
      HashPartitioning(Seq($"a", $"b", $"c"), 4),
      HashPartitioning(Seq($"a", $"b", $"c"), 4),
      compatible = true,
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      ClusteredDistribution(Seq($"a", $"b", $"c")))

    checkCompatible(
      HashPartitioning(Seq($"a", $"c"), 4),
      HashPartitioning(Seq($"a", $"c"), 8),
      compatible = true,
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      ClusteredDistribution(Seq($"a", $"b", $"c")))

    checkCompatible(
      HashPartitioning(Seq($"a", $"b"), 4),
      HashPartitioning(Seq($"b", $"a"), 8),
      compatible = true,
      ClusteredDistribution(Seq($"a", $"b")),
      ClusteredDistribution(Seq($"b", $"a")))

    checkCompatible(
      HashPartitioning(Seq($"a", $"b"), 4),
      HashPartitioning(Seq($"b", $"a"), 8),
      compatible = true,
      ClusteredDistribution(Seq($"c", $"a", $"b")),
      ClusteredDistribution(Seq($"d", $"b", $"a")))

    checkCompatible(
      HashPartitioning(Seq($"a", $"b"), 4),
      HashPartitioning(Seq($"a", $"b"), 8),
      compatible = true,
      ClusteredDistribution(Seq($"a", $"a", $"b")),
      ClusteredDistribution(Seq($"a", $"a", $"b")))

    // negative cases

    checkCompatible(
      HashPartitioning(Seq($"a"), 4),
      HashPartitioning(Seq($"b"), 4),
      compatible = false,
      ClusteredDistribution(Seq($"a", $"b")),
      ClusteredDistribution(Seq($"a", $"b")))

    checkCompatible(
      HashPartitioning(Seq($"a", $"b"), 4),
      HashPartitioning(Seq($"b", $"b"), 4),
      compatible = false,
      ClusteredDistribution(Seq($"a", $"b")),
      ClusteredDistribution(Seq($"b", $"a")))

    checkCompatible(
      HashPartitioning(Seq($"a", $"b"), 4),
      HashPartitioning(Seq($"a", $"b"), 4),
      compatible = false,
      ClusteredDistribution(Seq($"a", $"a", $"b")),
      ClusteredDistribution(Seq($"a", $"b", $"b")))
  }

  test("Compatibility: PartitionCollection") {
    checkPartitionCollectionCompatible(
      HashPartitioning(Seq($"a"), 4),
      PartitioningCollection(Seq(HashPartitioning(Seq($"a"), 4))),
      compatible = true,
      ClusteredDistribution(Seq($"a")),
      ClusteredDistribution(Seq($"a")))

    checkPartitionCollectionCompatible(
      HashPartitioning(Seq($"a"), 4),
      PartitioningCollection(Seq(HashPartitioning(Seq($"b"), 4), HashPartitioning(Seq($"a"), 4))),
      compatible = true,
      ClusteredDistribution(Seq($"a")),
      ClusteredDistribution(Seq($"a", $"b")))

    checkPartitionCollectionCompatible(
      PartitioningCollection(Seq(HashPartitioning(Seq($"a"), 4))),
      PartitioningCollection(Seq(HashPartitioning(Seq($"a"), 4))),
      compatible = true,
      ClusteredDistribution(Seq($"a")),
      ClusteredDistribution(Seq($"a")))

    checkPartitionCollectionCompatible(
      PartitioningCollection(Seq(HashPartitioning(Seq($"a"), 4), HashPartitioning(Seq($"b"), 4))),
      PartitioningCollection(Seq(HashPartitioning(Seq($"a"), 4), HashPartitioning(Seq($"b"), 4))),
      compatible = true,
      ClusteredDistribution(Seq($"a", $"b")),
      ClusteredDistribution(Seq($"a", $"b")))

    checkPartitionCollectionCompatible(
      PartitioningCollection(Seq(HashPartitioning(Seq($"a"), 4), HashPartitioning(Seq($"b"), 4))),
      PartitioningCollection(Seq(HashPartitioning(Seq($"b"), 4), HashPartitioning(Seq($"a"), 4))),
      compatible = true,
      ClusteredDistribution(Seq($"a", $"b")),
      ClusteredDistribution(Seq($"a", $"b")))

    checkPartitionCollectionCompatible(
      PartitioningCollection(Seq(HashPartitioning(Seq($"a"), 4))),
      PartitioningCollection(Seq(HashPartitioning(Seq($"a"), 4), HashPartitioning(Seq($"b"), 4))),
      compatible = true,
      ClusteredDistribution(Seq($"a")),
      ClusteredDistribution(Seq($"a", $"b")))

    checkPartitionCollectionCompatible(
      PartitioningCollection(Seq(HashPartitioning(Seq($"b"), 4), HashPartitioning(Seq($"a"), 4))),
      PartitioningCollection(Seq(HashPartitioning(Seq($"a"), 4), HashPartitioning(Seq($"c"), 4))),
      compatible = true,
      ClusteredDistribution(Seq($"b", $"a")),
      ClusteredDistribution(Seq($"a", $"c")))

    // negative cases

    checkPartitionCollectionCompatible(
      HashPartitioning(Seq($"a"), 4),
      PartitioningCollection(Seq(HashPartitioning(Seq($"b"), 4))),
      compatible = false,
      ClusteredDistribution(Seq($"a", $"b")),
      ClusteredDistribution(Seq($"a", $"b")))

    checkCompatible(
      PartitioningCollection(Seq(HashPartitioning(Seq($"a"), 4), HashPartitioning(Seq($"b"), 4))),
      PartitioningCollection(Seq(HashPartitioning(Seq($"c"), 4))),
      compatible = false,
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      ClusteredDistribution(Seq($"a", $"b", $"c")))
  }

  test("Compatibility: Others") {
    val partitionings: Seq[Partitioning] = Seq(UnknownPartitioning(1),
      BroadcastPartitioning(IdentityBroadcastMode),
      RoundRobinPartitioning(10),
      RangePartitioning(Seq($"a".asc), 10),
      PartitioningCollection(Seq(UnknownPartitioning(1)))
    )

    for (i <- partitionings.indices) {
      for (j <- partitionings.indices) {
        checkCompatible(partitionings(i), partitionings(j), compatible = false)
      }
    }

    // should always return false when comparing with `HashPartitioning` or `SinglePartition`
    partitionings.foreach { p =>
      checkCompatible(p, HashPartitioning(Seq($"a"), 10), compatible = false)
      checkCompatible(p, SinglePartition, compatible = false)
    }
  }
}
